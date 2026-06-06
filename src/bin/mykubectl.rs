use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use my_k8s::{
    client::Client,
    pod::{Pod, PodPhase},
    replicaset::ReplicaSet,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio_stream::StreamExt;

#[derive(Parser, Debug)]
#[command(name = "mykubectl", version)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080", env = "MY_K8S_SERVER")]
    server: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    Apply {
        #[arg(short)]
        file: PathBuf,
    },
    Get {
        resource: String,
        name: Option<String>,
        #[arg(short = 'w', long)]
        watch: bool,
    },
    Delete {
        resource: String,
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = Client::new(args.server);

    match args.cmd {
        Cmd::Apply { file } => apply(&client, &file).await,
        Cmd::Get {
            resource,
            name,
            watch,
        } => get(&client, &resource, name, watch).await,
        Cmd::Delete { resource, name } => delete(&client, &resource, &name).await,
    }
}

/// Just enough of the manifest to learn its `kind` before full parsing — so
/// `apply` can route a Pod vs a ReplicaSet to the right endpoint.
#[derive(Deserialize)]
struct TypeMeta {
    kind: String,
}

/// `apply` is an UPSERT: make the cluster match this file whether or not the
/// object exists. We peek at `kind`, then GET-to-branch: if present, copy its
/// current rv onto our object and PUT (rv is required for the optimistic-
/// concurrency check, and the YAML doesn't carry one); if absent, POST.
async fn apply(client: &Client, file: &Path) -> Result<()> {
    let yaml = std::fs::read_to_string(file).with_context(|| format!("reading {file:?}"))?;
    let meta: TypeMeta = serde_yaml_ng::from_str(&yaml).context("reading kind from manifest")?;

    match meta.kind.as_str() {
        "Pod" => {
            let mut pod: Pod = serde_yaml_ng::from_str(&yaml).context("parsing Pod YAML")?;
            match client.get_pod(&pod.metadata.name).await? {
                Some(existing) => {
                    pod.metadata.resource_version = existing.metadata.resource_version;
                    let updated = client.replace_pod_spec(&pod).await?;
                    println!("pod/{} replaced", updated.metadata.name);
                }
                None => {
                    let created = client.create_pod(&pod).await?;
                    println!("pod/{} created", created.metadata.name);
                }
            }
        }
        "ReplicaSet" => {
            let mut rs: ReplicaSet =
                serde_yaml_ng::from_str(&yaml).context("parsing ReplicaSet YAML")?;
            match client.get_replicaset(&rs.metadata.name).await? {
                Some(existing) => {
                    rs.metadata.resource_version = existing.metadata.resource_version;
                    let updated = client.replace_replicaset_spec(&rs).await?;
                    println!("replicaset/{} replaced", updated.metadata.name);
                }
                None => {
                    let created = client.create_replicaset(&rs).await?;
                    println!("replicaset/{} created", created.metadata.name);
                }
            }
        }
        other => bail!("unsupported kind {other:?}; only Pod and ReplicaSet are supported"),
    }
    Ok(())
}

async fn get(client: &Client, resource: &str, name: Option<String>, watch: bool) -> Result<()> {
    match resource {
        "pod" | "pods" => get_pods(client, name, watch).await,
        "rs" | "replicaset" | "replicasets" => get_replicasets(client, name).await,
        "node" | "nodes" => get_nodes(client, name).await,
        other => bail!("unsupported resource {other:?}; supported: pod(s), replicaset(s)/rs"),
    }
}

async fn get_pods(client: &Client, name: Option<String>, watch: bool) -> Result<()> {
    if watch {
        return watch_pods(client, name).await;
    }
    match name {
        Some(name) => match client.get_pod(&name).await? {
            Some(pod) => print!(
                "{}",
                serde_yaml_ng::to_string(&pod).context("serializing pod")?
            ),
            None => bail!("pod/{name:?} not found"),
        },
        None => print_pod_table(&client.list_pods().await?),
    }
    Ok(())
}

async fn get_replicasets(client: &Client, name: Option<String>) -> Result<()> {
    match name {
        Some(name) => match client.get_replicaset(&name).await? {
            Some(rs) => print!(
                "{}",
                serde_yaml_ng::to_string(&rs).context("serializing replicaset")?
            ),
            None => bail!("replicaset/{name:?} not found"),
        },
        None => print_rs_table(&client.list_replicasets().await?),
    }
    Ok(())
}

async fn get_nodes(client: &Client, name: Option<String>) -> Result<()> {
    match name {
        Some(name) => match client.get_node(&name).await? {
            Some(node) => print!(
                "{}",
                serde_yaml_ng::to_string(&node).context("serializing node")?
            ),
            None => bail!("node/{name:?} not found"),
        },
        None => print_node_table(&client.list_nodes().await?),
    }
    Ok(())
}

async fn watch_pods(client: &Client, name: Option<String>) -> Result<()> {
    let mut stream = client.watch_pods(None).await?;
    println!("{:<10} {:<10} NAME", "EVENT", "PHASE");
    while let Some(item) = stream.next().await {
        let ev = item?;
        if let Some(want) = &name
            && &ev.object.metadata.name != want
        {
            continue;
        }
        println!(
            "{:<10} {:<10} {}",
            format!("{:?}", ev.event_type),
            phase_str(&ev.object),
            ev.object.metadata.name,
        );
    }
    Ok(())
}

async fn delete(client: &Client, resource: &str, name: &str) -> Result<()> {
    match resource {
        "pod" | "pods" => match client.get_pod(name).await? {
            Some(pod) => {
                let rv = pod
                    .metadata
                    .resource_version
                    .ok_or_else(|| anyhow::anyhow!("pod/{name:?} has no resource version"))?;
                client.delete_pod(name, &rv).await?;
                println!("pod/{name} deleted");
            }
            None => bail!("pod/{name:?} not found"),
        },
        "rs" | "replicaset" | "replicasets" => match client.get_replicaset(name).await? {
            Some(rs) => {
                let rv = rs.metadata.resource_version.ok_or_else(|| {
                    anyhow::anyhow!("replicaset/{name:?} has no resource version")
                })?;
                client.delete_replicaset(name, &rv).await?;
                println!("replicaset/{name} deleted");
            }
            None => bail!("replicaset/{name:?} not found"),
        },
        other => bail!("unsupported resource {other:?}; supported: pod(s), replicaset(s)/rs"),
    }
    Ok(())
}

/// The `kubectl get pods` table. This is the PAYOFF of the §8 status loop: a
/// process that only talks to the apiserver renders live state the kubelet
/// reported — READY/RESTARTS come straight from `status.container_statuses`.
/// `{:<20}` is left-align-in-20-cols formatting for the columns.
fn print_pod_table(pods: &[Pod]) {
    println!(
        "{:<20} {:<10} {:<8} {:<10} AGE",
        "NAME", "PHASE", "READY", "RESTARTS",
    );

    for pod in pods {
        // Destructure status if present; a Pod with no status yet (just created)
        // shows 0/total ready. `.filter().count()` and `.map().sum()` are the
        // idiomatic iterator rollups.
        let (ready, total, restarts) = match &pod.status {
            Some(s) => (
                s.container_statuses.iter().filter(|c| c.ready).count(),
                pod.spec.containers.len(),
                s.container_statuses
                    .iter()
                    .map(|c| c.restart_count)
                    .sum::<u32>(),
            ),
            None => (0, pod.spec.containers.len(), 0),
        };
        let age = pod
            .metadata
            .creation_timestamp
            .as_deref()
            .map(age_str)
            .unwrap_or_else(|| "<unknown>".into());

        println!(
            "{:<20} {:<10} {:<8} {:<10} {}",
            pod.metadata.name,
            phase_str(pod),
            format!("{ready}/{total}"),
            restarts,
            age,
        );
    }
}

/// The `kubectl get rs` table: DESIRED (spec.replicas) vs CURRENT/READY (from
/// status, written by the controller). The gap between DESIRED and CURRENT is
/// the controller's work-in-progress.
fn print_rs_table(items: &[ReplicaSet]) {
    println!(
        "{:<20} {:<9} {:<9} {:<9} AGE",
        "NAME", "DESIRED", "CURRENT", "READY",
    );
    for rs in items {
        let (current, ready) = match &rs.status {
            Some(s) => (s.replicas, s.ready_replicas),
            None => (0, 0),
        };
        let age = rs
            .metadata
            .creation_timestamp
            .as_deref()
            .map(age_str)
            .unwrap_or_else(|| "<unknown>".into());
        println!(
            "{:<20} {:<9} {:<9} {:<9} {}",
            rs.metadata.name, rs.spec.replicas, current, ready, age,
        );
    }
}

fn phase_str(pod: &Pod) -> String {
    format!(
        "{:?}",
        pod.status
            .as_ref()
            .map(|s| s.phase)
            .unwrap_or(PodPhase::Pending)
    )
}

fn age_str(ts: &str) -> String {
    let Ok(created) = chrono::DateTime::parse_from_rfc3339(ts) else {
        return "<bad-ts>".into();
    };
    let secs = (chrono::Utc::now() - created.with_timezone(&chrono::Utc)).num_seconds();
    match secs {
        s if s < 0 => "0s".into(),
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86400 => format!("{}h", s / 3600),
        s => format!("{}d", s / (86400)),
    }
}

fn print_node_table(items: &[my_k8s::node::Node]) {
    let now = chrono::Utc::now();
    // Match the scheduler's default staleness window so the table agrees with
    // what the scheduler would actually do (a dead node reads NotReady here).
    const READY_WINDOW_SECS: i64 = 30;
    println!("{:<16} {:<8} AGE", "NAME", "READY");
    for n in items {
        let ready = if n.is_ready(now, READY_WINDOW_SECS) {
            "True"
        } else {
            "False"
        };
        let age = n
            .metadata
            .creation_timestamp
            .as_deref()
            .map(age_str)
            .unwrap_or_else(|| "<unknown>".into());
        println!("{:<16} {:<8} {}", n.metadata.name, ready, age);
    }
}
