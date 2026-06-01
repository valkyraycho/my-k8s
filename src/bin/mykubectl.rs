use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use my_k8s::{
    client::Client,
    pod::{Pod, PodPhase},
};
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

async fn apply(client: &Client, file: &Path) -> Result<()> {
    let yaml = std::fs::read_to_string(file).with_context(|| format!("reading {file:?}"))?;
    let mut pod = Pod::from_yaml(&yaml).context("parsing pod YAML")?;

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
    Ok(())
}

async fn get(client: &Client, resource: &str, name: Option<String>, watch: bool) -> Result<()> {
    if !matches!(resource, "pod" | "pods") {
        bail!("unsupported resource {resource:?}; only 'pod' and 'pods' are supported");
    }

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
        None => {
            let pods = client.list_pods().await?;
            print_pod_table(&pods);
        }
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
    if !matches!(resource, "pod" | "pods") {
        bail!("unsupported resource {resource:?}; only 'pod' and 'pods' are supported");
    }
    match client.get_pod(name).await? {
        Some(pod) => {
            let rv = pod
                .metadata
                .resource_version
                .ok_or_else(|| anyhow::anyhow!("pod/{name:?} has no resource version"))?;
            client.delete_pod(name, &rv).await?;
            println!("pod/{name} deleted");
        }
        None => bail!("pod/{name:?} not found"),
    }
    Ok(())
}

fn print_pod_table(pods: &[Pod]) {
    println!(
        "{:<20} {:<10} {:<8} {:<10} AGE",
        "NAME", "PHASE", "READY", "RESTARTS",
    );

    for pod in pods {
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
