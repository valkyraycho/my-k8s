//! The scheduler: places unscheduled pods onto nodes. Watches pods with no
//! `spec.nodeName`, runs a two-phase filter→score over the nodes (filter to
//! Ready + fresh-heartbeat candidates, score by least-loaded), and writes the
//! choice via the binding subresource. Mirrors controller-manager's structure:
//! informer + resync + worker over one work queue.

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    client::Client,
    controller::workqueue::{RateLimiter, WorkQueue, backoff_for},
    node::Node,
};
const RESYNC_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// A node is a scheduling candidate only if its heartbeat is within this window.
/// 30s prod / 2s test (so the stale-node test doesn't sleep 30s).
#[cfg(not(test))]
const STALENESS_WINDOW_SECS: i64 = 30;
#[cfg(test)]
const STALENESS_WINDOW_SECS: i64 = 2;

pub async fn run(client: Arc<Client>, cancel: CancellationToken) {
    let queue = WorkQueue::new();
    info!("scheduler started");

    let tasks = vec![
        tokio::spawn(pod_informer(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(resync_loop(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(worker_loop(client.clone(), queue.clone(), cancel.clone())),
    ];
    for t in tasks {
        let _ = t.await;
    }
    info!("scheduler stopped");
}

/// Watch ALL pods (no field selector — the scheduler must see unscheduled ones)
/// and enqueue any with no nodeName yet.
async fn pod_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_pods(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    ev = stream.next() => match ev {
                        Some(Ok(ev)) => {
                            if ev.object.spec.node_name.is_none() {
                                queue.add(ev.object.metadata.name.clone());
                            }
                        }
                        Some(Err(e)) => { warn!(error = ?e, "pod watch error; reconnecting"); break; }
                        None => { warn!("pod watch closed; reconnecting"); break; }
                    }
                }
            },
            Err(e) => warn!(error = ?e, "pod watch open failed; retrying"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

/// Safety net: every 30s re-enqueue every still-unscheduled pod. (First tick
/// fires immediately → an initial sweep of pre-existing unscheduled pods.)
async fn resync_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(RESYNC_INTERVAL);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => match client.list_pods().await {
                Ok(pods) => {
                    for p in pods {
                        if p.spec.node_name.is_none() {
                            queue.add(p.metadata.name);
                        }
                    }
                }
                Err(e) => warn!(error = ?e, "scheduler resync failed"),
            }
        }
    }
}

async fn worker_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let rl = RateLimiter::new();
    loop {
        let key = tokio::select! {
            _ = cancel.cancelled() => return,
            k = queue.get() => k,
        };
        match schedule(&key, &client).await {
            Ok(()) => {
                rl.forget(&key);
                queue.done(&key);
            }
            Err(e) => {
                let attempt = rl.failure(&key);
                let delay = backoff_for(attempt);
                warn!(error = ?e, pod = %key, attempt, "schedule failed; retrying after backoff");
                queue.done(&key);
                queue.add_after(key, delay);
            }
        }
    }
}

pub async fn schedule(pod_name: &str, client: &Client) -> Result<()> {
    let pod = match client.get_pod(pod_name).await? {
        Some(p) => p,
        None => return Ok(()),
    };

    if pod.spec.node_name.is_some() {
        return Ok(());
    }

    let now = Utc::now();
    let nodes = client.list_nodes().await?;
    let candidates: Vec<&Node> = nodes.iter().filter(|n| is_schedulable(n, now)).collect();
    if candidates.is_empty() {
        bail!("no Ready node available for pod {pod_name}");
    }

    let all_pods = client.list_pods().await?;

    let mut load: HashMap<&str, usize> = candidates
        .iter()
        .map(|n| (n.metadata.name.as_str(), 0))
        .collect();

    for p in &all_pods {
        if let Some(n) = &p.spec.node_name {
            load.entry(n.as_str()).and_modify(|v| *v += 1);
        }
    }
    let chosen = candidates
        .iter()
        .min_by_key(|n| load[n.metadata.name.as_str()])
        .expect("candidates is non-empty (checked above)");

    client.bind_pod(pod_name, &chosen.metadata.name).await?;
    info!(pod = %pod_name, node = %chosen.metadata.name, "scheduled pod");
    Ok(())
}

fn is_schedulable(node: &Node, now: DateTime<Utc>) -> bool {
    // Schedulable = not cordoned AND effectively Ready (Ready + fresh heartbeat).
    !node.spec.unschedulable && node.is_ready(now, STALENESS_WINDOW_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore},
    };
    use crate::meta::ObjectMeta;
    use crate::node::{Node, NodeSpec, NodeStatus};
    use crate::pod::{Container, Pod, PodSpec};
    use crate::replicaset::ReplicaSet;

    // ---- is_schedulable: pure predicate, table-driven ----

    fn node_with(ready: bool, hb: Option<&str>, unschedulable: bool) -> Node {
        Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: ObjectMeta {
                name: "n".into(),
                ..Default::default()
            },
            spec: NodeSpec {
                unschedulable,
                pod_cidr: None,
            },
            status: Some(NodeStatus {
                ready,
                last_heartbeat_time: hb.map(String::from),
            }),
        }
    }

    #[test]
    fn schedulable_predicate() {
        let now = Utc::now();
        let fresh = now.to_rfc3339();
        let stale = (now - chrono::Duration::seconds(STALENESS_WINDOW_SECS + 5)).to_rfc3339();

        assert!(is_schedulable(&node_with(true, Some(&fresh), false), now));
        // not ready
        assert!(!is_schedulable(&node_with(false, Some(&fresh), false), now));
        // cordoned
        assert!(!is_schedulable(&node_with(true, Some(&fresh), true), now));
        // stale heartbeat
        assert!(!is_schedulable(&node_with(true, Some(&stale), false), now));
        // no heartbeat
        assert!(!is_schedulable(&node_with(true, None, false), now));
        // no status at all
        let mut no_status = node_with(true, None, false);
        no_status.status = None;
        assert!(!is_schedulable(&no_status, now));
    }

    // ---- schedule(): against an in-process apiserver ----

    async fn spawn_apiserver() -> Arc<Client> {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let app = router(AppState {
            store: Arc::new(PodStore::from_db(db.clone()).unwrap()),
            rs_store: Arc::new(ResourceStore::<ReplicaSet>::from_db(db.clone()).unwrap()),
            node_store: Arc::new(ResourceStore::<Node>::from_db(db.clone()).unwrap()),
            svc_store: Arc::new(
                ResourceStore::<crate::service::Service>::from_db(db.clone()).unwrap(),
            ),
            ep_store: Arc::new(
                ResourceStore::<crate::endpoints::Endpoints>::from_db(db).unwrap(),
            ),
            write: crate::apiserver::handlers::WritePath::Direct,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Arc::new(Client::new(format!("http://{addr}")))
    }

    fn make_pod(name: &str) -> Pod {
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "c".into(),
                    image: "busybox".into(),
                    command: vec!["sleep".into(), "1".into()],
                }],
                node_name: None,
            },
            status: None,
        }
    }

    /// Register a Ready node with a fresh heartbeat (via create + status PUT,
    /// since create strips status).
    async fn register_ready_node(client: &Client, name: &str) {
        let node = Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: NodeSpec::default(),
            status: None,
        };
        let created = client.create_node(&node).await.unwrap();
        let rv = created.metadata.resource_version.unwrap();
        client
            .replace_node_status(
                name,
                &NodeStatus {
                    ready: true,
                    last_heartbeat_time: Some(Utc::now().to_rfc3339()),
                },
                &rv,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn schedule_binds_to_ready_node() {
        let client = spawn_apiserver().await;
        register_ready_node(&client, "node-a").await;
        client.create_pod(&make_pod("web")).await.unwrap();

        schedule("web", &client).await.unwrap();

        let pod = client.get_pod("web").await.unwrap().unwrap();
        assert_eq!(pod.spec.node_name.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn schedule_skips_already_bound_pod() {
        let client = spawn_apiserver().await;
        register_ready_node(&client, "node-a").await;
        client.create_pod(&make_pod("web")).await.unwrap();
        client.bind_pod("web", "node-z").await.unwrap(); // pre-bound elsewhere

        schedule("web", &client).await.unwrap();

        // Unchanged — scheduler must not re-bind an already-placed pod.
        let pod = client.get_pod("web").await.unwrap().unwrap();
        assert_eq!(pod.spec.node_name.as_deref(), Some("node-z"));
    }

    #[tokio::test]
    async fn schedule_errors_when_no_ready_node() {
        let client = spawn_apiserver().await;
        client.create_pod(&make_pod("web")).await.unwrap();
        // No nodes registered at all.
        let err = schedule("web", &client).await.unwrap_err();
        assert!(err.to_string().contains("no Ready node"), "got: {err}");
    }

    #[tokio::test]
    async fn schedule_picks_least_loaded_node() {
        let client = spawn_apiserver().await;
        register_ready_node(&client, "node-a").await;
        register_ready_node(&client, "node-b").await;
        register_ready_node(&client, "node-c").await;

        // Pre-load: node-a has 2 pods, node-b has 1, node-c has 0.
        for (n, node) in [("p1", "node-a"), ("p2", "node-a"), ("p3", "node-b")] {
            client.create_pod(&make_pod(n)).await.unwrap();
            client.bind_pod(n, node).await.unwrap();
        }
        client.create_pod(&make_pod("web")).await.unwrap();

        schedule("web", &client).await.unwrap();

        // node-c (0 pods) is least-loaded → web lands there.
        let pod = client.get_pod("web").await.unwrap().unwrap();
        assert_eq!(pod.spec.node_name.as_deref(), Some("node-c"));
    }

    #[tokio::test]
    async fn schedule_excludes_stale_node() {
        let client = spawn_apiserver().await;
        // Ready but heartbeat is well past the (test) staleness window.
        let node = Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: ObjectMeta {
                name: "node-a".into(),
                ..Default::default()
            },
            spec: NodeSpec::default(),
            status: None,
        };
        let created = client.create_node(&node).await.unwrap();
        let rv = created.metadata.resource_version.unwrap();
        let stale = (Utc::now() - chrono::Duration::seconds(STALENESS_WINDOW_SECS + 10)).to_rfc3339();
        client
            .replace_node_status(
                "node-a",
                &NodeStatus {
                    ready: true,
                    last_heartbeat_time: Some(stale),
                },
                &rv,
            )
            .await
            .unwrap();
        client.create_pod(&make_pod("web")).await.unwrap();

        let err = schedule("web", &client).await.unwrap_err();
        assert!(err.to_string().contains("no Ready node"), "got: {err}");
    }

    #[tokio::test]
    async fn run_schedules_unscheduled_pods_end_to_end() {
        let client = spawn_apiserver().await;
        register_ready_node(&client, "node-a").await;
        register_ready_node(&client, "node-b").await;

        let cancel = CancellationToken::new();
        tokio::spawn(run(client.clone(), cancel.clone()));

        // Create 4 unscheduled pods; the scheduler should bind them all.
        for i in 0..4 {
            client.create_pod(&make_pod(&format!("p{i}"))).await.unwrap();
        }

        // Poll until all 4 are bound (or time out).
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let pods = client.list_pods().await.unwrap();
            let bound = pods.iter().filter(|p| p.spec.node_name.is_some()).count();
            if bound == 4 || std::time::Instant::now() > deadline {
                assert_eq!(bound, 4, "all 4 pods should get scheduled");
                // Spread: with least-loaded over 2 nodes, neither gets all 4.
                let on_a = pods
                    .iter()
                    .filter(|p| p.spec.node_name.as_deref() == Some("node-a"))
                    .count();
                assert!(on_a >= 1 && on_a <= 3, "pods should spread across nodes, node-a={on_a}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
    }
}
