//! The controller-manager runtime: two informer loops (ReplicaSet + Pod) and a
//! periodic resync all feed RS keys into one work queue; a worker drains it and
//! calls `reconcile`. Everything funnels to an RS *name* — a Pod event maps to
//! its owning RS via ownerReferences.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::client::Client;
use crate::controller::replicaset::{reconcile, rs_key_for_pod};
use crate::controller::workqueue::{RateLimiter, WorkQueue, backoff_for};

const RESYNC_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Spawn all loops and run until `cancel` fires.
pub async fn run(client: Arc<Client>, cancel: CancellationToken) {
    let queue = WorkQueue::new();
    info!("controller-manager started");

    let tasks = vec![
        tokio::spawn(rs_informer(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(pod_informer(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(resync_loop(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(worker_loop(client.clone(), queue.clone(), cancel.clone())),
    ];
    for t in tasks {
        let _ = t.await;
    }
    info!("controller-manager stopped");
}

/// Watch ReplicaSets; every event enqueues that RS's own name.
async fn rs_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_replicasets(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    ev = stream.next() => match ev {
                        Some(Ok(ev)) => queue.add(ev.object.metadata.name.clone()),
                        Some(Err(e)) => { warn!(error = ?e, "rs watch error; reconnecting"); break; }
                        None => { warn!("rs watch closed; reconnecting"); break; }
                    }
                }
            },
            Err(e) => warn!(error = ?e, "rs watch open failed; retrying"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

/// Watch Pods; map each event to its owning RS (via ownerRef) and enqueue that.
async fn pod_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_pods(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    ev = stream.next() => match ev {
                        Some(Ok(ev)) => {
                            if let Some(rs) = rs_key_for_pod(&ev.object) {
                                queue.add(rs);
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

/// Safety net: every 30s, re-enqueue every RS so missed watch events self-heal.
/// The first `tick()` fires immediately → an initial full sync at startup.
async fn resync_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let mut tick = interval(RESYNC_INTERVAL);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => match client.list_replicasets().await {
                Ok(list) => {
                    for rs in list {
                        queue.add(rs.metadata.name);
                    }
                }
                Err(e) => warn!(error = ?e, "resync list failed"),
            }
        }
    }
}

/// Drain the queue and reconcile. On error, requeue with exponential backoff.
async fn worker_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let rl = RateLimiter::new();
    loop {
        let key = tokio::select! {
            _ = cancel.cancelled() => return,
            k = queue.get() => k,
        };

        match reconcile(&key, &client).await {
            Ok(()) => {
                rl.forget(&key);
                queue.done(&key);
            }
            Err(e) => {
                let attempt = rl.failure(&key);
                let delay = backoff_for(attempt);
                error!(error = ?e, rs = %key, attempt, "reconcile failed; retrying after backoff");
                queue.done(&key);
                queue.add_after(key, delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use crate::apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore},
    };
    use crate::meta::ObjectMeta;
    use crate::pod::{Container, PodSpec};
    use crate::replicaset::{
        LabelSelector, PodTemplateSpec, ReplicaSet, ReplicaSetSpec, TemplateObjectMeta,
    };

    /// Spin up an in-process apiserver AND the full controller-manager against
    /// it. Returns the client + a cancel token to stop the manager at test end.
    async fn spawn_apiserver_and_manager() -> (Arc<Client>, CancellationToken) {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let pod_store = Arc::new(PodStore::from_db(db.clone()).expect("pod store"));
        let rs_store = Arc::new(ResourceStore::<ReplicaSet>::from_db(db.clone()).expect("rs store"));
        let node_store =
            Arc::new(ResourceStore::<crate::node::Node>::from_db(db).expect("node store"));
        let app = router(AppState {
            store: pod_store,
            rs_store,
            node_store,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let client = Arc::new(Client::new(format!("http://{addr}")));
        let cancel = CancellationToken::new();
        tokio::spawn(run(client.clone(), cancel.clone()));
        (client, cancel)
    }

    fn make_rs(name: &str, replicas: u32) -> ReplicaSet {
        let mut match_labels = std::collections::BTreeMap::new();
        match_labels.insert("app".to_string(), name.to_string());
        ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas,
                selector: LabelSelector {
                    match_labels: match_labels.clone(),
                },
                template: PodTemplateSpec {
                    metadata: TemplateObjectMeta {
                        labels: match_labels,
                    },
                    spec: PodSpec {
                        containers: vec![Container {
                            name: "c".into(),
                            image: "busybox".into(),
                            command: vec!["sleep".into(), "1".into()],
                        }],
                        node_name: None,
                    },
                },
            },
            status: None,
        }
    }

    /// Poll until the pod count equals `want` or the deadline passes; returns
    /// the last observed count. Bounds test runtime since convergence is async
    /// across the informer → queue → worker tasks.
    async fn wait_for_pod_count(client: &Client, want: usize, ms: u64) -> usize {
        let deadline = Instant::now() + Duration::from_millis(ms);
        loop {
            let count = client.list_pods().await.unwrap().len();
            if count == want || Instant::now() > deadline {
                return count;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn controller_creates_pods_for_new_replicaset() {
        let (client, cancel) = spawn_apiserver_and_manager().await;
        client.create_replicaset(&make_rs("web", 3)).await.unwrap();

        // The RS watch event flows: informer → queue → worker → reconcile → 3 pods.
        let count = wait_for_pod_count(&client, 3, 3000).await;
        assert_eq!(count, 3, "controller should create 3 pods for the new RS");

        cancel.cancel();
    }

    #[tokio::test]
    async fn controller_recreates_a_deleted_pod() {
        let (client, cancel) = spawn_apiserver_and_manager().await;
        client.create_replicaset(&make_rs("web", 3)).await.unwrap();
        assert_eq!(wait_for_pod_count(&client, 3, 3000).await, 3);

        // Kill one pod. Its DELETED event carries ownerRefs → pod informer maps
        // it to "web" → enqueue → worker reconciles → recreates the missing one.
        let pods = client.list_pods().await.unwrap();
        let victim = &pods[0];
        let rv = victim.metadata.resource_version.clone().unwrap();
        client.delete_pod(&victim.metadata.name, &rv).await.unwrap();

        let count = wait_for_pod_count(&client, 3, 3000).await;
        assert_eq!(count, 3, "controller should recreate the deleted pod (self-heal)");

        cancel.cancel();
    }
}
