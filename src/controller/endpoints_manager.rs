use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::client::Client;
use crate::controller::endpoints::{reconcile, services_for_pod};
use crate::controller::workqueue::{RateLimiter, WorkQueue, backoff_for};

const RESYNC_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

pub async fn run(client: Arc<Client>, cancel: CancellationToken) {
    let queue = WorkQueue::new();
    info!("endpoints-controller started");

    let tasks = vec![
        tokio::spawn(service_informer(
            client.clone(),
            queue.clone(),
            cancel.clone(),
        )),
        tokio::spawn(pod_informer(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(resync_loop(client.clone(), queue.clone(), cancel.clone())),
        tokio::spawn(worker_loop(client.clone(), queue.clone(), cancel.clone())),
    ];
    for t in tasks {
        let _ = t.await;
    }
    info!("endpoints-controller stopped");
}

/// Watch Services; every event enqueues that Service's own name.
async fn service_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_services(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                                    _ = cancel.cancelled() => return,
                                    ev = stream.next() => match ev {
                                        Some(Ok(ev)) => queue.add(ev.object.metadata.name.clone()),
                                        Some(Err(e)) => { warn!(error = ?e, "service watch error;
                reconnecting"); break; }
                                        None => { warn!("service watch closed; reconnecting");
                break; }
                                    }
                                }
            },
            Err(e) => warn!(error = ?e, "service watch open failed; retrying"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

/// Watch Pods; a pod change can alter any Service whose selector matches it, so
/// we enqueue all matching Services. We must list Services to do the mapping
/// (no reverse index) — cheap at our scale.
async fn pod_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_pods(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    ev = stream.next() => match ev {
                        Some(Ok(ev)) => {
                            if let Ok(services) = client.list_services().await {
                                for key in services_for_pod(&ev.object, &services) {
                                    queue.add(key);
                                }
                            }
                        }
                        Some(Err(e)) => { warn!(error = ?e, "pod watch error;
                reconnecting"); break; }
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

/// Safety net: every 30s re-enqueue every Service. First tick fires immediately.
async fn resync_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let mut tick = interval(RESYNC_INTERVAL);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => match client.list_services().await {
                Ok(list) => {
                    for svc in list {
                        queue.add(svc.metadata.name);
                    }
                }
                Err(e) => warn!(error = ?e, "resync list failed"),
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

        match reconcile(&key, &client).await {
            Ok(()) => {
                rl.forget(&key);
                queue.done(&key);
            }
            Err(e) => {
                let attempt = rl.failure(&key);
                let delay = backoff_for(attempt);
                error!(error = ?e, svc = %key, attempt, "reconcile failed; retrying 
  after backoff");
                queue.done(&key);
                queue.add_after(key, delay);
            }
        }
    }
}
