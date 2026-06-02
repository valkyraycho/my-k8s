use std::sync::Arc;

use async_stream::try_stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::Stream;
use tracing::warn;

use crate::{
    apiserver::storage::{PodStore, StoreError},
    pod::Pod,
};

/// One watch frame. `#[serde(rename = "type")]` — `type` is a Rust keyword, so
/// the field is `event_type` in code but serializes to the K8s wire key `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchEvent {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: Pod,
}

/// `rename_all = "UPPERCASE"` matches K8s's `ADDED`/`MODIFIED`/`DELETED`.
/// `Copy` because it's a trivial fieldless enum — cheaper to copy than borrow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Error)]
pub enum WatchError {
    /// Receiver fell more than the broadcast channel's capacity behind.
    /// The HTTP layer maps this to 410 Gone — the client must re-list.
    #[error("watcher lagged behind, skipped {0} events")]
    Lagged(u64),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Build a watch stream: a snapshot "catch-up" phase, then a live phase.
///
/// Returns `impl Stream` (return-position impl Trait) because `try_stream!`'s
/// concrete type is unnameable. Correctness hinges on subscribing BEFORE the
/// `list()` snapshot: any write landing in between is then buffered in the
/// broadcast channel and replayed live (deduped by the rv filter), so nothing
/// slips through the gap.
pub fn stream_events(
    store: Arc<PodStore>,
    from_rv: u64,
) -> impl Stream<Item = Result<WatchEvent, WatchError>> {
    // `try_stream!` lets us write a generator as straight-line async with
    // `yield`, instead of hand-implementing `Stream::poll_next`. Inside it, `?`
    // YIELDS the error as the final item and ends the stream.
    try_stream! {
        let mut rx = store.subscribe();        // subscribe first — closes the list/subscribe race
        let (snapshot, snapshot_rv) = store.list()?;

        // Catch-up: replay everything newer than the client's resume point.
        for pod in snapshot {
            if pod_rv(&pod) > from_rv {
                yield WatchEvent {
                    event_type: WatchEventType::Added,
                    object: pod,
                }
            }
        }

        // Live: forward broadcast events strictly newer than the snapshot, so an
        // object present in BOTH phases isn't emitted twice.
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if pod_rv(&ev.object) > snapshot_rv {
                        yield ev
                    }
                }
                // Fell >channel-capacity behind: we can't silently skip (the
                // client's cache would desync forever), so END the stream with
                // an error → HTTP 410 → client re-lists. `?` does the terminate.
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "watch stream lagged; closing");
                    Err(WatchError::Lagged(skipped))?;
                }
                // Sender dropped (store gone) → clean end of stream.
                Err(RecvError::Closed) => break,
            }
        }
    }
}

fn pod_rv(pod: &Pod) -> u64 {
    pod.metadata
        .resource_version
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    use crate::pod::{Container, PodMetadata, PodSpec};

    fn store() -> Arc<PodStore> {
        Arc::new(PodStore::open_temporary().expect("temp sled"))
    }

    fn make_pod(name: &str) -> Pod {
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: PodMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "c".into(),
                    image: "busybox".into(),
                    command: vec!["sleep".into(), "1".into()],
                }],
            },
            status: None,
        }
    }

    /// Try to pull one item from the stream within `ms`. Returns None on
    /// timeout (i.e. stream stayed Pending). Use to bound test runtime —
    /// stream_events is open-ended by design.
    async fn try_next<S>(stream: &mut S, ms: u64) -> Option<S::Item>
    where
        S: Stream + Unpin,
    {
        timeout(Duration::from_millis(ms), stream.next())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn empty_store_catch_up_emits_nothing() {
        let store = store();
        let stream = stream_events(store, 0);
        tokio::pin!(stream);

        // Empty catch-up → stream parks at rx.recv().await; we time out cleanly.
        assert!(try_next(&mut stream, 50).await.is_none());
    }

    #[tokio::test]
    async fn catch_up_from_zero_emits_added_for_each_existing_pod() {
        let store = store();
        store.create(make_pod("a")).unwrap();
        store.create(make_pod("b")).unwrap();
        store.create(make_pod("c")).unwrap();

        let stream = stream_events(store.clone(), 0);
        tokio::pin!(stream);

        let mut events = Vec::new();
        for _ in 0..3 {
            let ev = try_next(&mut stream, 100)
                .await
                .expect("catch-up event")
                .expect("ok");
            events.push(ev);
        }

        assert!(events.iter().all(|e| e.event_type == WatchEventType::Added));
        let names: Vec<&str> = events
            .iter()
            .map(|e| e.object.metadata.name.as_str())
            .collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));

        // No more catch-up; live loop stays pending.
        assert!(try_next(&mut stream, 50).await.is_none());
    }

    #[tokio::test]
    async fn catch_up_filters_by_from_rv() {
        let store = store();
        store.create(make_pod("a")).unwrap(); // rv=1
        store.create(make_pod("b")).unwrap(); // rv=2
        store.create(make_pod("c")).unwrap(); // rv=3

        // from_rv=2 → only "c" (rv=3) should be emitted as catch-up.
        let stream = stream_events(store.clone(), 2);
        tokio::pin!(stream);

        let ev = try_next(&mut stream, 100)
            .await
            .expect("event")
            .expect("ok");
        assert_eq!(ev.event_type, WatchEventType::Added);
        assert_eq!(ev.object.metadata.name, "c");
        assert!(try_next(&mut stream, 50).await.is_none());
    }

    #[tokio::test]
    async fn live_events_after_catch_up_are_delivered() {
        let store = store();
        store.create(make_pod("a")).unwrap(); // rv=1

        let stream = stream_events(store.clone(), 0);
        tokio::pin!(stream);

        // Catch-up: ADDED for "a"
        let ev1 = try_next(&mut stream, 100)
            .await
            .expect("catch-up")
            .expect("ok");
        assert_eq!(ev1.event_type, WatchEventType::Added);
        assert_eq!(ev1.object.metadata.name, "a");

        // Live write — must arrive via broadcast forwarding.
        let store_w = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            store_w.create(make_pod("b")).unwrap();
        });

        let ev2 = try_next(&mut stream, 300)
            .await
            .expect("live event")
            .expect("ok");
        assert_eq!(ev2.event_type, WatchEventType::Added);
        assert_eq!(ev2.object.metadata.name, "b");
        assert!(pod_rv(&ev2.object) > 1, "live event rv must exceed snapshot_rv");
    }

    #[tokio::test]
    async fn lagged_receiver_terminates_stream_with_error() {
        let store = store();
        let stream = stream_events(store.clone(), 0);
        tokio::pin!(stream);

        // First poll: empty catch-up, stream parks at rx.recv().await.
        // Just need to trigger the subscribe.
        assert!(try_next(&mut stream, 10).await.is_none());

        // Flood past the broadcast capacity (256). The receiver hasn't been
        // polled since subscribing, so events queue then evict from the
        // ring buffer.
        for i in 0..300 {
            store.create(make_pod(&format!("p{i}"))).unwrap();
        }

        // Next poll should surface Lagged and end the stream.
        let item = try_next(&mut stream, 500).await.expect("should yield");
        assert!(
            matches!(item, Err(WatchError::Lagged(_))),
            "expected Lagged, got: {item:?}",
        );

        // try_stream's `?` operator ended the stream after the error.
        assert!(try_next(&mut stream, 50).await.is_none());
    }
}
