use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::Notify;

#[derive(Default)]
struct Inner {
    /// Keys ready to hand to a worker, in FIFO order.
    queue: VecDeque<String>,
    /// Every key that needs processing (queued OR in-flight). The dedup set:
    /// a key already here is not enqueued again.
    dirty: HashSet<String>,
    /// Keys currently checked out by a worker (between `get` and `done`).
    processing: HashSet<String>,
}

pub struct WorkQueue {
    inner: Mutex<Inner>,
    notify: Notify,
}

impl WorkQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(WorkQueue {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
        })
    }

    pub fn add(&self, key: String) {
        let mut inner = self.inner.lock().unwrap();
        if inner.dirty.contains(&key) {
            return;
        }

        inner.dirty.insert(key.clone());
        if inner.processing.contains(&key) {
            return;
        }

        inner.queue.push_back(key);
        drop(inner);
        self.notify.notify_one();
    }

    pub fn try_get(&self) -> Option<String> {
        let mut inner = self.inner.lock().unwrap();
        let key = inner.queue.pop_front()?;
        inner.dirty.remove(&key);
        inner.processing.insert(key.clone());
        Some(key)
    }

    pub async fn get(&self) -> String {
        loop {
            if let Some(key) = self.try_get() {
                return key;
            }
            self.notify.notified().await;
        }
    }

    pub fn done(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.processing.remove(key);
        if inner.dirty.contains(key) {
            inner.queue.push_back(key.to_string());
            drop(inner);
            self.notify.notify_one();
        }
    }

    pub fn add_after(self: &Arc<Self>, key: String, delay: Duration) {
        let q = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add(key);
        });
    }

    pub fn ready_len(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }
}

/// Per-key consecutive-failure counter driving exponential backoff on reconcile
/// errors. Kept SEPARATE from the queue (as client-go does) so a key's retry
/// delay is independent of its position in the queue.
#[derive(Default)]
pub struct RateLimiter {
    failures: Mutex<HashMap<String, u32>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failure; returns the new consecutive-failure count for `key`.
    pub fn failure(&self, key: &str) -> u32 {
        let mut f = self.failures.lock().unwrap();
        let n = f.entry(key.to_string()).or_insert(0);
        *n += 1;
        *n
    }

    /// Reset a key's failure count after a successful reconcile.
    pub fn forget(&self, key: &str) {
        self.failures.lock().unwrap().remove(key);
    }
}

/// Exponential backoff: BASE * 2^(n-1), capped at MAX. Same saturating-shift
/// math as the kubelet's CrashLoopBackOff so a huge `n` can't overflow.
pub fn backoff_for(failures: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const MAX: Duration = Duration::from_secs(300);
    let exp = failures.saturating_sub(1).min(20);
    let mult = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
    Duration::from_millis(BASE_MS.saturating_mul(mult)).min(MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_add_is_coalesced() {
        let q = WorkQueue::new();
        q.add("web".into());
        q.add("web".into()); // already dirty → ignored
        q.add("db".into());
        assert_eq!(q.ready_len(), 2, "duplicate 'web' must collapse to one");

        assert_eq!(q.try_get(), Some("web".into()));
        assert_eq!(q.try_get(), Some("db".into()));
        assert_eq!(q.try_get(), None);
    }

    /// The whole point of the three-set design: a key re-added WHILE it's being
    /// processed is not queued immediately (no concurrent processing), but is
    /// requeued by done(). This is the step 3→4→5→7 trace from the walkthrough.
    #[test]
    fn readd_during_processing_requeues_once_on_done() {
        let q = WorkQueue::new();

        q.add("web".into()); // step 1: queued
        assert_eq!(q.try_get(), Some("web".into())); // step 3: now processing
        assert_eq!(q.ready_len(), 0);

        // step 4: re-added while in flight — must NOT hit the ready queue.
        q.add("web".into());
        assert_eq!(
            q.ready_len(),
            0,
            "re-add during processing must not queue a second copy",
        );
        // ...and a worker calling get() now sees nothing (no concurrent dup).
        assert_eq!(q.try_get(), None);

        // step 5: done() sees it's still dirty → requeues exactly once.
        q.done("web");
        assert_eq!(q.ready_len(), 1, "done must requeue the re-added key");
        assert_eq!(q.try_get(), Some("web".into()));

        // step 7: this time nothing was re-added → done is terminal.
        q.done("web");
        assert_eq!(q.ready_len(), 0, "clean done must not requeue");
        assert_eq!(q.try_get(), None);
    }

    #[tokio::test]
    async fn get_parks_until_a_key_arrives() {
        let q = WorkQueue::new();
        let q2 = q.clone();

        // Producer adds after a short delay; the awaiting get() must wake.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            q2.add("web".into());
        });

        let key = tokio::time::timeout(Duration::from_secs(1), q.get())
            .await
            .expect("get() should wake when a key is added");
        assert_eq!(key, "web");
    }

    #[tokio::test]
    async fn add_after_enqueues_following_the_delay() {
        let q = WorkQueue::new();
        q.add_after("web".into(), Duration::from_millis(40));

        // Not ready immediately.
        assert_eq!(q.ready_len(), 0);

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(q.ready_len(), 1, "key should appear after the delay");
        assert_eq!(q.try_get(), Some("web".into()));
    }

    #[test]
    fn rate_limiter_counts_failures_and_forgets() {
        let rl = RateLimiter::new();
        assert_eq!(rl.failure("web"), 1);
        assert_eq!(rl.failure("web"), 2);
        assert_eq!(rl.failure("db"), 1); // independent per key
        rl.forget("web");
        assert_eq!(rl.failure("web"), 1, "forget resets the count");
    }

    #[test]
    fn backoff_grows_then_caps() {
        // n=1 → BASE (500ms), doubling each failure...
        assert_eq!(backoff_for(1), Duration::from_millis(500));
        assert_eq!(backoff_for(2), Duration::from_millis(1000));
        assert_eq!(backoff_for(3), Duration::from_millis(2000));
        // ...and saturating at the 300s cap for large n (no overflow).
        assert_eq!(backoff_for(100), Duration::from_secs(300));
    }
}
