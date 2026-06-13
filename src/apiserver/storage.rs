//! Persistent Pod storage — our "etcd". A sled (embedded KV) database holding
//! each Pod as JSON, plus a monotonic resourceVersion counter and a broadcast
//! channel that fans every write out to watch streams.

use std::path::Path;

use crate::{
    apiserver::watch::{WatchEvent, WatchEventType},
    meta::ResourceMeta,
    pod::Pod,
};
// `as TxnAbort`: import-rename to shorten the very long `Conflictable...` name
// at every use site. The two sled error types matter (see `unwrap_txn` below).
use sled::transaction::{ConflictableTransactionError as TxnAbort, TransactionError};
use thiserror::Error;
use tokio::sync::broadcast;

const RV_COUNTER_KEY: &[u8] = b"rv_counter";
const RV_INDEX_TREE: &str = "rv_index";
/// Broadcast ring-buffer size. A watcher that falls >256 events behind gets
/// `Lagged` and must re-list (see `watch.rs`). Bigger = more slack, more memory.
const WATCH_CHANNEL_CAPACITY: usize = 256;

/// `thiserror` enum. `#[from]` on Sled/Json auto-derives `From`, so `?` on a
/// sled or serde call converts for free. `#[error(transparent)]` forwards the
/// inner error's `Display` verbatim (no wrapping prefix) — right when this
/// variant is a pure pass-through of an underlying error.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("conflict: current rv {current}, provided {provided}")]
    Conflict { current: String, provided: String },
    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
/// A Pod store is just a `ResourceStore` specialized to `Pod`. The alias keeps
/// every existing `PodStore::...` call site (handlers, tests) compiling while
/// the underlying type is now generic.
pub type PodStore = ResourceStore<Pod>;

/// Open the shared sled DB once, then hand the same handle to every
/// `ResourceStore` so they share ONE global `rv_counter` (etcd has a single
/// global revision). Opening the same path twice would hit sled's exclusive
/// single-writer lock — so multi-resource callers MUST go through this.
pub fn open_db(path: impl AsRef<Path>) -> Result<sled::Db, StoreError> {
    Ok(sled::open(path)?)
}

pub struct ResourceStore<T: ResourceMeta> {
    db: sled::Db,
    rv_tree: sled::Tree,
    watch_tx: broadcast::Sender<WatchEvent<T>>,
}

impl<T: ResourceMeta> ResourceStore<T> {
    pub fn from_db(db: sled::Db) -> Result<Self, StoreError> {
        let rv_tree = db.open_tree(RV_INDEX_TREE)?;
        let (watch_tx, _) = broadcast::channel(WATCH_CHANNEL_CAPACITY);
        Ok(Self {
            db,
            rv_tree,
            watch_tx,
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::from_db(sled::open(path)?)
    }

    pub fn open_temporary() -> Result<Self, StoreError> {
        Self::from_db(sled::Config::default().temporary(true).open()?)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent<T>> {
        self.watch_tx.subscribe()
    }

    pub fn current_rv(&self) -> Result<u64, StoreError> {
        Ok(self.db.get(RV_COUNTER_KEY)?.map(decode_u64).unwrap_or(0))
    }

    /// Atomically fetch-and-increment a named counter, returning the value
    /// BEFORE the increment (first call yields 0). Separate from `rv_counter` —
    /// used for one-off ID allocation like per-node PodCIDR indices. Persists in
    /// sled, so an index is never reused across apiserver restarts.
    pub fn next_index(&self, counter_key: &[u8]) -> Result<u64, StoreError> {
        // `fetch_and_update(key, closure)`: sled reads the current bytes, runs
        // `closure(old) -> new bytes`, and CAS-writes them — retrying the whole
        // thing if another writer raced in. So the closure is the atomic unit.
        // It returns the PREVIOUS value (`Option<IVec>`); `?` turns a sled I/O
        // error into StoreError via the `#[from]` on StoreError::Sled.
        let previous = self.db.fetch_and_update(counter_key, |old| {
            // `old: Option<&[u8]>` — None on first use, else the raw stored bytes.
            let current = old
                // &[u8] -> [u8; 8] via TryInto (fails if len != 8); .ok() drops
                // the error so `and_then` yields Option<[u8; 8]>.
                .and_then(|b| b.try_into().ok())
                // reinterpret the 8 bytes as a u64 (little-endian, matching how
                // we write below and how `decode_u64` reads elsewhere).
                .map(u64::from_le_bytes)
                // missing/garbage key -> start the counter at 0.
                .unwrap_or(0);
            // New value to store: current+1 as 8 LE bytes. `saturating_add`
            // can't wrap; `to_vec` because the closure must return owned bytes
            // (Option<Vec<u8>>, which sled accepts as Into<IVec>).
            Some(current.saturating_add(1).to_le_bytes().to_vec())
        })?;
        // `previous` is the bytes that were there before our increment; decode
        // back to u64 (None on the very first call -> 0).
        Ok(previous.map(decode_u64).unwrap_or(0))
    }

    fn key(&self, name: &str) -> String {
        format!("{}{}", T::KIND_PREFIX, name)
    }

    pub fn get(&self, name: &str) -> Result<Option<T>, StoreError> {
        match self.db.get(self.key(name).as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn list(&self) -> Result<(Vec<T>, u64), StoreError> {
        let items = self
            .db
            .scan_prefix(T::KIND_PREFIX.as_bytes())
            .map(|entry| {
                let (_, bytes) = entry?;
                Ok(serde_json::from_slice(&bytes)?)
            })
            .collect::<Result<Vec<T>, StoreError>>()?;
        let rv = self.current_rv()?;
        Ok((items, rv))
    }

    pub fn create(&self, mut obj: T) -> Result<T, StoreError> {
        // One source of truth for create-stamping, shared with the Raft leader
        // path. Direct mode behaviour is unchanged.
        obj.stamp_for_create();
        self.create_prestamped(obj)
    }

    /// `create` minus uid/timestamp generation — the object arrives already
    /// stamped (the Raft leader did it once, pre-propose). Every replica runs
    /// THIS on the committed command, so no clock/RNG at apply → identical
    /// stores. rv is still assigned here (deterministic: apply order matches).
    pub fn create_prestamped(&self, obj: T) -> Result<T, StoreError> {
        let name = obj.meta().name.clone();
        let key = self.key(&name);

        // `db.transaction(closure)` runs the closure ATOMICALLY (sled retries it
        // on contention). The existence check, rv bump, and insert must be one
        // indivisible unit — else two concurrent creates could both pass the
        // "doesn't exist" check. The closure returns `Result<_, TxnAbort<_>>`.
        let obj = self
            .db
            .transaction(|tx| {
                if tx.get(key.as_bytes())?.is_some() {
                    // `TxnAbort::Abort(e)` aborts WITHOUT retry, carrying our
                    // typed error out (vs a storage error, which sled may retry).
                    return Err(TxnAbort::Abort(StoreError::AlreadyExists(name.clone())));
                }
                let rv = bump_rv(tx)?;
                let mut o = obj.clone();
                o.meta_mut().resource_version = Some(rv.to_string());
                tx.insert(key.as_bytes(), to_json(&o)?)?;
                Ok(o)
            })
            // `unwrap_txn` collapses sled's two-layer `TransactionError` back
            // into our flat `StoreError` (see its definition below).
            .map_err(unwrap_txn)?;

        self.index_rv(&obj)?;
        // Fan the write out to all watch subscribers. Done AFTER the txn commits
        // so watchers never see an event for a write that rolled back.
        emit(&self.watch_tx, WatchEventType::Added, obj.clone());
        Ok(obj)
    }

    pub fn replace_spec(&self, name: &str, new_obj: T) -> Result<T, StoreError> {
        let key = self.key(name);
        let provided_rv = new_obj.meta().resource_version.clone();

        let updated = self
            .db
            .transaction(|tx| {
                let current = load_required::<T>(tx, &key, name)?;
                check_rv(&current, provided_rv.as_deref())?;

                let rv = bump_rv(tx)?;
                let mut o = new_obj.clone();
                {
                    let cm = current.meta().clone();
                    let m = o.meta_mut();
                    m.uid = cm.uid;
                    m.creation_timestamp = cm.creation_timestamp;
                    m.generation = Some(cm.generation.unwrap_or(0) + 1);
                    m.resource_version = Some(rv.to_string());
                }
                o.inherit_status(&current);

                tx.insert(key.as_bytes(), to_json(&o)?)?;
                Ok(o)
            })
            .map_err(unwrap_txn)?;

        self.index_rv(&updated)?;
        emit(&self.watch_tx, WatchEventType::Modified, updated.clone());
        Ok(updated)
    }

    /// Replace status via a caller-supplied mutator. The store stays ignorant of
    /// the concrete status type — the closure (which knows `T`) does the field
    /// assignment, e.g. `|p| p.status = Some(new_status)`.
    pub fn replace_status<F: Fn(&mut T)>(
        &self,
        name: &str,
        expected_rv: &str,
        mutate: F,
    ) -> Result<T, StoreError> {
        let key = self.key(name);

        let updated = self
            .db
            .transaction(|tx| {
                let mut current = load_required::<T>(tx, &key, name)?;
                check_rv(&current, Some(expected_rv))?;

                let rv = bump_rv(tx)?;
                mutate(&mut current);
                current.meta_mut().resource_version = Some(rv.to_string());

                tx.insert(key.as_bytes(), to_json(&current)?)?;
                Ok(current)
            })
            .map_err(unwrap_txn)?;

        self.index_rv(&updated)?;
        emit(&self.watch_tx, WatchEventType::Modified, updated.clone());
        Ok(updated)
    }

    pub fn delete(&self, name: &str, expected_rv: &str) -> Result<T, StoreError> {
        let key = self.key(name);

        let removed = self
            .db
            .transaction(|tx| {
                let mut current = load_required::<T>(tx, &key, name)?;
                check_rv(&current, Some(expected_rv))?;

                let rv = bump_rv(tx)?;
                current.meta_mut().resource_version = Some(rv.to_string());

                tx.remove(key.as_bytes())?;
                Ok(current)
            })
            .map_err(unwrap_txn)?;

        emit(&self.watch_tx, WatchEventType::Deleted, removed.clone());
        Ok(removed)
    }
    fn index_rv(&self, obj: &T) -> Result<(), StoreError> {
        let Some(rv_str) = &obj.meta().resource_version else {
            return Ok(());
        };
        let Ok(rv) = rv_str.parse::<u64>() else {
            return Ok(());
        };
        self.rv_tree
            .insert(format!("{rv:020}").as_bytes(), obj.meta().name.as_bytes())?;
        Ok(())
    }
}

fn decode_u64(v: sled::IVec) -> u64 {
    v.as_ref().try_into().map(u64::from_le_bytes).unwrap_or(0)
}

fn to_json<T: ResourceMeta>(obj: &T) -> Result<Vec<u8>, TxnAbort<StoreError>> {
    serde_json::to_vec(obj).map_err(|e| TxnAbort::Abort(StoreError::Json(e)))
}

fn load_required<T: ResourceMeta>(
    tx: &sled::transaction::TransactionalTree,
    key: &str,
    name: &str,
) -> Result<T, TxnAbort<StoreError>> {
    let bytes = tx
        .get(key.as_bytes())?
        .ok_or_else(|| TxnAbort::Abort(StoreError::NotFound(name.into())))?;

    serde_json::from_slice(&bytes).map_err(|e| TxnAbort::Abort(StoreError::Json(e)))
}

/// The optimistic-concurrency gate. The caller must echo back the rv it read;
/// a mismatch (or a missing rv — a client that forgot to fetch-then-PUT) is a
/// `Conflict`. `match` with a guard (`Some(p) if p == current_rv`) is the
/// idiomatic three-way branch: matches, mismatches, absent.
fn check_rv<T: ResourceMeta>(
    current: &T,
    provided: Option<&str>,
) -> Result<(), TxnAbort<StoreError>> {
    let current_rv = current.meta().resource_version.as_deref().unwrap_or("");
    match provided {
        Some(p) if p == current_rv => Ok(()),
        Some(p) => Err(TxnAbort::Abort(StoreError::Conflict {
            current: current_rv.into(),
            provided: p.into(),
        })),
        None => Err(TxnAbort::Abort(StoreError::Conflict {
            current: current_rv.into(),
            provided: "(missing)".into(),
        })),
    }
}
/// The single monotonic counter behind every resourceVersion. Read-add-write
/// INSIDE the transaction so it's atomic with the data write — that's what
/// makes concurrent writers serialize correctly. `saturating_add` won't wrap
/// (a u64 counter won't realistically overflow, but no UB if it somehow did).
fn bump_rv(tx: &sled::transaction::TransactionalTree) -> Result<u64, TxnAbort<StoreError>> {
    let current = tx.get(RV_COUNTER_KEY)?.map(decode_u64).unwrap_or(0);
    let next = current.saturating_add(1);
    tx.insert(RV_COUNTER_KEY, &next.to_le_bytes())?;
    Ok(next)
}

/// Collapse sled's TWO-LAYER transaction error into our flat `StoreError`.
/// `TransactionError::Abort` = our own typed error returned via `TxnAbort::Abort`;
/// `Storage` = an underlying sled I/O failure. Callers only want one error type,
/// so we merge them here with `.map_err(unwrap_txn)`.
fn unwrap_txn(e: TransactionError<StoreError>) -> StoreError {
    match e {
        TransactionError::Abort(e) => e,
        TransactionError::Storage(e) => StoreError::Sled(e),
    }
}

fn emit<T: ResourceMeta>(
    tx: &broadcast::Sender<WatchEvent<T>>,
    event_type: WatchEventType,
    object: T,
) {
    let _ = tx.send(WatchEvent { event_type, object });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::{
        Container, ContainerStatus, ContainerStatusState, PodMetadata, PodPhase, PodSpec, PodStatus,
    };

    fn store() -> PodStore {
        PodStore::open_temporary().expect("temporary sled should open")
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
                node_name: None,
            },
            status: None,
        }
    }

    fn running_status() -> PodStatus {
        PodStatus {
            phase: PodPhase::Running,
            container_statuses: vec![ContainerStatus {
                name: "c".into(),
                ready: true,
                restart_count: 0,
                state: ContainerStatusState::Running {
                    started_at: "2026-05-17T10:00:00Z".into(),
                },
            }],
            observed_generation: Some(1),
            pod_ip: None,
        }
    }

    #[test]
    fn create_assigns_apiserver_fields_and_first_rv() {
        let store = store();
        let created = store.create(make_pod("web")).expect("create");

        assert!(created.metadata.uid.is_some(), "uid must be assigned");
        assert_eq!(created.metadata.generation, Some(1));
        assert!(created.metadata.creation_timestamp.is_some());
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));
        assert!(created.status.is_none(), "status not set on create");
    }

    #[test]
    fn create_prestamped_preserves_uid_and_timestamp_but_assigns_rv() {
        let store = store();
        // The leader's pre-stamped object: uid + creationTimestamp already set.
        let mut pod = make_pod("web");
        pod.metadata.uid = Some("leader-uid-123".into());
        pod.metadata.creation_timestamp = Some("2026-06-12T10:00:00Z".into());

        let created = store.create_prestamped(pod).expect("create_prestamped");

        // Determinism: the stamped fields are TRUSTED, not regenerated.
        assert_eq!(created.metadata.uid.as_deref(), Some("leader-uid-123"));
        assert_eq!(
            created.metadata.creation_timestamp.as_deref(),
            Some("2026-06-12T10:00:00Z")
        );
        // rv is still assigned here (deterministic — apply order is identical).
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));
    }

    #[test]
    fn create_prestamped_still_rejects_duplicates() {
        let store = store();
        store.create_prestamped(make_pod("web")).unwrap();
        let err = store.create_prestamped(make_pod("web")).unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(ref n) if n == "web"));
    }

    #[test]
    fn create_rejects_duplicate_name() {
        let store = store();
        store.create(make_pod("web")).unwrap();
        let err = store.create(make_pod("web")).unwrap_err();
        assert!(
            matches!(err, StoreError::AlreadyExists(ref n) if n == "web"),
            "expected AlreadyExists, got: {err:?}",
        );
    }

    #[test]
    fn create_clobbers_client_provided_apiserver_fields() {
        let store = store();
        let mut pod = make_pod("web");
        pod.metadata.uid = Some("client-uid".into());
        pod.metadata.resource_version = Some("999".into());
        pod.metadata.generation = Some(42);
        pod.status = Some(running_status());

        let created = store.create(pod).unwrap();
        assert_ne!(
            created.metadata.uid.as_deref(),
            Some("client-uid"),
            "apiserver must mint a fresh uid",
        );
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));
        assert_eq!(created.metadata.generation, Some(1));
        assert!(created.status.is_none(), "client status must be discarded");
    }

    #[test]
    fn get_returns_none_for_missing() {
        assert!(store().get("does-not-exist").unwrap().is_none());
    }

    #[test]
    fn get_returns_round_tripped_pod() {
        let store = store();
        let created = store.create(make_pod("web")).unwrap();
        let fetched = store.get("web").unwrap().expect("present");
        assert_eq!(fetched, created);
    }

    #[test]
    fn list_returns_pods_and_current_rv_consistently() {
        let store = store();
        store.create(make_pod("a")).unwrap();
        store.create(make_pod("b")).unwrap();
        let (pods, rv) = store.list().unwrap();
        assert_eq!(pods.len(), 2);
        let names: Vec<&str> = pods.iter().map(|p| p.metadata.name.as_str()).collect();
        assert!(names.contains(&"a") && names.contains(&"b"));
        assert!(
            rv >= 2,
            "rv should be at-or-after the last write (got {rv})"
        );
    }

    #[test]
    fn replace_spec_bumps_rv_and_generation_preserves_uid_and_status() {
        let store = store();
        let created = store.create(make_pod("web")).unwrap();

        let with_status = store
            .replace_status("web", "1", |p| p.status = Some(running_status()))
            .unwrap();
        assert_eq!(with_status.metadata.resource_version.as_deref(), Some("2"));
        assert_eq!(
            with_status.metadata.generation,
            Some(1),
            "status writes do NOT bump generation",
        );

        let mut new_pod = make_pod("web");
        new_pod.metadata.resource_version = Some("2".into());
        new_pod.spec.containers[0].command = vec!["echo".into(), "new".into()];
        let replaced = store.replace_spec("web", new_pod).unwrap();

        assert_eq!(replaced.metadata.uid, created.metadata.uid, "uid preserved");
        assert_eq!(
            replaced.metadata.creation_timestamp, created.metadata.creation_timestamp,
            "creationTimestamp preserved",
        );
        assert_eq!(
            replaced.metadata.generation,
            Some(2),
            "spec write bumps generation"
        );
        assert_eq!(replaced.metadata.resource_version.as_deref(), Some("3"));
        assert!(
            replaced.status.is_some(),
            "status preserved across spec write"
        );
        assert_eq!(replaced.spec.containers[0].command, vec!["echo", "new"]);
    }

    #[test]
    fn replace_spec_rejects_stale_rv() {
        let store = store();
        store.create(make_pod("web")).unwrap();

        let mut stale = make_pod("web");
        stale.metadata.resource_version = Some("999".into());
        let err = store.replace_spec("web", stale).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Conflict { ref current, ref provided }
                    if current == "1" && provided == "999"
            ),
            "expected Conflict {{1, 999}}, got: {err:?}",
        );
    }

    #[test]
    fn replace_spec_missing_rv_is_rejected_as_conflict() {
        let store = store();
        store.create(make_pod("web")).unwrap();
        // make_pod() leaves resource_version = None — simulates a client
        // that forgot to fetch-then-PUT.
        let err = store.replace_spec("web", make_pod("web")).unwrap_err();
        assert!(matches!(err, StoreError::Conflict { .. }));
    }

    #[test]
    fn replace_status_bumps_rv_but_not_generation() {
        let store = store();
        store.create(make_pod("web")).unwrap();
        let updated = store
            .replace_status("web", "1", |p| p.status = Some(running_status()))
            .unwrap();

        assert_eq!(updated.metadata.resource_version.as_deref(), Some("2"));
        assert_eq!(
            updated.metadata.generation,
            Some(1),
            "status writes do NOT bump generation",
        );
        assert_eq!(updated.status.as_ref().unwrap().phase, PodPhase::Running);
    }

    #[test]
    fn delete_rejects_stale_rv_then_removes_on_correct_rv() {
        let store = store();
        store.create(make_pod("web")).unwrap();

        let err = store.delete("web", "999").unwrap_err();
        assert!(matches!(err, StoreError::Conflict { .. }));
        assert!(
            store.get("web").unwrap().is_some(),
            "rejected delete must not remove",
        );

        let removed = store.delete("web", "1").unwrap();
        assert_eq!(removed.metadata.name, "web");
        assert_eq!(
            removed.metadata.resource_version.as_deref(),
            Some("2"),
            "delete bumps rv so DELETED event has fresh rv",
        );
        assert!(
            store.get("web").unwrap().is_none(),
            "successful delete removes"
        );
    }

    #[test]
    fn delete_missing_pod_returns_not_found() {
        let err = store().delete("nope", "1").unwrap_err();
        assert!(matches!(err, StoreError::NotFound(ref n) if n == "nope"));
    }

    #[test]
    fn writes_emit_watch_events_in_order() {
        let store = store();
        let mut rx = store.subscribe(); // subscribe FIRST, before any writes

        let created = store.create(make_pod("web")).unwrap();
        let updated = store
            .replace_status("web", "1", |p| p.status = Some(running_status()))
            .unwrap();
        let deleted = store.delete("web", "2").unwrap();

        let ev1 = rx.try_recv().expect("ADDED event");
        assert_eq!(ev1.event_type, WatchEventType::Added);
        assert_eq!(ev1.object, created);

        let ev2 = rx.try_recv().expect("MODIFIED event");
        assert_eq!(ev2.event_type, WatchEventType::Modified);
        assert_eq!(ev2.object, updated);

        let ev3 = rx.try_recv().expect("DELETED event");
        assert_eq!(ev3.event_type, WatchEventType::Deleted);
        assert_eq!(ev3.object, deleted);

        // No more events queued.
        assert!(rx.try_recv().is_err());
    }
}

/// Proves the generic store works for ANY `ResourceMeta` type, not just `Pod`.
/// A tiny `TestResource` (no relation to the real API types) exercises the same
/// CRUD + rv + conflict machinery — if this passes, the abstraction is sound
/// independent of the concrete resources, which is the whole point of step 2.
#[cfg(test)]
mod generic_tests {
    use super::*;
    use crate::meta::ObjectMeta;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
    struct TestResource {
        metadata: ObjectMeta,
        payload: u32,
        status: Option<u32>,
    }

    impl ResourceMeta for TestResource {
        const KIND_PREFIX: &'static str = "tests/";
        fn meta(&self) -> &ObjectMeta {
            &self.metadata
        }
        fn meta_mut(&mut self) -> &mut ObjectMeta {
            &mut self.metadata
        }
        fn clear_status(&mut self) {
            self.status = None;
        }
        fn inherit_status(&mut self, current: &Self) {
            self.status = current.status;
        }
    }

    fn store() -> ResourceStore<TestResource> {
        ResourceStore::open_temporary().expect("temp sled")
    }

    fn make(name: &str, payload: u32) -> TestResource {
        TestResource {
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            payload,
            status: None,
        }
    }

    #[test]
    fn create_get_list_roundtrip_for_arbitrary_type() {
        let store = store();
        let created = store.create(make("a", 7)).unwrap();
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));
        assert_eq!(created.payload, 7);
        assert!(created.metadata.uid.is_some());

        assert_eq!(store.get("a").unwrap(), Some(created));
        let (items, rv) = store.list().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(rv, 1);
    }

    #[test]
    fn stale_rv_replace_conflicts_for_arbitrary_type() {
        let store = store();
        store.create(make("a", 1)).unwrap(); // rv=1
        let mut stale = make("a", 2);
        stale.metadata.resource_version = Some("99".into());
        let err = store.replace_spec("a", stale).unwrap_err();
        assert!(matches!(err, StoreError::Conflict { .. }), "got: {err:?}");
    }

    #[test]
    fn replace_status_closure_mutates_arbitrary_type() {
        let store = store();
        store.create(make("a", 1)).unwrap(); // rv=1
        let updated = store
            .replace_status("a", "1", |r| r.status = Some(42))
            .unwrap();
        assert_eq!(updated.status, Some(42));
        assert_eq!(updated.metadata.resource_version.as_deref(), Some("2"));
    }

    #[test]
    fn prefix_isolates_kinds_sharing_one_db() {
        // Two stores over the SAME sled::Db but different KIND_PREFIX must not
        // see each other's objects, yet must share the global rv_counter.
        let db = sled::Config::default().temporary(true).open().unwrap();
        let tests = ResourceStore::<TestResource>::from_db(db.clone()).unwrap();
        let pods = ResourceStore::<Pod>::from_db(db.clone()).unwrap();

        tests.create(make("a", 1)).unwrap(); // global rv -> 1
        let p = pods
            .create(Pod {
                api_version: "v1".into(),
                kind: "Pod".into(),
                metadata: ObjectMeta {
                    name: "web".into(),
                    ..Default::default()
                },
                spec: crate::pod::PodSpec {
                    containers: vec![],
                    node_name: None,
                },
                status: None,
            })
            .unwrap(); // global rv -> 2

        // Each store lists only its own kind...
        assert_eq!(tests.list().unwrap().0.len(), 1);
        assert_eq!(pods.list().unwrap().0.len(), 1);
        assert!(tests.get("web").unwrap().is_none());
        assert!(pods.get("a").unwrap().is_none());

        // ...but the rv counter is shared: the Pod got rv=2, not rv=1.
        assert_eq!(p.metadata.resource_version.as_deref(), Some("2"));
    }
}
