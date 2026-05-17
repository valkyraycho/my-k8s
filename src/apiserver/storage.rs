use std::path::Path;

use crate::{
    apiserver::watch::{WatchEvent, WatchEventType},
    pod::{Pod, PodStatus},
};
use chrono::Utc;
use sled::transaction::{ConflictableTransactionError as TxnAbort, TransactionError};
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

const RV_COUNTER_KEY: &[u8] = b"rv_counter";
const POD_PREFIX: &str = "pods/";
const RV_INDEX_TREE: &str = "rv_index";
const WATCH_CHANNEL_CAPACITY: usize = 256;

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

pub struct PodStore {
    db: sled::Db,
    rv_tree: sled::Tree,
    watch_tx: broadcast::Sender<WatchEvent>,
}

impl PodStore {
    fn from_db(db: sled::Db) -> Result<Self, StoreError> {
        let rv_tree = db.open_tree(RV_INDEX_TREE)?;
        let (watch_tx, _) = broadcast::channel(WATCH_CHANNEL_CAPACITY);
        Ok(PodStore {
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

    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent> {
        self.watch_tx.subscribe()
    }

    pub fn current_rv(&self) -> Result<u64, StoreError> {
        Ok(self.db.get(RV_COUNTER_KEY)?.map(decode_u64).unwrap_or(0))
    }

    pub fn get(&self, name: &str) -> Result<Option<Pod>, StoreError> {
        let key = pod_key(name);
        match self.db.get(key.as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn list(&self) -> Result<(Vec<Pod>, u64), StoreError> {
        let pods = self
            .db
            .scan_prefix(POD_PREFIX.as_bytes())
            .map(|entry| {
                let (_, bytes) = entry?;
                Ok(serde_json::from_slice(&bytes)?)
            })
            .collect::<Result<Vec<Pod>, StoreError>>()?;
        let rv = self.current_rv()?;
        Ok((pods, rv))
    }

    pub fn create(&self, mut pod: Pod) -> Result<Pod, StoreError> {
        pod.metadata.uid = Some(Uuid::new_v4().to_string());
        pod.metadata.generation = Some(1);
        pod.metadata.creation_timestamp = Some(Utc::now().to_rfc3339());
        pod.metadata.resource_version = None;
        pod.status = None;

        let name = pod.metadata.name.clone();
        let key = pod_key(&name);

        let pod = self
            .db
            .transaction(|tx| {
                if tx.get(key.as_bytes())?.is_some() {
                    return Err(TxnAbort::Abort(StoreError::AlreadyExists(name.clone())));
                }
                let rv = bump_rv(tx)?;
                let mut p = pod.clone();
                p.metadata.resource_version = Some(rv.to_string());
                tx.insert(key.as_bytes(), to_json(&p)?)?;
                Ok(p)
            })
            .map_err(unwrap_txn)?;

        self.index_rv(&pod)?;
        emit(&self.watch_tx, WatchEventType::Added, pod.clone());
        Ok(pod)
    }

    pub fn replace_spec(&self, name: &str, new_pod: Pod) -> Result<Pod, StoreError> {
        let key = pod_key(name);
        let provided_rv = new_pod.metadata.resource_version.clone();

        let updated = self
            .db
            .transaction(|tx| {
                let current = load_required_pod(tx, &key, name)?;
                check_rv(&current, provided_rv.as_deref())?;

                let rv = bump_rv(tx)?;
                let mut p = new_pod.clone();

                p.metadata.uid = current.metadata.uid;
                p.metadata.creation_timestamp = current.metadata.creation_timestamp;
                p.metadata.generation = Some(current.metadata.generation.unwrap_or(0) + 1);
                p.metadata.resource_version = Some(rv.to_string());
                p.status = current.status;

                tx.insert(key.as_bytes(), to_json(&p)?)?;
                Ok(p)
            })
            .map_err(unwrap_txn)?;

        self.index_rv(&updated)?;
        emit(&self.watch_tx, WatchEventType::Modified, updated.clone());
        Ok(updated)
    }

    pub fn replace_status(
        &self,
        name: &str,
        status: PodStatus,
        expected_rv: &str,
    ) -> Result<Pod, StoreError> {
        let key = pod_key(name);

        let updated = self
            .db
            .transaction(|tx| {
                let mut current = load_required_pod(tx, &key, name)?;
                check_rv(&current, Some(expected_rv))?;

                let rv = bump_rv(tx)?;

                current.status = Some(status.clone());
                current.metadata.resource_version = Some(rv.to_string());

                tx.insert(key.as_bytes(), to_json(&current)?)?;
                Ok(current)
            })
            .map_err(unwrap_txn)?;

        self.index_rv(&updated)?;
        emit(&self.watch_tx, WatchEventType::Modified, updated.clone());
        Ok(updated)
    }

    pub fn delete(&self, name: &str, expected_rv: &str) -> Result<Pod, StoreError> {
        let key = pod_key(name);

        let removed = self
            .db
            .transaction(|tx| {
                let mut current: Pod = load_required_pod(tx, &key, name)?;
                check_rv(&current, Some(expected_rv))?;

                let rv = bump_rv(tx)?;
                current.metadata.resource_version = Some(rv.to_string());

                tx.remove(key.as_bytes())?;
                Ok(current)
            })
            .map_err(unwrap_txn)?;

        emit(&self.watch_tx, WatchEventType::Deleted, removed.clone());
        Ok(removed)
    }

    fn index_rv(&self, pod: &Pod) -> Result<(), StoreError> {
        let Some(rv_str) = &pod.metadata.resource_version else {
            return Ok(());
        };
        let Ok(rv) = rv_str.parse::<u64>() else {
            return Ok(());
        };
        self.rv_tree
            .insert(format!("{rv:020}").as_bytes(), pod.metadata.name.as_bytes())?;
        Ok(())
    }
}

fn pod_key(name: &str) -> String {
    format!("{}{}", POD_PREFIX, name)
}

fn decode_u64(v: sled::IVec) -> u64 {
    v.as_ref().try_into().map(u64::from_le_bytes).unwrap_or(0)
}

fn to_json(pod: &Pod) -> Result<Vec<u8>, TxnAbort<StoreError>> {
    serde_json::to_vec(pod).map_err(|e| TxnAbort::Abort(StoreError::Json(e)))
}

fn from_json(bytes: &[u8]) -> Result<Pod, TxnAbort<StoreError>> {
    serde_json::from_slice(bytes).map_err(|e| TxnAbort::Abort(StoreError::Json(e)))
}

fn load_required_pod(
    tx: &sled::transaction::TransactionalTree,
    key: &str,
    name: &str,
) -> Result<Pod, TxnAbort<StoreError>> {
    let bytes = tx
        .get(key.as_bytes())?
        .ok_or_else(|| TxnAbort::Abort(StoreError::NotFound(name.into())))?;
    from_json(&bytes)
}

fn check_rv(current: &Pod, provided: Option<&str>) -> Result<(), TxnAbort<StoreError>> {
    let current_rv = current.metadata.resource_version.as_deref().unwrap_or("");
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
fn bump_rv(tx: &sled::transaction::TransactionalTree) -> Result<u64, TxnAbort<StoreError>> {
    let current = tx.get(RV_COUNTER_KEY)?.map(decode_u64).unwrap_or(0);
    let next = current.saturating_add(1);
    tx.insert(RV_COUNTER_KEY, &next.to_le_bytes())?;
    Ok(next)
}

fn unwrap_txn(e: TransactionError<StoreError>) -> StoreError {
    match e {
        TransactionError::Abort(e) => e,
        TransactionError::Storage(e) => StoreError::Sled(e),
    }
}

fn emit(tx: &broadcast::Sender<WatchEvent>, event_type: WatchEventType, object: Pod) {
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

        let with_status = store.replace_status("web", running_status(), "1").unwrap();
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
        let updated = store.replace_status("web", running_status(), "1").unwrap();

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
        let updated = store.replace_status("web", running_status(), "1").unwrap();
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
