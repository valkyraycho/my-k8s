//! The replicated state machine: applies committed StoreCommands to the five
//! typed stores, in log order. Every replica runs this on the same command
//! stream → identical stores. Deterministic by construction: uses ONLY the
//! command bytes + current store contents (no clocks, no RNG — the leader
//! pre-stamped uid/timestamp before proposing).

use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::apiserver::command::{ApplyOutcome, Op, StoreCommand};
use crate::apiserver::storage::{PodStore, ResourceStore, StoreError};
use crate::endpoints::Endpoints;
use crate::meta::ResourceMeta;
use crate::node::Node;
use crate::pod::Pod;
use crate::replicaset::ReplicaSet;
use crate::service::Service;

/// Holds the same store Arcs as AppState — the apply loop and the read handlers
/// share one set of stores per replica.
pub struct Applier {
    pub pods: Arc<PodStore>,
    pub replicasets: Arc<ResourceStore<ReplicaSet>>,
    pub nodes: Arc<ResourceStore<Node>>,
    pub services: Arc<ResourceStore<Service>>,
    pub endpoints: Arc<ResourceStore<Endpoints>>,
}

impl Applier {
    /// Dispatch by kind to the right typed store. if/else, not match: KIND_PREFIX
    /// consts can't be match patterns (a bare path binds a variable, not compares).
    pub fn apply(&self, cmd: StoreCommand) -> ApplyOutcome {
        let k = cmd.kind.as_str();
        if k == Pod::KIND_PREFIX {
            apply_op(&self.pods, cmd.op, set_pod_status)
        } else if k == ReplicaSet::KIND_PREFIX {
            apply_op(&self.replicasets, cmd.op, set_rs_status)
        } else if k == Node::KIND_PREFIX {
            apply_op(&self.nodes, cmd.op, set_node_status)
        } else if k == Service::KIND_PREFIX {
            apply_op(&self.services, cmd.op, |_, _| {})
        } else if k == Endpoints::KIND_PREFIX {
            apply_op(&self.endpoints, cmd.op, |_, _| {})
        } else {
            ApplyOutcome::Internal(format!("unknown command kind: {k}"))
        }
    }
}

/// Generic per-store applier: Create/ReplaceSpec/Delete are fully generic over
/// `T`; only the status assignment differs per kind, so it's injected as a
/// closure (Service/Endpoints pass a no-op — they have no status subresource).
fn apply_op<T: ResourceMeta>(
    store: &ResourceStore<T>,
    op: Op,
    set_status: impl Fn(&mut T, Value),
) -> ApplyOutcome {
    let result: Result<T, StoreError> = match op {
        Op::Create { obj } => match serde_json::from_value::<T>(obj) {
            Ok(o) => store.create_prestamped(o),
            Err(e) => {
                return ApplyOutcome::Internal(format!("bad create payload: {e}"));
            }
        },
        Op::ReplaceSpec { name, obj } => match serde_json::from_value::<T>(obj) {
            Ok(o) => store.replace_spec(&name, o),
            Err(e) => {
                return ApplyOutcome::Internal(format!("bad replace payload: {e}"));
            }
        },
        // `move` + `clone()`: replace_status takes an `Fn` (sled may RETRY the
        // txn closure), so the status must be re-usable on each call.
        Op::ReplaceStatus { name, rv, status } => {
            store.replace_status(&name, &rv, move |t| set_status(t, status.clone()))
        }
        Op::Delete { name, rv } => store.delete(&name, &rv),
    };
    outcome(result)
}

fn outcome<T: Serialize>(result: Result<T, StoreError>) -> ApplyOutcome {
    match result {
        Ok(obj) => match serde_json::to_value(&obj) {
            Ok(v) => ApplyOutcome::Ok(v),
            Err(e) => ApplyOutcome::Internal(format!("serialize result: {e}")),
        },
        Err(StoreError::NotFound(n)) => ApplyOutcome::NotFound(n),
        Err(StoreError::AlreadyExists(n)) => ApplyOutcome::AlreadyExists(n),
        Err(StoreError::Conflict { current, provided }) => {
            ApplyOutcome::Conflict { current, provided }
        }
        Err(StoreError::Sled(e)) => ApplyOutcome::Internal(e.to_string()),
        Err(StoreError::Json(e)) => ApplyOutcome::Internal(e.to_string()),
    }
}

fn set_pod_status(pod: &mut Pod, status: Value) {
    if let Ok(s) = serde_json::from_value(status) {
        pod.status = Some(s);
    }
}
fn set_rs_status(rs: &mut ReplicaSet, status: Value) {
    if let Ok(s) = serde_json::from_value(status) {
        rs.status = Some(s);
    }
}
fn set_node_status(node: &mut Node, status: Value) {
    if let Ok(s) = serde_json::from_value(status) {
        node.status = Some(s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    /// A fresh Applier over five temp stores sharing ONE sled db (so the global
    /// rv counter is shared, exactly like a real replica).
    fn applier() -> Applier {
        let db = sled::Config::default().temporary(true).open().unwrap();
        Applier {
            pods: Arc::new(PodStore::from_db(db.clone()).unwrap()),
            replicasets: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            nodes: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            services: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            endpoints: Arc::new(ResourceStore::from_db(db).unwrap()),
        }
    }

    fn cmd(kind: &str, op: Op) -> StoreCommand {
        StoreCommand {
            id: Uuid::nil(),
            kind: kind.into(),
            op,
        }
    }

    /// A fully pre-stamped pod (as the leader would build it before proposing).
    fn pod_obj(name: &str) -> Value {
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": name,
                "uid": format!("uid-{name}"),
                "creationTimestamp": "2026-06-12T10:00:00Z"
            },
            "spec": { "containers": [], "nodeName": null }
        })
    }

    /// Dump every pod as (name, rv, uid, json) — the comparison key for the
    /// determinism test.
    fn dump_pods(a: &Applier) -> Vec<(String, Option<String>, Option<String>)> {
        let (mut pods, _) = a.pods.list().unwrap();
        pods.sort_by(|x, y| x.metadata.name.cmp(&y.metadata.name));
        pods.into_iter()
            .map(|p| (p.metadata.name, p.metadata.resource_version, p.metadata.uid))
            .collect()
    }

    #[test]
    fn create_then_get_via_store() {
        let a = applier();
        let out = a.apply(cmd("pods/", Op::Create { obj: pod_obj("web") }));
        match out {
            ApplyOutcome::Ok(v) => {
                // uid is the leader-stamped one (NOT regenerated), rv assigned.
                assert_eq!(v["metadata"]["uid"], "uid-web");
                assert_eq!(v["metadata"]["resourceVersion"], "1");
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        assert!(a.pods.get("web").unwrap().is_some());
    }

    #[test]
    fn duplicate_create_is_already_exists() {
        let a = applier();
        a.apply(cmd("pods/", Op::Create { obj: pod_obj("web") }));
        let out = a.apply(cmd("pods/", Op::Create { obj: pod_obj("web") }));
        assert_eq!(out, ApplyOutcome::AlreadyExists("web".into()));
    }

    #[test]
    fn delete_with_wrong_rv_is_conflict() {
        let a = applier();
        a.apply(cmd("pods/", Op::Create { obj: pod_obj("web") })); // rv=1
        let out = a.apply(cmd(
            "pods/",
            Op::Delete {
                name: "web".into(),
                rv: "999".into(),
            },
        ));
        assert!(matches!(out, ApplyOutcome::Conflict { .. }));
    }

    #[test]
    fn delete_missing_is_not_found() {
        let a = applier();
        let out = a.apply(cmd(
            "pods/",
            Op::Delete {
                name: "ghost".into(),
                rv: "1".into(),
            },
        ));
        assert_eq!(out, ApplyOutcome::NotFound("ghost".into()));
    }

    #[test]
    fn replace_status_assigns_typed_status_and_bumps_rv() {
        let a = applier();
        a.apply(cmd("pods/", Op::Create { obj: pod_obj("web") })); // rv=1
        let out = a.apply(cmd(
            "pods/",
            Op::ReplaceStatus {
                name: "web".into(),
                rv: "1".into(),
                status: json!({ "phase": "Running", "containerStatuses": [] }),
            },
        ));
        match out {
            ApplyOutcome::Ok(v) => {
                assert_eq!(v["status"]["phase"], "Running");
                assert_eq!(v["metadata"]["resourceVersion"], "2"); // bumped
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn unknown_kind_is_internal() {
        let a = applier();
        let out = a.apply(cmd("widgets/", Op::Create { obj: json!({}) }));
        assert!(matches!(out, ApplyOutcome::Internal(_)));
    }

    /// THE determinism test: two INDEPENDENT appliers (think: two replicas) fed
    /// the EXACT same command stream must end byte-identical — same objects,
    /// same resourceVersions, same uids. This is the property the whole phase
    /// rests on; if any clock/RNG leaked into apply, this fails.
    #[test]
    fn two_appliers_same_stream_converge_identically() {
        let stream = vec![
            cmd("pods/", Op::Create { obj: pod_obj("a") }),
            cmd("pods/", Op::Create { obj: pod_obj("b") }),
            cmd(
                "pods/",
                Op::ReplaceStatus {
                    name: "a".into(),
                    rv: "1".into(), // a got rv=1, b got rv=2
                    status: json!({ "phase": "Running", "containerStatuses": [] }),
                },
            ),
            cmd("pods/", Op::Create { obj: pod_obj("c") }),
            cmd(
                "pods/",
                Op::Delete {
                    name: "b".into(),
                    rv: "2".into(),
                },
            ),
        ];

        let replica_a = applier();
        let replica_b = applier();
        // Two separate stores, same input sequence, same order.
        for c in &stream {
            let oa = replica_a.apply(c.clone());
            let ob = replica_b.apply(c.clone());
            assert_eq!(oa, ob, "outcomes diverged on {:?}", c.op);
        }

        // Full state dumps must be byte-identical (incl rv + uid).
        assert_eq!(dump_pods(&replica_a), dump_pods(&replica_b));
        // And concretely: a and c survive, b was deleted, rvs are deterministic.
        let dump = dump_pods(&replica_a);
        let names: Vec<&str> = dump.iter().map(|(n, _, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "c"]);
    }
}
