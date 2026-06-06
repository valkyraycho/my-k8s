//! The ReplicaSet controller's reconcile function: drive the actual number of
//! Pods toward `spec.replicas`. Level-triggered — it re-reads full state from
//! the apiserver every call and converges, so it's safe to run on any trigger
//! (watch event, resync, retry) and idempotent across repeats.

use std::collections::BTreeMap;

use anyhow::Result;
use tracing::{info, warn};
use uuid::Uuid;

use crate::client::Client;
use crate::meta::{ObjectMeta, OwnerReference};
use crate::pod::{Pod, PodPhase};
use crate::replicaset::{LabelSelector, ReplicaSet, ReplicaSetStatus};

/// Does this label set satisfy the selector? Every `matchLabels` entry must be
/// present with the exact value. An EMPTY selector matches everything (the
/// vacuous `all()` over zero predicates) — which is why the apiserver rejects
/// empty selectors at create time.
pub fn matches_selector(labels: &BTreeMap<String, String>, selector: &LabelSelector) -> bool {
    selector
        .match_labels
        .iter()
        .all(|(k, v)| labels.get(k) == Some(v))
}

/// Which ReplicaSet (if any) controls this Pod, per its ownerReferences. Used
/// by the controller-manager to map a Pod event back to the RS key to enqueue.
pub fn rs_key_for_pod(pod: &Pod) -> Option<String> {
    pod.metadata
        .owner_references
        .iter()
        .find(|o| o.kind == "ReplicaSet" && o.controller)
        .map(|o| o.name.clone())
}

/// The reconcile entry point, keyed by ReplicaSet name.
pub async fn reconcile(rs_name: &str, client: &Client) -> Result<()> {
    let rs = match client.get_replicaset(rs_name).await? {
        Some(rs) => rs,
        None => {
            // RS is gone → cascade-delete the Pods it owned.
            return cascade_delete(rs_name, client).await;
        }
    };

    // Gather the Pods this RS owns, adopting matching orphans along the way.
    let all_pods = client.list_pods().await?;
    let mut owned: Vec<Pod> = Vec::new();
    for pod in all_pods {
        if is_owned_by(&pod, rs_name) {
            owned.push(pod);
        } else if can_adopt(&pod, &rs) {
            match adopt(&pod, &rs, client).await {
                Ok(updated) => {
                    info!(pod = %updated.metadata.name, rs = %rs_name, "adopted 
  orphan pod");
                    owned.push(updated);
                }
                Err(e) => warn!(error = ?e, pod = %pod.metadata.name, "adoption 
  failed"),
            }
        }
    }

    let desired = rs.spec.replicas as usize;
    if owned.len() < desired {
        for _ in 0..(desired - owned.len()) {
            let pod = pod_from_template(&rs);
            match client.create_pod(&pod).await {
                Ok(p) => info!(pod = %p.metadata.name, rs = %rs_name, "created 
  pod"),
                Err(e) => warn!(error = ?e, "create pod failed"),
            }
        }
    } else if owned.len() > desired {
        // Scale down: delete the oldest Pods first (stable, deterministic).
        owned.sort_by(|a, b| {
            a.metadata
                .creation_timestamp
                .cmp(&b.metadata.creation_timestamp)
        });
        let surplus = owned.len() - desired;
        for pod in owned.iter().take(surplus) {
            if let Some(rv) = pod.metadata.resource_version.clone() {
                match client.delete_pod(&pod.metadata.name, &rv).await {
                    Ok(()) => info!(pod = %pod.metadata.name, rs = %rs_name,
  "deleted surplus pod"),
                    Err(e) => warn!(error = ?e, "delete surplus pod failed"),
                }
            }
        }
    }

    update_status(rs_name, &rs, client).await
}

async fn cascade_delete(rs_name: &str, client: &Client) -> Result<()> {
    let pods = client.list_pods().await?;
    for pod in pods {
        if is_owned_by(&pod, rs_name)
            && let Some(rv) = pod.metadata.resource_version.clone()
        {
            let _ = client.delete_pod(&pod.metadata.name, &rv).await;
            info!(pod = %pod.metadata.name, rs = %rs_name, "cascade-deleted 
  pod");
        }
    }
    Ok(())
}

/// Recompute status from current reality and PUT it — but only if it CHANGED.
/// The dedup is essential: a status PUT generates an RS MODIFIED watch event
/// that re-enqueues this RS; without the equality guard we'd PUT → event →
/// reconcile → PUT → ... forever.
async fn update_status(rs_name: &str, rs: &ReplicaSet, client: &Client) -> Result<()> {
    let pods = client.list_pods().await?;
    let owned: Vec<Pod> = pods
        .into_iter()
        .filter(|p| is_owned_by(p, rs_name))
        .collect();
    let new_status = ReplicaSetStatus {
        replicas: owned.len() as u32,
        ready_replicas: owned.iter().filter(|p| is_ready(p)).count() as u32,
        observed_generation: rs.metadata.generation.unwrap_or(0),
    };

    if rs.status.as_ref() != Some(&new_status)
        && let Some(rv) = rs.metadata.resource_version.clone()
    {
        client.replace_rs_status(rs_name, &new_status, &rv).await?;
    }
    Ok(())
}

fn is_owned_by(pod: &Pod, rs_name: &str) -> bool {
    pod.metadata
        .owner_references
        .iter()
        .any(|o| o.kind == "ReplicaSet" && o.controller && o.name == rs_name)
}

/// A Pod is adoptable if it matches our selector AND has no controller owner
/// yet. (A Pod controlled by some OTHER RS is off-limits.)
fn can_adopt(pod: &Pod, rs: &ReplicaSet) -> bool {
    let has_controller = pod.metadata.owner_references.iter().any(|o| o.controller);
    !has_controller && matches_selector(&pod.metadata.labels, &rs.spec.selector)
}

async fn adopt(pod: &Pod, rs: &ReplicaSet, client: &Client) -> Result<Pod> {
    let mut adopted = pod.clone();
    adopted.metadata.owner_references.push(owner_ref(rs));
    Ok(client.replace_pod_spec(&adopted).await?)
}

fn owner_ref(rs: &ReplicaSet) -> OwnerReference {
    OwnerReference {
        api_version: "apps/v1".into(),
        kind: "ReplicaSet".into(),
        name: rs.metadata.name.clone(),
        uid: rs.metadata.uid.clone().unwrap_or_default(),
        controller: true,
    }
}

fn pod_from_template(rs: &ReplicaSet) -> Pod {
    // Random 5-char suffix → unique pod name, like K8s's generateName.
    let suffix = Uuid::new_v4().simple().to_string()[..5].to_string();
    Pod {
        api_version: "v1".into(),
        kind: "Pod".into(),
        metadata: ObjectMeta {
            name: format!("{}-{}", rs.metadata.name, suffix),
            labels: rs.spec.template.metadata.labels.clone(),
            owner_references: vec![owner_ref(rs)],
            ..Default::default()
        },
        spec: rs.spec.template.spec.clone(),
        status: None,
    }
}

fn is_ready(pod: &Pod) -> bool {
    pod.status.as_ref().is_some_and(|s| {
        s.phase == PodPhase::Running
            && !s.container_statuses.is_empty()
            && s.container_statuses.iter().all(|c| c.ready)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore},
    };
    use crate::pod::{Container, PodSpec};
    use crate::replicaset::{PodTemplateSpec, ReplicaSetSpec, TemplateObjectMeta};

    // ---- matches_selector: pure, table-driven ----

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn selector(pairs: &[(&str, &str)]) -> LabelSelector {
        LabelSelector {
            match_labels: labels(pairs),
        }
    }

    #[test]
    fn selector_matching() {
        // Empty selector matches everything (vacuous all()).
        assert!(matches_selector(&labels(&[]), &selector(&[])));
        assert!(matches_selector(&labels(&[("app", "web")]), &selector(&[])));

        // Subset match: pod has the required label (plus extras) → match.
        assert!(matches_selector(
            &labels(&[("app", "web"), ("tier", "fe")]),
            &selector(&[("app", "web")]),
        ));

        // Missing required label → no match.
        assert!(!matches_selector(
            &labels(&[("tier", "fe")]),
            &selector(&[("app", "web")]),
        ));

        // Wrong value → no match.
        assert!(!matches_selector(
            &labels(&[("app", "db")]),
            &selector(&[("app", "web")]),
        ));

        // Multiple required: ALL must match.
        assert!(matches_selector(
            &labels(&[("app", "web"), ("tier", "fe")]),
            &selector(&[("app", "web"), ("tier", "fe")]),
        ));
        assert!(!matches_selector(
            &labels(&[("app", "web")]),
            &selector(&[("app", "web"), ("tier", "fe")]),
        ));
    }

    #[test]
    fn rs_key_for_pod_reads_controller_owner() {
        let mut pod = make_bare_pod("web-x");
        assert_eq!(rs_key_for_pod(&pod), None);

        pod.metadata.owner_references.push(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "web".into(),
            uid: "u1".into(),
            controller: true,
        });
        assert_eq!(rs_key_for_pod(&pod), Some("web".into()));
    }

    // ---- reconcile: against an in-process apiserver ----

    async fn spawn_apiserver() -> Client {
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
        Client::new(format!("http://{addr}"))
    }

    fn make_rs(name: &str, replicas: u32) -> ReplicaSet {
        ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas,
                selector: selector(&[("app", name)]),
                template: PodTemplateSpec {
                    metadata: TemplateObjectMeta {
                        labels: labels(&[("app", name)]),
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

    fn make_bare_pod(name: &str) -> Pod {
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

    async fn owned_pod_count(client: &Client, rs_name: &str) -> usize {
        client
            .list_pods()
            .await
            .unwrap()
            .iter()
            .filter(|p| is_owned_by(p, rs_name))
            .count()
    }

    #[tokio::test]
    async fn reconcile_creates_deficit_pods() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 3)).await.unwrap();

        reconcile("web", &client).await.unwrap();

        let pods = client.list_pods().await.unwrap();
        assert_eq!(pods.len(), 3, "should create 3 pods to meet replicas");
        for p in &pods {
            assert!(is_owned_by(p, "web"), "pod {} missing ownerRef", p.metadata.name);
            assert_eq!(p.metadata.labels.get("app").map(String::as_str), Some("web"));
            assert!(p.metadata.name.starts_with("web-"));
        }
    }

    #[tokio::test]
    async fn reconcile_is_idempotent() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 2)).await.unwrap();

        reconcile("web", &client).await.unwrap();
        reconcile("web", &client).await.unwrap(); // must NOT over-create
        reconcile("web", &client).await.unwrap();

        assert_eq!(owned_pod_count(&client, "web").await, 2);
    }

    #[tokio::test]
    async fn reconcile_deletes_surplus_pods() {
        let client = spawn_apiserver().await;
        let rs = client.create_replicaset(&make_rs("web", 1)).await.unwrap();

        for i in 0..3 {
            let mut pod = make_bare_pod(&format!("web-pre{i}"));
            pod.metadata.labels = labels(&[("app", "web")]);
            pod.metadata.owner_references.push(OwnerReference {
                api_version: "apps/v1".into(),
                kind: "ReplicaSet".into(),
                name: "web".into(),
                uid: rs.metadata.uid.clone().unwrap(),
                controller: true,
            });
            client.create_pod(&pod).await.unwrap();
        }
        assert_eq!(owned_pod_count(&client, "web").await, 3);

        reconcile("web", &client).await.unwrap();

        assert_eq!(
            owned_pod_count(&client, "web").await,
            1,
            "should delete 2 surplus pods down to replicas=1",
        );
    }

    #[tokio::test]
    async fn reconcile_adopts_matching_orphan() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 1)).await.unwrap();

        let mut orphan = make_bare_pod("lonely");
        orphan.metadata.labels = labels(&[("app", "web")]);
        client.create_pod(&orphan).await.unwrap();

        reconcile("web", &client).await.unwrap();

        let pods = client.list_pods().await.unwrap();
        assert_eq!(pods.len(), 1, "orphan adopted, no extra pod created");
        assert!(is_owned_by(&pods[0], "web"));
        assert_eq!(pods[0].metadata.name, "lonely");
    }

    #[tokio::test]
    async fn reconcile_does_not_adopt_pod_owned_by_another_rs() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 1)).await.unwrap();

        let mut taken = make_bare_pod("taken");
        taken.metadata.labels = labels(&[("app", "web")]);
        taken.metadata.owner_references.push(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "other".into(),
            uid: "other-uid".into(),
            controller: true,
        });
        client.create_pod(&taken).await.unwrap();

        reconcile("web", &client).await.unwrap();

        assert_eq!(owned_pod_count(&client, "web").await, 1);
        let taken_now = client.get_pod("taken").await.unwrap().unwrap();
        assert!(!is_owned_by(&taken_now, "web"), "must not steal another RS's pod");
    }

    #[tokio::test]
    async fn reconcile_cascade_deletes_when_rs_gone() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 2)).await.unwrap();
        reconcile("web", &client).await.unwrap();
        assert_eq!(owned_pod_count(&client, "web").await, 2);

        let current = client.get_replicaset("web").await.unwrap().unwrap();
        let rv = current.metadata.resource_version.clone().unwrap();
        client.delete_replicaset("web", &rv).await.unwrap();

        reconcile("web", &client).await.unwrap();

        assert_eq!(
            owned_pod_count(&client, "web").await,
            0,
            "cascade delete should remove all owned pods",
        );
    }

    #[tokio::test]
    async fn reconcile_updates_status_replica_count() {
        let client = spawn_apiserver().await;
        client.create_replicaset(&make_rs("web", 2)).await.unwrap();

        reconcile("web", &client).await.unwrap();

        let rs = client.get_replicaset("web").await.unwrap().unwrap();
        let status = rs.status.expect("status should be set");
        assert_eq!(status.replicas, 2, "status.replicas reflects owned pods");
        assert_eq!(status.ready_replicas, 0, "no kubelet → none ready");
    }
}
