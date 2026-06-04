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
