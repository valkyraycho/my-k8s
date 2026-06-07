//! Node: a machine (logical, in our single-host dev setup) that can run pods.
//! Each kubelet self-registers a Node and heartbeats its status; the scheduler
//! only places pods onto Nodes that are Ready with a recent heartbeat.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ResourceMeta};
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: NodeSpec,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status: Option<NodeStatus>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    /// If true, the scheduler skips this node (cordoned). Defaults false.
    #[serde(default)]
    pub unschedulable: bool,
    // The /24 slice of the cluster pod CIDR this node owns (e.g.
    // "10.244.1.0/24"), assigned by the apiserver on registration. The kubelet
    // allocates pod IPs from it. Lives in SPEC: it's an assigned intent the
    // kubelet must obey, not something it observes. `rename = "podCIDR"` keeps
    // the K8s wire key (camelCase would give `podCidr`).
    #[serde(rename = "podCIDR", skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,
}

/// Flat status — just enough for "is this node Ready and heartbeating recently?"
/// (Real K8s uses a `conditions: Vec<NodeCondition>` list; that adds no Phase-4
/// learning since we only ever ask the one Ready+freshness question.)
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    pub ready: bool,
    /// RFC3339 timestamp of the kubelet's last heartbeat. The scheduler treats
    /// the node as NotReady if this is older than the staleness window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_time: Option<String>,
}

impl Node {
    /// Effective readiness: the kubelet reported Ready AND its heartbeat is
    /// within `max_age_secs`. A dead kubelet leaves `status.ready == true`
    /// forever (nothing flips it), so freshness — not the bool alone — is what
    /// actually says the node is alive. Missing/unparseable timestamp → not ready.
    pub fn is_ready(&self, now: DateTime<Utc>, max_age_secs: i64) -> bool {
        let Some(status) = &self.status else {
            return false;
        };
        if !status.ready {
            return false;
        }
        match &status.last_heartbeat_time {
            Some(ts) => match DateTime::parse_from_rfc3339(ts) {
                Ok(hb) => (now - hb.with_timezone(&Utc)).num_seconds() < max_age_secs,
                Err(_) => false,
            },
            None => false,
        }
    }
}

impl ResourceMeta for Node {
    const KIND_PREFIX: &'static str = "nodes/";
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
        self.status = current.status.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> Node {
        Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: NodeSpec::default(),
            status: None,
        }
    }

    #[test]
    fn is_ready_requires_ready_bool_and_fresh_heartbeat() {
        let now = Utc::now();
        let window = 30;
        let fresh = now.to_rfc3339();
        let stale = (now - chrono::Duration::seconds(40)).to_rfc3339();

        let mut n = node("node-a");

        // No status → not ready.
        assert!(!n.is_ready(now, window));

        // Ready + fresh heartbeat → ready.
        n.status = Some(NodeStatus {
            ready: true,
            last_heartbeat_time: Some(fresh.clone()),
        });
        assert!(n.is_ready(now, window));

        // Ready bool true but heartbeat stale (a dead kubelet) → NOT ready.
        n.status = Some(NodeStatus {
            ready: true,
            last_heartbeat_time: Some(stale),
        });
        assert!(!n.is_ready(now, window));

        // ready=false even with a fresh heartbeat → not ready.
        n.status = Some(NodeStatus {
            ready: false,
            last_heartbeat_time: Some(fresh),
        });
        assert!(!n.is_ready(now, window));

        // Ready bool true but no heartbeat recorded → not ready.
        n.status = Some(NodeStatus {
            ready: true,
            last_heartbeat_time: None,
        });
        assert!(!n.is_ready(now, window));
    }

    /// Round-trips through JSON with camelCase status keys and a Ready+heartbeat
    /// status. `lastHeartbeatTime` (not `last_heartbeat_time`) is the wire key.
    #[test]
    fn node_roundtrips_with_status_camelcase() {
        let mut n = node("node-a");
        n.status = Some(NodeStatus {
            ready: true,
            last_heartbeat_time: Some("2026-06-04T10:00:00Z".into()),
        });

        let json = serde_json::to_string(&n).unwrap();
        assert!(json.contains(r#""ready":true"#), "got: {json}");
        assert!(
            json.contains(r#""lastHeartbeatTime":"2026-06-04T10:00:00Z""#),
            "got: {json}",
        );

        let parsed: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, n);
    }

    /// A status-less node (just registered, no heartbeat yet) and an absent
    /// heartbeat omit their keys entirely — not `null`.
    #[test]
    fn absent_status_and_heartbeat_are_omitted() {
        let json = serde_json::to_string(&node("node-a")).unwrap();
        assert!(!json.contains("status"), "got: {json}");

        let n_no_hb = Node {
            status: Some(NodeStatus {
                ready: false,
                last_heartbeat_time: None,
            }),
            ..node("node-a")
        };
        let json = serde_json::to_string(&n_no_hb).unwrap();
        assert!(!json.contains("lastHeartbeatTime"), "got: {json}");

        // unschedulable defaults false and round-trips.
        let parsed: Node = serde_json::from_str(&json).unwrap();
        assert!(!parsed.spec.unschedulable);
    }

    /// `pod_cidr` uses the real K8s wire key `podCIDR` (NOT camelCase
    /// `podCidr`), is omitted when None, and round-trips when present.
    #[test]
    fn node_spec_pod_cidr_uses_podcidr_wire_key_and_skips_none() {
        // Absent → key omitted (not `null`), parses back to None.
        let json = serde_json::to_string(&node("node-a")).unwrap();
        assert!(!json.contains("podCIDR"), "None should omit the key: {json}");

        let with_cidr = Node {
            spec: NodeSpec {
                unschedulable: false,
                pod_cidr: Some("10.244.1.0/24".into()),
            },
            ..node("node-a")
        };
        let json = serde_json::to_string(&with_cidr).unwrap();
        assert!(json.contains(r#""podCIDR":"10.244.1.0/24""#), "got: {json}");
        assert!(!json.contains("podCidr"), "must not camelCase: {json}");

        let parsed: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.spec.pod_cidr.as_deref(), Some("10.244.1.0/24"));
    }
}
