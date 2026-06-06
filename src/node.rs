//! Node: a machine (logical, in our single-host dev setup) that can run pods.
//! Each kubelet self-registers a Node and heartbeats its status; the scheduler
//! only places pods onto Nodes that are Ready with a recent heartbeat.

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
}
