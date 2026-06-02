//! Object metadata shared by every API resource (Pod, ReplicaSet, ...).
//!
//! In real Kubernetes this is `metav1.ObjectMeta` — the common envelope every
//! object carries regardless of kind. Factoring it out here lets a single
//! generic store and a single set of metadata-handling rules serve all types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_timestamp: Option<String>,
    /// Key/value tags used by selectors (e.g. a ReplicaSet finds its Pods by
    /// matching labels). BTreeMap (not HashMap) for deterministic serialization.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Links this object to the controller(s) that own it. A ReplicaSet stamps
    /// one of these (with `controller: true`) onto each Pod it creates, which is
    /// how cascade-deletion and "is this Pod mine?" queries work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owner_references: Vec<OwnerReference>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
    /// True for the single managing controller (a Pod has at most one). K8s
    /// also has a `blockOwnerDeletion` field; we omit it (no finalizers yet).
    pub controller: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Empty labels + owner_references must be ABSENT from the JSON (not `{}`
    /// / `[]`) thanks to skip_serializing_if. This keeps old stored objects and
    /// the common no-labels case clean, and makes the field addition backward
    /// compatible with JSON written before these fields existed.
    #[test]
    fn empty_collections_are_omitted_from_json() {
        let meta = ObjectMeta {
            name: "web".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(!json.contains("labels"), "got: {json}");
        assert!(!json.contains("ownerReferences"), "got: {json}");
        // And JSON lacking those keys still deserializes (serde default).
        let parsed: ObjectMeta = serde_json::from_str(r#"{"name":"web"}"#).unwrap();
        assert_eq!(parsed, meta);
    }

    /// labels serialize as a camelCase-free map (keys are user data, not field
    /// names) and owner_references uses camelCase field keys.
    #[test]
    fn labels_and_owner_references_roundtrip() {
        let mut meta = ObjectMeta {
            name: "web-abc12".into(),
            ..Default::default()
        };
        meta.labels.insert("app".into(), "web".into());
        meta.labels.insert("tier".into(), "frontend".into());
        meta.owner_references.push(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "web".into(),
            uid: "rs-uid-1".into(),
            controller: true,
        });

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains(r#""labels":{"app":"web","tier":"frontend"}"#), "got: {json}");
        assert!(json.contains(r#""ownerReferences""#), "got: {json}");
        assert!(json.contains(r#""apiVersion":"apps/v1""#), "got: {json}");
        assert!(json.contains(r#""controller":true"#), "got: {json}");

        let parsed: ObjectMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, meta);
    }

    /// BTreeMap guarantees deterministic key order — the same labels always
    /// serialize to the same bytes, regardless of insertion order.
    #[test]
    fn labels_serialize_in_deterministic_order() {
        let mut a = ObjectMeta { name: "x".into(), ..Default::default() };
        a.labels.insert("z".into(), "1".into());
        a.labels.insert("a".into(), "2".into());

        let mut b = ObjectMeta { name: "x".into(), ..Default::default() };
        b.labels.insert("a".into(), "2".into());
        b.labels.insert("z".into(), "1".into());

        // Inserted in opposite orders, yet serialize identically (a before z).
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap(),
        );
    }
}
