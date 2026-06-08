use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ResourceMeta};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    /// Pods whose labels match ALL of these are this Service's backends.
    /// `BTreeMap` (not HashMap) so serialization order is deterministic.
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
    /// The port the VIP listens on (what clients hit).
    pub port: u16,
    /// The port on the backing pod to forward to. Plain integer — our Container
    /// has no named ports.
    pub target_port: u16,
    /// The virtual IP, assigned by the apiserver on create from the service CIDR
    /// (10.96.0.0/16). `None` until assigned. `rename = "clusterIP"` keeps the
    /// K8s wire key (camelCase would give `clusterIp`).
    #[serde(rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,
}

impl ResourceMeta for Service {
    const KIND_PREFIX: &'static str = "services/";
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(name: &str) -> Service {
        let mut selector = BTreeMap::new();
        selector.insert("app".into(), "web".into());
        Service {
            api_version: "v1".into(),
            kind: "Service".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ServiceSpec {
                selector,
                port: 80,
                target_port: 8080,
                cluster_ip: Some("10.96.0.5".into()),
            },
        }
    }

    /// Round-trips through JSON with the K8s wire keys: `clusterIP` (NOT
    /// camelCase `clusterIp`) and `targetPort`. Selector survives intact.
    #[test]
    fn service_roundtrips_with_clusterip_wire_key() {
        let svc = service("web");
        let json = serde_json::to_string(&svc).unwrap();
        assert!(json.contains(r#""clusterIP":"10.96.0.5""#), "got: {json}");
        assert!(!json.contains("clusterIp"), "must not camelCase: {json}");
        assert!(json.contains(r#""targetPort":8080"#), "got: {json}");
        assert!(json.contains(r#""selector":{"app":"web"}"#), "got: {json}");

        let parsed: Service = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, svc);
    }

    /// An unassigned ClusterIP omits the key entirely (not `null`); a missing
    /// selector defaults to empty.
    #[test]
    fn service_omits_none_clusterip_and_defaults_selector() {
        let mut svc = service("web");
        svc.spec.cluster_ip = None;
        let json = serde_json::to_string(&svc).unwrap();
        assert!(!json.contains("clusterIP"), "None should omit key: {json}");

        // A minimal Service with no selector still parses (selector defaults).
        let minimal = r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"x"},"spec":{"port":80,"targetPort":8080}}"#;
        let parsed: Service = serde_json::from_str(minimal).unwrap();
        assert!(parsed.spec.selector.is_empty());
        assert_eq!(parsed.spec.cluster_ip, None);
    }
}
