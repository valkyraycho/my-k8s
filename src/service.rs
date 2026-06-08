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
