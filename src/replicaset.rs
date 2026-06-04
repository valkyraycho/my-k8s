//! ReplicaSet: the first higher-order resource. It declares "I want N Pods
//! matching this selector" and a controller makes reality match.

use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ResourceMeta};
use crate::pod::PodSpec;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSet {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: ReplicaSetSpec,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status: Option<ReplicaSetStatus>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetSpec {
    pub replicas: u32,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub match_labels: BTreeMap<String, String>,
}

/// The Pod blueprint a ReplicaSet stamps out. Note it carries ONLY labels in
/// its metadata — no name/uid/resourceVersion, since those are per-instance and
/// assigned when each Pod is actually created.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PodTemplateSpec {
    pub metadata: TemplateObjectMeta,
    pub spec: PodSpec,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct TemplateObjectMeta {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetStatus {
    pub replicas: u32,
    pub ready_replicas: u32,
    pub observed_generation: u64,
}

impl ResourceMeta for ReplicaSet {
    const KIND_PREFIX: &'static str = "replicasets/";
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

    /// A realistic ReplicaSet manifest (as a user would `apply`) parses into
    /// the full type graph: spec.replicas, selector.matchLabels, and a Pod
    /// template carrying labels + containers.
    #[test]
    fn parses_replicaset_manifest() {
        let yaml = r#"
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: web
spec:
  replicas: 3
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: server
          image: busybox
          command: ["httpd", "-f", "-p", "8080"]
"#;
        let rs: ReplicaSet = serde_yaml_ng::from_str(yaml).expect("valid RS YAML");
        assert_eq!(rs.metadata.name, "web");
        assert_eq!(rs.spec.replicas, 3);
        assert_eq!(
            rs.spec.selector.match_labels.get("app").map(String::as_str),
            Some("web")
        );
        assert_eq!(
            rs.spec
                .template
                .metadata
                .labels
                .get("app")
                .map(String::as_str),
            Some("web")
        );
        assert_eq!(rs.spec.template.spec.containers.len(), 1);
        assert_eq!(rs.spec.template.spec.containers[0].name, "server");
        // No status on a freshly-applied manifest.
        assert!(rs.status.is_none());
    }

    /// camelCase wire convention holds across the RS type graph: matchLabels,
    /// readyReplicas, observedGeneration, apiVersion.
    #[test]
    fn replicaset_roundtrips_camelcase() {
        let mut selector = LabelSelector::default();
        selector.match_labels.insert("app".into(), "web".into());
        let mut tmpl_labels = TemplateObjectMeta::default();
        tmpl_labels.labels.insert("app".into(), "web".into());

        let rs = ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: ObjectMeta {
                name: "web".into(),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 2,
                selector,
                template: PodTemplateSpec {
                    metadata: tmpl_labels,
                    spec: PodSpec { containers: vec![] },
                },
            },
            status: Some(ReplicaSetStatus {
                replicas: 2,
                ready_replicas: 1,
                observed_generation: 5,
            }),
        };

        let json = serde_json::to_string(&rs).unwrap();
        assert!(json.contains(r#""matchLabels""#), "got: {json}");
        assert!(json.contains(r#""readyReplicas":1"#), "got: {json}");
        assert!(json.contains(r#""observedGeneration":5"#), "got: {json}");

        let parsed: ReplicaSet = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, rs);
    }
}
