//! Pod schema and YAML parsing.
//!
//! This is a deliberately tiny subset of the real Kubernetes Pod schema —
//! enough to demonstrate the orchestration patterns in Phase 1. We DO model
//! fields like `image` that we don't yet implement, so the schema is
//! forward-compatible.

use serde::{Deserialize, Serialize};

pub type PodName = String;
pub type ContainerName = String;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    pub api_version: String,
    pub kind: String,
    pub metadata: PodMetadata,
    pub spec: PodSpec,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PodMetadata {
    pub name: PodName,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PodSpec {
    pub containers: Vec<Container>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Container {
    pub name: ContainerName,
    /// Parsed but ignored in Phase 1 — every container runs from the shared
    /// busybox rootfs. Will gain real semantics when image-pull lands.
    pub image: String,
    /// ENTRYPOINT-style command vector. First element is the binary, rest are args.
    pub command: Vec<String>,
}

impl Pod {
    /// Parse a Pod from a YAML string.
    pub fn from_yaml(s: &str) -> Result<Self, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_container_pod() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: web
spec:
  containers:
    - name: server
      image: busybox
      command: ["httpd", "-f", "-p", "8080"]
"#;
        let pod = Pod::from_yaml(yaml).expect("valid YAML should parse");

        assert_eq!(pod.api_version, "v1");
        assert_eq!(pod.kind, "Pod");
        assert_eq!(pod.metadata.name, "web");
        assert_eq!(pod.spec.containers.len(), 1);
        assert_eq!(pod.spec.containers[0].name, "server");
        assert_eq!(pod.spec.containers[0].image, "busybox");
        assert_eq!(
            pod.spec.containers[0].command,
            vec!["httpd", "-f", "-p", "8080"],
        );
    }

    #[test]
    fn parses_multi_container_pod() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: app
spec:
  containers:
    - name: server
      image: busybox
      command: ["httpd", "-f", "-p", "8080"]
    - name: log-tail
      image: busybox
      command: ["sh", "-c", "while true; do echo tick; sleep 5; done"]
"#;
        let pod = Pod::from_yaml(yaml).expect("valid YAML should parse");
        assert_eq!(pod.spec.containers.len(), 2);

        let names: Vec<&str> = pod
            .spec
            .containers
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["server", "log-tail"]);
    }

    #[test]
    fn rejects_garbage_input() {
        let yaml = "this is not a Pod, just a bare string";
        let err = Pod::from_yaml(yaml).expect_err("garbage should fail to parse");
        // The exact error message is serde_yaml_ng's; we just want to confirm we got an Err.
        let _ = err.to_string();
    }
}
