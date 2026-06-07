//! Pod schema and (de)serialization.
//!
//! A deliberately tiny subset of the real Kubernetes Pod schema. Phase 1
//! used only `spec`; Phase 2 added `status` + apiserver-managed metadata so
//! the type round-trips through the HTTP API. We model fields we don't yet
//! implement (e.g. `image`) to keep the wire format forward-compatible.

use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ResourceMeta};

pub type PodName = String;
pub type ContainerName = String;

/// The top-level resource. The `spec`/`status` split is fundamental to K8s:
/// `spec` is *desired* state (written by clients), `status` is *observed*
/// state (written by the kubelet, see reconciler `push_status`).
///
/// `#[serde(rename_all = "camelCase")]` maps every Rust `snake_case` field to
/// K8s's `camelCase` wire keys (`api_version` ⇄ `apiVersion`) in one line, so
/// snippets copy-paste straight from K8s docs.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    pub api_version: String,
    pub kind: String,
    pub metadata: PodMetadata,
    pub spec: PodSpec,
    // `skip_serializing_if`: a spec-only Pod (just submitted, no status yet)
    // serializes with NO `status` key at all, rather than `"status": null`.
    // `default`: deserializing a Pod that lacks the key yields `None`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status: Option<PodStatus>,
}

impl ResourceMeta for Pod {
    const KIND_PREFIX: &'static str = "pods/";
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

/// Pod metadata is just the shared ObjectMeta. The alias keeps every existing
/// `PodMetadata { .. }` call site compiling while unifying the type across
/// resources (so the generic store in step 2 can treat them uniformly).
pub type PodMetadata = ObjectMeta;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    pub containers: Vec<Container>,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node_name: Option<String>,
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

/// Observed state, reported by the kubelet. `observed_generation` echoes the
/// `metadata.generation` this status reflects — that's how a reader tells
/// "has the kubelet acted on my latest spec edit yet?"
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodStatus {
    pub phase: PodPhase,
    pub container_statuses: Vec<ContainerStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<u64>,
    // The pod's IPv4 address, assigned by the kubelet's IPAM when the sandbox
    // network comes up; `None` until then. Lives in STATUS (observed), not spec:
    // the kubelet discovers it, the apiserver just stores what it reports.
    // `rename = "podIP"`: camelCase would emit `podIp`, but real K8s keeps the
    // initialism fully capitalized.
    #[serde(rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,
}

/// NOTE: deliberately NO `#[serde(rename_all)]`. K8s phases are PascalCase on
/// the wire (`"Running"`, not `"running"`); the unit variants already match,
/// so adding a rename would silently break compatibility. `#[default]` (a
/// derived-Default enum picks one variant) makes `Pending` the zero value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
pub enum PodPhase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatus {
    pub name: ContainerName,
    pub ready: bool,
    pub restart_count: u32,
    pub state: ContainerStatusState,
}

/// A data-carrying enum that leans on serde's DEFAULT "externally tagged"
/// representation to match K8s's container-state wire shape exactly:
///   - unit variant   → bare string:        `"waiting"`
///   - struct variant → single-key object:  `{"running": {"startedAt": "..."}}`
///
/// `rename_all` lowercases the variant tags; `rename_all_fields` camelCases the
/// inner fields (`started_at` → `startedAt`). No hand-written (de)serializer.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ContainerStatusState {
    Waiting,
    Running { started_at: String },
    Terminated { exit_code: i32 },
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

    /// K8s wire format uses PascalCase pod phases ("Pending", "Running", ...).
    /// Guards against accidentally adding `#[serde(rename_all = "camelCase")]`
    /// to PodPhase, which would lowercase them and silently break compatibility.
    #[test]
    fn pod_phase_serializes_as_pascalcase() {
        let json = serde_json::to_string(&PodPhase::Running).unwrap();
        assert_eq!(json, r#""Running""#);
        let parsed: PodPhase = serde_json::from_str(r#""Pending""#).unwrap();
        assert_eq!(parsed, PodPhase::Pending);
    }

    /// External tagging (serde default for enums): unit variants become bare
    /// strings, struct variants become single-key objects whose key is the
    /// (camelCased) variant name. Mirrors K8s's container state wire shape.
    #[test]
    fn container_status_state_uses_external_tagging() {
        let waiting = serde_json::to_string(&ContainerStatusState::Waiting).unwrap();
        assert_eq!(waiting, r#""waiting""#);

        let running = ContainerStatusState::Running {
            started_at: "2026-05-17T10:00:00Z".into(),
        };
        let running_json = serde_json::to_string(&running).unwrap();
        assert_eq!(
            running_json,
            r#"{"running":{"startedAt":"2026-05-17T10:00:00Z"}}"#,
        );

        let terminated = ContainerStatusState::Terminated { exit_code: 137 };
        let terminated_json = serde_json::to_string(&terminated).unwrap();
        assert_eq!(terminated_json, r#"{"terminated":{"exitCode":137}}"#);

        // Round-trip: parsed JSON equals the original value.
        let parsed: ContainerStatusState = serde_json::from_str(&running_json).unwrap();
        assert_eq!(parsed, running);
    }

    /// All four new apiserver-managed fields round-trip, and their JSON keys
    /// are camelCase (resourceVersion, creationTimestamp), not snake_case.
    /// Locks in the K8s wire convention.
    #[test]
    fn pod_metadata_roundtrips_with_apiserver_fields_camelcase() {
        let metadata = PodMetadata {
            name: "web".into(),
            uid: Some("550e8400-e29b-41d4-a716-446655440000".into()),
            resource_version: Some("42".into()),
            generation: Some(3),
            creation_timestamp: Some("2026-05-17T10:00:00Z".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains(r#""resourceVersion":"42""#), "got: {json}");
        assert!(
            json.contains(r#""creationTimestamp":"2026-05-17T10:00:00Z""#),
            "got: {json}",
        );
        assert!(json.contains(r#""uid":"550e"#), "got: {json}");

        let parsed: PodMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, metadata);
    }

    /// A Pod that came back from the apiserver (status populated) must
    /// round-trip through serde. Also confirms `skip_serializing_if` does NOT
    /// drop a present-but-default `phase: Pending` (defaults are real values,
    /// not "missing" — only None on Options gets skipped).
    #[test]
    fn pod_roundtrips_with_status_present() {
        let pod = Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: PodMetadata {
                name: "web".into(),
                resource_version: Some("17".into()),
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "server".into(),
                    image: "busybox".into(),
                    command: vec!["httpd".into()],
                }],
                node_name: None,
            },
            status: Some(PodStatus {
                phase: PodPhase::Running,
                container_statuses: vec![ContainerStatus {
                    name: "server".into(),
                    ready: true,
                    restart_count: 0,
                    state: ContainerStatusState::Running {
                        started_at: "2026-05-17T10:00:00Z".into(),
                    },
                }],
                observed_generation: Some(1),
                pod_ip: Some("10.244.1.5".into()),
            }),
        };

        let json = serde_json::to_string(&pod).unwrap();
        let parsed: Pod = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, pod);

        // JSON should carry both spec and status fields.
        assert!(json.contains(r#""status""#));
        assert!(json.contains(r#""phase":"Running""#));
        assert!(json.contains(r#""containerStatuses""#));
    }

    /// `pod_ip` uses the real K8s wire key `podIP` (NOT camelCase `podIp`), and
    /// is omitted entirely when None rather than serialized as `null`.
    #[test]
    fn pod_status_pod_ip_uses_podip_wire_key_and_skips_none() {
        let with_ip = PodStatus {
            phase: PodPhase::Running,
            container_statuses: vec![],
            observed_generation: None,
            pod_ip: Some("10.244.2.7".into()),
        };
        let json = serde_json::to_string(&with_ip).unwrap();
        assert!(json.contains(r#""podIP":"10.244.2.7""#), "got: {json}");
        assert!(!json.contains("podIp"), "must not camelCase to podIp: {json}");

        let parsed: PodStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, with_ip);

        // None → key omitted, not `null`.
        let no_ip = PodStatus::default();
        let json = serde_json::to_string(&no_ip).unwrap();
        assert!(!json.contains("podIP"), "None should omit the key: {json}");
        let parsed: PodStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.pod_ip, None);
    }
}
