use std::pin::Pin;

use reqwest::StatusCode;

use serde::Serialize;
use tokio_stream::{Stream, StreamExt};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
};

use crate::{
    apiserver::watch::WatchEvent,
    node::NodeStatus,
    replicaset::{ReplicaSet, ReplicaSetStatus},
};
use crate::{
    node::Node,
    pod::{Pod, PodStatus},
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP {status}: {message}")]
    Http { status: u16, message: String },
    #[error("conflict: {message}")]
    Conflict { message: String },
    #[error("already exists")]
    AlreadyExists,
    #[error("not found")]
    NotFound,
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ClientError>;

pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches("/").to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn list_pods(&self) -> Result<Vec<Pod>> {
        self.list_resource("/api/v1/pods").await
    }

    /// Absence is `Ok(None)`, not an error — a missing Pod is a valid answer,
    /// so callers `match` on the Option instead of catching a NotFound error.
    pub async fn get_pod(&self, name: &str) -> Result<Option<Pod>> {
        self.get_resource(&format!("/api/v1/pods/{name}")).await
    }

    pub async fn create_pod(&self, pod: &Pod) -> Result<Pod> {
        self.create_resource("/api/v1/pods", pod).await
    }

    pub async fn replace_pod_spec(&self, pod: &Pod) -> Result<Pod> {
        self.put_resource(&format!("/api/v1/pods/{}", pod.metadata.name), pod)
            .await
    }

    pub async fn replace_pod_status(
        &self,
        name: &str,
        status: &PodStatus,
        rv: &str,
    ) -> Result<Pod> {
        self.put_resource(
            &format!("/api/v1/pods/{name}/status?resourceVersion={rv}"),
            status,
        )
        .await
    }

    pub async fn delete_pod(&self, name: &str, rv: &str) -> Result<()> {
        self.delete_resource(&format!("/api/v1/pods/{name}?resourceVersion={rv}"))
            .await
    }

    pub async fn watch_pods(
        &self,
        from_rv: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<Pod>>> + Send>>> {
        let path = match from_rv {
            Some(rv) => format!("/api/v1/pods?watch=true&resourceVersion={rv}"),
            None => "/api/v1/pods?watch=true".to_string(),
        };
        self.watch_resource(&path).await
    }

    pub async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
        self.list_resource(&format!(
            "/api/v1/pods?fieldSelector=spec.nodeName={node_name}"
        ))
        .await
    }

    pub async fn watch_pods_on_node(
        &self,
        node_name: &str,
        from_rv: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<Pod>>> + Send>>> {
        let path = match from_rv {
            Some(rv) => format!(
                "/api/v1/pods?watch=true&fieldSelector=spec.nodeName={node_name}&resourceVersion={rv}"
            ),
            None => format!("/api/v1/pods?watch=true&fieldSelector=spec.nodeName={node_name}"),
        };
        self.watch_resource(&path).await
    }

    pub async fn bind_pod(&self, pod_name: &str, node_name: &str) -> Result<Pod> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Binding {
            node_name: String,
        }

        self.create_resource(
            &format!("/api/v1/pods/{pod_name}/binding"),
            &Binding {
                node_name: node_name.to_string(),
            },
        )
        .await
    }

    pub async fn list_nodes(&self) -> Result<Vec<Node>> {
        self.list_resource("/api/v1/nodes").await
    }

    pub async fn get_node(&self, name: &str) -> Result<Option<Node>> {
        self.get_resource(&format!("/api/v1/nodes/{name}")).await
    }

    pub async fn create_node(&self, node: &Node) -> Result<Node> {
        self.create_resource("/api/v1/nodes", node).await
    }

    pub async fn replace_node_status(
        &self,
        name: &str,
        status: &NodeStatus,
        rv: &str,
    ) -> Result<Node> {
        self.put_resource(
            &format!("/api/v1/nodes/{name}/status?resourceVersion={rv}"),
            status,
        )
        .await
    }

    async fn list_resource<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        let res = self.http.get(self.url(path)).send().await?;
        let list: ListEnvelope<T> = parse_json(res).await?;
        Ok(list.items)
    }

    async fn get_resource<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let res = self.http.get(self.url(path)).send().await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(parse_json(res).await?))
    }

    async fn create_resource<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let res = self.http.post(self.url(path)).json(body).send().await?;
        parse_json(res).await
    }

    async fn put_resource<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let res = self.http.put(self.url(path)).json(body).send().await?;
        parse_json(res).await
    }

    async fn delete_resource(&self, path: &str) -> Result<()> {
        let res = self.http.delete(self.url(path)).send().await?;
        check_status(res).await?;
        Ok(())
    }

    async fn watch_resource<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        path: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<T>>> + Send>>> {
        let res = self.http.get(self.url(path)).send().await?;
        let res = check_status(res).await?;

        let bytes = res
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other));
        let reader = StreamReader::new(bytes);
        let lines = FramedRead::new(reader, LinesCodec::new());
        let events = lines.map(|line_res| -> Result<WatchEvent<T>> {
            let line =
                line_res.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            Ok(serde_json::from_str(&line)?)
        });
        Ok(Box::pin(events))
    }

    pub async fn list_replicasets(&self) -> Result<Vec<ReplicaSet>> {
        self.list_resource("/api/v1/replicasets").await
    }

    pub async fn get_replicaset(&self, name: &str) -> Result<Option<ReplicaSet>> {
        self.get_resource(&format!("/api/v1/replicasets/{name}"))
            .await
    }

    pub async fn create_replicaset(&self, rs: &ReplicaSet) -> Result<ReplicaSet> {
        self.create_resource("/api/v1/replicasets", rs).await
    }

    pub async fn replace_replicaset_spec(&self, rs: &ReplicaSet) -> Result<ReplicaSet> {
        self.put_resource(&format!("/api/v1/replicasets/{}", rs.metadata.name), rs)
            .await
    }

    pub async fn replace_rs_status(
        &self,
        name: &str,
        status: &ReplicaSetStatus,
        rv: &str,
    ) -> Result<ReplicaSet> {
        self.put_resource(
            &format!("/api/v1/replicasets/{name}/status?resourceVersion={rv}"),
            status,
        )
        .await
    }

    pub async fn delete_replicaset(&self, name: &str, rv: &str) -> Result<()> {
        self.delete_resource(&format!("/api/v1/replicasets/{name}?resourceVersion={rv}"))
            .await
    }

    pub async fn watch_replicasets(
        &self,
        from_rv: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<ReplicaSet>>> + Send>>> {
        let path = match from_rv {
            Some(rv) => format!("/api/v1/replicasets?watch=true&resourceVersion={rv}"),
            None => "/api/v1/replicasets?watch=true".to_string(),
        };
        self.watch_resource(&path).await
    }
}

#[derive(Debug, serde::Deserialize)]
struct ListEnvelope<T> {
    items: Vec<T>,
}

#[derive(Debug, serde::Deserialize)]
struct StatusEnvelope {
    code: u16,
    message: String,
    reason: Option<String>,
}

async fn check_status(res: reqwest::Response) -> Result<reqwest::Response> {
    let status = res.status();
    if status.is_success() {
        return Ok(res);
    }

    let bytes = res.bytes().await?;
    if let Ok(env) = serde_json::from_slice::<StatusEnvelope>(&bytes) {
        return Err(map_envelope(status, env));
    }

    Err(ClientError::Http {
        status: status.as_u16(),
        message: String::from_utf8_lossy(&bytes).to_string(),
    })
}

async fn parse_json<T: serde::de::DeserializeOwned>(res: reqwest::Response) -> Result<T> {
    let res = check_status(res).await?;
    let bytes = res.bytes().await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Translate the server's `Status` envelope back into a typed `ClientError`, so
/// callers (the reconciler's conflict-retry, mykubectl) match on Rust variants
/// instead of HTTP codes. Matching on the `(status, reason)` TUPLE distinguishes
/// the two 409s: `AlreadyExists` (duplicate create) vs `Conflict` (stale rv).
fn map_envelope(status: StatusCode, env: StatusEnvelope) -> ClientError {
    match (status, env.reason.as_deref()) {
        (StatusCode::NOT_FOUND, _) => ClientError::NotFound,
        (StatusCode::CONFLICT, Some("AlreadyExists")) => ClientError::AlreadyExists,
        (StatusCode::CONFLICT, _) => ClientError::Conflict {
            message: env.message,
        },
        _ => ClientError::Http {
            status: env.code,
            message: env.message,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore},
    };
    use crate::node::{Node, NodeSpec, NodeStatus};
    use crate::pod::{Container, Pod, PodMetadata, PodPhase, PodSpec};
    use crate::replicaset::ReplicaSet;

    /// Spin up a real apiserver router on an OS-assigned port and return a
    /// Client pointed at it, plus the shared pod store so tests can drive writes
    /// from "outside" the HTTP path (useful for watch tests).
    async fn spawn_test_apiserver() -> (Client, Arc<PodStore>) {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let store = Arc::new(PodStore::from_db(db.clone()).expect("pod store"));
        let rs_store =
            Arc::new(ResourceStore::<ReplicaSet>::from_db(db.clone()).expect("rs store"));
        let node_store =
            Arc::new(ResourceStore::<crate::node::Node>::from_db(db).expect("node store"));
        let app = router(AppState {
            store: store.clone(),
            rs_store,
            node_store,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = Client::new(format!("http://{addr}"));
        (client, store)
    }

    fn make_pod(name: &str) -> Pod {
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: PodMetadata {
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

    #[tokio::test]
    async fn create_then_list_roundtrip() {
        let (client, _) = spawn_test_apiserver().await;
        let pod = client.create_pod(&make_pod("web")).await.unwrap();
        assert_eq!(pod.metadata.name, "web");
        assert!(pod.metadata.uid.is_some(), "apiserver should assign uid");
        assert_eq!(pod.metadata.resource_version.as_deref(), Some("1"));

        let listed = client.list_pods().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].metadata.name, "web");
    }

    #[tokio::test]
    async fn get_missing_pod_returns_none_not_error() {
        let (client, _) = spawn_test_apiserver().await;
        let result = client.get_pod("nope").await.unwrap();
        assert!(result.is_none(), "missing pod must surface as Ok(None)");
    }

    #[tokio::test]
    async fn get_existing_pod_returns_some() {
        let (client, _) = spawn_test_apiserver().await;
        client.create_pod(&make_pod("web")).await.unwrap();
        let pod = client.get_pod("web").await.unwrap().expect("Some(pod)");
        assert_eq!(pod.metadata.name, "web");
    }

    #[tokio::test]
    async fn duplicate_create_maps_to_already_exists_variant() {
        let (client, _) = spawn_test_apiserver().await;
        client.create_pod(&make_pod("web")).await.unwrap();
        let err = client.create_pod(&make_pod("web")).await.unwrap_err();
        assert!(
            matches!(err, ClientError::AlreadyExists),
            "expected AlreadyExists variant, got: {err:?}",
        );
    }

    #[tokio::test]
    async fn stale_rv_put_maps_to_conflict_variant() {
        let (client, _) = spawn_test_apiserver().await;
        client.create_pod(&make_pod("web")).await.unwrap();
        let mut stale = make_pod("web");
        stale.metadata.resource_version = Some("999".into());
        let err = client.replace_pod_spec(&stale).await.unwrap_err();
        assert!(
            matches!(err, ClientError::Conflict { .. }),
            "expected Conflict variant, got: {err:?}",
        );
    }

    #[tokio::test]
    async fn delete_with_rv_removes_pod() {
        let (client, _) = spawn_test_apiserver().await;
        let pod = client.create_pod(&make_pod("web")).await.unwrap();
        let rv = pod.metadata.resource_version.unwrap();
        client.delete_pod("web", &rv).await.unwrap();
        assert!(client.get_pod("web").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn replace_status_persists_phase() {
        let (client, _) = spawn_test_apiserver().await;
        let pod = client.create_pod(&make_pod("web")).await.unwrap();
        let rv = pod.metadata.resource_version.unwrap();
        let status = PodStatus {
            phase: PodPhase::Running,
            container_statuses: vec![],
            observed_generation: Some(1),
        };
        let updated = client
            .replace_pod_status("web", &status, &rv)
            .await
            .unwrap();
        assert_eq!(updated.status.unwrap().phase, PodPhase::Running);
    }

    #[tokio::test]
    async fn watch_stream_receives_added_event() {
        let (client, store) = spawn_test_apiserver().await;
        let mut stream = client.watch_pods(Some("0")).await.unwrap();

        // Drive a write from the store side AFTER the watch is open. A small
        // delay gives the server's stream-body future a chance to subscribe
        // to the broadcast channel before the write fires.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            store.create(make_pod("web")).unwrap();
        });

        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for ADDED event")
            .expect("stream ended before event arrived")
            .expect("watch event was Err");

        assert_eq!(event.object.metadata.name, "web");
    }

    // ---- ReplicaSet client methods (over the generic helpers) ----

    fn make_rs(name: &str, replicas: u32) -> ReplicaSet {
        use crate::replicaset::{
            LabelSelector, PodTemplateSpec, ReplicaSetSpec, TemplateObjectMeta,
        };
        let mut selector = LabelSelector::default();
        selector.match_labels.insert("app".into(), name.into());
        let mut tmpl = TemplateObjectMeta::default();
        tmpl.labels.insert("app".into(), name.into());
        ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: PodMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas,
                selector,
                template: PodTemplateSpec {
                    metadata: tmpl,
                    spec: PodSpec {
                        containers: vec![],
                        node_name: None,
                    },
                },
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn rs_create_then_list_and_get() {
        let (client, _) = spawn_test_apiserver().await;
        let created = client.create_replicaset(&make_rs("web", 3)).await.unwrap();
        assert_eq!(created.spec.replicas, 3);
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));

        let listed = client.list_replicasets().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].metadata.name, "web");

        let got = client.get_replicaset("web").await.unwrap();
        assert_eq!(got.expect("Some").metadata.name, "web");
        assert!(client.get_replicaset("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rs_replace_status_and_delete() {
        use crate::replicaset::ReplicaSetStatus;
        let (client, _) = spawn_test_apiserver().await;
        let created = client.create_replicaset(&make_rs("web", 2)).await.unwrap();
        let rv = created.metadata.resource_version.unwrap();

        let status = ReplicaSetStatus {
            replicas: 2,
            ready_replicas: 1,
            observed_generation: 1,
        };
        let updated = client.replace_rs_status("web", &status, &rv).await.unwrap();
        assert_eq!(updated.status.unwrap().ready_replicas, 1);

        let rv2 = updated.metadata.resource_version.unwrap();
        client.delete_replicaset("web", &rv2).await.unwrap();
        assert!(client.get_replicaset("web").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rs_watch_stream_receives_added_event() {
        let (client, _) = spawn_test_apiserver().await;
        let mut stream = client.watch_replicasets(Some("0")).await.unwrap();

        let c2 = Client::new(client.base_url.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c2.create_replicaset(&make_rs("web", 1)).await.unwrap();
        });

        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("event was Err");
        assert_eq!(event.object.metadata.name, "web");
        assert_eq!(event.object.spec.replicas, 1);
    }

    // ---- node binding + node-filtered access + Node CRUD ----

    fn make_node(name: &str) -> Node {
        Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: PodMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: NodeSpec::default(),
            status: None,
        }
    }

    #[tokio::test]
    async fn bind_pod_sets_node_name() {
        let (client, _) = spawn_test_apiserver().await;
        client.create_pod(&make_pod("web")).await.unwrap();
        let bound = client.bind_pod("web", "node-a").await.unwrap();
        assert_eq!(bound.spec.node_name.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn list_pods_on_node_filters() {
        let (client, _) = spawn_test_apiserver().await;
        client.create_pod(&make_pod("a")).await.unwrap();
        client.create_pod(&make_pod("b")).await.unwrap();
        client.bind_pod("a", "node-a").await.unwrap();
        client.bind_pod("b", "node-b").await.unwrap();

        let on_a = client.list_pods_on_node("node-a").await.unwrap();
        assert_eq!(on_a.len(), 1);
        assert_eq!(on_a[0].metadata.name, "a");
        assert_eq!(client.list_pods().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn node_create_get_list_and_status() {
        let (client, _) = spawn_test_apiserver().await;
        let n = client.create_node(&make_node("node-a")).await.unwrap();
        assert_eq!(n.metadata.name, "node-a");
        assert!(client.get_node("node-a").await.unwrap().is_some());
        assert!(client.get_node("nope").await.unwrap().is_none());
        assert_eq!(client.list_nodes().await.unwrap().len(), 1);

        let rv = n.metadata.resource_version.unwrap();
        let status = NodeStatus {
            ready: true,
            last_heartbeat_time: Some("2026-06-04T10:00:00Z".into()),
        };
        let updated = client
            .replace_node_status("node-a", &status, &rv)
            .await
            .unwrap();
        assert!(updated.status.unwrap().ready);
    }

    #[tokio::test]
    async fn watch_pods_on_node_delivers_only_matching() {
        let (client, _) = spawn_test_apiserver().await;
        let mut stream = client.watch_pods_on_node("node-a", Some("0")).await.unwrap();

        let c2 = Client::new(client.base_url.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c2.create_pod(&make_pod("a")).await.unwrap();
            c2.bind_pod("a", "node-b").await.unwrap(); // filtered out
            c2.create_pod(&make_pod("b")).await.unwrap();
            c2.bind_pod("b", "node-a").await.unwrap(); // delivered
        });

        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("event was Err");
        assert_eq!(event.object.metadata.name, "b");
        assert_eq!(event.object.spec.node_name.as_deref(), Some("node-a"));
    }
}
