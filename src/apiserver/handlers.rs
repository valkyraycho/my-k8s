use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_stream::StreamExt;
use tracing::warn;

use crate::{
    apiserver::{
        storage::{PodStore, ResourceStore, StoreError},
        watch::stream_events,
    },
    node::{Node, NodeStatus},
    pod::{Pod, PodStatus},
    replicaset::{ReplicaSet, ReplicaSetStatus},
};

/// Shared handler state. `Arc<PodStore>` so every handler (and every spawned
/// watch-stream future) shares ONE store cheaply. `#[derive(Clone)]` is
/// required: axum clones the state per request, and cloning an `Arc` is just a
/// refcount bump.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<PodStore>,
    pub rs_store: Arc<ResourceStore<ReplicaSet>>,
    pub node_store: Arc<ResourceStore<Node>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodList {
    pub kind: String,
    pub api_version: String,
    pub items: Vec<Pod>,
}

impl PodList {
    fn new(items: Vec<Pod>) -> Self {
        Self {
            kind: "PodList".to_string(),
            api_version: "v1".to_string(),
            items,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub kind: String,
    pub api_version: String,
    pub code: u16,
    pub message: String,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("conflict: current rv {current}, provided {provided}")]
    Conflict { current: String, provided: String },
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("internal: {0}")]
    Internal(String),
}

// `From<StoreError>` lets handlers use `?` on store calls: the storage-layer
// error auto-converts into the HTTP-layer error. Storage internals (Sled/Json)
// collapse to a single opaque `Internal` — we don't leak DB details to clients.
impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound(e) => ApiError::NotFound(e),
            StoreError::AlreadyExists(e) => ApiError::AlreadyExists(e),
            StoreError::Conflict { current, provided } => ApiError::Conflict { current, provided },
            StoreError::Sled(e) => ApiError::Internal(e.to_string()),
            StoreError::Json(e) => ApiError::Internal(e.to_string()),
        }
    }
}

// `IntoResponse` is axum's "how do I become an HTTP response?" trait. Because
// `ApiError` implements it, handlers can return `Result<Response, ApiError>`
// and axum turns an `Err` into a proper status + JSON `Status` envelope body —
// the K8s-style machine-readable error shape, keyed on `reason`.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (statue_code, reason) = match &self {
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "NotFound"),
            ApiError::AlreadyExists(_) => (StatusCode::CONFLICT, "AlreadyExists"),
            ApiError::Conflict { .. } => (StatusCode::CONFLICT, "Conflict"),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BadRequest"),
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal"),
        };

        let body = Status {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            code: statue_code.as_u16(),
            message: self.to_string(),
            reason: reason.to_string(),
        };
        (statue_code, Json(body)).into_response()
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListWatchParams {
    pub watch: Option<bool>,
    pub resource_version: Option<String>,
    pub field_selector: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RvParams {
    pub resource_version: Option<String>,
}

/// The body of a binding request. Real K8s uses `{ target: { name } }`; we
/// flatten to just the node name — placement is the only thing binding does.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Binding {
    pub node_name: String,
}

/// One endpoint, two behaviors. axum EXTRACTORS in the args do the parsing:
/// `State` pulls the shared store, `Query` deserializes `?watch=&resourceVersion=`
/// into `ListWatchParams`. `?watch=true` → a streaming NDJSON body; otherwise →
/// a one-shot `PodList`.
pub async fn list_or_watch_pods(
    State(state): State<AppState>,
    Query(params): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let node_filter = parse_node_name_selector(params.field_selector.as_deref());
    if params.watch.unwrap_or(false) {
        let from_rv = parse_rv(params.resource_version.as_deref());

        // One owned closure (captures the Option<String> by move → 'static +
        // Send). Branching on the captured data rather than picking between two
        // closure types means a single concrete type, so no Box/dyn needed.
        let filter = move |p: &Pod| match &node_filter {
            Some(node) => p.spec.node_name.as_deref() == Some(node.as_str()),
            None => true,
        };

        // Adapt the WatchEvent stream into a byte stream: each event → JSON +
        // '\n' (NDJSON), which the client line-decodes. `map` transforms each
        // item; errors become io::Errors that terminate the HTTP body.
        let stream = stream_events(state.store.clone(), from_rv, filter).map(|res| match res {
            Ok(ev) => {
                let mut bytes = serde_json::to_vec(&ev).map_err(std::io::Error::other)?;
                bytes.push(b'\n');
                // Turbofish annotates the closure's return type so the compiler
                // can infer the stream's `Item` (it can't from the bytes alone).
                Ok::<Vec<u8>, std::io::Error>(bytes)
            }
            Err(e) => {
                warn!(error = %e, "watch stream error; closing connection");
                Err(std::io::Error::other(e.to_string()))
            }
        });
        // `Body::from_stream` streams the body incrementally — the response is
        // open-ended (a watch never "finishes"), so we can't buffer it.
        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .map_err(|e| ApiError::Internal(format!("response build: {e}")))?;
        Ok(response)
    } else {
        let (mut pods, _rv) = state.store.list()?;
        if let Some(node) = node_filter {
            pods.retain(|p| p.spec.node_name.as_deref() == Some(node.as_str()));
        }
        Ok((StatusCode::OK, Json(PodList::new(pods))).into_response())
    }
}

pub async fn create_pod(
    State(state): State<AppState>,
    Json(pod): Json<Pod>,
) -> Result<Response, ApiError> {
    validate_pod(&pod)?;
    let created = state.store.create(pod)?;
    Ok((StatusCode::CREATED, Json(created)).into_response())
}

pub async fn get_pod(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let pod = state
        .store
        .get(&name)?
        .ok_or_else(|| ApiError::NotFound(name.clone()))?;
    Ok((StatusCode::OK, Json(pod)).into_response())
}

pub async fn replace_pod_spec(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(pod): Json<Pod>,
) -> Result<Response, ApiError> {
    validate_pod(&pod)?;
    if pod.metadata.name != name {
        return Err(ApiError::BadRequest(format!(
            "name in body ({}) does not match URL path ({name})",
            pod.metadata.name
        )));
    }
    let updated = state.store.replace_spec(&name, pod)?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

pub async fn delete_pod(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let deleted = state.store.delete(&name, &rv.to_string())?;
    Ok((StatusCode::OK, Json(deleted)).into_response())
}

pub async fn replace_pod_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
    Json(status): Json<PodStatus>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let updated = state
        .store
        .replace_status(&name, &rv, |p| p.status = Some(status.clone()))?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

/// The binding subresource: how a pod gets *placed*. The scheduler POSTs here;
/// we read the pod, stamp `spec.node_name`, and write it back via replace_spec
/// (which preserves status and does its own rv check). Once node_name is set,
/// that node's kubelet — and only that one — will run the pod.
pub async fn bind_pod(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(binding): Json<Binding>,
) -> Result<Response, ApiError> {
    if binding.node_name.is_empty() {
        return Err(ApiError::BadRequest(
            "binding.node_name must not be empty".into(),
        ));
    }

    let mut pod = state
        .store
        .get(&name)?
        .ok_or_else(|| ApiError::NotFound(name.clone()))?;
    pod.spec.node_name = Some(binding.node_name);
    let updated = state.store.replace_spec(&name, pod)?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetList {
    pub kind: String,
    pub api_version: String,
    pub items: Vec<ReplicaSet>,
}

impl ReplicaSetList {
    fn new(items: Vec<ReplicaSet>) -> Self {
        Self {
            kind: "ReplicaSetList".to_string(),
            api_version: "apps/v1".to_string(),
            items,
        }
    }
}

fn validate_replicaset(rs: &ReplicaSet) -> Result<(), ApiError> {
    if rs.metadata.name.is_empty() {
        return Err(ApiError::BadRequest(
            "metadata.name must not be empty".into(),
        ));
    }
    if rs.spec.selector.match_labels.is_empty() {
        return Err(ApiError::BadRequest(
            "spec.selector.matchLabels must not be empty".into(),
        ));
    }
    Ok(())
}

pub async fn list_or_watch_replicasets(
    State(state): State<AppState>,
    Query(params): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    if params.watch.unwrap_or(false) {
        let from_rv = parse_rv(params.resource_version.as_deref());
        let stream =
            stream_events(state.rs_store.clone(), from_rv, |_| true).map(|res| match res {
                Ok(ev) => {
                    let mut bytes = serde_json::to_vec(&ev).map_err(std::io::Error::other)?;
                    bytes.push(b'\n');
                    Ok::<Vec<u8>, std::io::Error>(bytes)
                }
                Err(e) => {
                    warn!(error = %e, "watch stream error; closing connection");
                    Err(std::io::Error::other(e.to_string()))
                }
            });
        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .map_err(|e| ApiError::Internal(format!("response build: {e}")))?;
        Ok(response)
    } else {
        let (items, _rv) = state.rs_store.list()?;
        Ok((StatusCode::OK, Json(ReplicaSetList::new(items))).into_response())
    }
}

pub async fn create_replicaset(
    State(state): State<AppState>,
    Json(rs): Json<ReplicaSet>,
) -> Result<Response, ApiError> {
    validate_replicaset(&rs)?;
    let created = state.rs_store.create(rs)?;
    Ok((StatusCode::CREATED, Json(created)).into_response())
}

pub async fn get_replicaset(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let rs = state
        .rs_store
        .get(&name)?
        .ok_or_else(|| ApiError::NotFound(name.clone()))?;
    Ok((StatusCode::OK, Json(rs)).into_response())
}

pub async fn replace_replicaset_spec(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(rs): Json<ReplicaSet>,
) -> Result<Response, ApiError> {
    validate_replicaset(&rs)?;
    if rs.metadata.name != name {
        return Err(ApiError::BadRequest(format!(
            "name in body ({}) does not match URL path ({name})",
            rs.metadata.name
        )));
    }
    let updated = state.rs_store.replace_spec(&name, rs)?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

pub async fn delete_replicaset(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let deleted = state.rs_store.delete(&name, &rv)?;
    Ok((StatusCode::OK, Json(deleted)).into_response())
}

pub async fn replace_replicaset_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
    Json(status): Json<ReplicaSetStatus>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let updated = state
        .rs_store
        .replace_status(&name, &rv, |rs| rs.status = Some(status.clone()))?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

fn parse_rv(s: Option<&str>) -> u64 {
    s.and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Parse the one field selector we support: `spec.nodeName=<value>`.
/// Returns `Some(node_name)` to filter by, or `None` (no/unsupported selector
/// → no filtering, which is the safe default).
fn parse_node_name_selector(field_selector: Option<&str>) -> Option<String> {
    field_selector?
        .strip_prefix("spec.nodeName=")
        .map(|v| v.to_string())
}

/// Boundary validation: reject malformed input at the HTTP edge with a 400,
/// before it ever touches the store. Validate at the boundary, trust the core.
fn validate_pod(pod: &Pod) -> Result<(), ApiError> {
    if pod.metadata.name.is_empty() {
        return Err(ApiError::BadRequest(
            "metadata.name must not be empty".into(),
        ));
    }
    if pod.spec.containers.is_empty() {
        return Err(ApiError::BadRequest(
            "spec.containers must not be empty".into(),
        ));
    }
    Ok(())
}

// ---- Node handlers (mirror the Pod/ReplicaSet handlers, over node_store) ----

// Cluster pod CIDR is 10.244.0.0/16, carved into per-node /24 slices: node
// index n -> 10.244.n.0/24. The counter persists in sled (NODE_CIDR_COUNTER),
// so slices are stable and never reused across apiserver restarts.
const CLUSTER_POD_CIDR_PREFIX: &str = "10.244";
const NODE_CIDR_COUNTER: &[u8] = b"node_cidr_counter";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeList {
    pub kind: String,
    pub api_version: String,
    pub items: Vec<Node>,
}

impl NodeList {
    fn new(items: Vec<Node>) -> Self {
        Self {
            kind: "NodeList".to_string(),
            api_version: "v1".to_string(),
            items,
        }
    }
}

fn validate_node(node: &Node) -> Result<(), ApiError> {
    if node.metadata.name.is_empty() {
        return Err(ApiError::BadRequest(
            "metadata.name must not be empty".into(),
        ));
    }
    Ok(())
}

pub async fn list_or_watch_nodes(
    State(state): State<AppState>,
    Query(params): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    if params.watch.unwrap_or(false) {
        let from_rv = parse_rv(params.resource_version.as_deref());
        let stream =
            stream_events(state.node_store.clone(), from_rv, |_| true).map(|res| match res {
                Ok(ev) => {
                    let mut bytes = serde_json::to_vec(&ev).map_err(std::io::Error::other)?;
                    bytes.push(b'\n');
                    Ok::<Vec<u8>, std::io::Error>(bytes)
                }
                Err(e) => {
                    warn!(error = %e, "node watch stream error; closing connection");
                    Err(std::io::Error::other(e.to_string()))
                }
            });
        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .map_err(|e| ApiError::Internal(format!("response build: {e}")))?;
        Ok(response)
    } else {
        let (items, _rv) = state.node_store.list()?;
        Ok((StatusCode::OK, Json(NodeList::new(items))).into_response())
    }
}

pub async fn create_node(
    State(state): State<AppState>,
    Json(mut node): Json<Node>,
) -> Result<Response, ApiError> {
    validate_node(&node)?;
    // Assign a /24 PodCIDR only if the node didn't bring its own AND isn't
    // already registered. The get()-guard means a kubelet RESTART (which
    // re-POSTs the same node) doesn't burn a fresh slice — `create` below will
    // return AlreadyExists and the stored node keeps its original CIDR.
    if node.spec.pod_cidr.is_none() && state.node_store.get(&node.metadata.name)?.is_none() {
        let idx = state.node_store.next_index(NODE_CIDR_COUNTER)?;
        if idx > 255 {
            return Err(ApiError::Internal(format!(
                "pod CIDR space exhausted (node index {idx} exceeds 255)"
            )));
        }
        node.spec.pod_cidr = Some(format!("{CLUSTER_POD_CIDR_PREFIX}.{idx}.0/24"));
    }
    let created = state.node_store.create(node)?;
    Ok((StatusCode::CREATED, Json(created)).into_response())
}

pub async fn get_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let node = state
        .node_store
        .get(&name)?
        .ok_or_else(|| ApiError::NotFound(name.clone()))?;
    Ok((StatusCode::OK, Json(node)).into_response())
}

pub async fn replace_node_spec(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(node): Json<Node>,
) -> Result<Response, ApiError> {
    validate_node(&node)?;
    if node.metadata.name != name {
        return Err(ApiError::BadRequest(format!(
            "name in body ({}) does not match URL path ({name})",
            node.metadata.name
        )));
    }
    let updated = state.node_store.replace_spec(&name, node)?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

pub async fn delete_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let deleted = state.node_store.delete(&name, &rv)?;
    Ok((StatusCode::OK, Json(deleted)).into_response())
}

pub async fn replace_node_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RvParams>,
    Json(status): Json<NodeStatus>,
) -> Result<Response, ApiError> {
    let rv = params
        .resource_version
        .ok_or_else(|| ApiError::BadRequest("resourceVersion query param required".into()))?;
    let updated = state
        .node_store
        .replace_status(&name, &rv, |n| n.status = Some(status.clone()))?;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        http::{Method, Request},
    };
    use tower::ServiceExt;

    use crate::apiserver::routes::router;
    use crate::apiserver::storage::ResourceStore;
    use crate::pod::{Container, PodMetadata, PodPhase, PodSpec};
    use crate::replicaset::{
        LabelSelector, PodTemplateSpec, ReplicaSet, ReplicaSetSpec, ReplicaSetStatus,
        TemplateObjectMeta,
    };

    /// Build a router backed by both stores sharing ONE temp sled::Db (so the
    /// global rv_counter is shared, mirroring production). Returns both stores
    /// for tests that drive writes directly.
    fn setup_full() -> (
        axum::Router,
        Arc<PodStore>,
        Arc<ResourceStore<ReplicaSet>>,
        Arc<ResourceStore<Node>>,
    ) {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let pod_store = Arc::new(PodStore::from_db(db.clone()).expect("pod store"));
        let rs_store =
            Arc::new(ResourceStore::<ReplicaSet>::from_db(db.clone()).expect("rs store"));
        let node_store = Arc::new(ResourceStore::<Node>::from_db(db).expect("node store"));
        let app = router(AppState {
            store: pod_store.clone(),
            rs_store: rs_store.clone(),
            node_store: node_store.clone(),
        });
        (app, pod_store, rs_store, node_store)
    }

    fn setup() -> (axum::Router, Arc<PodStore>) {
        let (app, pod_store, _rs, _node) = setup_full();
        (app, pod_store)
    }

    fn make_replicaset(name: &str, replicas: u32) -> ReplicaSet {
        let mut selector = LabelSelector::default();
        selector.match_labels.insert("app".into(), name.into());
        let mut tmpl = TemplateObjectMeta::default();
        tmpl.labels.insert("app".into(), name.into());
        ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: crate::meta::ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas,
                selector,
                template: PodTemplateSpec {
                    metadata: tmpl,
                    spec: PodSpec {
                        containers: vec![Container {
                            name: "c".into(),
                            image: "busybox".into(),
                            command: vec!["sleep".into(), "1".into()],
                        }],
                        node_name: None,
                    },
                },
            },
            status: None,
        }
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

    fn make_status() -> PodStatus {
        PodStatus {
            phase: PodPhase::Running,
            container_statuses: vec![],
            observed_generation: Some(1),
            pod_ip: None,
        }
    }

    fn empty_req(method: Method, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request")
    }

    fn json_req(method: Method, uri: &str, value: &impl Serialize) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .expect("request")
    }

    async fn body_value(res: Response) -> serde_json::Value {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).expect("valid JSON body")
    }

    async fn body_pod(res: Response) -> Pod {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).expect("valid Pod body")
    }

    #[tokio::test]
    async fn list_empty_returns_empty_podlist() {
        let (app, _) = setup();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/pods"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_value(res).await;
        assert_eq!(body["kind"], "PodList");
        assert_eq!(body["apiVersion"], "v1");
        assert_eq!(body["items"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn list_returns_existing_pods_wrapped() {
        let (app, store) = setup();
        store.create(make_pod("a")).unwrap();
        store.create(make_pod("b")).unwrap();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/pods"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_value(res).await;
        assert_eq!(body["kind"], "PodList");
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn create_returns_201_with_assigned_apiserver_fields() {
        let (app, _) = setup();
        let res = app
            .oneshot(json_req(Method::POST, "/api/v1/pods", &make_pod("web")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let pod = body_pod(res).await;
        assert_eq!(pod.metadata.name, "web");
        assert!(pod.metadata.uid.is_some());
        assert_eq!(pod.metadata.resource_version.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn create_duplicate_returns_409_status_envelope() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(json_req(Method::POST, "/api/v1/pods", &make_pod("web")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let body = body_value(res).await;
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["code"], 409);
        assert_eq!(body["reason"], "AlreadyExists");
    }

    #[tokio::test]
    async fn create_invalid_pod_returns_400() {
        let (app, _) = setup();
        let mut bad = make_pod("ignored"); // we'll blank the name
        bad.metadata.name = String::new();
        let res = app
            .oneshot(json_req(Method::POST, "/api/v1/pods", &bad))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_value(res).await;
        assert_eq!(body["reason"], "BadRequest");
    }

    #[tokio::test]
    async fn get_existing_pod_returns_200() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/pods/web"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let pod = body_pod(res).await;
        assert_eq!(pod.metadata.name, "web");
    }

    #[tokio::test]
    async fn get_missing_pod_returns_404_status_envelope() {
        let (app, _) = setup();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/pods/nope"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_value(res).await;
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["reason"], "NotFound");
    }

    #[tokio::test]
    async fn replace_spec_returns_409_on_stale_rv() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap(); // rv=1
        let mut stale = make_pod("web");
        stale.metadata.resource_version = Some("999".into());
        let res = app
            .oneshot(json_req(Method::PUT, "/api/v1/pods/web", &stale))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let body = body_value(res).await;
        assert_eq!(body["reason"], "Conflict");
    }

    #[tokio::test]
    async fn replace_spec_name_mismatch_returns_400() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let mut wrong = make_pod("other");
        wrong.metadata.resource_version = Some("1".into());
        let res = app
            .oneshot(json_req(Method::PUT, "/api/v1/pods/web", &wrong))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_with_correct_rv_removes_pod() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(empty_req(
                Method::DELETE,
                "/api/v1/pods/web?resourceVersion=1",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(store.get("web").unwrap().is_none(), "pod should be removed");
    }

    #[tokio::test]
    async fn delete_without_rv_returns_400() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(empty_req(Method::DELETE, "/api/v1/pods/web"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(
            store.get("web").unwrap().is_some(),
            "pod must not be deleted without rv",
        );
    }

    #[tokio::test]
    async fn replace_status_updates_status_field() {
        let (app, store) = setup();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(json_req(
                Method::PUT,
                "/api/v1/pods/web/status?resourceVersion=1",
                &make_status(),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let pod = body_pod(res).await;
        assert!(pod.status.is_some());
        assert_eq!(pod.status.as_ref().unwrap().phase, PodPhase::Running);
    }

    // ---- ReplicaSet routes (parallel to the pod routes above) ----

    async fn body_rs(res: Response) -> ReplicaSet {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).expect("valid ReplicaSet body")
    }

    #[tokio::test]
    async fn rs_create_returns_201_with_apiserver_fields() {
        let (app, _, _, _) = setup_full();
        let res = app
            .oneshot(json_req(
                Method::POST,
                "/api/v1/replicasets",
                &make_replicaset("web", 3),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let rs = body_rs(res).await;
        assert_eq!(rs.metadata.name, "web");
        assert_eq!(rs.spec.replicas, 3);
        assert!(rs.metadata.uid.is_some());
        assert_eq!(rs.metadata.resource_version.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn rs_create_rejects_empty_selector() {
        let (app, _, _, _) = setup_full();
        let mut rs = make_replicaset("web", 3);
        rs.spec.selector.match_labels.clear(); // empty selector → matches everything
        let res = app
            .oneshot(json_req(Method::POST, "/api/v1/replicasets", &rs))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_value(res).await;
        assert_eq!(body["reason"], "BadRequest");
    }

    #[tokio::test]
    async fn rs_list_wraps_in_replicasetlist() {
        let (app, _, rs_store, _node) = setup_full();
        rs_store.create(make_replicaset("a", 1)).unwrap();
        rs_store.create(make_replicaset("b", 2)).unwrap();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/replicasets"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_value(res).await;
        assert_eq!(body["kind"], "ReplicaSetList");
        assert_eq!(body["apiVersion"], "apps/v1");
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn rs_get_missing_returns_404() {
        let (app, _, _, _) = setup_full();
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/replicasets/nope"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rs_replace_status_persists_replica_counts() {
        let (app, _, rs_store, _node) = setup_full();
        rs_store.create(make_replicaset("web", 3)).unwrap(); // rv=1
        let status = ReplicaSetStatus {
            replicas: 3,
            ready_replicas: 2,
            observed_generation: 1,
        };
        let res = app
            .oneshot(json_req(
                Method::PUT,
                "/api/v1/replicasets/web/status?resourceVersion=1",
                &status,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let rs = body_rs(res).await;
        let st = rs.status.expect("status set");
        assert_eq!(st.replicas, 3);
        assert_eq!(st.ready_replicas, 2);
    }

    /// Pod and ReplicaSet stores share the global rv_counter (one sled::Db), so
    /// interleaved creates get strictly increasing rvs across BOTH kinds.
    #[tokio::test]
    async fn pod_and_rs_share_global_resource_version() {
        let (_app, pod_store, rs_store, _node) = setup_full();
        let p = pod_store.create(make_pod("web")).unwrap(); // rv=1
        let rs = rs_store.create(make_replicaset("web", 1)).unwrap(); // rv=2
        assert_eq!(p.metadata.resource_version.as_deref(), Some("1"));
        assert_eq!(rs.metadata.resource_version.as_deref(), Some("2"));
    }

    // ---- Node routes ----

    fn make_node(name: &str) -> Node {
        Node {
            api_version: "v1".into(),
            kind: "Node".into(),
            metadata: crate::meta::ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: crate::node::NodeSpec::default(),
            status: None,
        }
    }

    async fn body_node(res: Response) -> Node {
        let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).expect("valid Node body")
    }

    #[tokio::test]
    async fn node_create_get_list() {
        let (app, _, _, _) = setup_full();
        let res = app
            .clone()
            .oneshot(json_req(
                Method::POST,
                "/api/v1/nodes",
                &make_node("node-a"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let created = body_node(res).await;
        assert_eq!(created.metadata.name, "node-a");
        assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));

        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/nodes"))
            .await
            .unwrap();
        let body = body_value(res).await;
        assert_eq!(body["kind"], "NodeList");
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn node_replace_status_sets_heartbeat() {
        let (app, _, _, node_store) = setup_full();
        node_store.create(make_node("node-a")).unwrap(); // rv=1
        let status = NodeStatus {
            ready: true,
            last_heartbeat_time: Some("2026-06-04T10:00:00Z".into()),
        };
        let res = app
            .oneshot(json_req(
                Method::PUT,
                "/api/v1/nodes/node-a/status?resourceVersion=1",
                &status,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let n = body_node(res).await;
        let st = n.status.expect("status set");
        assert!(st.ready);
        assert_eq!(
            st.last_heartbeat_time.as_deref(),
            Some("2026-06-04T10:00:00Z")
        );
    }

    /// Two distinct nodes registering (via the handler) get consecutive,
    /// disjoint /24 slices — the IPAM coordination guarantee.
    #[tokio::test]
    async fn create_node_assigns_distinct_pod_cidrs() {
        let (app, _, _, _) = setup_full();
        let a = body_node(
            app.clone()
                .oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("node-a")))
                .await
                .unwrap(),
        )
        .await;
        let b = body_node(
            app.oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("node-b")))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(a.spec.pod_cidr.as_deref(), Some("10.244.0.0/24"));
        assert_eq!(b.spec.pod_cidr.as_deref(), Some("10.244.1.0/24"));
    }

    /// A kubelet RESTART re-POSTs its node → 409 AlreadyExists, and crucially
    /// does NOT consume a CIDR index (the next new node still gets .1, not .2).
    #[tokio::test]
    async fn create_node_reregistration_does_not_burn_a_cidr() {
        let (app, _, _, _) = setup_full();
        let a = body_node(
            app.clone()
                .oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("node-a")))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(a.spec.pod_cidr.as_deref(), Some("10.244.0.0/24"));

        // Re-register node-a → rejected, no allocation.
        let res = app
            .clone()
            .oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("node-a")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);

        // node-b proves the counter wasn't advanced by the failed re-register.
        let b = body_node(
            app.oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("node-b")))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(b.spec.pod_cidr.as_deref(), Some("10.244.1.0/24"));
    }

    /// A node that brings its own pod_cidr keeps it — the apiserver only fills
    /// the gap, it doesn't override an explicit assignment.
    #[tokio::test]
    async fn create_node_preserves_explicit_pod_cidr() {
        let (app, _, _, _) = setup_full();
        let mut node = make_node("node-x");
        node.spec.pod_cidr = Some("10.244.7.0/24".into());
        let created = body_node(
            app.oneshot(json_req(Method::POST, "/api/v1/nodes", &node))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(created.spec.pod_cidr.as_deref(), Some("10.244.7.0/24"));
    }

    #[tokio::test]
    async fn node_create_rejects_empty_name() {
        let (app, _, _, _) = setup_full();
        let res = app
            .oneshot(json_req(Method::POST, "/api/v1/nodes", &make_node("")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    // ---- Pod binding subresource ----

    fn bind_body(node_name: &str) -> serde_json::Value {
        serde_json::json!({ "nodeName": node_name })
    }

    #[tokio::test]
    async fn binding_sets_node_name() {
        let (app, store, _, _) = setup_full();
        store.create(make_pod("web")).unwrap(); // unscheduled: node_name None
        assert!(store.get("web").unwrap().unwrap().spec.node_name.is_none());

        let res = app
            .oneshot(json_req(
                Method::POST,
                "/api/v1/pods/web/binding",
                &bind_body("node-a"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bound = body_pod(res).await;
        assert_eq!(bound.spec.node_name.as_deref(), Some("node-a"));
        // Persisted, not just echoed.
        assert_eq!(
            store.get("web").unwrap().unwrap().spec.node_name.as_deref(),
            Some("node-a"),
        );
    }

    #[tokio::test]
    async fn binding_missing_pod_returns_404() {
        let (app, _, _, _) = setup_full();
        let res = app
            .oneshot(json_req(
                Method::POST,
                "/api/v1/pods/ghost/binding",
                &bind_body("node-a"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn binding_empty_node_name_returns_400() {
        let (app, store, _, _) = setup_full();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(json_req(
                Method::POST,
                "/api/v1/pods/web/binding",
                &bind_body(""),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn binding_is_idempotent() {
        let (app, store, _, _) = setup_full();
        store.create(make_pod("web")).unwrap();
        for _ in 0..2 {
            let res = app
                .clone()
                .oneshot(json_req(
                    Method::POST,
                    "/api/v1/pods/web/binding",
                    &bind_body("node-a"),
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }
        assert_eq!(
            store.get("web").unwrap().unwrap().spec.node_name.as_deref(),
            Some("node-a"),
        );
    }

    /// Route-collision guard: POST .../web/binding must hit bind_pod, NOT the
    /// 3-segment /pods/:name handler (which only answers GET/PUT/DELETE). If the
    /// router mis-dispatched, we'd get 405/404 instead of a successful bind.
    #[tokio::test]
    async fn binding_route_does_not_collide_with_pod_name_route() {
        let (app, store, _, _) = setup_full();
        store.create(make_pod("web")).unwrap();
        let res = app
            .oneshot(json_req(
                Method::POST,
                "/api/v1/pods/web/binding",
                &bind_body("node-a"),
            ))
            .await
            .unwrap();
        // A successful 200 (not 404/405) proves the 4-segment route matched.
        assert_eq!(res.status(), StatusCode::OK);
    }

    // ---- server-side fieldSelector (spec.nodeName) ----

    /// Create a pod already bound to `node` (skips the binding round-trip).
    fn make_pod_on(name: &str, node: &str) -> Pod {
        let mut p = make_pod(name);
        p.spec.node_name = Some(node.into());
        p
    }

    /// Pull one watch event within `ms`, unwrapping both the timeout and the
    /// stream's `Result`. `None` = stream stayed pending (e.g. filtered out).
    async fn next_within<S>(
        stream: &mut S,
        ms: u64,
    ) -> Option<crate::apiserver::watch::WatchEvent<Pod>>
    where
        S: tokio_stream::Stream<
                Item = Result<
                    crate::apiserver::watch::WatchEvent<Pod>,
                    crate::apiserver::watch::WatchError,
                >,
            > + Unpin,
    {
        tokio::time::timeout(std::time::Duration::from_millis(ms), stream.next())
            .await
            .ok()
            .flatten()
            .map(|r| r.expect("watch event was Err"))
    }

    #[tokio::test]
    async fn list_filters_by_node_name() {
        let (app, store, _, _) = setup_full();
        store.create(make_pod_on("a", "node-a")).unwrap();
        store.create(make_pod_on("b", "node-b")).unwrap();
        store.create(make_pod("c")).unwrap(); // unscheduled (node_name None)

        // Filtered list → only node-a's pod.
        let res = app
            .clone()
            .oneshot(empty_req(
                Method::GET,
                "/api/v1/pods?fieldSelector=spec.nodeName=node-a",
            ))
            .await
            .unwrap();
        let body = body_value(res).await;
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["metadata"]["name"], "a");

        // Unfiltered list → all three.
        let res = app
            .oneshot(empty_req(Method::GET, "/api/v1/pods"))
            .await
            .unwrap();
        let body = body_value(res).await;
        assert_eq!(body["items"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn watch_filters_catch_up_and_live_by_node_name() {
        let (_app, store, _, _) = setup_full();
        // Pre-existing pods: one on node-a, one on node-b.
        store.create(make_pod_on("a", "node-a")).unwrap();
        store.create(make_pod_on("b", "node-b")).unwrap();

        // Watch node-a from rv 0 (catch-up should yield only "a").
        let node_filter = parse_node_name_selector(Some("spec.nodeName=node-a"));
        assert_eq!(node_filter.as_deref(), Some("node-a"));
        let filter = move |p: &Pod| match &node_filter {
            Some(n) => p.spec.node_name.as_deref() == Some(n.as_str()),
            None => true,
        };
        let stream = stream_events(store.clone(), 0, filter);
        tokio::pin!(stream);

        // Catch-up: exactly one event, for "a".
        let ev = next_within(&mut stream, 100).await.expect("catch-up ev");
        assert_eq!(ev.object.metadata.name, "a");
        assert!(
            next_within(&mut stream, 50).await.is_none(),
            "b must be filtered out"
        );

        // Live: a new node-a pod is delivered; a node-b pod is not.
        let store_w = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            store_w.create(make_pod_on("c", "node-b")).unwrap(); // filtered out
            store_w.create(make_pod_on("d", "node-a")).unwrap(); // delivered
        });
        let ev = next_within(&mut stream, 500).await.expect("live ev");
        assert_eq!(
            ev.object.metadata.name, "d",
            "only the node-a live pod arrives"
        );
    }
}
