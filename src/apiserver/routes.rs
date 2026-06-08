use axum::{
    Router,
    routing::{get, post, put},
};

use crate::apiserver::handlers::{
    AppState, bind_pod, create_endpoints, create_node, create_pod, create_replicaset,
    create_service, delete_endpoints, delete_node, delete_pod, delete_replicaset, delete_service,
    get_endpoints, get_node, get_pod, get_replicaset, get_service, list_or_watch_endpoints,
    list_or_watch_nodes, list_or_watch_pods, list_or_watch_replicasets, list_or_watch_services,
    replace_endpoints, replace_node_spec, replace_node_status, replace_pod_spec,
    replace_pod_status, replace_replicaset_spec, replace_replicaset_status, replace_service_spec,
};

/// The REST surface, in one place. Note `get(...).post(...)` chaining attaches
/// multiple HTTP methods to one path. `list_or_watch_pods` handles both the
/// list and the `?watch=true` streaming case (K8s puts them on one endpoint).
/// `with_state` injects the shared `AppState` (the `Arc<PodStore>`) into every
/// handler via the `State` extractor — axum's dependency-injection mechanism.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/pods", get(list_or_watch_pods).post(create_pod))
        .route(
            "/api/v1/pods/:name",
            get(get_pod).put(replace_pod_spec).delete(delete_pod),
        )
        .route("/api/v1/pods/:name/status", put(replace_pod_status))
        .route(
            "/api/v1/replicasets",
            get(list_or_watch_replicasets).post(create_replicaset),
        )
        .route(
            "/api/v1/replicasets/:name",
            get(get_replicaset)
                .put(replace_replicaset_spec)
                .delete(delete_replicaset),
        )
        .route(
            "/api/v1/replicasets/:name/status",
            put(replace_replicaset_status),
        )
        .route("/api/v1/nodes", get(list_or_watch_nodes).post(create_node))
        .route(
            "/api/v1/nodes/:name",
            get(get_node).put(replace_node_spec).delete(delete_node),
        )
        .route("/api/v1/nodes/:name/status", put(replace_node_status))
        .route("/api/v1/pods/:name/binding", post(bind_pod))
        .route(
            "/api/v1/services",
            get(list_or_watch_services).post(create_service),
        )
        .route(
            "/api/v1/services/:name",
            get(get_service)
                .put(replace_service_spec)
                .delete(delete_service),
        )
        .route(
            "/api/v1/endpoints",
            get(list_or_watch_endpoints).post(create_endpoints),
        )
        .route(
            "/api/v1/endpoints/:name",
            get(get_endpoints)
                .put(replace_endpoints)
                .delete(delete_endpoints),
        )
        .with_state(state)
}
