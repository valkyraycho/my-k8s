use axum::{
    Router,
    routing::{get, put},
};

use crate::apiserver::handlers::{
    AppState, create_pod, delete_pod, get_pod, list_or_watch_pods, replace_pod_spec,
    replace_pod_status,
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
        .with_state(state)
}
