use axum::{
    Router,
    routing::{get, put},
};

use crate::apiserver::handlers::{
    AppState, create_pod, delete_pod, get_pod, list_or_watch_pods, replace_pod_spec,
    replace_pod_status,
};

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
