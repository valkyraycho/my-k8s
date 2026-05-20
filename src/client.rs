use std::pin::Pin;

use reqwest::StatusCode;

use tokio_stream::{Stream, StreamExt};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
};

use crate::apiserver::watch::WatchEvent;
use crate::pod::{Pod, PodStatus};

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
        let res = self.http.get(self.url("/api/v1/pods")).send().await?;
        let list: PodList = parse_json(res).await?;
        Ok(list.items)
    }

    pub async fn get_pod(&self, name: &str) -> Result<Option<Pod>> {
        let res = self
            .http
            .get(self.url(&format!("/api/v1/pods/{}", name)))
            .send()
            .await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(parse_json(res).await?))
    }
    pub async fn create_pod(&self, pod: &Pod) -> Result<Pod> {
        let res = self
            .http
            .post(self.url("/api/v1/pods"))
            .json(pod)
            .send()
            .await?;
        parse_json(res).await
    }

    pub async fn replace_pod_spec(&self, pod: &Pod) -> Result<Pod> {
        let url = self.url(&format!("/api/v1/pods/{}", pod.metadata.name));
        let res = self.http.put(&url).json(pod).send().await?;
        parse_json(res).await
    }

    pub async fn replace_pod_status(
        &self,
        name: &str,
        status: &PodStatus,
        rv: &str,
    ) -> Result<Pod> {
        let url = self.url(&format!("/api/v1/pods/{name}/status?resourceVersion={rv}"));
        let res = self.http.put(&url).json(status).send().await?;
        parse_json(res).await
    }

    pub async fn delete_pod(&self, name: &str, rv: &str) -> Result<()> {
        let url = self.url(&format!("/api/v1/pods/{name}?resourceVersion={rv}"));
        let res = self.http.delete(&url).send().await?;
        check_status(res).await?;
        Ok(())
    }

    pub async fn watch_pods(
        &self,
        from_rv: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent>> + Send>>> {
        let url = match from_rv {
            Some(rv) => self.url(&format!("/api/v1/pods?watch=true&resourceVersion={rv}")),
            None => self.url("/api/v1/pods?watch=true"),
        };
        let res = self.http.get(&url).send().await?;
        let res = check_status(res).await?;

        let bytes = res
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other));
        let reader = StreamReader::new(bytes);
        let lines = FramedRead::new(reader, LinesCodec::new());
        let events = lines.map(|line_res| -> Result<WatchEvent> {
            let line =
                line_res.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            Ok(serde_json::from_str(&line)?)
        });
        Ok(Box::pin(events))
    }
}

#[derive(Debug, serde::Deserialize)]
struct PodList {
    items: Vec<Pod>,
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

    use crate::apiserver::{handlers::AppState, routes::router, storage::PodStore};
    use crate::pod::{Container, Pod, PodMetadata, PodPhase, PodSpec};

    /// Spin up a real apiserver router on an OS-assigned port and return a
    /// Client pointed at it, plus the shared store so tests can drive writes
    /// from "outside" the HTTP path (useful for watch tests).
    async fn spawn_test_apiserver() -> (Client, Arc<PodStore>) {
        let store = Arc::new(PodStore::open_temporary().expect("temp store"));
        let app = router(AppState {
            store: store.clone(),
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
}
