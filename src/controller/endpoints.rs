use std::collections::BTreeMap;

use anyhow::Result;
use tracing::info;

use crate::{
    client::Client,
    endpoints::{EndpointAddress, Endpoints},
    pod::{Pod, PodPhase},
    service::Service,
};

// Empty selector matches NOTHING (guard against the vacuous `all()`-is-true).
fn pod_matches(pod: &Pod, selector: &BTreeMap<String, String>) -> bool {
    !selector.is_empty()
        && selector
            .iter()
            .all(|(k, v)| pod.metadata.labels.get(k) == Some(v))
}

// A backend only if Running AND it has reported an IP — a Running pod without an
// IP yet would be a DNAT black hole.
fn backend_ip(pod: &Pod) -> Option<String> {
    let status = pod.status.as_ref()?;
    if status.phase != PodPhase::Running {
        return None;
    }
    status.pod_ip.clone()
}

/// Services whose selector matches this pod — maps a pod event to the Service
/// keys to enqueue (pods carry no back-reference to Services, unlike ownerRefs).
pub fn services_for_pod(pod: &Pod, services: &[Service]) -> Vec<String> {
    services
        .iter()
        .filter(|svc| pod_matches(pod, &svc.spec.selector))
        .map(|svc| svc.metadata.name.clone())
        .collect()
}

pub async fn reconcile(svc_name: &str, client: &Client) -> Result<()> {
    let svc = match client.get_service(svc_name).await? {
        Some(svc) => svc,
        None => {
            if let Some(existing) = client.get_endpoints(svc_name).await?
                && let Some(rv) = existing.metadata.resource_version.clone()
            {
                let _ = client.delete_endpoints(svc_name, &rv).await;
                info!(svc = %svc_name, "service gone; deleted endpoints");
            }
            return Ok(());
        }
    };
    let mut addresses: Vec<EndpointAddress> = client
        .list_pods()
        .await?
        .into_iter()
        .filter(|pod| pod_matches(pod, &svc.spec.selector))
        .filter_map(|pod| {
            backend_ip(&pod).map(|ip| EndpointAddress {
                ip,
                port: svc.spec.target_port,
            })
        })
        .collect();

    addresses.sort_by(|a, b| a.ip.cmp(&b.ip));
    write_endpoints(svc_name, addresses, client).await
}

async fn write_endpoints(
    svc_name: &str,
    addresses: Vec<EndpointAddress>,
    client: &Client,
) -> Result<()> {
    match client.get_endpoints(svc_name).await? {
        Some(mut existing) => {
            if existing.addresses == addresses {
                return Ok(()); // unchanged → no write, no loop
            }
            existing.addresses = addresses;
            client.replace_endpoints(&existing).await?;
            info!(svc = %svc_name, "updated endpoints");
        }
        None => {
            let ep = Endpoints {
                api_version: "v1".into(),
                kind: "Endpoints".into(),
                metadata: crate::meta::ObjectMeta {
                    name: svc_name.to_string(),
                    ..Default::default()
                },
                addresses,
            };
            client.create_endpoints(&ep).await?;
            info!(svc = %svc_name, "created endpoints");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore},
    };
    use crate::meta::ObjectMeta;
    use crate::pod::{Container, Pod, PodSpec, PodStatus};
    use crate::service::ServiceSpec;

    async fn spawn_apiserver() -> Client {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let app = router(AppState {
            store: Arc::new(PodStore::from_db(db.clone()).unwrap()),
            rs_store: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            node_store: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            svc_store: Arc::new(ResourceStore::from_db(db.clone()).unwrap()),
            ep_store: Arc::new(ResourceStore::from_db(db).unwrap()),
            write: crate::apiserver::handlers::WritePath::Direct,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Client::new(format!("http://{addr}"))
    }

    fn svc(name: &str) -> Service {
        let mut selector = BTreeMap::new();
        selector.insert("app".into(), "web".into());
        Service {
            api_version: "v1".into(),
            kind: "Service".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ServiceSpec {
                selector,
                port: 80,
                target_port: 8080,
                cluster_ip: None,
            },
        }
    }

    /// A pod with the given app label, phase, and optional pod IP.
    fn pod(name: &str, app: &str, phase: PodPhase, ip: Option<&str>) -> Pod {
        let mut labels = BTreeMap::new();
        labels.insert("app".into(), app.into());
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: ObjectMeta {
                name: name.into(),
                labels,
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "c".into(),
                    image: "busybox".into(),
                    command: vec!["sh".into()],
                }],
                node_name: Some("node-a".into()),
            },
            status: Some(PodStatus {
                phase,
                pod_ip: ip.map(String::from),
                ..Default::default()
            }),
        }
    }

    /// Create a pod, then PUT its status (create strips status).
    async fn create_with_status(client: &Client, p: &Pod) {
        let created = client.create_pod(p).await.unwrap();
        let rv = created.metadata.resource_version.unwrap();
        client
            .replace_pod_status(&p.metadata.name, p.status.as_ref().unwrap(), &rv)
            .await
            .unwrap();
    }

    #[test]
    fn pod_matches_requires_all_labels_and_nonempty_selector() {
        let mut sel = BTreeMap::new();
        sel.insert("app".into(), "web".into());
        assert!(pod_matches(&pod("p", "web", PodPhase::Running, None), &sel));
        assert!(!pod_matches(&pod("p", "api", PodPhase::Running, None), &sel));
        // Empty selector matches nothing.
        assert!(!pod_matches(&pod("p", "web", PodPhase::Running, None), &BTreeMap::new()));
    }

    #[tokio::test]
    async fn reconcile_populates_endpoints_from_running_pods_with_ip() {
        let client = spawn_apiserver().await;
        client.create_service(&svc("web")).await.unwrap();
        create_with_status(&client, &pod("p1", "web", PodPhase::Running, Some("10.244.0.2"))).await;
        create_with_status(&client, &pod("p2", "web", PodPhase::Running, Some("10.244.1.3"))).await;
        // A matching pod that is Running but has NO IP yet → excluded.
        create_with_status(&client, &pod("p3", "web", PodPhase::Running, None)).await;
        // A matching pod that is Pending → excluded.
        create_with_status(&client, &pod("p4", "web", PodPhase::Pending, Some("10.244.0.9"))).await;
        // A non-matching pod → excluded.
        create_with_status(&client, &pod("other", "api", PodPhase::Running, Some("10.244.0.7"))).await;

        reconcile("web", &client).await.unwrap();

        let ep = client.get_endpoints("web").await.unwrap().expect("endpoints created");
        let ips: Vec<&str> = ep.addresses.iter().map(|a| a.ip.as_str()).collect();
        assert_eq!(ips, vec!["10.244.0.2", "10.244.1.3"]);
        assert!(ep.addresses.iter().all(|a| a.port == 8080));
    }

    #[tokio::test]
    async fn reconcile_is_idempotent_no_rewrite_when_unchanged() {
        let client = spawn_apiserver().await;
        client.create_service(&svc("web")).await.unwrap();
        create_with_status(&client, &pod("p1", "web", PodPhase::Running, Some("10.244.0.2"))).await;

        reconcile("web", &client).await.unwrap();
        let rv1 = client
            .get_endpoints("web")
            .await
            .unwrap()
            .unwrap()
            .metadata
            .resource_version;

        // Second reconcile with no change must NOT bump the rv (no write).
        reconcile("web", &client).await.unwrap();
        let rv2 = client
            .get_endpoints("web")
            .await
            .unwrap()
            .unwrap()
            .metadata
            .resource_version;
        assert_eq!(rv1, rv2, "unchanged endpoints should not be rewritten");
    }

    #[tokio::test]
    async fn reconcile_deletes_endpoints_when_service_gone() {
        let client = spawn_apiserver().await;
        client.create_service(&svc("web")).await.unwrap();
        create_with_status(&client, &pod("p1", "web", PodPhase::Running, Some("10.244.0.2"))).await;
        reconcile("web", &client).await.unwrap();
        assert!(client.get_endpoints("web").await.unwrap().is_some());

        // Delete the Service, then reconcile → Endpoints cleaned up.
        let svc = client.get_service("web").await.unwrap().unwrap();
        let rv = svc.metadata.resource_version.unwrap();
        client.delete_service("web", &rv).await.unwrap();

        reconcile("web", &client).await.unwrap();
        assert!(client.get_endpoints("web").await.unwrap().is_none());
    }
}
