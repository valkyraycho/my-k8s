use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ResourceMeta};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoints {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    /// One entry per ready backend. Real K8s groups these into "subsets" by
    /// shared port; we keep a flat list since we have one port per Service.
    #[serde(default)]
    pub addresses: Vec<EndpointAddress>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointAddress {
    /// A backend pod's IP.
    pub ip: String,
    /// The pod port to send to (the Service's targetPort).
    pub port: u16,
}

impl ResourceMeta for Endpoints {
    const KIND_PREFIX: &'static str = "endpoints/";
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_roundtrips_with_address_list() {
        let ep = Endpoints {
            api_version: "v1".into(),
            kind: "Endpoints".into(),
            metadata: ObjectMeta {
                name: "web".into(),
                ..Default::default()
            },
            addresses: vec![
                EndpointAddress {
                    ip: "10.244.0.2".into(),
                    port: 8080,
                },
                EndpointAddress {
                    ip: "10.244.1.3".into(),
                    port: 8080,
                },
            ],
        };
        let json = serde_json::to_string(&ep).unwrap();
        assert!(json.contains(r#""ip":"10.244.0.2""#), "got: {json}");
        assert!(json.contains(r#""port":8080"#), "got: {json}");

        let parsed: Endpoints = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ep);
    }

    /// An Endpoints with no backends (no matching pods yet) round-trips with an
    /// empty list, and a manifest omitting `addresses` defaults to empty.
    #[test]
    fn endpoints_addresses_default_to_empty() {
        let minimal = r#"{"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"web"}}"#;
        let parsed: Endpoints = serde_json::from_str(minimal).unwrap();
        assert!(parsed.addresses.is_empty());
    }
}
