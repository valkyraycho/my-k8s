use std::collections::HashMap;

use crate::{
    pod::{Pod, PodName},
    runtime::sandbox::PodSandbox,
};

/// Everything the kubelet knows about one Pod: the desired manifest plus
/// the live sandbox holding its containers.
pub struct PodState {
    /// The manifest this sandbox was created from.
    pub pod: Pod,
    /// Live sandbox (pause container + app container handles).
    pub sandbox: PodSandbox,
}

#[derive(Default)]
pub struct Store {
    pods: HashMap<PodName, PodState>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, state: PodState) {
        let name = state.pod.metadata.name.clone();
        self.pods.insert(name, state);
    }

    pub fn remove(&mut self, name: &str) -> Option<PodState> {
        self.pods.remove(name)
    }

    pub fn get(&self, name: &str) -> Option<&PodState> {
        self.pods.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut PodState> {
        self.pods.get_mut(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.pods.contains_key(name)
    }

    pub fn len(&self) -> usize {
        self.pods.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pods.is_empty()
    }

    pub fn names(&self) -> Vec<PodName> {
        self.pods.keys().cloned().collect()
    }

    pub fn drain(&mut self) -> Vec<(PodName, PodState)> {
        self.pods.drain().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::pod::{Pod, PodMetadata, PodSpec};

    /// Build a minimal PodState with just enough structure for store tests.
    /// The sandbox here is inert — none of its methods get called in these tests.
    fn make_pod_state(name: &str) -> PodState {
        let pod = Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: PodMetadata { name: name.into() },
            spec: PodSpec { containers: vec![] },
        };
        let sandbox = PodSandbox::new(
            name.into(),
            PathBuf::from("/tmp/store-test-pods"),
            PathBuf::from("/tmp/store-test-rootfs"),
        );
        PodState { pod, sandbox }
    }

    #[test]
    fn new_store_is_empty() {
        let store = Store::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn insert_then_get_returns_same_pod() {
        let mut store = Store::new();
        store.insert(make_pod_state("web"));
        let name = "web".to_string();
        let state = store
            .get(&name)
            .expect("inserted pod should be retrievable");
        assert_eq!(state.pod.metadata.name, "web");
        assert!(store.contains(&name));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn remove_returns_state_and_clears_entry() {
        let mut store = Store::new();
        store.insert(make_pod_state("web"));
        let name = "web".to_string();
        let removed = store.remove(&name).expect("should remove");
        assert_eq!(removed.pod.metadata.name, "web");
        assert!(!store.contains(&name));
        assert!(store.is_empty());
    }

    #[test]
    fn remove_missing_returns_none() {
        let mut store = Store::new();
        assert!(store.remove("nonexistent").is_none());
    }

    #[test]
    fn names_returns_all_inserted_pods() {
        let mut store = Store::new();
        store.insert(make_pod_state("web"));
        store.insert(make_pod_state("worker"));
        let mut names = store.names();
        names.sort(); // HashMap iteration order is unspecified
        assert_eq!(names, vec!["web".to_string(), "worker".to_string()]);
    }

    #[test]
    fn drain_empties_store_and_returns_all_entries() {
        let mut store = Store::new();
        store.insert(make_pod_state("web"));
        store.insert(make_pod_state("worker"));
        let drained = store.drain();
        assert_eq!(drained.len(), 2);
        assert!(store.is_empty(), "drain should leave store empty");
    }
}
