//! my-k8s: a learning-focused mini-Kubernetes implementation.
//!
//! This library hosts the shared types and logic used by the three binaries
//! in `src/bin/` (`apiserver`, `kubelet`, `mykubectl`), which are thin
//! entry points — the real work lives here.
//!
//! Module map, roughly control-plane top to node bottom:
//! - [`pod`]       — the wire types (Pod/spec/status) every layer exchanges.
//! - [`apiserver`] — HTTP API + sled persistence + watch streams (the source of truth).
//! - [`client`]    — typed HTTP client the kubelet/CLI use to talk to the apiserver.
//! - [`reconciler`]— the kubelet's brain: watch desired state, run it, report status.
//! - [`runtime`]   — the CRI-shaped runtime trait + libcontainer impl + Pod sandbox.
//! - [`store`]     — the kubelet's in-memory map of live Pod sandboxes.

pub mod apiserver;
pub mod client;
pub mod meta;
pub mod pod;
pub mod reconciler;
pub mod replicaset;
pub mod runtime;
pub mod store;
