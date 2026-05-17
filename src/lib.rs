//! my-k8s: a learning-focused mini-Kubernetes implementation.
//!
//! This library hosts the shared types and logic used by the orchestrator
//! binaries (`kubelet`, eventually `apiserver`, `scheduler`, etc.). Each
//! binary in `src/bin/` is a thin entry point; the real work lives here.

pub mod apiserver;
pub mod pod;
pub mod reconciler;
pub mod runtime;
pub mod store;
pub mod watcher;
