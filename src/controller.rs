//! Controller-side machinery: the work queue and (next step) the reconcile
//! loops that drain it.

pub mod endpoints;
pub mod endpoints_manager;
pub mod manager;
pub mod replicaset;
pub mod workqueue;
