//! The apiserver: the cluster's source of truth. Owns persistent Pod state and
//! serves it over HTTP, including watch streams. Submodules:
//! - [`storage`]  — sled-backed `PodStore` + optimistic concurrency (the data).
//! - [`watch`]    — turns the store's broadcast channel into list-then-watch streams.
//! - [`handlers`] — axum request handlers (the REST verbs + error mapping).
//! - [`routes`]   — wires paths/methods to handlers.

pub mod handlers;
pub mod routes;
pub mod storage;
pub mod watch;
