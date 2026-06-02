//! Container runtime abstraction (our mini-CRI).
//!
//! This module defines [`RuntimeClient`], the trait every container runtime
//! must satisfy. The reconciler calls this trait — never libcontainer
//! directly — so we can swap in a mock for tests without needing root,
//! Linux, or a real OCI bundle.
//!
//! In real Kubernetes, this role is played by CRI (the Container Runtime
//! Interface, a gRPC API). Our trait is a tiny in-process equivalent.

pub mod bundle;
pub mod sandbox;
pub mod youki;

use std::path::Path;

use thiserror::Error;

/// The lifecycle states a container can be in, from the orchestrator's view.
///
/// Maps loosely to OCI runtime spec states, but flattened — we don't need
/// to distinguish `creating` from `created`, for example.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerState {
    /// Created but not started.
    Created,
    /// Init process is running.
    Running,
    /// Init process has exited.
    Stopped,
    /// No container with this ID exists in the runtime.
    NotFound,
}

/// Errors a runtime can return. Variants are matched on by callers, so this
/// is a real public API surface — keep it small and stable.
///
/// Rust idiom — `thiserror`: `#[derive(Error)]` generates the `Display` +
/// `std::error::Error` impls from the `#[error("...")]` format strings, so we
/// get a typed, matchable error enum without hand-writing boilerplate. (Use
/// `thiserror` for *library* errors you match on; `anyhow` for *application*
/// errors you only propagate.)
#[derive(Debug, Error)]
pub enum RuntimeError {
    // `{0:?}` formats the first tuple field with Debug.
    #[error("container {0:?} not found")]
    NotFound(String),

    #[error("container {0:?} already exists")]
    AlreadyExists(String),

    #[error("invalid bundle at {path:?}: {reason}")]
    InvalidBundle {
        path: std::path::PathBuf,
        reason: String,
    },

    /// Catch-all for runtime-internal failures we don't want to model precisely.
    /// `#[from]` auto-generates `From<anyhow::Error>`, so `?` on any `anyhow`
    /// result converts into this variant for free. `{0:#}` prints the full
    /// anyhow cause chain.
    #[error("runtime error: {0:#}")]
    Other(#[from] anyhow::Error),
}

/// Rust idiom — crate-local `Result` alias: shadows `std::result::Result` so
/// every signature in this module can write `Result<T>` and default the error
/// to `RuntimeError`. Common in modules with one dominant error type.
pub type Result<T> = std::result::Result<T, RuntimeError>;

/// The contract every container runtime in this project must implement.
///
/// All methods are sync because the underlying syscalls (fork/exec/clone)
/// are sync; the reconciler bridges to async via `spawn_blocking` if needed.
///
/// All methods take `&mut self` because runtimes typically hold mutable
/// per-container state (open file descriptors, child PIDs, etc.) that
/// can't be safely shared.
pub trait RuntimeClient {
    /// Create a container from an OCI bundle. The bundle directory must
    /// contain a `config.json` and any rootfs referenced from it.
    ///
    /// Idempotency: returns [`RuntimeError::AlreadyExists`] if `id` is in use.
    fn create_container(&mut self, id: &str, bundle_path: &Path) -> Result<()>;

    /// Start a previously-created container's init process.
    fn start_container(&mut self, id: &str) -> Result<()>;

    /// Send `signal` to the container's init process. Use libc constants
    /// (e.g., `libc::SIGTERM`, `libc::SIGKILL`).
    fn kill_container(&mut self, id: &str, signal: i32) -> Result<()>;

    /// Delete a container's runtime state. If `force`, kill the process first.
    /// After this returns successfully, [`Self::container_state`] returns [`ContainerState::NotFound`].
    fn delete_container(&mut self, id: &str, force: bool) -> Result<()>;

    /// Read the current state of a container. Cheap (single /proc read).
    fn container_state(&mut self, id: &str) -> Result<ContainerState>;

    /// Get the PID of the container's init process, or `None` if not running.
    ///
    /// **Critical for the sandbox pattern**: the pause container's PID is what
    /// app containers use as `/proc/{PID}/ns/net` to join its network namespace.
    fn container_pid(&mut self, id: &str) -> Result<Option<u32>>;

    /// Walk the runtime's state directory and rebuild in-memory handles for
    /// every container still present on disk. Called on kubelet startup —
    /// closes the Phase 1 gap where a restart lost all sandbox knowledge.
    ///
    /// Added to the trait in Phase 2, which means every impl must provide it —
    /// the real `YoukiRuntime` scans the state dir, the test `MockRuntime`
    /// just returns an empty `Vec`.
    fn recover_all(&mut self) -> Result<Vec<RecoveredContainer>>;
}

/// What `recover_all` reports per surviving container: enough for the
/// reconciler to decide "reattach this sandbox" vs "destroy this orphan".
/// A plain data struct (no methods) — it's a return value, not a behavior.
#[derive(Debug, Clone)]
pub struct RecoveredContainer {
    pub id: String,
    pub state: ContainerState,
    pub pid: Option<u32>,
}
