//! The real [`RuntimeClient`] impl: a thin adapter over youki's `libcontainer`.
//! This is the ONLY module that touches libcontainer — everything above the
//! trait stays runtime-agnostic (and test-mockable).

use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use libcontainer::{
    container::{Container, ContainerStatus, builder::ContainerBuilder},
    syscall::syscall::SyscallType,
    workload::default::DefaultExecutor,
};
use nix::sys::signal::Signal;

use crate::runtime::RecoveredContainer;

use super::{ContainerState, Result, RuntimeClient, RuntimeError};

/// `state_dir` is the on-disk half (libcontainer's `--root`, where per-container
/// `state.json` lives); the `HashMap` is the in-memory half — the live
/// `Container` handles, which hold open fds and aren't cheap to rebuild. We
/// cache them so every call after `create` is a map lookup, not a reload.
pub struct YoukiRuntime {
    state_dir: PathBuf,
    containers: HashMap<String, Container>,
}

impl YoukiRuntime {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            containers: HashMap::new(),
        }
    }
}

impl RuntimeClient for YoukiRuntime {
    fn create_container(&mut self, id: &str, bundle_path: &std::path::Path) -> Result<()> {
        if self.containers.contains_key(id) {
            return Err(RuntimeError::AlreadyExists(id.to_string()));
        }

        let container = ContainerBuilder::new(id.to_owned(), SyscallType::default())
            .with_root_path(self.state_dir.clone())
            .context("ContainerBuilder::with_root_path")?
            .with_executor(DefaultExecutor {})
            .as_init(bundle_path)
            .with_systemd(false)
            .build()
            .context("ContainerBuilder::build")?;

        self.containers.insert(id.to_string(), container);
        Ok(())
    }

    fn start_container(&mut self, id: &str) -> Result<()> {
        let container = self
            .containers
            .get_mut(id)
            .ok_or_else(|| RuntimeError::NotFound(id.to_string()))?;
        container.start().context("Container::start")?;
        Ok(())
    }

    fn kill_container(&mut self, id: &str, signal: i32) -> Result<()> {
        let container = self
            .containers
            .get_mut(id)
            .ok_or_else(|| RuntimeError::NotFound(id.to_string()))?;
        // Boundary conversion: the trait takes a raw `i32` so callers can pass
        // `libc::SIGTERM` without depending on `nix`; libcontainer wants a typed
        // `nix::Signal`. `TryFrom` is the fallible-conversion idiom (an invalid
        // signal number is an error, not a panic).
        let sig =
            Signal::try_from(signal).with_context(|| format!("invalid signal number {signal}"))?;
        container.kill(sig, false).context("Container::kill")?;
        Ok(())
    }

    fn delete_container(&mut self, id: &str, force: bool) -> Result<()> {
        let mut container = self
            .containers
            .remove(id)
            .ok_or_else(|| RuntimeError::NotFound(id.to_string()))?;
        container.delete(force).context("Container::delete")?;
        Ok(())
    }

    fn container_state(&mut self, id: &str) -> Result<ContainerState> {
        // Unknown id is a valid answer (NotFound), not an error — so the
        // reconciler's liveness check can treat "gone" as "needs restart".
        let container = match self.containers.get_mut(id) {
            Some(c) => c,
            None => return Ok(ContainerState::NotFound),
        };

        // `refresh_status()` re-reads /proc — cheap, but not free; this is the
        // polling primitive liveness reconciliation calls once per tick.
        container
            .refresh_status()
            .context("Container::refresh_status")?;

        // Flatten libcontainer's 5 statuses into our 4: the orchestrator never
        // pauses, and Creating/Created are a transient distinction it ignores.
        // An exhaustive match (no `_` arm) means a new libcontainer status would
        // force a compile error here — deliberate, so we can't silently mishandle.
        Ok(match container.status() {
            ContainerStatus::Created | ContainerStatus::Creating => ContainerState::Created,
            ContainerStatus::Running | ContainerStatus::Paused => ContainerState::Running,
            ContainerStatus::Stopped => ContainerState::Stopped,
        })
    }

    fn container_pid(&mut self, id: &str) -> Result<Option<u32>> {
        let container = self
            .containers
            .get_mut(id)
            .ok_or_else(|| RuntimeError::NotFound(id.to_string()))?;
        Ok(container.pid().map(|p| p.as_raw() as u32))
    }

    /// Rebuild handles from libcontainer's on-disk state after a kubelet
    /// restart. `Container::load(dir)` reconstructs a handle from the
    /// `state.json` the kernel-side bookkeeping left behind — that's what lets
    /// us re-adopt still-running containers instead of orphaning them.
    fn recover_all(&mut self) -> Result<Vec<RecoveredContainer>> {
        let mut recovered = Vec::new();
        // No state dir yet (first boot) → nothing to recover, not an error.
        if !self.state_dir.exists() {
            return Ok(recovered);
        }

        let entries = std::fs::read_dir(&self.state_dir)
            .with_context(|| format!("reading state dir {:?}", self.state_dir))?;

        for entry in entries {
            let entry = entry.context("reading state dir entry")?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }

            let container_root = entry.path();

            if !container_root.join("state.json").exists() {
                continue;
            }

            let id = entry.file_name().to_string_lossy().into_owned();

            let container = Container::load(container_root)
                .with_context(|| format!("Container::load for {id}"))?;

            let state = match container.status() {
                ContainerStatus::Created | ContainerStatus::Creating => ContainerState::Created,
                ContainerStatus::Running | ContainerStatus::Paused => ContainerState::Running,
                ContainerStatus::Stopped => ContainerState::Stopped,
            };

            let pid = container.pid().map(|p| p.as_raw() as u32);

            self.containers.insert(id.clone(), container);
            recovered.push(RecoveredContainer { id, state, pid });
        }

        Ok(recovered)
    }
}
