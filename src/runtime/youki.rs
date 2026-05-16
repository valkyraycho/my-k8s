use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use libcontainer::{
    container::{Container, ContainerStatus, builder::ContainerBuilder},
    syscall::syscall::SyscallType,
    workload::default::DefaultExecutor,
};
use nix::sys::signal::Signal;

use super::{ContainerState, Result, RuntimeClient, RuntimeError};

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
        let container = match self.containers.get_mut(id) {
            Some(c) => c,
            None => return Ok(ContainerState::NotFound),
        };

        container
            .refresh_status()
            .context("Container::refresh_status")?;

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
}
