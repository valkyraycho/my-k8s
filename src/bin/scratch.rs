//! Phase 0 smoke test: confirm libcontainer integration by running a busybox
//! container end-to-end. THROWAWAY — delete once Phase 1 has a real runtime
//! abstraction.
//!
//! Prereq: run `scripts/prepare-bundle.sh` first to create /tmp/scratch-bundle.
//! Then `sudo cargo run --bin scratch` (root needed for namespace creation
//! unless user namespaces are configured).

use anyhow::{Context, Result, bail};
use libcontainer::container::ContainerStatus;
use libcontainer::container::builder::ContainerBuilder;
use libcontainer::syscall::syscall::SyscallType;
use libcontainer::workload::default::DefaultExecutor;
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

const CONTAINER_ID: &str = "scratch-test";

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let bundle = PathBuf::from("/tmp/scratch-bundle");
    let state = PathBuf::from("/tmp/scratch-state");

    if !bundle.join("config.json").exists() {
        bail!("missing {:?}/config.json; run scripts/prepare-bundle.sh first", bundle);
    }
    std::fs::create_dir_all(&state)?;

    let _ = std::fs::remove_dir_all(state.join(CONTAINER_ID));

    info!(?bundle, ?state, "building container");
    let mut container = ContainerBuilder::new(CONTAINER_ID.to_owned(), SyscallType::default())
        .with_root_path(state.clone())
        .context("with_root_path")?
        .with_executor(DefaultExecutor {})
        .as_init(&bundle)
        .with_systemd(false)
        .build()
        .context("build container")?;

    info!("starting container");
    container.start().context("start container")?;

    loop {
        container.refresh_status().context("refresh_status")?;
        if matches!(container.status(), ContainerStatus::Stopped) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    info!(status = ?container.status(), "container exited");
    container.delete(true).ok();
    Ok(())
}
