use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// my-k8s kubelet — watches a manifests directory and runs the Pods inside it.
#[derive(Debug, Parser)]
#[command(name = "kubelet", version)]
struct Args {
    /// Directory containing Pod manifest YAML files (the "desired state").
    /// Created on startup if missing.
    #[arg(long, default_value = "./manifests/active")]
    manifests_dir: PathBuf,

    /// Where libcontainer keeps per-container state (analogous to runc's --root).
    /// Created on startup if missing.
    #[arg(long, default_value = "/var/lib/my-k8s/state")]
    state_dir: PathBuf,

    /// Read-only base rootfs shared by every container.                                                    
    /// MUST exist; prepared once via `scripts/prepare-rootfs.sh`.
    #[arg(long, default_value = "/var/lib/my-k8s/rootfs-base")]
    rootfs_base: PathBuf,
}

impl Args {
    fn validate_and_prepare(&self) -> Result<()> {
        ensure_dir(&self.manifests_dir).context("preparing manifests dir")?;
        ensure_dir(&self.state_dir).context("preparing state dir")?;
        if !self.rootfs_base.is_dir() {
            bail!(
                "rootfs-base {:?} does not exist or is not a directory.\n\
                   Hint: run `sudo bash scripts/prepare-rootfs.sh` to create it.",
                self.rootfs_base,
            )
        }
        Ok(())
    }
}

fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("creating directory {path:?}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    args.validate_and_prepare()?;
    info!(?args, "kubelet starting");

    Ok(())
}
