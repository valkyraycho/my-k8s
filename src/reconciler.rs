use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::task::block_in_place;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn};

use crate::{
    pod::{Pod, PodName},
    runtime::{ContainerState, RuntimeClient, sandbox::PodSandbox},
    store::{PodState, Store},
    watcher,
};

const TICK: Duration = Duration::from_secs(2);

// CrashLoopBackOff parameters. Production defaults follow real K8s
// (10s base, 5min cap). Tests get tiny values so they run fast.
#[cfg(not(test))]
const BACKOFF_BASE: Duration = Duration::from_secs(10);
#[cfg(test)]
const BACKOFF_BASE: Duration = Duration::from_millis(50);

#[cfg(not(test))]
const BACKOFF_MAX: Duration = Duration::from_secs(300);
#[cfg(test)]
const BACKOFF_MAX: Duration = Duration::from_millis(500);

/// Per-container restart bookkeeping. Lives in the reconciler's `restart_state`
/// map, keyed by container_id ("{pod}__{container}"). Cleared when the container
/// is observed Running (success resets the backoff) or when its Pod is removed.
struct RestartTracker {
    /// Number of restart attempts since last sustained Running.
    restart_count: u32,
    /// Earliest time we're allowed to attempt the next restart.
    next_retry_at: Instant,
}

pub struct Reconciler<R: RuntimeClient> {
    manifests_dir: PathBuf,
    pods_dir: PathBuf,
    rootfs_base: PathBuf,
    store: Store,
    runtime: R,
    restart_state: HashMap<String, RestartTracker>,
}

impl<R: RuntimeClient> Reconciler<R> {
    pub fn new(
        manifests_dir: PathBuf,
        pods_dir: PathBuf,
        rootfs_base: PathBuf,
        runtime: R,
    ) -> Self {
        Self {
            manifests_dir,
            pods_dir,
            rootfs_base,
            store: Store::new(),
            runtime,
            restart_state: HashMap::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(TICK);
        info!("reconciler started");

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.reconcile_once().await {
                        error!(error = ?e, "failed to reconcile");
                    }
                }
            }
        }

        info!("reconciler shutting down; destroying all sandboxes");
        block_in_place(|| self.shutdown());
        Ok(())
    }

    async fn reconcile_once(&mut self) -> Result<()> {
        let desired = watcher::scan(&self.manifests_dir)
            .await
            .context("scanning manifests")?;
        block_in_place(|| self.apply_diff(&desired));
        Ok(())
    }

    fn apply_diff(&mut self, desired: &HashMap<PodName, Pod>) {
        for (name, pod) in desired {
            if !self.store.contains(name) {
                let _span = info_span!("create pod", name = %name).entered();
                if let Err(e) = self.create_pod(pod) {
                    error!(error = ?e, "failed to create pod");
                }
            }
        }

        let gone: Vec<PodName> = self
            .store
            .names()
            .into_iter()
            .filter(|name| !desired.contains_key(name))
            .collect();

        for name in gone {
            let _span = info_span!("remove pod", name = %name).entered();
            if let Err(e) = self.remove_pod(&name) {
                error!(error = ?e, "failed to remove pod");
            }
        }

        for (name, pod) in desired {
            let _span = info_span!("reconcile pod", name = %name).entered();
            if let Err(e) = self.reconcile_liveness(name, pod) {
                error!(error = ?e, "failed to reconcile pod");
            }
        }
    }

    fn create_pod(&mut self, pod: &Pod) -> Result<()> {
        info!("creating pod {}", pod.metadata.name);
        let mut sandbox = PodSandbox::new(
            pod.metadata.name.clone(),
            self.pods_dir.clone(),
            self.rootfs_base.clone(),
        );
        sandbox
            .create(&mut self.runtime)
            .context("create sandbox")?;

        // Past this point, sandbox owns a live pause container. Any failure
        // in add_container would leak it, so we roll back on failure by
        // destroying the partially-built sandbox before returning the error.
        for container in &pod.spec.containers {
            if let Err(e) = sandbox.add_container(&mut self.runtime, container) {
                warn!(
                    error = ?e,
                    container = %container.name,
                    "add_container failed; rolling back partial sandbox",
                );
                let _ = sandbox.destroy(&mut self.runtime);
                return Err(e).with_context(|| format!("add container {}", container.name));
            }
        }

        self.store.insert(PodState {
            pod: pod.clone(),
            sandbox,
        });
        Ok(())
    }

    fn remove_pod(&mut self, name: &str) -> Result<()> {
        info!("removing pod {}", name);
        if let Some(mut state) = self.store.remove(name) {
            // Clear backoff trackers for this pod's containers — otherwise the
            // map grows unboundedly across Pod churn.
            for container in &state.pod.spec.containers {
                let container_id = format!("{name}__{}", container.name);
                self.restart_state.remove(&container_id);
            }
            state.sandbox.destroy(&mut self.runtime)?;
        }
        Ok(())
    }

    fn reconcile_liveness(&mut self, name: &str, pod: &Pod) -> Result<()> {
        // Disjoint borrows: we need &mut for store, runtime, AND restart_state.
        let Self {
            store,
            runtime,
            restart_state,
            ..
        } = self;
        let state = match store.get_mut(name) {
            Some(s) => s,
            None => return Ok(()),
        };

        for container in &pod.spec.containers {
            let container_id = format!("{name}__{}", container.name);
            let s = runtime
                .container_state(&container_id)
                .with_context(|| format!("read state for {container_id}"))?;

            match s {
                ContainerState::Stopped | ContainerState::NotFound => {
                    let tracker =
                        restart_state
                            .entry(container_id.clone())
                            .or_insert_with(|| RestartTracker {
                                restart_count: 0,
                                next_retry_at: Instant::now(),
                            });

                    // In backoff? Skip this tick.
                    let now = Instant::now();
                    if now < tracker.next_retry_at {
                        let remaining = tracker.next_retry_at.duration_since(now);
                        warn!(
                            container = %container.name,
                            restart_count = tracker.restart_count,
                            backoff_remaining_secs = remaining.as_secs(),
                            "container stopped but in CrashLoopBackOff; skipping restart",
                        );
                        continue;
                    }

                    // Bump count and schedule the next backoff window BEFORE
                    // attempting the restart, so a crash-then-recover loop
                    // can't bypass backoff by failing the restart itself.
                    tracker.restart_count += 1;
                    let backoff = compute_backoff(tracker.restart_count);
                    tracker.next_retry_at = now + backoff;

                    warn!(
                        container = %container.name,
                        restart_count = tracker.restart_count,
                        next_backoff_secs = backoff.as_secs(),
                        "restarting container",
                    );

                    let _ = state.sandbox.remove_container(runtime, &container.name);
                    state
                        .sandbox
                        .add_container(runtime, container)
                        .with_context(|| format!("restart container {}", container.name))?;
                }
                ContainerState::Running => {
                    // Container is alive — clear backoff so the next crash
                    // (if any) gets an immediate restart attempt.
                    if restart_state.remove(&container_id).is_some() {
                        info!(container = %container.name, "container Running; cleared backoff");
                    }
                }
                ContainerState::Created => {
                    // Just-created; hasn't reached Running yet. Let it cook.
                }
            }
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        for (name, mut state) in self.store.drain() {
            if let Err(e) = state.sandbox.destroy(&mut self.runtime) {
                warn!(error = ?e, pod = %name, "failed to destroy sandbox");
            }
        }
        self.restart_state.clear();
    }
}

/// Exponential backoff: BASE * 2^(n-1), capped at MAX.
/// n=1 → BASE, n=2 → 2*BASE, ... until we hit the cap.
fn compute_backoff(restart_count: u32) -> Duration {
    // Cap the exponent so the shift doesn't overflow. After ~20 doublings
    // we'd hit BACKOFF_MAX anyway.
    let exp = restart_count.saturating_sub(1).min(20);
    let multiplier = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
    let base_micros = BACKOFF_BASE.as_micros() as u64;
    let micros = base_micros.saturating_mul(multiplier);
    Duration::from_micros(micros).min(BACKOFF_MAX)
}
