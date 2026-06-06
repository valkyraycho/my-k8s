use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::task::block_in_place;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn};

use crate::{
    apiserver::watch::{WatchEvent, WatchEventType},
    client::{Client, ClientError},
    pod::{ContainerStatus, ContainerStatusState, Pod, PodName, PodPhase, PodStatus},
    runtime::{ContainerState, RecoveredContainer, RuntimeClient, sandbox::PodSandbox},
    store::{PodState, Store},
};

/// How often to poll container states and restart crashed ones.
/// 2s matches Phase 1 — fast enough to feel responsive, cheap enough.
const LIVENESS_INTERVAL: Duration = Duration::from_secs(2);

/// How often to relist from the apiserver as a defense against missed watch
/// events. Real K8s informers default to 10 minutes; 30s is fine for dev.
const RESYNC_INTERVAL: Duration = Duration::from_secs(30);

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

/// The kubelet's brain. Generic over `R: RuntimeClient` (static dispatch) so
/// the real `YoukiRuntime` and the test `MockRuntime` exercise the SAME logic.
///
/// Two maps that look similar but differ: `cache` is desired state (mirrors the
/// apiserver, fed by watch/resync); `store` is actual state (live sandboxes).
/// `restart_state` tracks CrashLoopBackOff; `last_pushed_status` dedups status
/// PUTs so we only report on change, not every tick.
pub struct Reconciler<R: RuntimeClient> {
    client: Arc<Client>,
    pods_dir: PathBuf,
    rootfs_base: PathBuf,
    store: Store,
    runtime: R,
    restart_state: HashMap<String, RestartTracker>,
    debug_dump_path: Option<PathBuf>,
    cache: HashMap<PodName, Pod>,
    last_pushed_status: HashMap<PodName, PodStatus>,
}

impl<R: RuntimeClient> Reconciler<R> {
    pub fn new(
        client: Arc<Client>,
        pods_dir: PathBuf,
        rootfs_base: PathBuf,
        runtime: R,
        debug_dump_path: Option<PathBuf>,
    ) -> Self {
        Self {
            client,
            pods_dir,
            rootfs_base,
            store: Store::new(),
            runtime,
            restart_state: HashMap::new(),
            debug_dump_path,
            cache: HashMap::new(),
            last_pushed_status: HashMap::new(),
        }
    }

    /// The informer loop. Takes `mut self` by value (it runs until shutdown,
    /// then drops everything), driven by three independent clocks plus cancel.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        info!("reconciler starting");

        if let Err(e) = self.startup().await {
            error!(error = ?e, "startup failed");
            return Err(e);
        }

        let mut watch_stream = self
            .client
            .watch_pods(Some("0"))
            .await
            .context("opening watch stream")?;

        // First `.tick()` on a tokio interval fires IMMEDIATELY; we await-and-
        // discard it here so the real loop doesn't do a resync + liveness pass
        // the instant it starts (we just did startup()).
        let mut liveness_interval = tokio::time::interval(LIVENESS_INTERVAL);
        let mut resync_interval = tokio::time::interval(RESYNC_INTERVAL);
        resync_interval.tick().await;
        liveness_interval.tick().await;

        info!("reconciler entered steady state");

        loop {
            // `select!` races all arms; `biased` makes it check them top-to-
            // bottom in order instead of randomly — so cancel always wins over
            // doing one more tick. The three concerns are deliberately separate
            // clocks: watch = react to spec changes fast; resync = correctness
            // backstop; liveness = container health.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                // `.next()` on the watch Stream yields Option<Result<Event>>.
                event = watch_stream.next() => match event {
                    // `block_in_place`: apply_watch_event does sync libcontainer
                    // work; this tells tokio "this worker thread is going
                    // blocking, move other tasks off it" (multi_thread runtime only).
                    Some(Ok(event)) => block_in_place(|| self.apply_watch_event(event)),
                    Some(Err(e)) => warn!(error = ?e, "watch stream error; resync will resseed"),
                    None => warn!("watch stream closed; resync will reseed"),
                },
                  _ = resync_interval.tick() => {
                      if let Err(e) = self.resync().await {
                          error!(error = ?e, "resync failed");
                      }
                  }
                  _ = liveness_interval.tick() => {
                    // Sync/async split: compute the dirty set under block_in_place
                    // (blocking runtime calls), then `.await` the HTTP status
                    // pushes OUTSIDE it — you must never `.await` inside
                    // block_in_place.
                    let dirty = block_in_place(|| self.tick_liveness());
                    for (name, status) in dirty {
                        if let Err(e) = self.push_status(&name, &status).await {
                            error!(error = ?e, "push status failed");
                        }
                    }
                  },
            }
        }

        info!("reconciler shutting down; destroying all sandboxes");
        block_in_place(|| self.shutdown());
        Ok(())
    }

    async fn startup(&mut self) -> Result<()> {
        let recovered = block_in_place(|| self.runtime.recover_all())
            .context("recover containers from runtime state dir")?;
        info!(
            count = recovered.len(),
            "recovered containers from runtime state"
        );

        let initial_pods = self
            .client
            .list_pods()
            .await
            .context("initial list of pods from apiserver")?;
        info!(count = initial_pods.len(), "listed pods from apiserver");

        // Group recovered containers by pod name (the "{pod}__" prefix).
        let mut by_pod: HashMap<String, Vec<RecoveredContainer>> = HashMap::new();
        for rc in recovered {
            if let Some((pod_name, _)) = rc.id.split_once("__") {
                by_pod.entry(pod_name.to_string()).or_default().push(rc);
            }
        }

        for pod in initial_pods {
            let name = pod.metadata.name.clone();
            let pause_id = format!("{name}__pause");

            let recovered_for_pod = by_pod.remove(&name);
            match recovered_for_pod
                .as_ref()
                .and_then(|rcs| rcs.iter().find(|rc| rc.id == pause_id))
                .and_then(|p| p.pid)
            {
                Some(pause_pid) => {
                    let app_names: Vec<String> = recovered_for_pod
                        .unwrap()
                        .into_iter()
                        .filter(|rc| rc.id != pause_id)
                        .filter_map(|rc| rc.id.split_once("__").map(|(_, n)| n.to_string()))
                        .collect();
                    info!(pod = %name, pause_pid, apps = app_names.len(),
                            "reattached recovered sandbox");
                    let sandbox = PodSandbox::from_recovered(
                        name.clone(),
                        pause_pid,
                        app_names,
                        self.pods_dir.clone(),
                        self.rootfs_base.clone(),
                    );
                    self.store.insert(PodState {
                        pod: pod.clone(),
                        sandbox,
                    });
                }
                None => {
                    info!(pod = %name, "no recovered pause; creating fresh sandbox");
                    if let Err(e) = block_in_place(|| self.create_pod(&pod)) {
                        error!(error = ?e, pod = %name, "create failed during startup");
                    }
                }
            }
            self.cache.insert(name, pod);
        }

        for (orphan_pod, containers) in by_pod {
            warn!(pod = %orphan_pod, count = containers.len(),
                    "orphan containers (not in apiserver); destroying");
            for rc in containers {
                if let Err(e) = self.runtime.delete_container(&rc.id, true) {
                    error!(error = ?e, id = %rc.id, "failed to delete orphan");
                }
            }
        }

        Ok(())
    }

    fn apply_watch_event(&mut self, event: WatchEvent<Pod>) {
        let name = event.object.metadata.name.clone();
        let _span = info_span!("watch", pod = %name, ty = ?event.event_type).entered();
        match event.event_type {
            WatchEventType::Added | WatchEventType::Modified => {
                let pod = event.object;
                let is_new = !self.cache.contains_key(&name);
                self.cache.insert(name.clone(), pod.clone());
                if is_new && let Err(e) = self.create_pod(&pod) {
                    error!(error = ?e, "failed to create pod");
                }
            }
            WatchEventType::Deleted => {
                self.cache.remove(&name);
                if let Err(e) = self.remove_pod(&name) {
                    error!(error = ?e, "failed to remove pod");
                }
            }
        }
    }

    async fn resync(&mut self) -> Result<()> {
        let pods = self.client.list_pods().await.context("resync list")?;
        let desired: HashMap<PodName, Pod> = pods
            .into_iter()
            .map(|p| (p.metadata.name.clone(), p))
            .collect();
        self.cache = desired.clone();
        block_in_place(|| self.apply_diff(&desired));
        Ok(())
    }

    /// Runs every 2s. Returns the *dirty set* — Pods whose freshly-computed
    /// status differs from what we last pushed — so the caller only PUTs
    /// changes (level-triggered reporting), not an identical status every tick.
    fn tick_liveness(&mut self) -> Vec<(PodName, PodStatus)> {
        // Collect names up front: we iterate while `reconcile_liveness` mutably
        // borrows `self`, so we can't hold an iterator into `self.cache`.
        let names = self.cache.keys().cloned().collect::<Vec<PodName>>();
        let mut dirty: Vec<(PodName, PodStatus)> = Vec::new();

        for name in names {
            // let-else: bind `pod` or bail this iteration. Cleaner than
            // `match { Some => .., None => continue }`.
            let Some(pod) = self.cache.get(&name).cloned() else {
                continue;
            };

            // `.entered()` attaches this span to the current thread for the
            // rest of the scope, so every log inside is tagged `pod=<name>`.
            let _span = info_span!("liveness", pod = %name).entered();
            if let Err(e) = self.reconcile_liveness(&name, &pod) {
                error!(error = ?e, "liveness failed");
            }

            // The dedup check. `Some(&&new_status)`: `.get()` gives
            // `Option<&PodStatus>`, `.as_ref()` makes it `Option<&&PodStatus>`,
            // so we compare against a `&&`. Differs (or absent) → dirty.
            let new_status = self.compute_pod_status(&pod);
            if self.last_pushed_status.get(&name).as_ref() != Some(&&new_status) {
                dirty.push((name.clone(), new_status));
            }
        }

        if self.debug_dump_path.is_some()
            && let Err(e) = self.write_debug_snapshot()
        {
            warn!(error = ?e, "failed to write debug snapshot");
        }

        dirty
    }

    /// Roll the live container states up into one Pod `PodStatus`. The phase
    /// precedence (below) mirrors real K8s: "not fully up" dominates "up".
    fn compute_pod_status(&mut self, pod: &Pod) -> PodStatus {
        let name = &pod.metadata.name;
        // `with_capacity`: we know exactly how many we'll push — one alloc, no
        // re-growth. A small but idiomatic perf habit when the size is known.
        let mut container_statuses = Vec::with_capacity(pod.spec.containers.len());
        let mut any_waiting = false;
        let mut any_running = false;
        // Starts true ONLY if there are containers; an empty Pod isn't "all
        // terminated". Each non-Stopped container flips it false below.
        let mut all_terminated = !pod.spec.containers.is_empty();

        for c in &pod.spec.containers {
            let id = format!("{}__{}", name, c.name);
            let state = self
                .runtime
                .container_state(&id)
                .unwrap_or(ContainerState::NotFound);
            let restart_count = self
                .restart_state
                .get(&id)
                .map(|t| t.restart_count)
                .unwrap_or(0);

            let (cs_state, ready) = match state {
                ContainerState::Running => {
                    any_running = true;
                    all_terminated = false;
                    (
                        ContainerStatusState::Running {
                            started_at: "unknown".into(),
                        },
                        true,
                    )
                }
                ContainerState::Created | ContainerState::NotFound => {
                    any_waiting = true;
                    all_terminated = false;
                    (ContainerStatusState::Waiting, false)
                }
                // Placeholders: we don't yet capture real exit codes (always 0)
                // or start timestamps ("unknown" above). Honest Phase 2 shortcuts.
                ContainerState::Stopped => {
                    (ContainerStatusState::Terminated { exit_code: 0 }, false)
                }
            };

            container_statuses.push(ContainerStatus {
                name: c.name.clone(),
                state: cs_state,
                restart_count,
                ready,
            });
        }

        // Precedence ORDER matters: a Pod with one container still coming up is
        // Pending even if a sibling is already Running. Checked waiting-first.
        let phase = if any_waiting {
            PodPhase::Pending
        } else if all_terminated {
            PodPhase::Failed
        } else if any_running {
            PodPhase::Running
        } else {
            PodPhase::Unknown
        };

        PodStatus {
            phase,
            container_statuses,
            observed_generation: pod.metadata.generation,
        }
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

        if self.debug_dump_path.is_some()
            && let Err(e) = self.write_debug_snapshot()
        {
            warn!(error = ?e, "failed to write debug snapshot");
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
            self.last_pushed_status.remove(name);
            state.sandbox.destroy(&mut self.runtime)?;
        }
        Ok(())
    }

    fn reconcile_liveness(&mut self, name: &str, pod: &Pod) -> Result<()> {
        // Rust idiom — DISJOINT BORROWS via destructuring. We need `&mut` on
        // three fields at once (store, runtime, restart_state). Calling
        // `self.store.get_mut()` then `self.runtime...` would fail: the first
        // borrows all of `*self`. Destructuring `let Self { .. } = self` splits
        // it into three independent field borrows the checker accepts. `..`
        // ignores the fields we don't touch here.
        let Self {
            store,
            runtime,
            restart_state,
            ..
        } = self;
        // Pod might have been removed between cache read and here — `None` is
        // fine, just nothing to reconcile.
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
                    // `entry(...).or_insert_with(...)`: get the tracker, or
                    // create one on first crash — the canonical "upsert into a
                    // map" idiom, one lookup instead of contains+insert.
                    let tracker = restart_state
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

    fn write_debug_snapshot(&self) -> Result<()> {
        let path = match &self.debug_dump_path {
            Some(p) => p,
            None => return Ok(()),
        };

        let now = Instant::now();
        let pods: Vec<_> = self
            .store
            .names()
            .into_iter()
            .map(|name| {
                let state = self.store.get(&name).expect("name came from store");
                let containers: Vec<_> = state
                    .pod
                    .spec
                    .containers
                    .iter()
                    .map(|c| {
                        let id = format!("{name}__{}", c.name);
                        let tracker = self.restart_state.get(&id);
                        serde_json::json!({
                            "name": c.name,
                            "command": c.command,
                            "restart_count": tracker.map(|t| t.restart_count).unwrap_or(0),
                            "backoff_remaining_secs": tracker.map(|t| {
                                t.next_retry_at.saturating_duration_since(now).as_secs()
                            }).unwrap_or(0),
                        })
                    })
                    .collect();
                serde_json::json!({
                    "name": name,
                    "pause_pid": state.sandbox.pause_pid(),
                    "containers": containers,
                })
            })
            .collect();

        let snapshot = serde_json::json!({
            "ts_unix": SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
            "pod_count": self.store.len(),
            "pods": pods,
        });

        std::fs::write(path, serde_json::to_string_pretty(&snapshot)?)
            .with_context(|| format!("writing debug snapshot to {path:?}"))?;
        Ok(())
    }

    /// Write observed status to the apiserver, optimistic-concurrency style.
    /// The cached rv goes stale whenever the apiserver advances it (a spec edit,
    /// or our own resync), so a Conflict is EXPECTED, not exceptional — we
    /// refetch the fresh rv and retry ONCE. A single retry (not a loop) avoids
    /// livelock against a hot-edited Pod; the 2s tick provides natural backoff.
    async fn push_status(&mut self, name: &PodName, status: &PodStatus) -> Result<()> {
        // `and_then` chains the Options: no cached pod OR no rv yet → skip
        // (nothing to write against). Flattens two `Option` layers cleanly.
        let rv = match self
            .cache
            .get(name)
            .and_then(|p| p.metadata.resource_version.clone())
        {
            Some(rv) => rv,
            None => return Ok(()),
        };

        // Match on the specific error VARIANT: only Conflict triggers the
        // refetch-retry; any other error propagates.
        match self.client.replace_pod_status(name, status, &rv).await {
            Ok(updated) => {
                // Server echoes back the pod with its NEW rv — refresh the cache
                // so the next push uses an up-to-date version.
                self.cache.insert(name.clone(), updated);
                self.last_pushed_status.insert(name.clone(), status.clone());
                Ok(())
            }
            Err(ClientError::Conflict { .. }) => {
                let latest = self
                    .client
                    .get_pod(name)
                    .await
                    .context("refetch on status conflict")?;
                let Some(latest) = latest else {
                    return Ok(());
                };
                let new_rv = latest
                    .metadata
                    .resource_version
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("refetched pod missing rv"))?;
                self.cache.insert(name.clone(), latest);
                let updated = self
                    .client
                    .replace_pod_status(name, status, &new_rv)
                    .await
                    .context("status push retry after conflict")?;

                self.cache.insert(name.clone(), updated);
                self.last_pushed_status.insert(name.clone(), status.clone());
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(e)).context("status push "),
        }
    }
}

/// Exponential backoff: BASE * 2^(n-1), capped at MAX.
/// n=1 → BASE, n=2 → 2*BASE, ... until we hit the cap.
///
/// Note the defensive integer math — overflow here would be a real bug:
/// `saturating_sub` (n−1 won't underflow at n=0), `.min(20)` caps the shift
/// (`1 << 64` is UB-adjacent; `checked_shl` returns None past the width, and
/// `.unwrap_or(u64::MAX)` saturates), `saturating_mul` clamps instead of
/// wrapping. The final `.min(BACKOFF_MAX)` enforces the cap.
fn compute_backoff(restart_count: u32) -> Duration {
    let exp = restart_count.saturating_sub(1).min(20);
    let multiplier = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
    let base_micros = BACKOFF_BASE.as_micros() as u64;
    let micros = base_micros.saturating_mul(multiplier);
    Duration::from_micros(micros).min(BACKOFF_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::Path;

    use crate::pod::{Container, PodMetadata, PodSpec};
    use crate::runtime::{RecoveredContainer, Result as RuntimeResult, RuntimeError};

    /// Mock runtime. Records every call. Killed containers automatically
    /// report Stopped on subsequent state queries (simulates termination)
    /// so tests don't have to manually set up state sequences for the
    /// sandbox's grace-period polling.
    #[derive(Default)]
    struct MockRuntime {
        calls: Vec<String>,
        /// Explicit FIFO state overrides per container_id. Consumed before
        /// the killed/default logic kicks in.
        state_seq: HashMap<String, Vec<ContainerState>>,
        pids: HashMap<String, u32>,
        /// IDs whose create_container should return Err (for rollback tests).
        create_should_fail: HashSet<String>,
        /// Containers that have been killed but not yet recreated. They
        /// report Stopped on state queries until delete+create cycles them.
        killed: HashSet<String>,
    }

    impl RuntimeClient for MockRuntime {
        fn create_container(&mut self, id: &str, _bundle_path: &Path) -> RuntimeResult<()> {
            self.calls.push(format!("create({id})"));
            if self.create_should_fail.contains(id) {
                return Err(RuntimeError::Other(anyhow::anyhow!(
                    "injected create failure"
                )));
            }
            self.killed.remove(id); // fresh container — no longer "killed"
            Ok(())
        }

        fn start_container(&mut self, id: &str) -> RuntimeResult<()> {
            self.calls.push(format!("start({id})"));
            Ok(())
        }

        fn kill_container(&mut self, id: &str, signal: i32) -> RuntimeResult<()> {
            self.calls.push(format!("kill({id},{signal})"));
            self.killed.insert(id.to_string());
            Ok(())
        }

        fn delete_container(&mut self, id: &str, force: bool) -> RuntimeResult<()> {
            self.calls.push(format!("delete({id},force={force})"));
            self.killed.remove(id); // gone
            Ok(())
        }

        fn container_state(&mut self, id: &str) -> RuntimeResult<ContainerState> {
            self.calls.push(format!("state({id})"));
            let seq = self.state_seq.entry(id.to_string()).or_default();
            if !seq.is_empty() {
                return Ok(seq.remove(0));
            }
            if self.killed.contains(id) {
                return Ok(ContainerState::Stopped);
            }
            Ok(ContainerState::Running)
        }

        fn container_pid(&mut self, id: &str) -> RuntimeResult<Option<u32>> {
            self.calls.push(format!("pid({id})"));
            Ok(self.pids.get(id).copied())
        }

        fn recover_all(&mut self) -> RuntimeResult<Vec<RecoveredContainer>> {
            // Reconciler tests never exercise restart recovery directly;
            // they build sandboxes through the normal create() path.
            Ok(Vec::new())
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("my-k8s-test-reconciler-{label}-{pid}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_pod(name: &str, containers: Vec<(&str, Vec<&str>)>) -> Pod {
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: PodMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: PodSpec {
                containers: containers
                    .into_iter()
                    .map(|(cname, cmd)| Container {
                        name: cname.into(),
                        image: "busybox".into(),
                        command: cmd.into_iter().map(String::from).collect(),
                    })
                    .collect(),
                node_name: None,
            },
            status: None,
        }
    }

    fn make_reconciler(label: &str) -> Reconciler<MockRuntime> {
        let pods_dir = unique_temp_dir(label);
        let rootfs = std::env::temp_dir().join("nonexistent-rootfs");
        // Dummy Client — tests drive apply_diff directly and never hit HTTP.
        // Pointing at a closed port catches any accidental call with a fast
        // ConnectionRefused instead of hanging.
        let client = Arc::new(Client::new("http://127.0.0.1:1"));
        Reconciler::new(client, pods_dir, rootfs, MockRuntime::default(), None)
    }

    fn desired_map(pods: Vec<Pod>) -> HashMap<PodName, Pod> {
        pods.into_iter()
            .map(|p| (p.metadata.name.clone(), p))
            .collect()
    }

    /// Count how many times a specific call string appears in the recorded log.
    fn count_calls(r: &Reconciler<MockRuntime>, needle: &str) -> usize {
        r.runtime.calls.iter().filter(|c| *c == needle).count()
    }

    #[test]
    fn empty_desired_with_empty_store_is_noop() {
        let mut r = make_reconciler("empty");
        r.apply_diff(&desired_map(vec![]));
        assert!(r.runtime.calls.is_empty());
        assert!(r.store.is_empty());
    }

    #[test]
    fn new_pod_creates_sandbox_then_containers() {
        let mut r = make_reconciler("new-pod");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd", "-f"])]);

        r.apply_diff(&desired_map(vec![pod]));

        assert!(r.store.contains("web"));
        // Verify lifecycle order: pause must be created+started+pid'd before app.
        let calls = &r.runtime.calls;
        let create_pause = calls
            .iter()
            .position(|c| c == "create(web__pause)")
            .unwrap();
        let start_pause = calls.iter().position(|c| c == "start(web__pause)").unwrap();
        let pid_pause = calls.iter().position(|c| c == "pid(web__pause)").unwrap();
        let create_app = calls
            .iter()
            .position(|c| c == "create(web__server)")
            .unwrap();
        let start_app = calls
            .iter()
            .position(|c| c == "start(web__server)")
            .unwrap();
        assert!(create_pause < start_pause);
        assert!(start_pause < pid_pause);
        assert!(pid_pause < create_app);
        assert!(create_app < start_app);
    }

    #[test]
    fn removed_pod_destroys_sandbox() {
        let mut r = make_reconciler("remove");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![pod]));
        assert!(r.store.contains("web"));

        // Tick 2: pod no longer in desired.
        r.apply_diff(&desired_map(vec![]));
        assert!(!r.store.contains("web"), "pod should be removed");
        // Pause must have been deleted as part of sandbox.destroy.
        assert!(
            r.runtime
                .calls
                .contains(&"delete(web__pause,force=true)".to_string()),
            "expected pause delete; got calls: {:?}",
            r.runtime.calls,
        );
    }

    #[test]
    fn stopped_container_triggers_restart() {
        let mut r = make_reconciler("restart");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![pod.clone()]));

        r.runtime.calls.clear();
        // First call after clear: container is observed Stopped.
        r.runtime
            .state_seq
            .insert("web__server".into(), vec![ContainerState::Stopped]);

        r.apply_diff(&desired_map(vec![pod]));

        // Liveness saw Stopped → restart path: delete + create + start.
        assert_eq!(count_calls(&r, "delete(web__server,force=true)"), 1);
        assert_eq!(count_calls(&r, "create(web__server)"), 1);
        assert_eq!(count_calls(&r, "start(web__server)"), 1);
        // Tracker should now exist.
        assert!(r.restart_state.contains_key("web__server"));
    }

    #[test]
    fn backoff_skips_restart_within_window_then_fires_after_expiry() {
        let mut r = make_reconciler("backoff");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![pod.clone()]));

        // Stays stopped across many ticks.
        r.runtime.state_seq.insert(
            "web__server".into(),
            std::iter::repeat_n(ContainerState::Stopped, 20).collect(),
        );

        // Tick A: first observation → restart (count=1, next backoff = 50ms in test mode).
        r.apply_diff(&desired_map(vec![pod.clone()]));
        let restarts_after_a = count_calls(&r, "create(web__server)");

        // Tick B: immediately again — still in backoff, NO additional restart.
        r.apply_diff(&desired_map(vec![pod.clone()]));
        let restarts_after_b = count_calls(&r, "create(web__server)");
        assert_eq!(
            restarts_after_a, restarts_after_b,
            "tick within backoff window must NOT trigger an additional restart",
        );

        // Sleep past the backoff window (BACKOFF_BASE in test = 50ms).
        std::thread::sleep(Duration::from_millis(80));

        // Tick C: backoff expired → restart fires again.
        r.apply_diff(&desired_map(vec![pod]));
        let restarts_after_c = count_calls(&r, "create(web__server)");
        assert_eq!(
            restarts_after_c,
            restarts_after_b + 1,
            "after backoff expires, restart should fire exactly once more",
        );
    }

    #[test]
    fn running_container_clears_backoff_tracker() {
        let mut r = make_reconciler("clear");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![pod.clone()]));

        // Trigger a restart so a tracker exists.
        r.runtime
            .state_seq
            .insert("web__server".into(), vec![ContainerState::Stopped]);
        r.apply_diff(&desired_map(vec![pod.clone()]));
        assert!(r.restart_state.contains_key("web__server"));

        // Now container is back to Running.
        r.runtime
            .state_seq
            .insert("web__server".into(), vec![ContainerState::Running]);
        r.apply_diff(&desired_map(vec![pod]));
        assert!(
            !r.restart_state.contains_key("web__server"),
            "tracker should be cleared after observing Running",
        );
    }

    #[test]
    fn partial_create_failure_rolls_back_sandbox() {
        let mut r = make_reconciler("rollback");
        r.runtime.pids.insert("web__pause".into(), 4242);
        // Second app container's create fails — first one will have already started.
        r.runtime.create_should_fail.insert("web__second".into());
        let pod = make_pod(
            "web",
            vec![("first", vec!["ok"]), ("second", vec!["fails"])],
        );

        r.apply_diff(&desired_map(vec![pod]));

        assert!(
            !r.store.contains("web"),
            "pod must NOT be in store after partial-failure rollback",
        );
        // Rollback destroys the sandbox: pause container deleted.
        assert!(
            r.runtime
                .calls
                .contains(&"delete(web__pause,force=true)".to_string()),
            "rollback should delete pause; got calls: {:?}",
            r.runtime.calls,
        );
        // And the partially-added "first" container also got cleaned up.
        assert!(
            r.runtime
                .calls
                .contains(&"delete(web__first,force=true)".to_string()),
            "rollback should delete the successfully-added container too",
        );
    }

    #[test]
    fn remove_pod_clears_restart_trackers() {
        let mut r = make_reconciler("clear-trackers");
        r.runtime.pids.insert("web__pause".into(), 4242);
        let pod = make_pod("web", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![pod.clone()]));

        // Trigger a restart so a tracker exists.
        r.runtime
            .state_seq
            .insert("web__server".into(), vec![ContainerState::Stopped]);
        r.apply_diff(&desired_map(vec![pod]));
        assert!(r.restart_state.contains_key("web__server"));

        // Now remove the pod from desired.
        r.apply_diff(&desired_map(vec![]));
        assert!(
            !r.restart_state.contains_key("web__server"),
            "tracker must be removed when its pod is removed",
        );
    }

    #[test]
    fn shutdown_destroys_all_pods_and_clears_trackers() {
        let mut r = make_reconciler("shutdown");
        r.runtime.pids.insert("a__pause".into(), 1);
        r.runtime.pids.insert("b__pause".into(), 2);
        let a = make_pod("a", vec![("server", vec!["httpd"])]);
        let b = make_pod("b", vec![("server", vec!["httpd"])]);
        r.apply_diff(&desired_map(vec![a, b]));
        assert_eq!(r.store.len(), 2);
        // Synthesize a tracker so we can verify it's cleared too.
        r.restart_state.insert(
            "a__server".into(),
            RestartTracker {
                restart_count: 1,
                next_retry_at: Instant::now(),
            },
        );

        r.shutdown();

        assert!(r.store.is_empty());
        assert!(r.restart_state.is_empty());
        assert!(
            r.runtime
                .calls
                .contains(&"delete(a__pause,force=true)".to_string())
        );
        assert!(
            r.runtime
                .calls
                .contains(&"delete(b__pause,force=true)".to_string())
        );
    }

    // ---- compute_pod_status: phase rollup precedence ----

    #[test]
    fn compute_status_running_when_all_containers_running() {
        let mut r = make_reconciler("status-running");
        let pod = make_pod("web", vec![("a", vec!["sleep"]), ("b", vec!["sleep"])]);
        // MockRuntime defaults unknown ids to Running.
        let status = r.compute_pod_status(&pod);
        assert_eq!(status.phase, PodPhase::Running);
        assert_eq!(status.container_statuses.len(), 2);
        assert!(
            status.container_statuses.iter().all(|c| c.ready),
            "running containers should be ready",
        );
    }

    #[test]
    fn compute_status_pending_when_any_container_waiting() {
        let mut r = make_reconciler("status-pending");
        let pod = make_pod("web", vec![("a", vec!["sleep"]), ("b", vec!["sleep"])]);
        // a is Running (default), b is still Created → whole pod is Pending.
        r.runtime
            .state_seq
            .insert("web__b".into(), vec![ContainerState::Created]);
        let status = r.compute_pod_status(&pod);
        assert_eq!(
            status.phase,
            PodPhase::Pending,
            "any waiting container forces Pending, even if others are Running",
        );
    }

    #[test]
    fn compute_status_failed_when_all_containers_stopped() {
        let mut r = make_reconciler("status-failed");
        let pod = make_pod("web", vec![("a", vec!["sleep"]), ("b", vec!["sleep"])]);
        r.runtime
            .state_seq
            .insert("web__a".into(), vec![ContainerState::Stopped]);
        r.runtime
            .state_seq
            .insert("web__b".into(), vec![ContainerState::Stopped]);
        let status = r.compute_pod_status(&pod);
        assert_eq!(status.phase, PodPhase::Failed);
        assert!(
            status.container_statuses.iter().all(|c| !c.ready),
            "stopped containers are not ready",
        );
    }

    #[test]
    fn compute_status_carries_restart_count_and_generation() {
        let mut r = make_reconciler("status-meta");
        let mut pod = make_pod("web", vec![("a", vec!["sleep"])]);
        pod.metadata.generation = Some(7);
        r.restart_state.insert(
            "web__a".into(),
            RestartTracker {
                restart_count: 3,
                next_retry_at: Instant::now(),
            },
        );
        let status = r.compute_pod_status(&pod);
        assert_eq!(
            status.observed_generation,
            Some(7),
            "observed_generation echoes the spec generation",
        );
        assert_eq!(status.container_statuses[0].restart_count, 3);
    }

    // ---- tick_liveness: dirty detection + dedup ----

    #[test]
    fn tick_liveness_marks_dirty_then_dedups_after_push() {
        let mut r = make_reconciler("dedup");
        let pod = make_pod("web", vec![("a", vec!["sleep"])]);
        r.cache.insert("web".into(), pod);

        // First tick: nothing pushed yet → web's status is dirty.
        let dirty1 = r.tick_liveness();
        assert_eq!(dirty1.len(), 1);
        assert_eq!(dirty1[0].0, "web");

        // Simulate a successful push by recording what we just computed.
        let (name, status) = dirty1.into_iter().next().unwrap();
        r.last_pushed_status.insert(name, status);

        // Second tick, state unchanged: computed == last pushed → not dirty.
        let dirty2 = r.tick_liveness();
        assert!(
            dirty2.is_empty(),
            "unchanged status must be deduped to avoid spurious /status PUTs",
        );
    }
}
