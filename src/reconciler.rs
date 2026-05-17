use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
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
    debug_dump_path: Option<PathBuf>,
}

impl<R: RuntimeClient> Reconciler<R> {
    pub fn new(
        manifests_dir: PathBuf,
        pods_dir: PathBuf,
        rootfs_base: PathBuf,
        runtime: R,
        debug_dump_path: Option<PathBuf>,
    ) -> Self {
        Self {
            manifests_dir,
            pods_dir,
            rootfs_base,
            store: Store::new(),
            runtime,
            restart_state: HashMap::new(),
            debug_dump_path,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::Path;

    use crate::pod::{Container, PodMetadata, PodSpec};
    use crate::runtime::{Result as RuntimeResult, RuntimeError};

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
            },
            status: None,
        }
    }

    fn make_reconciler(label: &str) -> Reconciler<MockRuntime> {
        let pods_dir = unique_temp_dir(label);
        let manifests_dir = unique_temp_dir(&format!("{label}-manifests"));
        let rootfs = std::env::temp_dir().join("nonexistent-rootfs");
        Reconciler::new(
            manifests_dir,
            pods_dir,
            rootfs,
            MockRuntime::default(),
            None,
        )
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
}
