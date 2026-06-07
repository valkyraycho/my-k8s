use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tracing::warn;

use crate::{
    pod::{Container, PodName},
    runtime::{ContainerState, RuntimeClient, RuntimeError, bundle::write_bundle},
};

/// How long to wait for a container to exit after SIGTERM before force-killing.
const TERMINATION_GRACE: Duration = Duration::from_secs(5);

/// How often to poll container state while waiting out the grace period.
const POLLING_INTERVAL: Duration = Duration::from_millis(100);

/// Owns one Pod's lifecycle. The `pause_pid` is the linchpin: the pause
/// container is a do-nothing process that *holds* the shared net/ipc/uts
/// namespaces, so app containers (which join `/proc/{pause_pid}/ns/*`) keep
/// their network identity even across their own restarts.
///
/// `pause_pid: Option<u32>` — `None` until `create()` runs (a sandbox exists
/// as a value before its pause is started), `Some` afterward.
pub struct PodSandbox {
    pod_name: PodName,
    pause_id: String,
    pause_pid: Option<u32>,
    app_containers: Vec<String>,
    pods_dir: PathBuf,
    rootfs_base: PathBuf,
    pod_ip: Option<String>,
}

impl PodSandbox {
    pub fn new(pod_name: PodName, pods_dir: PathBuf, rootfs_base: PathBuf) -> Self {
        let pause_id = format!("{pod_name}__pause");
        Self {
            pod_name,
            pause_id,
            pause_pid: None,
            app_containers: Vec::new(),
            pods_dir,
            rootfs_base,
            pod_ip: None,
        }
    }

    /// Rebuild a sandbox handle for containers that survived a kubelet restart
    /// (see reconciler `startup`). Unlike `new` + `create`, this does NOT touch
    /// the runtime — the containers are already running. It just reconstructs
    /// the same `pause_id`/`pause_pid`/app-name bookkeeping the original
    /// `create()` produced, so subsequent calls line up with the live ids.
    pub fn from_recovered(
        pod_name: PodName,
        pause_pid: u32,
        app_container_names: Vec<String>,
        pods_dir: PathBuf,
        rootfs_base: PathBuf,
        pod_ip: Option<String>,
    ) -> Self {
        let pause_id = format!("{pod_name}__pause");
        Self {
            pod_name,
            pause_id,
            pause_pid: Some(pause_pid),
            app_containers: app_container_names,
            pods_dir,
            rootfs_base,
            pod_ip,
        }
    }

    pub fn pod_name(&self) -> &PodName {
        &self.pod_name
    }

    pub fn pause_pid(&self) -> Option<u32> {
        self.pause_pid
    }

    pub fn pod_ip(&self) -> Option<&str> {
        self.pod_ip.as_deref()
    }

    pub fn app_container_names(&self) -> &[String] {
        &self.app_containers
    }

    pub fn contains_container(&self, name: &str) -> bool {
        self.app_containers.contains(&name.to_string())
    }

    /// Generic over `R: RuntimeClient` (static dispatch) so the real
    /// `YoukiRuntime` and the test `MockRuntime` share this exact code path —
    /// monomorphized per type, no vtable. Pause MUST come up first: it mints
    /// the namespaces every later container joins, and we must capture its pid.
    pub fn create<R: RuntimeClient>(
        &mut self,
        runtime: &mut R,
        pod_ip: Option<String>,
    ) -> Result<()> {
        let pause_container = pause_container_spec();
        let pause_bundle_dir = self.bundle_dir_for(&pause_container.name);
        // `None` share-pid → the pause creates fresh namespaces (see bundle.rs).
        write_bundle(&pause_container, &pause_bundle_dir, &self.rootfs_base, None)
            .context("write pause bundle")?;

        runtime
            .create_container(&self.pause_id, &pause_bundle_dir)
            .context("create pause container")?;
        runtime
            .start_container(&self.pause_id)
            .context("start pause container")?;

        // `?` then `.ok_or_else(...)?`: first `?` unwraps the Result, the
        // ok_or_else turns the inner `Option<u32>` into an error if the pause
        // has no pid (it must, right after a successful start).
        let pid = runtime
            .container_pid(&self.pause_id)
            .context("get pause container pid")?
            .ok_or_else(|| anyhow::anyhow!("pause container pid not found"))?;
        self.pause_pid = Some(pid);
        self.pod_ip = pod_ip;

        // Networking shells out to `ip`/`nsenter` (needs root + a real netns),
        // so it's compiled OUT of test builds. With an IP → full veth+bridge
        // wiring; without → loopback only (a pod created before IPAM assigns it
        // an address). Match on the stored value via `as_deref` (Option<String>
        // → Option<&str>) so we borrow rather than move `self.pod_ip`.
        #[cfg(not(test))]
        match self.pod_ip.as_deref() {
            Some(ip) => {
                setup_pod_network(pid, &self.pod_name, ip).context("configure pod network")?
            }
            None => loopback_only(pid).context("configure loopback")?,
        }

        Ok(())
    }

    /// Add an app container that JOINS the pause's namespaces. Requires
    /// `create()` to have run first — enforced by reading `pause_pid` up front
    /// (an unstarted sandbox has `None` → error, not a silent misconfig).
    pub fn add_container<R: RuntimeClient>(
        &mut self,
        runtime: &mut R,
        container: &Container,
    ) -> Result<()> {
        let pause_pid = self.pause_pid.ok_or_else(|| {
            anyhow::anyhow!("sandbox {} not created (pause has no pid)", self.pod_name)
        })?;

        let container_id = self.container_id_for(&container.name);
        let container_bundle_dir = self.bundle_dir_for(&container.name);
        write_bundle(
            container,
            &container_bundle_dir,
            &self.rootfs_base,
            Some(pause_pid),
        )
        .context("write container bundle")?;

        runtime
            .create_container(&container_id, &container_bundle_dir)
            .context("create container")?;
        runtime
            .start_container(&container_id)
            .context("start container")?;
        self.app_containers.push(container.name.clone());
        Ok(())
    }

    /// Graceful-termination *policy* (SIGTERM → wait ≤grace → force delete)
    /// lives here, NOT in `RuntimeClient`: the trait exposes mechanisms
    /// (`kill_container`, `container_state`), the sandbox composes them into
    /// policy. A different sandbox could choose a different grace without
    /// touching the runtime layer.
    pub fn remove_container<R: RuntimeClient>(
        &mut self,
        runtime: &mut R,
        name: &str,
    ) -> Result<()> {
        let container_id = self.container_id_for(name);

        match runtime.kill_container(&container_id, libc::SIGTERM) {
            Ok(()) => {
                // Poll until Stopped/NotFound or the grace deadline. Sync sleep
                // is fine: the reconciler runs this inside `block_in_place`.
                let deadline = Instant::now() + TERMINATION_GRACE;
                while Instant::now() < deadline {
                    let state = runtime
                        .container_state(&container_id)
                        .context("polling container state during graceful shutdown")?;
                    if matches!(state, ContainerState::Stopped | ContainerState::NotFound) {
                        break;
                    }
                    std::thread::sleep(POLLING_INTERVAL);
                }
            }
            // SIGTERM failing (e.g. already gone) isn't fatal — fall through to
            // delete, which is the thing that actually frees the state.
            Err(e) => {
                warn!(error = ?e, %container_id, "SIGTERM failed during remove; proceeding to delete");
            }
        }

        // Rust idiom — or-pattern in a match arm: treat "deleted" and "already
        // NotFound" identically (both mean "it's gone, success"); only a real
        // error propagates. Makes delete idempotent.
        match runtime.delete_container(&container_id, true) {
            Ok(()) | Err(RuntimeError::NotFound(_)) => {}
            Err(e) => return Err(e).context(format!("delete container {name}")),
        }

        let container_bundle_dir = self.bundle_dir_for(name);
        // `let _ =`: best-effort cleanup; a leftover bundle dir is harmless, so
        // we deliberately discard the Result rather than fail the removal.
        let _ = std::fs::remove_dir_all(&container_bundle_dir);
        // `retain`: drop this name from the tracked set, keep the rest.
        self.app_containers.retain(|n| n != name);
        Ok(())
    }

    /// Tear down in REVERSE dependency order: all app containers first, THEN
    /// the pause. If the pause died first, every app container's shared
    /// net/ipc/uts namespace would vanish out from under it mid-cleanup.
    /// Errors are logged, not propagated — teardown is best-effort, we want to
    /// get as far as possible rather than bail on the first failure.
    pub fn destroy<R: RuntimeClient>(&mut self, runtime: &mut R) -> Result<()> {
        // Clone the names so we don't hold a borrow of `self.app_containers`
        // while `remove_container` mutates it inside the loop.
        let names: Vec<String> = self.app_containers.clone();

        for name in &names {
            if let Err(e) = self.remove_container(runtime, name) {
                warn!(error = ?e, pod = %self.pod_name, container = %name, "failed to remove container during destroy");
            }
        }

        if let Err(e) = runtime.delete_container(&self.pause_id, true) {
            warn!(error = ?e, pod = %self.pod_name, "failed to delete pause container");
        }

        let pod_dir = self.pods_dir.join(&self.pod_name);
        let _ = std::fs::remove_dir_all(&pod_dir);

        // Tear down the host end of the veth. The peer (eth0) dies with the
        // pause netns, but the host end can linger on the bridge, so delete it
        // explicitly. Best-effort (`let _`): a missing link is fine. The name is
        // recomputed from the pod name — deterministic, so it matches what
        // `create` made even across a kubelet restart.
        #[cfg(not(test))]
        {
            let _ = run(&["ip", "link", "del", &host_veth_name(&self.pod_name)]);
        }

        self.pause_pid = None;
        self.pod_ip = None;
        Ok(())
    }

    fn container_id_for(&self, container_name: &str) -> String {
        format!("{}__{}", self.pod_name, container_name)
    }

    fn bundle_dir_for(&self, container_name: &str) -> PathBuf {
        self.pods_dir.join(&self.pod_name).join(container_name)
    }
}

fn pause_container_spec() -> Container {
    Container {
        name: "__pause".into(),
        image: "busybox".into(),
        command: vec!["/bin/busybox".into(), "sleep".into(), "infinity".into()],
    }
}

// The cluster pod CIDR is 10.244.0.0/16; `mykube0` is the one shared host
// bridge every pod's veth plugs into, and .0.1 is its gateway. A /16 on the
// bridge means all pods (across every per-node /24) sit on ONE L2 segment —
// that's what makes cross-(logical-)node pod-to-pod traffic work with no routing.
const BRIDGE_NAME: &str = "mykube0";
const BRIDGE_GATEWAY: &str = "10.244.0.1";
const BRIDGE_CIDR: &str = "10.244.0.1/16";

/// Idempotent host bridge setup, run once by the kubelet at startup. Multiple
/// kubelets share ONE host, so the first creates `mykube0` and the rest find it
/// present — `ip`'s "File exists" is success, not failure (`run_tolerate`).
#[cfg(not(test))]
pub fn ensure_bridge() -> anyhow::Result<()> {
    run_tolerate(
        &["ip", "link", "add", BRIDGE_NAME, "type", "bridge"],
        "File exists",
    )?;
    run_tolerate(
        &["ip", "addr", "add", BRIDGE_CIDR, "dev", BRIDGE_NAME],
        "File exists",
    )?;
    run(&["ip", "link", "set", BRIDGE_NAME, "up"])?;
    // Pods route out via the bridge gateway; forwarding must be enabled.
    run(&["sysctl", "-w", "net.ipv4.ip_forward=1"])?;
    Ok(())
}

/// Test stub: the reconciler calls `ensure_bridge` unconditionally, but tests
/// have no root/netns. A no-op keeps the reconciler's test build linking.
#[cfg(test)]
pub fn ensure_bridge() -> anyhow::Result<()> {
    Ok(())
}

/// Wire a pod's netns into `mykube0`: create a veth pair (a virtual cable),
/// put the host end on the bridge, move the peer end into the pause netns as
/// `eth0` with `pod_ip`, then default-route via the gateway. Run from the host
/// (which holds CAP_NET_ADMIN) via `nsenter` for the in-namespace steps.
#[cfg(not(test))]
fn setup_pod_network(pause_pid: u32, pod_name: &str, pod_ip: &str) -> anyhow::Result<()> {
    let host_veth = host_veth_name(pod_name);
    let peer = peer_veth_name(pod_name);
    let pid = pause_pid.to_string();

    // Create the cable, then attach the host end to the bridge and bring it up.
    run(&[
        "ip", "link", "add", &host_veth, "type", "veth", "peer", "name", &peer,
    ])?;
    run(&["ip", "link", "set", &host_veth, "master", BRIDGE_NAME])?;
    run(&["ip", "link", "set", &host_veth, "up"])?;
    // Move the peer end INTO the pod's network namespace (targeted by pause PID).
    run(&["ip", "link", "set", &peer, "netns", &pid])?;
    // Inside the netns: rename to eth0 (only after the move), address it /16 so
    // it shares the cluster L2, bring eth0 + lo up, default route via the gw.
    nsenter(&pid, &["ip", "link", "set", &peer, "name", "eth0"])?;
    nsenter(
        &pid,
        &["ip", "addr", "add", &format!("{pod_ip}/16"), "dev", "eth0"],
    )?;
    nsenter(&pid, &["ip", "link", "set", "eth0", "up"])?;
    nsenter(&pid, &["ip", "link", "set", "lo", "up"])?;
    nsenter(
        &pid,
        &["ip", "route", "add", "default", "via", BRIDGE_GATEWAY],
    )?;
    Ok(())
}

/// Loopback-only setup (the Phase-1 behavior) for a pod with no IP yet.
#[cfg(not(test))]
fn loopback_only(pause_pid: u32) -> anyhow::Result<()> {
    nsenter(&pause_pid.to_string(), &["ip", "link", "set", "lo", "up"])
}

/// Run `args[0]` with the rest as arguments; error (with captured stderr) on a
/// non-zero exit. Taking `&[&str]` lets call sites read like shell lines.
#[cfg(not(test))]
fn run(args: &[&str]) -> anyhow::Result<()> {
    let output = std::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        .with_context(|| format!("running {args:?}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "command {args:?} failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(())
}

/// Like `run`, but treats a failure whose stderr contains `tolerate` as success
/// — used for idempotent setup (e.g. `ip link add` returns "File exists" when
/// the bridge is already there). String-matching is fragile in general, but
/// `ip`'s wording is stable; it's the CLI equivalent of ignoring an EEXIST.
#[cfg(not(test))]
fn run_tolerate(args: &[&str], tolerate: &str) -> anyhow::Result<()> {
    let output = std::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        .with_context(|| format!("running {args:?}"))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains(tolerate) {
        return Ok(());
    }

    anyhow::bail!(
        "command {args:?} failed: status={} stderr={}",
        output.status,
        stderr.trim()
    );
}

/// Prepend `nsenter -t <pid> -n` so `args` runs INSIDE that PID's network
/// namespace (the pause container's), even though we invoke it from the host.
#[cfg(not(test))]
fn nsenter(pid: &str, args: &[&str]) -> anyhow::Result<()> {
    let mut full = vec!["nsenter", "-t", pid, "-n"];
    full.extend_from_slice(args);
    run(&full)
}

// Host-side ('v') and in-pod-pre-rename ('p') veth names. Both must be ≤ 15
// bytes (IFNAMSIZ) and deterministic from the pod name so `destroy` can
// recompute the host name to delete it.
fn host_veth_name(pod_name: &str) -> String {
    veth_name('v', pod_name)
}

fn peer_veth_name(pod_name: &str) -> String {
    veth_name('p', pod_name)
}

/// Derive a ≤15-char interface name: 1-char prefix + 14 hex digits of a hash of
/// the pod name (pod names can exceed IFNAMSIZ or hold invalid chars). NOTE:
/// `DefaultHasher::new()` uses FIXED keys (unlike HashMap's randomized
/// RandomState), so the name is STABLE across process restarts — essential so
/// `destroy` after a kubelet restart computes the same name `create` did.
fn veth_name(prefix: char, pod_name: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    pod_name.hash(&mut hasher);
    // Mask to 56 bits → 14 hex digits; prefix brings the total to 15.
    format!("{prefix}{:014x}", hasher.finish() & 0x00ff_ffff_ffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;

    use crate::runtime::{RecoveredContainer, Result as RuntimeResult, RuntimeClient};

    /// Records every trait call and serves canned responses. Lets us drive
    /// PodSandbox through every state without libcontainer or root.
    #[derive(Default)]
    struct MockRuntime {
        /// Every call, appended in order. e.g. "create(test-pod__pause)".
        calls: Vec<String>,
        /// FIFO sequence of states per id. Empty means "default to Running."
        state_seq: HashMap<String, Vec<ContainerState>>,
        /// PIDs returned by container_pid(). Missing → returns Ok(None).
        pids: HashMap<String, u32>,
        /// IDs that should produce NotFound on kill_container.
        kill_not_found: Vec<String>,
    }

    impl RuntimeClient for MockRuntime {
        fn create_container(&mut self, id: &str, _bundle_path: &Path) -> RuntimeResult<()> {
            self.calls.push(format!("create({id})"));
            Ok(())
        }
        fn start_container(&mut self, id: &str) -> RuntimeResult<()> {
            self.calls.push(format!("start({id})"));
            Ok(())
        }
        fn kill_container(&mut self, id: &str, signal: i32) -> RuntimeResult<()> {
            self.calls.push(format!("kill({id},{signal})"));
            if self.kill_not_found.iter().any(|x| x == id) {
                return Err(RuntimeError::NotFound(id.to_string()));
            }
            Ok(())
        }
        fn delete_container(&mut self, id: &str, force: bool) -> RuntimeResult<()> {
            self.calls.push(format!("delete({id},force={force})"));
            Ok(())
        }
        fn container_state(&mut self, id: &str) -> RuntimeResult<ContainerState> {
            self.calls.push(format!("state({id})"));
            let seq = self.state_seq.entry(id.to_string()).or_default();
            if seq.is_empty() {
                return Ok(ContainerState::Running);
            }
            Ok(seq.remove(0))
        }
        fn container_pid(&mut self, id: &str) -> RuntimeResult<Option<u32>> {
            self.calls.push(format!("pid({id})"));
            Ok(self.pids.get(id).copied())
        }
        fn recover_all(&mut self) -> RuntimeResult<Vec<RecoveredContainer>> {
            // Sandbox tests never recover; nothing to enumerate.
            Ok(Vec::new())
        }
    }

    /// Unique tempdir per test, prefixed with a label for readability.
    /// No cleanup — bundles are tiny, /tmp is ephemeral.
    fn unique_temp_dir(label: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("my-k8s-test-{label}-{pid}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_app_container() -> Container {
        Container {
            name: "web".into(),
            image: "busybox".into(),
            command: vec!["/bin/busybox".into(), "httpd".into(), "-f".into()],
        }
    }

    /// Standard fixture: a fresh sandbox + a mock runtime that already has a
    /// canned PID for the pause container, so `create()` can complete.
    fn make_sandbox(label: &str) -> (PodSandbox, MockRuntime) {
        let pods_dir = unique_temp_dir(label);
        let rootfs = std::env::temp_dir().join("nonexistent-rootfs");
        let mut runtime = MockRuntime::default();
        runtime.pids.insert("test-pod__pause".into(), 4242);
        let sandbox = PodSandbox::new("test-pod".into(), pods_dir, rootfs);
        (sandbox, runtime)
    }

    #[test]
    fn create_calls_runtime_in_order_and_captures_pause_pid() {
        let (mut sandbox, mut runtime) = make_sandbox("create");
        sandbox
            .create(&mut runtime, None)
            .expect("create should succeed");
        assert_eq!(sandbox.pause_pid(), Some(4242));
        // No IP passed → loopback-only path; pod_ip stays None.
        assert_eq!(sandbox.pod_ip(), None);
        assert_eq!(
            runtime.calls,
            vec![
                "create(test-pod__pause)",
                "start(test-pod__pause)",
                "pid(test-pod__pause)",
            ],
        );
    }

    #[test]
    fn create_with_ip_records_pod_ip() {
        let (mut sandbox, mut runtime) = make_sandbox("create-ip");
        sandbox
            .create(&mut runtime, Some("10.244.1.5".into()))
            .expect("create should succeed");
        // The IP bookkeeping is NOT cfg-gated, so it runs in tests even though
        // the veth/bridge shell-outs are compiled out.
        assert_eq!(sandbox.pod_ip(), Some("10.244.1.5"));
    }

    #[test]
    fn add_container_fails_before_create() {
        let (mut sandbox, mut runtime) = make_sandbox("add-no-create");
        let result = sandbox.add_container(&mut runtime, &sample_app_container());
        assert!(result.is_err(), "add without prior create should fail");
        assert!(
            runtime.calls.is_empty(),
            "no runtime calls should have happened"
        );
    }

    #[test]
    fn add_container_creates_and_starts_after_create() {
        let (mut sandbox, mut runtime) = make_sandbox("add-after-create");
        sandbox.create(&mut runtime, None).unwrap();
        let pre = runtime.calls.len();
        sandbox
            .add_container(&mut runtime, &sample_app_container())
            .expect("add should succeed");
        assert_eq!(
            &runtime.calls[pre..],
            &["create(test-pod__web)", "start(test-pod__web)"],
        );
        assert!(sandbox.contains_container("web"));
    }

    #[test]
    fn remove_container_does_graceful_term_then_delete() {
        let (mut sandbox, mut runtime) = make_sandbox("remove-graceful");
        sandbox.create(&mut runtime, None).unwrap();
        sandbox
            .add_container(&mut runtime, &sample_app_container())
            .unwrap();
        // First state poll returns Stopped → we exit the wait loop immediately,
        // so the test runs in milliseconds, not the full 5-second grace period.
        runtime
            .state_seq
            .insert("test-pod__web".into(), vec![ContainerState::Stopped]);
        let pre = runtime.calls.len();
        sandbox
            .remove_container(&mut runtime, "web")
            .expect("remove should succeed");
        let post = &runtime.calls[pre..];
        assert_eq!(post[0], format!("kill(test-pod__web,{})", libc::SIGTERM));
        assert_eq!(post[1], "state(test-pod__web)");
        assert_eq!(post[2], "delete(test-pod__web,force=true)");
        assert!(!sandbox.contains_container("web"));
    }

    #[test]
    fn remove_container_tolerates_already_gone() {
        let (mut sandbox, mut runtime) = make_sandbox("remove-gone");
        sandbox.create(&mut runtime, None).unwrap();
        sandbox
            .add_container(&mut runtime, &sample_app_container())
            .unwrap();
        runtime.kill_not_found.push("test-pod__web".into());
        let pre = runtime.calls.len();
        sandbox
            .remove_container(&mut runtime, "web")
            .expect("remove should tolerate NotFound on kill");
        let post = &runtime.calls[pre..];
        assert_eq!(post[0], format!("kill(test-pod__web,{})", libc::SIGTERM));
        assert_eq!(post[1], "delete(test-pod__web,force=true)");
        assert!(
            !post.iter().any(|c| c.starts_with("state(")),
            "no state polling should occur after NotFound on kill",
        );
    }

    #[test]
    fn from_recovered_populates_fields_without_touching_runtime() {
        let pods_dir = unique_temp_dir("from-recovered");
        let rootfs = std::env::temp_dir().join("nonexistent-rootfs");

        let sandbox = PodSandbox::from_recovered(
            "test-pod".into(),
            9999,
            vec!["web".into(), "log-tail".into()],
            pods_dir,
            rootfs,
            Some("10.244.1.5".into()),
        );

        // pause_id is derived from pod_name; restart-recovery must reconstruct
        // the same id the original create() would have produced.
        assert_eq!(sandbox.pod_name(), "test-pod");
        assert_eq!(sandbox.pause_pid(), Some(9999));
        // The recovered IP (read from the apiserver's persisted status) is
        // carried through so the kubelet can re-reserve it in IPAM.
        assert_eq!(sandbox.pod_ip(), Some("10.244.1.5"));
        assert_eq!(
            sandbox.app_container_names(),
            &["web".to_string(), "log-tail".to_string()],
        );
        assert!(sandbox.contains_container("web"));
        assert!(sandbox.contains_container("log-tail"));
        assert!(!sandbox.contains_container("nope"));
    }

    #[test]
    fn destroy_removes_app_containers_before_pause() {
        let (mut sandbox, mut runtime) = make_sandbox("destroy");
        sandbox.create(&mut runtime, None).unwrap();
        sandbox
            .add_container(&mut runtime, &sample_app_container())
            .unwrap();
        runtime
            .state_seq
            .insert("test-pod__web".into(), vec![ContainerState::Stopped]);

        sandbox
            .destroy(&mut runtime)
            .expect("destroy should succeed");

        let web_delete_idx = runtime
            .calls
            .iter()
            .position(|c| c == "delete(test-pod__web,force=true)")
            .expect("web container should be deleted");
        let pause_delete_idx = runtime
            .calls
            .iter()
            .position(|c| c == "delete(test-pod__pause,force=true)")
            .expect("pause container should be deleted");
        assert!(
            web_delete_idx < pause_delete_idx,
            "app containers must be removed before pause (so they don't lose their netns mid-flight)",
        );
    }

    #[test]
    fn veth_names_fit_ifnamsiz_and_are_deterministic() {
        // Even an absurdly long pod name must yield a ≤15-byte interface name.
        let long = "a-really-long-pod-name-that-exceeds-ifnamsiz-by-a-lot";
        let host = host_veth_name(long);
        let peer = peer_veth_name(long);
        assert!(host.len() <= 15, "host veth {host:?} exceeds IFNAMSIZ");
        assert!(peer.len() <= 15, "peer veth {peer:?} exceeds IFNAMSIZ");

        // Host vs peer differ (distinct prefixes), so the pair can't clash.
        assert_ne!(host, peer);
        assert!(host.starts_with('v'));
        assert!(peer.starts_with('p'));

        // Deterministic: the SAME pod name always maps to the SAME name, so
        // `destroy` (which recomputes it) targets the link `create` made.
        assert_eq!(host, host_veth_name(long));
        // Different pod names map to different host veths (no collision).
        assert_ne!(host_veth_name("pod-a"), host_veth_name("pod-b"));
    }
}
