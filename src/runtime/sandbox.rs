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

const TERMINATION_GRACE: Duration = Duration::from_secs(5);

const POLLING_INTERVAL: Duration = Duration::from_millis(100);

pub struct PodSandbox {
    pod_name: PodName,
    pause_id: String,
    pause_pid: Option<u32>,
    app_containers: Vec<String>,
    pods_dir: PathBuf,
    rootfs_base: PathBuf,
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
        }
    }

    pub fn pod_name(&self) -> &PodName {
        &self.pod_name
    }

    pub fn pause_pid(&self) -> Option<u32> {
        self.pause_pid
    }

    pub fn app_container_names(&self) -> &[String] {
        &self.app_containers
    }

    pub fn contains_container(&self, name: &str) -> bool {
        self.app_containers.contains(&name.to_string())
    }

    pub fn create<R: RuntimeClient>(&mut self, runtime: &mut R) -> Result<()> {
        let pause_container = pause_container_spec();
        let pause_bundle_dir = self.bundle_dir_for(&pause_container.name);
        write_bundle(&pause_container, &pause_bundle_dir, &self.rootfs_base, None)
            .context("write pause bundle")?;

        runtime
            .create_container(&self.pause_id, &pause_bundle_dir)
            .context("create pause container")?;
        runtime
            .start_container(&self.pause_id)
            .context("start pause container")?;

        let pid = runtime
            .container_pid(&self.pause_id)
            .context("get pause container pid")?
            .ok_or_else(|| anyhow::anyhow!("pause container pid not found"))?;
        self.pause_pid = Some(pid);

        #[cfg(not(test))]
        setup_pod_network(pid).context("configure pod network")?;

        Ok(())
    }

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

    pub fn remove_container<R: RuntimeClient>(
        &mut self,
        runtime: &mut R,
        name: &str,
    ) -> Result<()> {
        let container_id = self.container_id_for(name);

        match runtime.kill_container(&container_id, libc::SIGTERM) {
            Ok(()) => {
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
            Err(e) => {
                warn!(error = ?e, %container_id, "SIGTERM failed during remove; proceeding to delete");
            }
        }

        match runtime.delete_container(&container_id, true) {
            Ok(()) | Err(RuntimeError::NotFound(_)) => {}
            Err(e) => return Err(e).context(format!("delete container {name}")),
        }

        let container_bundle_dir = self.bundle_dir_for(name);
        let _ = std::fs::remove_dir_all(&container_bundle_dir);
        self.app_containers.retain(|n| n != name);
        Ok(())
    }

    pub fn destroy<R: RuntimeClient>(&mut self, runtime: &mut R) -> Result<()> {
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

        self.pause_pid = None;
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

/// Bring up the loopback interface inside a pod's network namespace.
/// Runs from the host (which has CAP_NET_ADMIN) via nsenter — avoids
/// granting CAP_NET_ADMIN to the pause container itself.
///
/// Phase 1 stand-in for the CNI `loopback` plugin.
#[cfg(not(test))]
fn setup_pod_network(pause_pid: u32) -> anyhow::Result<()> {
    let output = std::process::Command::new("nsenter")
        .args([
            "-t",
            &pause_pid.to_string(),
            "-n",
            "ip",
            "link",
            "set",
            "lo",
            "up",
        ])
        .output()
        .context("invoking `nsenter ... ip link set lo up`")?;
    if !output.status.success() {
        anyhow::bail!(
            "loopback setup failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;

    use crate::runtime::{Result as RuntimeResult, RuntimeClient};

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
        sandbox.create(&mut runtime).expect("create should succeed");
        assert_eq!(sandbox.pause_pid(), Some(4242));
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
        sandbox.create(&mut runtime).unwrap();
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
        sandbox.create(&mut runtime).unwrap();
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
        sandbox.create(&mut runtime).unwrap();
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
    fn destroy_removes_app_containers_before_pause() {
        let (mut sandbox, mut runtime) = make_sandbox("destroy");
        sandbox.create(&mut runtime).unwrap();
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
}
