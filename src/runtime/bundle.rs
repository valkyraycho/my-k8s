//! Translates our `Container` type into an OCI runtime `Spec` (`config.json`)
//! that libcontainer can instantiate. The bridge between Pod-world and OCI-world.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use oci_spec::runtime::{
    LinuxBuilder, LinuxNamespace, LinuxNamespaceBuilder, LinuxNamespaceType, MountBuilder,
    ProcessBuilder, RootBuilder, Spec, UserBuilder,
};

use crate::pod::Container;

/// Build the OCI `Spec`. The one knob that matters is `share_namespaces_from_pid`:
/// `None` → pause container (creates fresh namespaces); `Some(pid)` → app
/// container (joins the pause's net/ipc/uts). See [`build_namespaces`].
///
/// A *pure* function (inputs → `Spec`, no I/O) — easy to unit-test without root,
/// which is why the namespace-wiring tests live against this and not the runtime.
pub fn build_spec(
    container: &Container,
    rootfs_base: &Path,
    share_namespaces_from_pid: Option<u32>,
) -> Result<Spec> {
    // Rust idiom — the builder pattern (`oci-spec` uses `derive_builder`):
    // chained owned-self setters then a `.build()` that VALIDATES and returns
    // `Result`, hence the `?` on every builder. Verbose, but type-checked —
    // far safer than hand-assembling the config.json by hand.
    let process = ProcessBuilder::default()
        .terminal(false)
        .user(UserBuilder::default().uid(0u32).gid(0u32).build()?)
        .args(container.command.clone())
        .env(vec!["PATH=/bin".into(), "HOME=/".into()])
        .cwd("/")
        .no_new_privileges(true) // hardening: child can't gain privs via setuid
        .build()
        .context("building process spec")?;

    // readonly(true): every container shares ONE on-disk rootfs, so read-only
    // stops one from corrupting another. Writable scratch comes from the /tmp
    // tmpfs mount below.
    let root = RootBuilder::default()
        .path(rootfs_base.to_path_buf())
        .readonly(true)
        .build()
        .context("building root spec")?;

    let mounts = vec![
        MountBuilder::default()
            .destination("/proc")
            .typ("proc")
            .source("proc")
            .build()?,
        MountBuilder::default()
            .destination("/dev")
            .typ("tmpfs")
            .source("tmpfs")
            .options(
                ["nosuid", "strictatime", "mode=755", "size=65536k"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )
            .build()?,
        MountBuilder::default()
            .destination("/sys")
            .typ("sysfs")
            .source("sysfs")
            .options(
                ["nosuid", "noexec", "nodev", "ro"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )
            .build()?,
        MountBuilder::default()
            .destination("/tmp")
            .typ("tmpfs")
            .source("tmpfs")
            .options(
                ["nosuid", "nodev", "mode=1777", "size=16m"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )
            .build()?,
    ];

    let namespaces = build_namespaces(share_namespaces_from_pid)?;

    let linux = LinuxBuilder::default()
        .namespaces(namespaces)
        .build()
        .context("building linux spec")?;

    let mut spec = Spec::default();
    spec.set_version("1.0.2".into());
    spec.set_process(Some(process));
    spec.set_root(Some(root));
    spec.set_hostname(Some(container.name.clone()));
    spec.set_mounts(Some(mounts));
    spec.set_linux(Some(linux));

    Ok(spec)
}

/// The heart of the Pod model. A namespace entry with NO `path` means "create
/// a fresh one"; a `path` of `/proc/PID/ns/X` means "join the existing one
/// owned by PID" (libcontainer `setns(2)`s into it before exec).
///
/// - PID + mount: always fresh (own process tree, own filesystem view).
/// - net + ipc + uts: fresh for the pause (`None`), but JOINED for app
///   containers (`Some(pause_pid)`) — that's what gives a Pod one shared IP.
fn build_namespaces(share_namespaces_from_pid: Option<u32>) -> Result<Vec<LinuxNamespace>> {
    let mut namespaces: Vec<LinuxNamespace> = Vec::new();

    // Per-container namespaces: never shared, so no `path` is ever set.
    for ty in [LinuxNamespaceType::Pid, LinuxNamespaceType::Mount] {
        namespaces.push(
            LinuxNamespaceBuilder::default()
                .typ(ty)
                .build()
                .context("building per-container namespace")?,
        );
    }

    // Shared-from-pause namespaces: setting `.path(...)` only when we have a
    // pid is the whole pause/app distinction. `if let Some` on the Option is
    // the idiomatic "do this only when present" — no null, no unwrap.
    for (ty, ns_name) in [
        (LinuxNamespaceType::Network, "net"),
        (LinuxNamespaceType::Ipc, "ipc"),
        (LinuxNamespaceType::Uts, "uts"),
    ] {
        let mut builder = LinuxNamespaceBuilder::default().typ(ty);
        if let Some(pid) = share_namespaces_from_pid {
            builder = builder.path(PathBuf::from(format!("/proc/{pid}/ns/{ns_name}")));
        }
        namespaces.push(builder.build().context("building shared namespace")?);
    }
    Ok(namespaces)
}

pub fn write_bundle(
    container: &Container,
    bundle_dir: &Path,
    rootfs_base: &Path,
    share_namespaces_from_pid: Option<u32>,
) -> Result<()> {
    std::fs::create_dir_all(bundle_dir)
        .with_context(|| format!("creating bundle directory {bundle_dir:?}"))?;
    let spec = build_spec(container, rootfs_base, share_namespaces_from_pid)?;
    spec.save(bundle_dir.join("config.json"))
        .with_context(|| format!("writing config.json to {bundle_dir:?}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_container() -> Container {
        Container {
            name: "test".into(),
            image: "busybox".into(),
            command: vec!["/bin/echo".into(), "hello".into()],
        }
    }

    /// Find a namespace by type in the spec. Panics if not present — fine for
    /// tests; the panic message tells us which namespace was missing.
    fn find_ns(spec: &Spec, ty: LinuxNamespaceType) -> LinuxNamespace {
        spec.linux()
            .as_ref()
            .expect("linux section should be set")
            .namespaces()
            .as_ref()
            .expect("namespaces should be set")
            .iter()
            .find(|n| n.typ() == ty)
            .unwrap_or_else(|| panic!("namespace {ty:?} not found"))
            .clone()
    }

    #[test]
    fn pause_container_creates_all_new_namespaces() {
        let spec = build_spec(&sample_container(), Path::new("/rootfs"), None).unwrap();
        let nss = spec
            .linux()
            .as_ref()
            .unwrap()
            .namespaces()
            .as_ref()
            .unwrap();
        for ns in nss {
            assert_eq!(
                ns.path(),
                &None,
                "pause container should not join any existing namespace ({:?})",
                ns.typ(),
            );
        }
    }

    #[test]
    fn app_container_joins_pauses_net_ipc_uts() {
        let spec = build_spec(&sample_container(), Path::new("/rootfs"), Some(1234)).unwrap();
        assert_eq!(
            find_ns(&spec, LinuxNamespaceType::Network).path(),
            &Some(PathBuf::from("/proc/1234/ns/net")),
        );
        assert_eq!(
            find_ns(&spec, LinuxNamespaceType::Ipc).path(),
            &Some(PathBuf::from("/proc/1234/ns/ipc")),
        );
        assert_eq!(
            find_ns(&spec, LinuxNamespaceType::Uts).path(),
            &Some(PathBuf::from("/proc/1234/ns/uts")),
        );
    }

    #[test]
    fn app_container_keeps_pid_and_mount_per_container() {
        let spec = build_spec(&sample_container(), Path::new("/rootfs"), Some(1234)).unwrap();
        assert_eq!(
            find_ns(&spec, LinuxNamespaceType::Pid).path(),
            &None,
            "PID ns should NOT be shared by default (matches K8s shareProcessNamespace=false)",
        );
        assert_eq!(
            find_ns(&spec, LinuxNamespaceType::Mount).path(),
            &None,
            "Mount ns is always per-container",
        );
    }

    #[test]
    fn process_args_match_container_command() {
        let spec = build_spec(&sample_container(), Path::new("/rootfs"), None).unwrap();
        assert_eq!(
            spec.process().as_ref().unwrap().args(),
            &Some(vec!["/bin/echo".into(), "hello".into()]),
        );
    }
}
