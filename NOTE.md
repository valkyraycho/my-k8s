# NOTE.md — my-k8s working journal

> **What this is.** A distilled, append-only journal of the `my-k8s` project: what was built, why, and what I learned along the way. Optimized to be read **top-to-bottom** as the story of how the system was constructed *and* searched (Cmd-F) for a specific concept or step while coding.
>
> **Why it exists.** The companion Claude Code session (also called `my-k8s`) holds the full design discussion, but it's noisy and slow to navigate. This file is the boiled-down version I keep open while I code.
>
> **How it grows.** Sections are ordered by **construction sequence** — each one describes a piece built on top of the previous. Reading top-to-bottom = the dependency graph of the system. When a new thing is built, a new section is **appended at the bottom**. Past sections are not reorganized.
>
> Inside a section, these inline labels are used (only when they earn their place):
> - **Did:** what changed in the repo
> - **Concept:** a mental model or piece of K8s/Linux/Rust internals worth holding onto
> - **Decision:** a fork in the road and why we picked one branch
> - **Runbook:** a literal command/step needed to reproduce or operate the thing
> - **Gotcha:** something that surprised me or burned time

---

## Project at a glance

`my-k8s` is a from-scratch mini-Kubernetes built in Rust as a multi-month learning project. The goal is **implementer's intuition, not production**: I want to be able to talk about K8s having actually built the parts. Not a fork of upstream k8s. Not a wrapper around `kube-rs`. Not feature-complete.

**Tech locked in:**
- **Container runtime:** youki's `libcontainer` crate as a Rust dependency. Not shelling out to `runc`, not reusing my prior "build your own Docker" code. The point is to learn the *orchestrator* layer, so the runtime is a library.
- **Dev environment:** OrbStack on macOS hosting an Ubuntu VM (`mykube-dev`, arm64). Source lives on the Mac, builds and runs in Linux via VirtioFS. Wrapped in a devcontainer for reproducibility.
- **Pacing:** 6 phases over ~4–6 months, only one phase planned in depth at a time. After each phase ships, the next is replanned from scratch with what we've actually learned.

**The 6-phase arc** (canonical version: `~/.claude/plans/here-is-my-next-sequential-charm.md`):

| # | Phase | Demo at end |
|---|-------|-------------|
| 0 | Dev env + scaffold | `cargo run --bin scratch` runs busybox container ✅ |
| 1 | Single-node "mini-kubelet" — the **Pod sandbox** primitive | `mykubelet` watches a manifests dir, runs multi-container Pods ✅ |
| 2 | API server v1 | kubelet talks to API server over HTTP; multiple kubelets possible ⬅ **next** |
| 3 | Controllers (ReplicaSet) | Kill a pod, controller recreates it |
| 4 | Scheduler | Pods distributed across 2+ kubelets |
| 5 | Services & networking | `curl` a Service VIP, traffic load-balances |
| 6 | Distributed-systems track | Pick from: leader election, Raft, admission webhooks, CNI, RBAC |

Phase 6 explicitly **dropped** "write your own runtime" — already done that in the prior Docker project. Replaced with distributed-systems content where the marginal learning is highest.

---

# Phase 0 — Dev environment

## OrbStack + Linux VM + VirtioFS

**Did:** Stood up the Linux environment everything else depends on. OrbStack v2.1.3 on macOS, an Ubuntu Questing arm64 VM named `mykube-dev`, VirtioFS mount of the Mac source tree confirmed. Inside the VM: Rust 1.95.0 + cargo and the C toolchain pieces youki needs (`libseccomp-dev`, `pkg-config`, `build-essential`).

**Concept — why a Linux VM at all on macOS.** Container primitives (namespaces, cgroups, the `clone(2)` flags we need) are *Linux kernel features*. macOS doesn't have them. Even Docker Desktop / OrbStack run a Linux VM under the hood — they just hide it. Since we're building the orchestrator, we want to be *in* that VM, not ducking around it. VirtioFS gives us the best of both worlds: edit on the Mac with your normal tools, build/run on Linux.

**Runbook — the env, end-to-end:**
1. Install OrbStack on macOS.
2. Create an Ubuntu Questing arm64 VM, name it `mykube-dev`.
3. OrbStack mounts `~` into the VM via VirtioFS by default.
4. Inside the VM: `rustup` for Rust 1.95+; `apt install libseccomp-dev pkg-config build-essential`.
5. From inside the VM, `cargo build` against the source on the Mac side.

## libcontainer smoke test

**Did:** Throwaway binary `src/bin/scratch.rs` (since deleted) that drove libcontainer end-to-end: built an OCI bundle, called `ContainerBuilder` → `create` → `start`, busybox ran, container exited cleanly. Cgroup v2 was auto-detected.

**Why this came next.** Before building any of the orchestrator layer, prove that the runtime layer *works at all* in our environment. If libcontainer couldn't run a single container, none of the higher abstractions would matter. This is the "tracer bullet" — one straight line from clean repo to a running container, ignoring all design.

**Decision — libcontainer over alternatives.** Alternatives considered:
- *Shell out to `runc`*: slower (subprocess + JSON-over-pipe), weaker error handling, and we'd be wrapping someone else's CLI instead of learning the API.
- *Hand-roll `clone(2)` + `pivot_root` + cgroup setup*: already learned in the prior Docker project — would be repeating ourselves.
- *libcontainer*: in-process Rust crate exposing the same operations as runc. Keeps the runtime layer "library-thin" so interesting code lives in our orchestrator.

**Key facts learned for Phase 1:**
- The working construction is `ContainerBuilder::new(id, syscall).with_root_path(state).with_executor(DefaultExecutor{}).as_init(bundle).with_systemd(false).build()`.
- `Container` has `start()`, `refresh_status()`, `status()`, `delete(force)`, `pid()` — but **no `wait()`**. We poll `refresh_status()` to detect crashes.
- `oci_spec::runtime::LinuxNamespace` has an optional `path` field — `None` = create new namespace, `Some("/proc/PID/ns/X")` = join existing. **This is the API we'll use for the pause-container pattern.**
- Root is required for namespace creation. Kubelet runs as root, like real K8s.

## Devcontainer wrap

**Did:** Wrapped the VM's toolchain in `.devcontainer/` so the build environment is reproducible from clean state.

**Why this closed Phase 0.** The phase's definition of done was "one keystroke from clean repo to a running container." Devcontainer makes that real — no manual "remember to install libseccomp" step.

---

# Phase 1 — Mini-kubelet

Scope: a single binary, `mykubelet`, that watches a directory of pod manifests and reconciles the actual running containers to match. **No API server yet** — the manifests directory IS the desired state. **No image pull yet** — every container runs from a shared busybox rootfs (image field parsed but ignored). All state is in-memory (no kubelet-restart persistence).

The order below is the order in which things were built. Each piece depends on the ones above it.

## 1. CLI skeleton (`src/bin/kubelet.rs`)

**Did:** A clap-based binary that parses three args (`--manifests-dir`, `--state-dir`, `--rootfs-base`) with sensible defaults under `/var/lib/my-k8s/`. Validates the rootfs exists with a helpful hint pointing at the prep script. Initializes `tracing-subscriber` honoring `RUST_LOG`. Deleted the throwaway `src/bin/scratch.rs` at this point.

**Why first.** Every later piece needed *some* harness to run inside, and arg parsing is the cheapest possible scaffold. Even before there's a reconciler, you want `kubelet --help` to print sensible defaults — that doubles as the spec for "what does this thing need to know."

**Decision — fail fast at startup if rootfs is missing.** Catching the missing rootfs at startup, with a hint pointing at `prepare-rootfs.sh`, is a cheap kindness to future-me. The alternative — letting the first container crash an hour into a run — would be miserable to debug.

**Decision — split `state_dir` from `pods_dir`.** `state_dir/` holds libcontainer state, `state_dir/pods/` holds our per-pod bundle dirs. Same root, different concerns. The CLI only takes `--state-dir` and derives `pods_dir()` from it (see `Args::pods_dir`).

## 2. Pod schema (`src/pod.rs`)

**Did:** `Pod`, `PodMetadata`, `PodSpec`, `Container` structs with serde + `rename_all = "camelCase"`. `Pod::from_yaml(s)` parses a string. Three unit tests: single-container parse, multi-container parse, garbage rejection.

**Why next.** The whole system is built around reconciling *toward* a Pod spec. Until the type exists, nothing else can take it as an argument. Defining the types first also forces an early decision about what's in scope for Phase 1 (turns out: very little).

**Decision — model `image` even though it's ignored.** The `Container::image` field is parsed but does nothing in Phase 1. Every container runs from the shared busybox rootfs. Modeling the field keeps the schema forward-compatible for the day image-pull lands — and a `// parsed but ignored` comment in the type itself makes the deferral self-documenting.

**Decision — `serde_yaml_ng` over `serde_yaml`.** The original `serde_yaml` crate is unmaintained; `serde_yaml_ng` is the maintained fork with the same API.

**Decision — `camelCase` serde rename.** Matches real K8s YAML conventions (`apiVersion`, not `api_version`). Saves a brain context switch when copying snippets from K8s docs.

## 3. `RuntimeClient` trait — the mini-CRI (`src/runtime.rs`)

**Did:** A six-method trait on `&mut self`: `create_container` / `start_container` / `kill_container` / `delete_container` / `container_state` / `container_pid`. Plus a `RuntimeError` enum (`NotFound`, `AlreadyExists`, `InvalidBundle`, `Other` wrapping anyhow) and a flat `ContainerState` enum (`Created`, `Running`, `Stopped`, `NotFound`).

**Why a trait when there's only one impl.** Two reasons, both load-bearing:
1. **Testability.** Everything above the trait can be tested with a mock that records calls — no root, no libcontainer, no real OCI bundle. The sandbox, the reconciler, and the integration test all sit on top of this seam. Without it, every test path would need a real Linux VM.
2. **The abstraction is the lesson.** Real K8s has CRI (Container Runtime Interface) for exactly the same reason: separating *what* the orchestrator wants from *how* a runtime achieves it lets you swap containerd ↔ CRI-O without touching the kubelet. Building our own tiny version is how we earn intuition for why CRI exists.

**Decision — sync trait, not async.** The underlying syscalls (`fork`, `exec`, `clone`) are sync. Wrapping them in async would buy nothing and add coloring problems. The reconciler bridges to async with `block_in_place` (see §10) where needed.

**Decision — `&mut self` everywhere.** libcontainer's `Container` holds raw file descriptors and is not safely shareable. The trait propagates that constraint upward instead of hiding it behind interior mutability. The honest constraint is more useful than a polite lie.

**Decision — `container_pid` exists.** It's specifically there for the pause-container pattern (§7). The pause container's PID becomes the path component in `/proc/{pid}/ns/net` that app containers join. Without this method, the sandbox couldn't be built.

## 4. Rootfs preparation (`scripts/prepare-rootfs.sh`)

**Did:** Idempotent shell script: `apt install busybox-static`, wipe `/var/lib/my-k8s/rootfs-base/`, copy the busybox binary in, symlink the applet names we care about (`sh httpd sleep echo tail wget cat ls ps mkdir rm cp mv true false`) to `/bin/busybox`. Also drops minimal `/etc/hosts` and `/etc/resolv.conf`.

**Why before any container code runs.** Bundle construction (§5) sets `root.path` to this directory. If it doesn't exist, every container creation will fail. Building this once-and-forget script up-front means container code can assume a real rootfs is there.

**Concept — what is a rootfs.** A "rootfs" is the directory tree a container sees as its `/`. The kernel's `pivot_root(2)` (which libcontainer calls under the hood) makes the container's mount namespace see that directory as the new root. Everything in `/bin`, `/etc`, etc. inside the container comes from here. Real K8s gets this from container image layers; we shortcut by pointing every container at the same shared, read-only busybox tree.

**Decision — busybox-static (not glibc + dynamic binaries).** A static binary has zero shared-library dependencies; we don't need to mirror `/lib`, `/lib64`, the dynamic linker, etc. Tiny rootfs (~1MB), zero glibc-version headaches.

**Decision — read-only rootfs (`root.readonly = true` in bundle.rs).** Every container in every pod shares the same on-disk rootfs. Read-only enforces that they can't corrupt each other through it. Writable per-container space comes from the `/tmp` tmpfs mount (§5).

## 5. Bundle construction (`src/runtime/bundle.rs`)

**Did:** Pure function `build_spec(container, rootfs_base, share_namespaces_from_pid)` returning an `oci_spec::runtime::Spec`. Plus `write_bundle(...)` which writes that spec to `<bundle_dir>/config.json`. The `share_namespaces_from_pid: Option<u32>` argument is the key: `None` = create all new namespaces (pause container), `Some(pid)` = join net/ipc/uts from that PID (app container).

**Why now.** The runtime trait (§3) takes a `bundle_path` — that bundle has to come from somewhere. This is the bridge between *Pod-world* (our typed `Container`) and *OCI-world* (the `config.json` libcontainer reads).

**Concept — what's in an OCI bundle.** Two things: (a) `config.json` describing process, root, namespaces, mounts, capabilities, etc.; (b) the rootfs at the path `config.json::root.path` points to. libcontainer reads both and uses the kernel APIs to instantiate the container. The OCI runtime spec is just *the contract* between orchestrator and runtime — we're producing one side of it.

**Concept — the per-container vs shared namespace decision.**
- **Per-container** (always `path: None` → new namespace): **PID**, **mount**. Each container gets its own process tree and its own mount view.
- **Shared from pause** (`path: Some("/proc/PID/ns/X")` when `share_namespaces_from_pid` is `Some`): **network**, **IPC**, **UTS**. All containers in a Pod see the same NICs, can talk via localhost, share `/dev/shm`, share hostname.
- This split is exactly what real K8s does (with `shareProcessNamespace: false` being the default). The four-test cluster in `bundle.rs::tests` pins this contract down.

**Decision — every container's process spec gets a hardened baseline.** `terminal: false`, `no_new_privileges: true`, uid/gid 0 (we're root inside, like real K8s default), `PATH=/bin`, `HOME=/`, `cwd=/`. Minimal `/proc` (proc), `/dev` (tmpfs, nosuid, 64K), `/sys` (sysfs, ro, nosuid/noexec/nodev), `/tmp` (tmpfs, 16M, sticky). These are the "if I forget any of this, weird stuff breaks" mounts — modeled on what `runc spec` produces.

**Gotcha — `oci-spec`'s builder API.** Most builders return `Result<T>` from `build()` because of internal validation. You'll be sprinkling `?` and `.context(...)` everywhere. Worth it for the type safety; the alternative is hand-constructing JSON, which is exactly the kind of thing we're getting away from.

## 6. `YoukiRuntime` — libcontainer behind the trait (`src/runtime/youki.rs`)

**Did:** A struct holding `state_dir: PathBuf` + `containers: HashMap<String, libcontainer::Container>`, implementing the six `RuntimeClient` methods. `create_container` builds via `ContainerBuilder`, inserts into the map; the other methods look the container up by id and call the corresponding libcontainer method.

**Why we cache `Container` instances in a map.** libcontainer's `Container` holds open file descriptors and the cached state from `ContainerBuilder`. Reconstructing it every time would be expensive and error-prone. The map = "the runtime's working set."

**Concept — `state_dir` maps to `with_root_path(...)`.** libcontainer's "root path" is where it keeps per-container state files (similar to runc's `--root`). One runtime instance owns one state dir; one process tree per state dir.

**Concept — mapping libcontainer's status enum to ours.**
- `Creating | Created` → our `Created`
- `Running | Paused` → our `Running`
- `Stopped` → our `Stopped`
- Container missing from our map → our `NotFound`

Flattening like this hides distinctions the orchestrator doesn't care about. (We never call pause/resume, and "creating" vs "created" is a transient state we don't react to.)

**Decision — `nix::sys::signal::Signal::try_from(i32)` for the signal conversion.** The trait takes a raw `i32` (so callers can write `libc::SIGTERM` directly), but libcontainer's `Container::kill` wants a typed `nix::sys::signal::Signal`. The boundary conversion lives here, where it's cheap and obvious — not pushed up into the trait, where it would force every caller to depend on `nix`.

**Gotcha — `nix` alone wasn't enough.** We pulled in *both* `nix` (typed wrapper, used here for `Signal`) and `libc` (raw constants, used by callers for `libc::SIGTERM`, `libc::SIGKILL`). The typed wrapper doesn't expose the constants in a way that's ergonomic for callers, so the raw `libc` constants stayed.

## 7. Pod sandbox — the pause-container pattern (`src/runtime/sandbox.rs`)

This is the single most K8s-distinctive thing in Phase 1. Worth a slow read.

**Did:** A `PodSandbox` struct owning one Pod's lifecycle. Methods: `create()` builds + runs the pause container and captures its PID; `add_container(container)` builds an app container that joins the pause's net/ipc/uts via `share_namespaces_from_pid = Some(pause_pid)`, then runs it; `remove_container(name)` gracefully kills (SIGTERM, then poll for Stopped up to a 5s grace, then `delete(force=true)`); `destroy()` removes all app containers in turn, then the pause, then `rm -rf`s the bundle dir tree.

**Concept — what the pause container is for.** A Pod is a *group of containers that share a network identity*. The naive way to share namespaces is "container A creates them, B and C join A's." But what if A dies and gets restarted? B and C lose their network. The pause container solves this by being a long-lived, do-nothing process that *owns* the shared namespaces. App containers join *its* namespaces. When an app container dies, the pause is untouched — its PID stays, its namespaces stay, so when the replacement app container is started, it joins the same namespaces. **Pod IP survives container restarts.**

**Concept — what "join a namespace" actually means at the syscall level.** When the OCI spec says `LinuxNamespace { typ: Network, path: Some("/proc/4242/ns/net") }`, libcontainer calls `setns(2)` against that file path before exec'ing the container's process. The file `/proc/PID/ns/net` is a magic kernel-provided handle that represents PID's network namespace; opening it and passing the fd to `setns` puts the calling process into that namespace. The pause container's PID becomes the *anchor* that keeps the namespace alive (a namespace is destroyed when its last reference is dropped).

**Decision — pause runs `/bin/busybox sleep infinity`.** Real K8s uses a tiny purpose-built `pause` binary that ignores signals and reaps zombies. Ours is good enough for Phase 1 — busybox sleep infinity holds the namespaces, that's all we need.

**Decision — container ID convention `{pod_name}__{container_name}` (double underscore).** Single underscore would collide with pod or container names containing `_`. The double underscore is unlikely to appear in user input. The pause container's id is `{pod_name}__pause`.

**Decision — `destroy()` removes app containers BEFORE the pause.** The `destroy_removes_app_containers_before_pause` test pins this ordering down. If the pause goes first, app containers' shared namespaces (net/ipc/uts) get destroyed out from under them and they enter undefined behavior — the kernel may unbind `lo`, drop `/dev/shm`, etc. mid-cleanup. Removing apps first keeps their world intact until they themselves are gone.

**Decision — graceful term polling lives here, not in `RuntimeClient`.** SIGTERM-then-wait-then-SIGKILL is an *orchestrator* policy, not a *runtime* contract. The runtime exposes the primitives (`kill_container`, `container_state`); the sandbox composes them into a policy. This way, a different sandbox impl could have a different grace period without touching the runtime trait.

**Decision — loopback networking set up via `nsenter`** (`setup_pod_network`, gated `#[cfg(not(test))]`). After the pause container is up, we shell out to `nsenter -t {pause_pid} -n ip link set lo up`. This brings `lo` up *inside* the pod's network namespace. It's a stand-in for the CNI `loopback` plugin, which real K8s would invoke here. We do it from the host (which has `CAP_NET_ADMIN`) rather than granting that capability to the pause container itself — same security posture as real K8s.

**Gotcha — partial create rollback (handled in the reconciler, §10).** If you successfully `create()` the sandbox, `add_container()` containers 1..N-1, then container N fails — you've leaked a pause container and N-1 app containers. The reconciler wraps this in a destroy-on-failure rollback. The sandbox itself doesn't auto-rollback inside `add_container` because the *correct* recovery sometimes requires more context than the sandbox has.

## 8. In-memory pod store (`src/store.rs`)

**Did:** A `Store` newtype wrapping `HashMap<PodName, PodState>` where `PodState { pod, sandbox }` pairs the manifest with the live sandbox handle. Methods are exactly what the reconciler will need: `insert`, `remove`, `get`, `get_mut`, `contains`, `names()`, `drain()`.

**Why now.** The reconciler needs to answer "what Pods do I currently have running?" The store is that answer.

**Concept — pairing desired with actual.** `PodState` puts the *desired* state (the `Pod` manifest) and the *actual* state (the `PodSandbox` that's currently running) in the same record. Reconciling = walking these pairs and asking "does actual match desired? if not, what's the smallest change to make them match?"

**Decision — no `Arc<Mutex<...>>` here.** The store is owned directly by the reconciler. No sharing across tasks → no synchronization needed. This is the *simplest possible* design that works in Phase 1. When Phase 2 introduces an HTTP API server, the API handler will need concurrent read access — *that's* when this gets refactored, not before.

**Decision — `drain()` exists specifically for shutdown.** Graceful shutdown needs to consume every `PodState` exactly once and run `sandbox.destroy()` on it. `drain()` empties the store and yields owned values — clean, no `Option::take`-style juggling.

## 9. Manifest watcher (`src/watcher.rs`)

**Did:** A single async function `scan(manifest_dir) -> HashMap<PodName, Pod>`. Reads every `.yaml`/`.yml` file in the dir, parses each as a `Pod`, returns the map. Malformed or unreadable files log a warning and are skipped — one bad file does NOT take down the whole scan. Duplicate Pod names across files: last file wins, with a warning.

**Why "scan" not "watch".** Real K8s controllers use list-watch (a streaming API), but the canonical advice is *also* to do a periodic full resync as defense against missed events. Our Phase 1 takes a shortcut: we *only* do the periodic resync (every 2s in the reconciler). It's simpler, it works, and it teaches the right reconciler shape. Switching to `notify` for filesystem events later would be a polish-level addition, not an architectural change.

**Concept — accept-and-warn, never crash.** A single malformed manifest must not kill the kubelet. The pattern `Result → log warning → continue` is everywhere in this function. Real K8s controllers behave the same way: an unparseable resource is logged as an `Event` but doesn't take the controller down.

**Decision — accept both `.yaml` and `.yml`.** Both are common; rejecting one would just trip future-me up.

## 10. Reconciler loop (`src/reconciler.rs`)

This is the heart of K8s in 250 lines.

**Did:** A `Reconciler<R: RuntimeClient>` owning the store, the runtime, the manifests dir, the rootfs base, and a `restart_state: HashMap<String, RestartTracker>` for CrashLoopBackOff. `run(cancel)` loops on a 2-second `tokio::time::interval`, calling `reconcile_once()` each tick. `reconcile_once` does an async manifest scan, then enters `block_in_place` to do the sync diff-and-act work. Each tick: (a) new pods → create; (b) gone pods → destroy; (c) existing pods → reconcile liveness.

**Why this is "the heart."** Every higher-level K8s primitive (Deployments, ReplicaSets, StatefulSets, Jobs) is *just another reconciler* on top of *just another resource*. Understanding this loop in our 250-line version is understanding the orchestrator pattern at large.

**Concept — the reconcile loop.** Read desired state, observe actual state, compute the diff, apply the smallest change to converge. **Idempotent** (running twice with the same desired state does nothing the second time) and **self-healing** (any drift gets pulled back next tick). The actual diff here is simple set arithmetic: `desired - actual = creates`, `actual - desired = deletes`, `desired ∩ actual = liveness checks`.

**Concept — CrashLoopBackOff.** When a container keeps dying, we don't want to restart it as fast as we can — that wastes CPU and floods logs. Instead, after each failure we increase the wait before the next attempt: 10s, 20s, 40s, ..., capped at 5min (production defaults). Production K8s does the same. Our `RestartTracker { restart_count, next_retry_at }` plus `compute_backoff(n) = BASE * 2^(n-1) capped at MAX` is the standard implementation pattern.

**Decision — backoff windows are scheduled *before* the restart attempt** (`reconciler.rs:221-223`, comment: "so a crash-then-recover loop can't bypass backoff by failing the restart itself"). If we bumped the count *after* a successful restart, an immediately-crashing container could enter a tight loop on the *restart path*. Scheduling the next window before attempting the restart closes that hole.

**Decision — observing `Running` clears the backoff** (`reconciler.rs:241-244`). A container that's now alive deserves a clean slate for its next crash. Without this, a long-running container that crashes once an hour from now would be slow to restart unnecessarily. The "Production K8s" equivalent is `RestartCount` on the Pod status; same shape, different surface.

**Decision — rollback on partial create failure** (`reconciler.rs:142-154`). If `sandbox.create()` succeeded and *some* containers were added but one failed, the half-built sandbox is destroyed before the error propagates. The alternative — leaving a half-built sandbox in the store — would mean the next reconcile tick sees it as "existing" and tries to do liveness on phantom containers. Bad.

**Decision — `biased` `tokio::select!`** (`reconciler.rs:75`). Without `biased`, when both the cancel signal and the tick are ready, tokio picks pseudo-randomly. With `biased`, cancel always wins. Matters during shutdown — we want to *stop* on cancel, not do one more tick first.

**Decision — `block_in_place` for the sync work** (`reconciler.rs:86, 94`). libcontainer calls are sync (multi-millisecond, potentially much longer). Calling them directly inside an async task would block the tokio scheduler thread, hurting any other tasks on the runtime. `block_in_place` tells tokio "this thread is going sync for a while — move other tasks off it." It's a `multi_thread` runtime feature; on a current-thread runtime this would deadlock.

**Decision — disjoint mutable borrows via destructuring** (`reconciler.rs:179-184`). Inside `reconcile_liveness`, we need `&mut` on three fields of `self` simultaneously: `store`, `runtime`, `restart_state`. A naive `self.store.get_mut(...)` then `self.runtime.container_state(...)` would error — `self` is already borrowed mutably. Destructuring `let Self { store, runtime, restart_state, .. } = self;` splits the borrow into three independent ones. This is a real production pattern, not a hack.

**Gotcha — restart_state grows unboundedly without explicit cleanup.** When a pod is removed (`remove_pod`), we explicitly delete the tracker entries for its containers (`reconciler.rs:168-171`). Otherwise the map would grow forever across pod churn. The test `remove_pod_clears_restart_trackers` pins this down.

## 11. Graceful shutdown (`src/bin/kubelet.rs`)

**Did:** `main` spawns the reconciler task. A second async function `wait_for_shutdown_signal` listens on both SIGTERM and SIGINT. `tokio::select!` waits for either the reconciler to exit *or* a signal. On signal: `cancel.cancel()`, then `await` the reconciler task — which (because of the cancel-priority `biased` select in §10) breaks out of its loop and calls `self.shutdown()`, which drains every `PodState` and runs `sandbox.destroy()` on each.

**Why this earned its place in Phase 1.** "No premature simplification" — graceful termination is small and canonical. Skipping it would leave orphan containers + libcontainer state files every time you Ctrl-C the kubelet during dev iteration, which is *every minute*. Doing it now means `clean-state.sh` becomes a fallback for crashes, not a routine workflow.

**Concept — cooperative cancellation with `tokio-util::sync::CancellationToken`.** The token is a shared flag any task can await (`cancel.cancelled()`). One side calls `cancel()`, every awaiter unblocks. This is the idiomatic way to signal "wind down" across async tasks in tokio. The reconciler awaits it in its `biased` select; the binary calls `cancel()` on signal receipt.

**Decision — handle both SIGTERM and SIGINT.** SIGTERM is what `kill` / orchestrators send for shutdown; SIGINT is what Ctrl-C sends in the terminal. Treating them identically gives a single shutdown path.

**Gotcha — `expect("failed to register SIGTERM handler")` on the signal handler setup.** This `expect` is fine: if we can't install a signal handler, the OS is misbehaving and we genuinely cannot do useful work. A `panic!` is the right response, not graceful degradation.

## 12. Mock-runtime integration test

**Did:** A `MockRuntime` in `reconciler::tests` that implements `RuntimeClient` by recording every call as a string in a `calls: Vec<String>`. It also supports canned per-id state sequences, canned PIDs, and an injectable "this create should fail" set for rollback testing. The integration tests drive the reconciler through every interesting state: empty → first pod → liveness restart → backoff window → recovery → partial-failure rollback → pod removal → shutdown.

**Why this is worth the boilerplate.** This test suite runs on macOS, in CI, with no root, no Linux, no libcontainer. *All* of the orchestration logic (sandbox lifecycle ordering, restart trigger, backoff, tracker cleanup, rollback) is exercised — only the bottom-most "actually fork+exec" step is mocked. This is exactly the leverage the `RuntimeClient` trait was introduced for in §3.

**Concept — "record calls, assert order."** Many tests don't check return values; they check the *sequence* of calls the system under test made on the mock. E.g. `destroy_removes_app_containers_before_pause` finds the index of two specific call strings and asserts ordering. This style is common for orchestration code where the *protocol* is the behavior, not the return values.

**Decision — test-only constants.** `BACKOFF_BASE` is 10s in production but 50ms under `#[cfg(test)]`. Letting the production constant leak into tests would mean a backoff-recovery test takes 10+ seconds. The test override keeps the suite fast (~80ms for the backoff test, see `backoff_skips_restart_within_window_then_fires_after_expiry`).

**Decision — unique tempdirs per test.** Every test uses a `unique_temp_dir(label)` based on PID + nanos. Avoids cross-test contamination when `cargo test` runs tests in parallel.

## 13. End-to-end demo

**Did:** Ran the real kubelet against real libcontainer against real busybox in the VM. Sequence verified:
1. `cp manifests/examples/web.yml manifests/active/` → reconciler logs sandbox creation; busybox httpd starts inside the pod's namespaces.
2. `cp manifests/examples/sidecar.yml manifests/active/` → both containers come up; `readlink /proc/<httpd-pid>/ns/net == /proc/<log-tail-pid>/ns/net` confirms shared netns.
3. `sudo kill -9 <httpd-pid>` → within ~2s the reconciler logs the restart; sandbox + sidecar untouched; pod IP unchanged.
4. `rm manifests/active/web.yml` → reconciler tears down all containers + sandbox; `/var/lib/my-k8s/state/pods/web/` cleaned up.
5. `kill -TERM <kubelet-pid>` → graceful shutdown path runs; no orphan processes (verified via `pgrep -f /var/lib/my-k8s` → empty).

**Why this matters even with the integration test passing.** The mock validates the *orchestration logic*. The e2e demo validates that *libcontainer actually does what we think it does* — that pause-PID/ns/net is a real shareable handle, that loopback comes up, that SIGTERM propagates, that `pivot_root` lands on the rootfs we built. These are the assumptions the integration test takes on faith.

**Runbook — first-time setup + dev iteration:**
1. *(once)* `sudo bash scripts/prepare-rootfs.sh` → populates `/var/lib/my-k8s/rootfs-base`.
2. `cargo build` → produces `target/debug/kubelet`.
3. `sudo target/debug/kubelet --manifests-dir ./manifests/active`
4. *(in another shell)* `cp manifests/examples/web.yml manifests/active/`
5. To iterate: Ctrl-C the kubelet → if needed, `sudo bash scripts/clean-state.sh` → rebuild → re-run.

**Gotcha — `clean-state.sh` exists for crash recovery.** Phase 1 is in-memory only: if the kubelet is killed before graceful shutdown (e.g. `kill -9`), libcontainer state files + orphan busybox processes will linger. `scripts/clean-state.sh` does `pkill -9 -f /bin/busybox` + `rm -rf /var/lib/my-k8s/state/*`. Safe in the dedicated dev VM; would be much too aggressive elsewhere.

## 14. Debug snapshot (`src/reconciler.rs` + `scripts/myctl.sh`)

**Did:** At the end of every reconcile tick, the reconciler writes a pretty-printed JSON snapshot of its internal state to disk (default: `/var/lib/my-k8s/state/debug.json`). The snapshot includes the unix timestamp, pod count, and per-pod info: name, pause PID, plus per-container name/command/restart_count/backoff_remaining_secs. Wired in via a new `Option<PathBuf>` field on `Reconciler` so tests pass `None` and write nothing. Added a throwaway `scripts/myctl.sh` that `sudo cat`s the dump through `jq` for ad-hoc queries.

**Why this came AFTER the e2e demo.** The e2e demo (§13) is exactly what made the need obvious. Once you're driving the kubelet by hand and a container enters CrashLoopBackOff, you cannot tell *from logs alone* whether the backoff window is in flight, what restart count it's on, or when the next attempt fires. Logs tell you what *happened*; you also need to see what the system thinks *is*. The debug snapshot is the smallest possible answer to that — a single JSON file you can `cat` or `jq` against at any moment.

**Concept — observability before complexity.** Phase 1 added two non-trivial behaviors (CrashLoopBackOff, partial-create rollback) whose state lives entirely inside the reconciler. Without a way to see that state, you'd debug failures by `grep`ing logs and guessing. Materializing it as a queryable file is the same instinct real K8s satisfies with the kubelet's `/pods` and `/metrics` HTTP endpoints — we're just doing the *tiny* version of that. Every system that gets more complex than "one loop, one trace" needs *some* introspection surface; doing it now means future bugs in Phase 2+ will be easier to chase.

**Concept — the if-let chain (Rust 2024 edition).** `reconciler.rs:131-135` uses `if self.debug_dump_path.is_some() && let Err(e) = self.write_debug_snapshot() { warn!(...) }`. Combining a boolean condition with an `if let` in a single `if` is a 2024-edition feature. Before this it would have been a nested `if let Some(_) = ... { if let Err(e) = ... }`. Nicer to read.

**Decision — `Option<PathBuf>` instead of always-on.** Tests construct the reconciler with `None` and skip the write entirely. Three benefits: (a) test runs don't pollute `/tmp` with snapshot files; (b) the snapshot's correctness isn't a test surface — we're not asserting JSON shape in unit tests; (c) the production path is one ergonomic constructor away (`Some(state_dir.join("debug.json"))`). Keeping the path optional rather than gating on `#[cfg(debug_assertions)]` means a release build can still produce the snapshot — *the user might be the operator*, not just the dev.

**Decision — write at the end of every reconcile tick.** Simplest possible schedule. The cost is tiny (a few hundred bytes per pod, written every 2s). No separate "snapshot interval" knob, no background task, no debouncing. If snapshot writes ever become expensive, we can revisit — but YAGNI for Phase 1.

**Decision — best-effort, never fail the tick.** A failed snapshot write logs a warning and returns. The reconcile path itself doesn't care. Observability that takes down the orchestrator when it stumbles is worse than no observability.

**Decision — pretty-printed JSON, not protobuf / msgpack / line-delimited.** This file is meant for *human* eyes and `jq` queries. Wire-format efficiency is not a constraint here. The snapshot file is read at most a few times a minute, by a person.

**Decision — `myctl.sh` is explicitly throwaway.** The script's header comment says it: *"THROWAWAY — Phase 2 will replace this with proper API server endpoints + a real kubectl-shaped client."* Naming this expectation up front prevents the script from accreting features ("oh, let me add `myctl logs`...") that would just have to be torn out when the real API server lands. The right time to build a kubectl is when there's an API server for it to talk to.

**Decision — `serde_json` added as a dep.** We already had `serde_yaml_ng` for parsing manifests, but YAML output for a debug dump would be wrong: JSON is what `jq` speaks. The crate is essentially free in compile-time terms (already in the transitive graph via `oci-spec`).

**Runbook — using the snapshot:**
```bash
# Full state:
bash scripts/myctl.sh
# Just pod names:
bash scripts/myctl.sh '.pods[].name'
# A specific pod:
bash scripts/myctl.sh '.pods[] | select(.name=="web")'
# Restart counts across all containers:
bash scripts/myctl.sh '.pods[].containers[].restart_count'
```
Override the path with `MY_K8S_DEBUG=/path/to/debug.json bash scripts/myctl.sh`.

**Gotcha — the snapshot is up to one tick stale.** It's written at end-of-tick, not continuously, so a state change made by a long-running operation within a tick won't appear until that tick completes. For Phase 1's 2-second tick this is invisible; worth knowing if a future tick gets longer.

**Gotcha — the snapshot file is owned by root.** The kubelet runs as root, so the file is too — hence the `sudo cat` in `myctl.sh`. The alternative (chmod the dump world-readable) would be the kind of "convenience that leaks info" decision worth avoiding even in a learning project.

---

## Phase 1 wrap — what this earned us

A working single-node mini-kubelet that demonstrates the core K8s patterns: declarative reconciliation, the pause-container/shared-netns Pod sandbox, the CRI-shaped runtime abstraction, liveness reconciliation with CrashLoopBackOff, graceful shutdown. It hits `libcontainer` directly for the runtime layer (no shelling out, no kube-rs). All non-runtime logic is testable without root or Linux.

What we explicitly chose NOT to build yet: image pull, HTTP/exec probes, volumes, env vars, resource limits, the API server, kubelet-restart persistence, multi-node. Each of those is a phase (or part of one) further on.

**Phase 2 (next) is the API server.** The manifests directory stops being the desired state; an HTTP service does instead. The store moves out of the kubelet's process and into a server with watch streams. The kubelet becomes a *client* of the API server — and once it's a client, you can run multiple kubelets, which sets up everything that follows.
