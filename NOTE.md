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
| 2 | API server v1 | kubelet talks to API server over HTTP; multiple kubelets possible ⬅ **in progress** |
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

**What a kubelet is, and why it needs exactly these inputs.** A kubelet is the per-node agent — the thing that actually runs containers on *one* machine and keeps them matching a desired spec. To do that job it has to answer three questions, and the three CLI args map one-to-one onto them:

| Question the kubelet must answer | Arg | Phase 1 answer | Real K8s answer |
|---|---|---|---|
| "What am I supposed to be running?" (desired state) | `--manifests-dir` | a directory of YAML files | the API server, via a watch stream |
| "Where do I keep my bookkeeping?" (actual state) | `--state-dir` | `/var/lib/my-k8s/state` | `/var/lib/kubelet` |
| "What filesystem do containers get?" (rootfs) | `--rootfs-base` | one shared busybox tree | per-image layers from the image store |

So the arg list isn't arbitrary — it's the *minimal set of facts a kubelet cannot function without*. Reading those three lines tells you the kubelet's entire job.

**Did:** A clap-based binary parsing those three args with defaults under `/var/lib/my-k8s/`. Validates the rootfs exists (with a hint pointing at the prep script). Initializes `tracing-subscriber` honoring `RUST_LOG`. Deleted the throwaway `src/bin/scratch.rs` at this point.

**Concrete example — what running it looks like:**
```
$ sudo target/debug/kubelet --manifests-dir ./manifests/active
INFO kubelet starting args=Args { manifests_dir: "./manifests/active",
     state_dir: "/var/lib/my-k8s/state", rootfs_base: "/var/lib/my-k8s/rootfs-base" }
INFO reconciler started
```

**Visualization — what the three paths point at on disk:**
```
./manifests/active/          ← --manifests-dir  (desired state: you drop YAML here)
    web.yml

/var/lib/my-k8s/
├── rootfs-base/             ← --rootfs-base  (read-only, shared by EVERY container)
│   └── bin/busybox
└── state/                   ← --state-dir  (the kubelet's private bookkeeping)
    ├── debug.json           ←   the §14 snapshot
    └── pods/                ←   per-pod OCI bundles  (state_dir/pods = "pods_dir")
        └── web/
            ├── __pause/config.json
            └── server/config.json
```

**Why these defaults.** `/var/lib/<app>/` is the Filesystem Hierarchy Standard home for "variable state data a program maintains" — persists between runs, but isn't config and isn't logs. The real kubelet uses `/var/lib/kubelet`. Following the convention means anyone who knows Linux knows where to look.

**Why this comes first.** Every later piece needs *some* harness to run inside, and arg parsing is the cheapest scaffold. `kubelet --help` doubles as the spec for "what does this thing fundamentally need to know."

**Decision — fail fast if rootfs is missing.** Validated at startup with a hint pointing at `prepare-rootfs.sh`. The alternative — letting the first container crash an hour into a run — would be miserable to debug.

**Decision — split `state_dir` from `pods_dir`.** The CLI takes only `--state-dir`; `pods_dir()` is derived as `state_dir/pods` (see `Args::pods_dir`). The diagram above shows why: libcontainer's own state files and our per-pod bundle dirs live side by side under one root, but they're different concerns, so the code names them separately.

## 2. Pod schema (`src/pod.rs`)

**Did:** `Pod`, `PodMetadata`, `PodSpec`, `Container` structs with serde + `rename_all = "camelCase"`. `Pod::from_yaml(s)` parses a string. Three unit tests: single-container parse, multi-container parse, garbage rejection.

**How the YAML maps to the types.** serde's job is to turn the left into the right. The nesting is identical; the only transform is `camelCase` ↔ `snake_case`:
```
apiVersion: v1                 Pod {
kind: Pod                          api_version: "v1",
metadata:                          kind: "Pod",
  name: web              ──►       metadata: PodMetadata { name: "web" },
spec:                              spec: PodSpec {
  containers:                          containers: vec![
    - name: server                         Container {
      image: busybox                           name: "server",
      command: ["httpd"]                       image: "busybox",  // ignored (Phase 1)
                                               command: vec!["httpd"],
                                           },
                                       ],
                                   },
                               }
```

**Why this comes next.** The whole system reconciles *toward* a Pod spec. Until the type exists, nothing else can take it as an argument. Defining the types first also forces an early decision about what's in scope for Phase 1 (turns out: very little — four structs, three fields that matter).

**Decision — model `image` even though it's ignored.** Parsed but does nothing in Phase 1; every container runs from the shared busybox rootfs. Modeling the field keeps the schema forward-compatible for when image-pull lands, and a `// parsed but ignored` comment in the type makes the deferral self-documenting.

**Decision — `camelCase` serde rename.** Matches real K8s YAML (`apiVersion`, not `api_version`), so snippets copy-paste from K8s docs without edits. The `#[serde(rename_all = "camelCase")]` attribute does this for every field at once.

**Decision — `serde_yaml_ng` over `serde_yaml`.** The original `serde_yaml` crate is unmaintained; `serde_yaml_ng` is the maintained fork with the same API.

## 3. `RuntimeClient` trait — the mini-CRI (`src/runtime.rs`)

**The one-sentence idea.** A *trait* (Rust's version of an interface) that says "here is what a container runtime must be able to do" — without saying *how*. Everything above it speaks only this vocabulary; the real libcontainer code lives below it.

**Why this seam matters — the layering.** Picture the system as a stack. The trait is the horizontal line in the middle:
```
        ┌─────────────────────────────────────────┐
        │  Reconciler  (the orchestration logic)   │
        │  Sandbox     (pause-container lifecycle)  │
        └─────────────────────────────────────────┘
                          │  speaks only RuntimeClient
        ━━━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━━━━━━━━━  ← the trait (the seam)
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
   ┌─────────────────┐         ┌──────────────────┐
   │  YoukiRuntime   │         │   MockRuntime    │
   │  (real: fork/   │         │  (test: records  │
   │   exec via      │         │   calls in a     │
   │   libcontainer) │         │   Vec<String>)   │
   └─────────────────┘         └──────────────────┘
   needs root + Linux          runs anywhere, incl. CI on a Mac
```
Anything above the line never imports libcontainer. That's what lets the *exact same* reconciler logic run against the real runtime in the VM and against a mock in a unit test.

**Did:** A six-method trait on `&mut self`. Each method maps to one step of a container's life:
```
create_container(id, bundle)   build it from an OCI bundle, don't run yet
start_container(id)            run the init process
container_state(id) -> State   "is it Created / Running / Stopped / NotFound?"  (poll this)
container_pid(id)   -> Option  the init PID — needed so others can join its namespaces (§7)
kill_container(id, signal)     send SIGTERM / SIGKILL
delete_container(id, force)    remove runtime state (force = kill first)
```
Plus a `RuntimeError` enum (`NotFound`, `AlreadyExists`, `InvalidBundle`, `Other`-from-anyhow) and a flattened `ContainerState` enum (`Created`, `Running`, `Stopped`, `NotFound`).

**Why a trait when there's only one real impl.** Two reasons, both load-bearing:
1. **Testability.** Everything above the seam is tested with `MockRuntime`, which just appends each call to a `Vec<String>`. No root, no libcontainer, no OCI bundle. The reconciler's whole behavior (restart logic, backoff, teardown ordering) is verified by asserting on that recorded call list (§12). Without the seam, every test would need a real Linux VM.
2. **The abstraction *is* the lesson.** Real K8s has CRI (Container Runtime Interface) for exactly this reason: separate *what* the orchestrator wants from *how* a runtime delivers it, and you can swap containerd ↔ CRI-O without touching the kubelet. We rebuilt the tiny version, so the design pressure that produced CRI is now something you've felt, not just read about.

**Comparison to Go.** This is the same move as a Go interface — `type RuntimeClient interface { CreateContainer(...) error; ... }` with a real impl and a mock impl. The difference: in Go any type satisfies the interface implicitly if it has the methods; in Rust you write `impl RuntimeClient for YoukiRuntime` explicitly. Rust's version is checked at compile time and the intent is visible at the impl site.

**Decision — sync, not async.** The underlying syscalls (`fork`, `exec`, `clone`) are synchronous. Wrapping them in `async` buys nothing (there's no I/O to await) and spreads async "color" through code that doesn't need it. The reconciler bridges to async with `block_in_place` at the one boundary where it matters (§10).

**Decision — `&mut self` everywhere.** libcontainer's `Container` holds raw file descriptors and can't be safely shared. The trait surfaces that constraint honestly (`&mut`) rather than hiding it behind interior mutability. A polite lie here would just move the pain to a runtime panic later.

**Decision — `container_pid` exists *for* §7.** It looks odd on a generic runtime interface — why expose a PID? Because the pause-container pattern needs `/proc/{pid}/ns/net` as the handle app containers join. The method is a hook placed here specifically so the sandbox can be built on top. (Real CRI exposes sandbox info similarly.)

## 4. Rootfs preparation (`scripts/prepare-rootfs.sh`)

**Concept — what is a rootfs, concretely.** A "rootfs" is just a directory on the host that the container will see as its entire filesystem — its `/`. When the container starts, the kernel's `pivot_root(2)` (called by libcontainer) swaps the process's view of `/` to point at this directory. From inside, `/bin/sh` means `<rootfs>/bin/sh` on the host.
```
   On the host:                        Inside the container, this looks like:
   /var/lib/my-k8s/rootfs-base/   ──►   /
   ├── bin/busybox                      ├── bin/busybox
   ├── bin/sh  -> busybox               ├── bin/sh      (it's busybox)
   ├── bin/httpd -> busybox             ├── bin/httpd   (also busybox)
   ├── etc/hosts                        ├── etc/hosts
   └── etc/resolv.conf                  └── etc/resolv.conf
```
Real K8s assembles this directory from the layers of a container *image*. We skip image-pull entirely and point every container at one shared, prebuilt busybox tree.

**Did:** Idempotent script: `apt install busybox-static`, wipe `/var/lib/my-k8s/rootfs-base/`, copy the busybox binary, symlink the applets we use (`sh httpd sleep echo tail wget cat ls ps mkdir rm cp mv true false`) → `/bin/busybox`, drop minimal `/etc/hosts` + `/etc/resolv.conf`.

**Concept — why one binary serves every command.** busybox is a single executable that changes behavior based on the name it's invoked as (`argv[0]`). Symlink `sh`, `httpd`, `sleep` all to the one `busybox` file, and calling `/bin/httpd` makes busybox act like httpd. That's why a ~1MB rootfs has a dozen "commands" in it.

**Why before any container code runs.** Bundle construction (§5) sets `root.path` to this directory. If it's missing, every container creation fails. Building it once up-front lets all container code assume a real rootfs is present (and §1's startup check enforces it).

**Decision — busybox-*static* (not dynamic).** A static binary has zero shared-library dependencies, so we don't have to mirror `/lib`, `/lib64`, or the dynamic linker into the rootfs. Tiny, and no glibc-version mismatches between host and container.

**Decision — read-only rootfs (`root.readonly = true`, §5).** Every container shares the same on-disk tree, so read-only stops one container from corrupting another through it. Each container still gets writable scratch space via the per-container `/tmp` tmpfs mount (§5).

## 5. Bundle construction (`src/runtime/bundle.rs`)

**Where this sits.** The runtime trait (§3) takes a `bundle_path`. This is the code that *produces* that bundle. It's the translator between two worlds:
```
   Pod-world (our types)          OCI-world (what libcontainer reads)
   ┌──────────────────┐          ┌─────────────────────────────────┐
   │ Container {      │  build_  │ <bundle_dir>/config.json         │
   │   name, image,   │  spec()  │   { "process": {...},            │
   │   command }      │ ───────► │     "root": {"path": "<rootfs>"},│
   │ + rootfs path    │          │     "linux": {"namespaces":[..]},│
   │ + share-from-pid │          │     "mounts": [...] }            │
   └──────────────────┘          └─────────────────────────────────┘
```

**Concept — what an OCI bundle actually is.** Two things on disk: (a) a `config.json` describing the process to run, the root filesystem, namespaces, mounts, etc.; (b) the rootfs that `config.json`'s `root.path` points at (our §4 busybox tree). libcontainer reads the config and uses kernel APIs to build exactly that container. The "OCI runtime spec" is just the agreed *shape* of that JSON — a contract between orchestrators and runtimes. We're writing one side of it.

A trimmed `config.json` for an app container looks like:
```json
{
  "process": { "args": ["httpd","-f","-p","8080"], "cwd": "/",
               "user": {"uid": 0, "gid": 0}, "noNewPrivileges": true },
  "root": { "path": "/var/lib/my-k8s/rootfs-base", "readonly": true },
  "mounts": [ {"destination":"/proc","type":"proc"}, {"destination":"/tmp","type":"tmpfs"} ],
  "linux": { "namespaces": [
      {"type":"pid"},                                       // new (per-container)
      {"type":"mount"},                                     // new (per-container)
      {"type":"network","path":"/proc/4242/ns/net"},        // JOIN pause's
      {"type":"ipc",    "path":"/proc/4242/ns/ipc"},        // JOIN pause's
      {"type":"uts",    "path":"/proc/4242/ns/uts"} ] }     // JOIN pause's
}
```

**Did:** Pure function `build_spec(container, rootfs_base, share_namespaces_from_pid)` → `oci_spec::runtime::Spec`, plus `write_bundle(...)` that serializes it to `<bundle_dir>/config.json`. The whole pause-vs-app distinction rides on that one `Option<u32>` argument.

**Concept — the per-container vs shared namespace split (the heart of this file).** A namespace entry with no `path` means "make a fresh one"; with a `path` of `/proc/PID/ns/X` it means "join the existing one owned by PID." Which namespaces get which treatment is *the* Pod-defining decision:

| Namespace | App container gets | Why |
|---|---|---|
| **PID** | its own (new) | each container has its own process tree; `shareProcessNamespace: false` is the K8s default |
| **mount** | its own (new) | each container has its own filesystem view |
| **network** | **joins pause** | all containers in a Pod share one IP and can talk over `localhost` |
| **IPC** | **joins pause** | shared `/dev/shm`, SysV IPC between containers in the Pod |
| **UTS** | **joins pause** | shared hostname |

This is *exactly* what real K8s does. The `share_namespaces_from_pid` argument is `None` for the pause container (it creates all five fresh) and `Some(pause_pid)` for every app container (it joins the three shared ones). The four tests in `bundle.rs::tests` pin this contract down — e.g. `app_container_keeps_pid_and_mount_per_container` asserts those two stay `path: None`.

**Decision — every container gets a hardened, runc-like baseline.** `terminal: false`, `no_new_privileges: true`, uid/gid 0 (root inside, the K8s default), `PATH=/bin`, `HOME=/`, `cwd=/`; mounts for `/proc`, `/dev` (tmpfs 64K), `/sys` (ro), `/tmp` (tmpfs 16M, sticky). These are the "forget one and something breaks weirdly" defaults — copied from what `runc spec` generates, because reinventing them by trial-and-error is pure pain.

**Gotcha — `oci-spec`'s builders return `Result`.** `ProcessBuilder::default()....build()?` — most builders validate on `build()`, so you sprinkle `?` + `.context(...)` throughout. Verbose, but it's the price of not hand-writing JSON (which is exactly the footgun we're escaping).

## 6. `YoukiRuntime` — libcontainer behind the trait (`src/runtime/youki.rs`)

**What this is.** The *real* implementation of the §3 trait — the one place in the codebase that actually touches libcontainer. It's a thin adapter: each trait method translates into the matching libcontainer call.
```
   RuntimeClient method          →   what YoukiRuntime does
   create_container(id, bundle)  →   ContainerBuilder...build(); map.insert(id, container)
   start_container(id)           →   map[id].start()
   container_state(id)           →   map[id].refresh_status(); map status enum → ours
   container_pid(id)             →   map[id].pid()
   kill_container(id, sig)       →   map[id].kill(Signal::try_from(sig), false)
   delete_container(id, force)   →   map.remove(id).delete(force)
```

**Why we cache `Container` instances in a `HashMap`.** libcontainer's `Container` value holds open file descriptors and builder-derived state. Rebuilding it from scratch on every call would be expensive and lossy. So the runtime keeps a `HashMap<String, Container>` — think of it as "the set of containers this runtime currently knows about." `create` inserts, `delete` removes, everything else looks up by id.

**Concept — `state_dir` → libcontainer's `with_root_path(...)`.** libcontainer's "root path" is where it persists per-container state files on disk (the same idea as runc's `--root`). One `YoukiRuntime` owns one `state_dir`. This is the on-disk half; the `HashMap` is the in-memory half.

**Concept — flattening libcontainer's status into ours.** libcontainer has five status values; the orchestrator only cares about four states. We collapse:
```
   libcontainer status        our ContainerState
   ─────────────────────      ──────────────────
   Creating ┐
   Created  ┘            ──►   Created
   Running  ┐
   Paused   ┘            ──►   Running      (we never pause, so Paused ≈ Running)
   Stopped              ──►   Stopped
   (id not in our map)  ──►   NotFound
```
Hiding distinctions the caller doesn't act on keeps the reconciler's match arms small. This is a general API-design move: *the abstraction should expose only the states its consumer makes decisions on.*

**Decision — signal conversion lives at this boundary.** The trait takes a raw `i32` so callers write `libc::SIGTERM` without depending on `nix`. libcontainer's `Container::kill` wants a typed `nix::sys::signal::Signal`. So `Signal::try_from(i32)` happens *here*, at the adapter — the one place that already depends on `nix`. Pushing the typed signal up into the trait would force every caller (and the mock) to pull in `nix` for no benefit.

**Gotcha — why both `nix` AND `libc` are dependencies.** `nix` gives the typed `Signal` used inside this adapter; `libc` gives the raw `SIGTERM`/`SIGKILL` integer constants used by callers (the sandbox, §7). The typed wrapper doesn't expose those constants ergonomically for callers, so both crates stay. Not redundancy — two different jobs.

## 7. Pod sandbox — the pause-container pattern (`src/runtime/sandbox.rs`)

This is the single most K8s-distinctive thing in Phase 1. Worth a slow read.

**The problem it solves.** A Pod is "a group of containers that share one network identity" — same IP, can talk over `localhost`. Someone has to *own* the shared network/IPC/UTS namespaces. The obvious idea — "let the first app container own them, the rest join" — breaks the moment that container crashes and restarts: its namespaces die with it, and every other container in the Pod loses its network.

**The fix — a do-nothing anchor.** Introduce a tiny extra container (`pause`) whose *only* job is to hold the namespaces. App containers join the pause's namespaces, never each other's. The pause never crashes (it just `sleep infinity`s), so the namespaces — and the Pod IP — outlive any number of app-container restarts.
```
   ┌───────────────────────── Pod "web" ──────────────────────────┐
   │                                                               │
   │   ┌──────────┐   owns    net / ipc / uts namespaces           │
   │   │  pause   │◄──────────┐                                    │
   │   │ (sleep ∞)│           │ join (setns)                       │
   │   │ pid 4242 │           │                                    │
   │   └──────────┘     ┌─────┴──────┐      ┌────────────┐         │
   │                    │  server    │      │  log-tail  │         │
   │                    │ (own pid + │      │ (own pid + │         │
   │                    │  mount ns) │      │  mount ns) │         │
   │                    └────────────┘      └────────────┘         │
   │        all three share ONE IP, reach each other on localhost  │
   └───────────────────────────────────────────────────────────────┘
```

**Why it survives a crash — before/after.** Say `server` (pid 5001) is `kill -9`'d:
```
   BEFORE crash                         AFTER crash + restart
   pause   4242  ── owns net ns         pause   4242  ── STILL owns net ns  (untouched)
   server  5001  ── joined 4242's        server  ────  (gone)
   log-tail 5002 ── joined 4242's       log-tail 5002  ── STILL joined 4242's (untouched)
                                        server' 5099  ── joins 4242's net ns again
                                                          → same Pod IP, log-tail never noticed
```
Because the namespace's lifetime is tied to the *pause* PID, not the app PID, the restart is invisible to the rest of the Pod. **This is the whole reason the pattern exists**, and it's why real K8s has a pause container too.

**Concept — what "join a namespace" means at the syscall level.** When the OCI config says `{"type":"network","path":"/proc/4242/ns/net"}`, libcontainer opens that file and calls `setns(2)` on it before `exec`-ing the container's process. `/proc/PID/ns/net` is a kernel-provided magic handle representing PID 4242's network namespace; `setns` moves the calling process into it. A namespace stays alive as long as *something* references it — the pause process is that something. Kill the last referencer and the kernel tears the namespace down.

**Did:** A `PodSandbox` owning one Pod's lifecycle:
```
   create(runtime)          build+run pause; capture pause_pid; bring lo up (see below)
   add_container(rt, c)     build c's bundle with share_from_pid=Some(pause_pid); run it
   remove_container(rt, n)  SIGTERM → poll ≤5s for Stopped → delete(force=true) → rm bundle
   destroy(rt)              remove ALL app containers, THEN delete pause, THEN rm pod dir
```

**Decision — pause runs `/bin/busybox sleep infinity`.** Real K8s ships a purpose-built `pause` binary that ignores signals and reaps zombie processes. Ours just needs to hold namespaces and stay alive, which `sleep infinity` does. Good enough for Phase 1; the zombie-reaping nicety can come later if we ever share the PID namespace.

**Decision — container ID convention `{pod}__{container}` (double underscore).** A single `_` could collide with a pod or container name that legitimately contains `_`; the double underscore is far less likely in real input. Pause is `{pod}__pause`. This id is what's passed to every `RuntimeClient` call.

**Decision — `destroy()` removes app containers BEFORE the pause** (the ordering the diagram demands, in reverse). If the pause died first, every app container's shared net/ipc/uts namespace would be yanked out from under it mid-cleanup — the kernel could unbind `lo`, drop `/dev/shm`, etc., and the app teardown would hit undefined behavior. Tear down in reverse-dependency order: apps first (they're the dependents), pause last (it's the anchor). The `destroy_removes_app_containers_before_pause` test locks this in by asserting the delete-call ordering.

**Decision — graceful-term polling lives here, not in `RuntimeClient`.** "SIGTERM, wait up to 5s, then force-delete" is an *orchestrator policy*, not a *runtime primitive*. The trait (§3) exposes the primitives (`kill_container`, `container_state`); the sandbox *composes* them into the policy. Keeping policy out of the trait means a different sandbox could choose a different grace period without anyone touching the runtime layer. (General principle: mechanisms go in the lower layer, policy in the higher one.)

**Decision — loopback set up via `nsenter`** (`setup_pod_network`, `#[cfg(not(test))]`). Right after the pause is up, we run `nsenter -t {pause_pid} -n ip link set lo up` to bring `lo` up *inside* the Pod's fresh network namespace (a brand-new netns has `lo` down). This is our stand-in for the CNI `loopback` plugin. We run it from the host (which holds `CAP_NET_ADMIN`) rather than granting that capability to the pause container — same security posture as real K8s, where the kubelet/CNI does the wiring, not the Pod.

**Gotcha — partial-create rollback is the reconciler's job (§10), not the sandbox's.** If `create()` succeeds and containers 1..N-1 are added but container N fails, you've leaked a live pause + N-1 app containers. The sandbox deliberately does *not* self-rollback inside `add_container`, because correct recovery sometimes needs context the sandbox lacks (e.g. "is this a fresh create or a mid-life add?"). The reconciler wraps the create sequence and calls `destroy()` on failure.

## 8. In-memory pod store (`src/store.rs`)

**Did:** A `Store` newtype wrapping `HashMap<PodName, PodState>`, with the methods the reconciler needs: `insert`, `remove`, `get`, `get_mut`, `contains`, `names()`, `drain()`. The value type is the key idea:
```
   PodState {
       pod:     Pod,          // DESIRED — the manifest we were told to run
       sandbox: PodSandbox,   // ACTUAL  — the live pause+containers we're running
   }
```

**Concept — one record holds both sides of the comparison.** Reconciliation is forever asking "does actual match desired?" By storing the desired `Pod` and the actual `PodSandbox` *in the same struct*, that question becomes a local one: for each `PodState`, compare its two fields. The store is just "all the Pods I currently know about," keyed by name — the reconciler's source of truth for *actual* state.

**Decision — no `Arc<Mutex<...>>`.** The store is owned outright by the reconciler; nothing else touches it, so there's nothing to synchronize. This is the simplest thing that works. Phase 2's API server *will* need concurrent access — and that's exactly when we'll add the locking, not a moment sooner. (Adding it now would be speculative complexity for a sharing scenario that doesn't exist yet.)

**Decision — `drain()` exists for shutdown.** Graceful shutdown must consume each `PodState` exactly once and call `sandbox.destroy()` on it. `drain()` empties the map and hands back owned values, so there's no borrow-vs-move juggling — see §11.

## 9. Manifest watcher (`src/watcher.rs`)

**Did:** One async function `scan(manifest_dir) -> HashMap<PodName, Pod>`. Reads every `.yaml`/`.yml` file, parses each as a `Pod`, returns the map = the **desired state** for one reconcile tick. Malformed/unreadable files log a warning and are skipped; duplicate Pod names across files → last one wins (with a warning).

**Concept — "scan" vs "watch", and why we picked the dumber one.** Real K8s uses *list-watch*: list everything once, then stream incremental changes (a Pod was added/modified/deleted). It's efficient but has a failure mode — a dropped event leaves you out of sync. So real controllers *also* run a periodic full re-list ("resync") as a safety net. Phase 1 keeps only the safety net: every 2s the reconciler calls `scan()` and re-reads the whole directory. Less efficient, impossible to desync, and it teaches the correct reconciler shape (you reconcile against a full snapshot, not a delta). Swapping in the `notify` crate for real filesystem events later would be polish, not a redesign.
```
   real K8s:   list ──► watch (stream of deltas) ──► periodic resync (safety net)
   Phase 1:    ──────────────────────────────────── periodic resync only (every 2s)
```

**Concept — accept-and-warn, never crash.** One malformed manifest must not take down the kubelet. The pattern here is `Result → log warning → continue` on every file. Real controllers behave identically: a bad resource becomes a logged `Event`, not a process exit. A control-plane component that dies on the first bad input is useless in production.

**Decision — accept both `.yaml` and `.yml`.** Both extensions are common in the wild; rejecting one would just trip future-me up for no reason.

## 10. Reconciler loop (`src/reconciler.rs`)

This is the heart of K8s in 250 lines.

**Why this is "the heart."** Every higher-level K8s primitive — Deployments, ReplicaSets, StatefulSets, Jobs — is *just another reconciler watching just another resource*. Internalize this 250-line loop and you've internalized the orchestrator pattern that the entire control plane is built from.

**Concept — the reconcile loop, in one picture.** Don't issue commands ("create container X"). Instead, every tick: read what's *wanted*, observe what *is*, and make the smallest change that moves *is* toward *wanted*.
```
        ┌─────────────────────────────────────────────────┐
        │                  every 2 seconds                 │
        │                                                  │
        │   desired = watcher::scan()   ← manifests dir    │
        │   actual  = store             ← what's running   │
        │                                                  │
        │   diff(desired, actual):                         │
        │       in desired, not in actual   → CREATE pod   │
        │       in actual,  not in desired  → DESTROY pod  │
        │       in both                     → check LIVENESS│
        │                                                  │
        └─────────────────────────────────────────────────┘
                         repeat forever
```
Two properties fall out of this shape for free:
- **Idempotent** — run it twice on an unchanged world and the second run does nothing (the diff is empty).
- **Self-healing** — any drift (a crashed container, a hand-deleted manifest) is just diff that gets corrected on the next tick. Nobody has to *send* a repair command; the loop notices and converges.

**Concept — the three-way diff is plain set arithmetic.** Keys are Pod names:
```
   desired = { web, api }          (manifests on disk this tick)
   actual  = { web, cache }        (sandboxes currently running)

   desired − actual = { api }      → CREATE   (new manifest appeared)
   actual − desired = { cache }    → DESTROY  (manifest was removed)
   desired ∩ actual = { web }      → LIVENESS (already running; is it healthy?)
```

**Trace one tick (concrete).** Suppose `web.yml` exists and its `server` container was just `kill -9`'d:
```
   1. scan() → desired = { web }
   2. web is already in the store → not a create, not a destroy → liveness path
   3. reconcile_liveness("web"): for container "server",
        runtime.container_state("web__server") → Stopped
   4. Stopped → restart path:
        tracker = restart_state["web__server"]  (created, count 0)
        not in backoff (next_retry_at is now) → proceed
        tracker.count = 1; next_retry_at = now + 10s   ← schedule BEFORE acting
        sandbox.remove_container("server")   (kill+delete the dead one)
        sandbox.add_container("server")      (fresh one joins SAME pause netns)
   5. write_debug_snapshot()  (§14)
   → next tick, server is Running → tracker cleared
```

**Did:** A `Reconciler<R: RuntimeClient>` owning the store, runtime, the two dirs, and `restart_state: HashMap<String, RestartTracker>` for backoff. `run(cancel)` loops on a 2s `tokio::time::interval` calling `reconcile_once()`; that does the async `scan()` then `block_in_place` for the sync diff-and-act. The three diff branches live in `apply_diff` (creates, destroys, liveness).

**Concept — CrashLoopBackOff, on a timeline.** A container that keeps dying must not be restarted in a tight loop (that pegs a CPU and floods logs). After each failure, wait longer before the next attempt — `BASE · 2^(n−1)`, capped at `MAX`:
```
   crash #1 ─restart─► crash #2 ──wait 10s──► restart ─► crash #3 ──wait 20s──►
   restart ─► crash #4 ──wait 40s──► ... ──► capped at 5 min
   (production: BASE=10s, MAX=5min.  tests: BASE=50ms, MAX=500ms — so the suite is fast)
```
`RestartTracker { restart_count, next_retry_at }` per container; `compute_backoff(n) = BASE * 2^(n-1)` capped at `MAX`. A sustained `Running` observation clears the tracker, so the *next* crash starts the ladder over from 10s. This is precisely real K8s's behavior — same name, same curve.

**Decision — backoff windows are scheduled *before* the restart attempt** (`reconciler.rs:221-223`, comment: "so a crash-then-recover loop can't bypass backoff by failing the restart itself"). If we bumped the count *after* a successful restart, an immediately-crashing container could enter a tight loop on the *restart path*. Scheduling the next window before attempting the restart closes that hole.

**Decision — observing `Running` clears the backoff** (`reconciler.rs:241-244`). A container that's now alive deserves a clean slate for its next crash. Without this, a long-running container that crashes once an hour from now would be slow to restart unnecessarily. The "Production K8s" equivalent is `RestartCount` on the Pod status; same shape, different surface.

**Decision — rollback on partial create failure** (`reconciler.rs:142-154`). If `sandbox.create()` succeeded and *some* containers were added but one failed, the half-built sandbox is destroyed before the error propagates. The alternative — leaving a half-built sandbox in the store — would mean the next reconcile tick sees it as "existing" and tries to do liveness on phantom containers. Bad.

**Decision — `biased` `tokio::select!`** (`reconciler.rs:75`). Without `biased`, when both the cancel signal and the tick are ready, tokio picks pseudo-randomly. With `biased`, cancel always wins. Matters during shutdown — we want to *stop* on cancel, not do one more tick first.

**Decision — `block_in_place` for the sync work** (`reconciler.rs:86, 94`). libcontainer calls are sync (multi-millisecond, potentially much longer). Calling them directly inside an async task would block the tokio scheduler thread, hurting any other tasks on the runtime. `block_in_place` tells tokio "this thread is going sync for a while — move other tasks off it." It's a `multi_thread` runtime feature; on a current-thread runtime this would deadlock.

**Decision — disjoint mutable borrows via destructuring** (`reconciler.rs:179-184`). Inside `reconcile_liveness`, we need `&mut` on three fields of `self` simultaneously: `store`, `runtime`, `restart_state`. A naive `self.store.get_mut(...)` then `self.runtime.container_state(...)` would error — `self` is already borrowed mutably. Destructuring `let Self { store, runtime, restart_state, .. } = self;` splits the borrow into three independent ones. This is a real production pattern, not a hack.

**Gotcha — restart_state grows unboundedly without explicit cleanup.** When a pod is removed (`remove_pod`), we explicitly delete the tracker entries for its containers (`reconciler.rs:168-171`). Otherwise the map would grow forever across pod churn. The test `remove_pod_clears_restart_trackers` pins this down.

## 11. Graceful shutdown (`src/bin/kubelet.rs`)

**The flow — signal to clean exit.**
```
   SIGTERM / SIGINT ──► wait_for_shutdown_signal() returns
                              │
                              ▼
                        cancel.cancel()           (flip the shared flag)
                              │
                              ▼
   reconciler's `biased` select sees cancel WINS over the next tick
                              │
                              ▼
        loop breaks ──► shutdown(): store.drain() ──► sandbox.destroy() per Pod
                              │
                              ▼
              every container + pause killed, bundle dirs removed,
                       no orphans left behind → process exits
```

**Did:** `main` spawns the reconciler task. `wait_for_shutdown_signal` awaits SIGTERM or SIGINT. A `tokio::select!` races "reconciler finished" against "signal arrived"; on signal it calls `cancel.cancel()` then `await`s the task so teardown completes before `main` returns.

**Why this earned its place in Phase 1.** "No premature simplification" — graceful termination is small and canonical, and skipping it hurts *immediately*: every Ctrl-C during dev (i.e. constantly) would orphan containers and leave stale libcontainer state. Doing it now demotes `clean-state.sh` from "routine step" to "crash-only fallback."

**Concept — cooperative cancellation with `CancellationToken`.** The token is a shared flag many tasks can `await` via `cancel.cancelled()`. One `cancel()` call wakes every awaiter at once. It's the idiomatic tokio way to broadcast "wind down" — no channels to wire, no per-task plumbing. The reconciler awaits it inside its `biased` select (§10); the binary fires it on signal. *Cooperative* means nothing is force-killed: the reconciler chooses to stop at a safe point (between ticks), so teardown always runs on consistent state.

**Decision — SIGTERM and SIGINT share one path.** SIGTERM is what `kill`/orchestrators send; SIGINT is Ctrl-C. Both mean "stop," so both route to the same `cancel()` — one shutdown path, no duplicated logic.

**Gotcha — `expect()` on signal-handler registration is correct here.** If the OS won't let us install a SIGTERM handler, the process can't do its one job safely; panicking is the honest response. This is the rare case where `expect` beats graceful error handling — there's no meaningful recovery.

## 12. Mock-runtime integration test (`src/reconciler.rs` `#[cfg(test)]`)

**The core trick — assert on a recorded call log.** `MockRuntime` implements `RuntimeClient` (§3) by doing nothing real — it just appends a string for each call to a `Vec<String>`. Tests then assert on that list. Because the reconciler only ever speaks the trait, swapping the real runtime for this recorder exercises *all* the orchestration logic with zero containers:
```
   Reconciler ──RuntimeClient──► MockRuntime
                                   calls: [
                                     "create(web__pause)",
                                     "start(web__pause)",
                                     "pid(web__pause)",
                                     "create(web__server)",   ← assert pause precedes app
                                     "start(web__server)",
                                   ]
```

**Did:** A `MockRuntime` with: the `calls` log; canned per-id `state_seq` (so a test can say "report Stopped once, then Running"); canned `pids`; and an injectable `create_should_fail` set for the rollback test. The suite drives the reconciler through every interesting transition — empty → first pod → liveness restart → backoff window → recovery → partial-failure rollback → pod removal → shutdown.

**Why this is worth the boilerplate.** It runs on macOS, in CI, no root, no Linux, no libcontainer — yet covers sandbox lifecycle ordering, restart triggering, backoff math, tracker cleanup, and rollback. Only the bottom-most fork+exec is faked. *This payoff is the entire reason §3 introduced a trait.*

**Concept — testing the protocol, not the return value.** Orchestration bugs are usually *ordering* bugs ("we deleted the pause before the app"). So many assertions check the *sequence* of calls, not their results: `destroy_removes_app_containers_before_pause` finds the indices of two delete-call strings and asserts one precedes the other. When the behavior under test *is* a protocol, the call log is the natural thing to assert on.

**Decision — test-only constants via `#[cfg(test)]`.** `BACKOFF_BASE` is 10s in production but 50ms under test, `MAX` 5min vs 500ms. Without the override a single backoff-recovery test would take 10+ seconds; with it the whole suite stays in the ~ms range. Same code path, time-scaled.

**Decision — unique tempdirs per test.** Each test calls `unique_temp_dir(label)` (PID + nanos), so parallel `cargo test`/`nextest` runs never collide on the bundle-dir paths.

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

**Phase 2 is the API server.** The manifests directory stops being the desired state; an HTTP service does instead. The store moves out of the kubelet's process and into a server with watch streams. The kubelet becomes a *client* of the API server — and once it's a client, you can run multiple kubelets, which sets up everything that follows.

---

# Phase 2 — API server, watch streams, persistent store

**The architectural hinge.** Phase 1 had one process that owned everything: the manifests directory *was* the desired state, and the kubelet read it directly. Phase 2 cleaves the system in two:
```
   Phase 1:                          Phase 2:
   ┌─────────────────┐               ┌──────────────┐   HTTP    ┌──────────────────┐
   │     kubelet     │               │   kubelet    │ ◄───────► │    apiserver     │
   │  reads ./manifests              │  (a CLIENT)  │  watch +  │  owns the state  │
   │  owns all state │               │              │  REST     │  sled-backed     │
   └─────────────────┘               └──────────────┘           └──────────────────┘
                                          ▲                          ▲
                                          │   …and now N clients      │
                                          └── (future: scheduler, controllers) all
                                              coordinate through ONE source of truth
```
This is *the* move that makes K8s K8s: once desired state lives behind an API with watch streams, any number of components can observe and act on it independently. Everything later (controllers, scheduler, multi-node) is just "another client of this API."

**New crates:** `axum` (HTTP server), `sled` (embedded KV store — our stand-in for etcd), `reqwest` (HTTP client), `uuid` + `chrono` (identity + timestamps), `tokio-stream` / `async-stream` (the watch stream), `tower` (dev-dep, for testing axum handlers via `oneshot`).

**Status as of this writing.** The full vertical slice now compiles and tests green: library, `apiserver` binary, `Client`, and the migrated `kubelet` binary. `cargo check --all-targets` is clean. The kubelet reads desired state from the apiserver (informer loop, §7) *and* reports observed state back (status loop, §8). `src/watcher.rs` is gone — the apiserver replaces the directory watch. What's left for a full Phase 2 demo is an end-to-end run in the VM (apiserver + kubelet + `curl`-driven Pod create → container runs → status reported), plus a `mykubectl` client; the pieces are wired but the live multi-process demo hasn't been recorded here yet.

The order below mirrors the dependency order: wire types → storage → watch → HTTP surface → server bin → client → the kubelet's informer loop → the kubelet's status loop.

## 1. Wire format — Pod gains status + apiserver metadata (`src/pod.rs`)

**Why the type grew.** In Phase 1 a Pod was spec-only — purely "what I want." Now it round-trips through an apiserver, so it needs two new things: server-assigned **identity/versioning metadata**, and a **status** subresource ("what's actually true," reported back by whoever runs it).

**Concept — spec vs status.** This split is fundamental to K8s:
- **spec** = *desired* state. Written by the client (you). The apiserver never changes it except on an explicit spec write.
- **status** = *observed* state. Written by the component doing the work (the kubelet, via `PUT /status`). Phase 1 had no status because there was no one to report to.

`status: Option<PodStatus>` with `#[serde(skip_serializing_if = "Option::is_none")]` — a freshly-submitted spec-only Pod carries no `status` key on the wire at all, rather than a null.

**Concept — the four apiserver-managed metadata fields.** All server-owned; a client can't forge them:

| field | who sets it | meaning |
|---|---|---|
| `name` | client | the Pod's name — its unique key |
| `uid` | apiserver, on create | identity *across name reuse*: delete `web`, recreate `web` → different uid, so a stale actor can't confuse the two |
| `resourceVersion` | apiserver, on **every** write | optimistic-concurrency token; an opaque monotonic counter (see §2) |
| `generation` | apiserver, bumped only on **spec** change | "spec revision number" — does NOT move when only status changes |
| `creationTimestamp` | apiserver, on create | RFC3339 creation time |

**`PodStatus`** carries `phase` (`Pending`/`Running`/`Succeeded`/`Failed`/`Unknown`), `container_statuses`, and `observed_generation` — *which spec generation this status reflects*. That last field is the feedback signal: compare `status.observedGeneration` to `metadata.generation` to answer "has the kubelet caught up to my latest spec edit yet?"

**Gotcha — `PodPhase` stays PascalCase on the wire.** K8s uses `"Running"`, not `"running"`. The enum deliberately has *no* `rename_all`, and the test `pod_phase_serializes_as_pascalcase` guards against someone "tidying" it with a camelCase rename that would silently break wire compatibility.

**Concept — externally-tagged enums = K8s's container-state shape.** `ContainerStatusState` is a Rust enum with data-carrying variants. serde's default ("external tagging") serializes it exactly like K8s's container state:
```
   ContainerStatusState::Waiting                         → "waiting"
   ContainerStatusState::Running { started_at }          → {"running":{"startedAt": "..."}}
   ContainerStatusState::Terminated { exit_code: 137 }   → {"terminated":{"exitCode":137}}
```
Unit variant → bare string; struct variant → single-key object keyed by the (camelCased) variant name. We got the K8s wire shape for free from serde's defaults — pinned by `container_status_state_uses_external_tagging`.

## 2. PodStore — persistent storage + optimistic concurrency (`src/apiserver/storage.rs`)

This is our **etcd**. The single most important Phase 2 concept lives here.

**Did:** A `PodStore` over a `sled::Db`. Pods stored as JSON at key `pods/<name>`. A global monotonic counter at `rv_counter`. CRUD methods (`create`/`get`/`list`/`replace_spec`/`replace_status`/`delete`) that each enforce optimistic concurrency and emit a watch event. A secondary `rv_index` tree (zero-padded rv → name) for rv-ordered lookups.

**Concept — `resourceVersion` and optimistic concurrency (the heart of the K8s API).** Every write bumps one global counter; the new value is stamped onto the object as its `resourceVersion`. To update an object you must send back the rv you read. If it no longer matches the stored rv, your write is rejected — someone changed it underneath you.
```
   Client A         Client B            store (rv, pod)
   ───────          ───────             ──────────────
   read web (rv=5)                      (5, web)
                    read web (rv=5)     (5, web)
   PUT web rv=5 ───────────────────────►(6, web')   ✓ accepted, rv→6
                    PUT web rv=5 ───────►              ✗ Conflict{current:6, provided:5}
                    re-read web (rv=6), retry
```
This is lost-update prevention *without locks*: a read-modify-write that fails loudly instead of silently clobbering. Real K8s does exactly this; the rv is opaque to clients (they just echo it back).

**Concept — the atomic transaction.** The rv-check and the counter-bump must happen as one indivisible step, or two concurrent writers could both pass the check before either bumps. `sled`'s `transaction()` gives us that: inside the closure we `load_required_pod` → `check_rv` → `bump_rv` → `insert`, all-or-nothing. The `ConflictableTransactionError::Abort` path carries our typed `StoreError` (e.g. `Conflict`, `AlreadyExists`) back out; `unwrap_txn` unwraps it.

**Concept — create mints identity and *clobbers* client-supplied server fields.** On `create`, the store overwrites `uid` (fresh UUID), `generation` (=1), `creationTimestamp` (=now), `resourceVersion` (minted), and discards any client-sent `status`. Why: those fields are server-owned. A client must not be able to forge a uid, pin an rv, or pre-seed status. Pinned by `create_clobbers_client_provided_apiserver_fields`.

**Concept — `generation` vs `resourceVersion` (two counters, two jobs).**
```
   write kind        resourceVersion    generation
   ──────────        ───────────────    ──────────
   create            → 1                → 1
   replace_spec      → bumps            → bumps      (spec changed)
   replace_status    → bumps            → unchanged  (only observed state changed)
   delete            → bumps            → n/a        (fresh rv on the DELETED event)
```
`resourceVersion` answers "did *anything* change?" (drives watch). `generation` answers "did the *desired spec* change?" (drives `observedGeneration` reconciliation). A status write bumping rv-but-not-generation is the machinery behind "has the controller acted on my latest spec?" — locked in by `replace_status_bumps_rv_but_not_generation`.

**Decision — `replace_spec` preserves server history.** A spec update keeps the existing `uid`, `creationTimestamp`, and `status` (you're changing desired state, not erasing identity or the last observed state), bumps `generation` + `resourceVersion`. `delete` bumps rv *before* removing so the emitted DELETED event carries a fresh, ordered rv.

**Concept — every write emits a watch event.** Each successful mutation calls `emit()` on a `tokio::sync::broadcast` channel (`Added`/`Modified`/`Deleted` + the object). That broadcast is what §3 turns into live watch streams. Subscribers that aren't listening yet simply miss nothing *or* lag (handled in §3). `writes_emit_watch_events_in_order` pins the ordering.

## 3. Watch streaming (`src/apiserver/watch.rs`)

The watch is what makes K8s **reactive** instead of poll-based. Heavy.

**Concept — list-then-watch, in two phases.** A watcher wants "everything as of now, then every change after, with no gap and no duplicate." `stream_events(store, from_rv)` delivers exactly that:
```
   ┌─ phase 1: CATCH-UP ──────────────┐   ┌─ phase 2: LIVE ─────────────────┐
   │ list() snapshot @ snapshot_rv    │   │ subscribe to broadcast channel  │
   │ emit ADDED for each pod          │   │ forward each event              │
   │   with rv > from_rv              │ → │   with rv > snapshot_rv          │
   └──────────────────────────────────┘   └──────────────────────────────────┘
        (objects rv ≤ snapshot_rv)               (objects rv > snapshot_rv)
                          the rv boundary dedupes the handoff:
              nothing is emitted twice, nothing between the phases is lost
```

**Gotcha — subscribe BEFORE snapshotting (the correctness lynchpin).** The code does `store.subscribe()` *then* `store.list()`. Reverse that order and a write landing between list and subscribe would vanish — absent from the snapshot, and not yet subscribed. Subscribing first means any such write is buffered in the broadcast channel and replayed in the live phase; the `rv > snapshot_rv` filter discards it only if the snapshot already contained it. This subscribe-then-list ordering is the classic watch-cache argument, and `live_events_after_catch_up_are_delivered` exercises it.

**Concept — `Lagged` → 410 Gone → client must re-list.** The broadcast channel is bounded (256). A client that falls more than 256 events behind gets `RecvError::Lagged`. We *cannot* silently skip — the client's local cache would be permanently wrong. So we terminate the stream with `WatchError::Lagged`; the HTTP layer closes the connection; the client re-lists from scratch and starts a fresh watch. Real K8s returns `410 Gone` with identical meaning: "your resourceVersion is too old to resume from — start over." Pinned by `lagged_receiver_terminates_stream_with_error`.

**Why `async_stream::try_stream!`.** Implementing `Stream`/`poll_next` by hand is fiddly state-machine code. `try_stream!` lets us write the catch-up loop and the live loop as ordinary straight-line async with `yield`; a `?` inside cleanly ends the stream on error. The result is an `impl Stream<Item = Result<WatchEvent, WatchError>>`.

## 4. HTTP surface — routes + handlers (`src/apiserver/{routes,handlers}.rs`)

The REST API that wraps the store. Medium.

**The surface:**
```
   GET    /api/v1/pods                 list_or_watch_pods   → PodList  (or watch stream)
   POST   /api/v1/pods                 create_pod           → 201 + Pod
   GET    /api/v1/pods/:name           get_pod              → Pod | 404
   PUT    /api/v1/pods/:name           replace_pod_spec     → Pod
   DELETE /api/v1/pods/:name           delete_pod           → Pod   (needs ?resourceVersion=)
   PUT    /api/v1/pods/:name/status    replace_pod_status   → Pod   (needs ?resourceVersion=)
```

**Concept — list and watch are the same endpoint.** `GET /pods` returns a `PodList` JSON; `GET /pods?watch=true` returns a *streaming* body of newline-delimited `WatchEvent`s instead. One route, switched by a query param — exactly K8s's convention. The `ListWatchParams` extractor carries `watch` + `resourceVersion`.

**Concept — the `Status` envelope.** Errors don't return a bare string; they return a structured `{kind:"Status", apiVersion, code, message, reason}` object. This mirrors K8s and lets clients machine-match on `reason` rather than parsing prose. The mapping is a two-step chain:
```
   StoreError            ──From──►   ApiError          ──IntoResponse──►   HTTP
   NotFound                          NotFound                              404  reason "NotFound"
   AlreadyExists                     AlreadyExists                         409  reason "AlreadyExists"
   Conflict{current,provided}        Conflict                             409  reason "Conflict"
   (handler validation)              BadRequest                           400  reason "BadRequest"
   Sled / Json                       Internal                             500  reason "Internal"
```

**Decision — destructive/versioned writes require `?resourceVersion=`.** `delete` and `replace_status` reject the request with 400 if the rv query param is missing. This pushes the optimistic-concurrency contract (§2) all the way out to the HTTP boundary — you can't delete or status-update without saying which version you think you're acting on. (`replace_spec` carries its rv in the body's `metadata.resourceVersion` instead.)

**Decision — boundary validation in `validate_pod`.** Name non-empty, containers non-empty → 400 before anything touches the store. Validate at the edge; trust the core.

**Concept — NDJSON for the watch body.** The watch handler maps each `WatchEvent` → `serde_json::to_vec` + `b'\n'`, and hands the stream to `Body::from_stream`. Newline-delimited JSON is trivially decodable line-by-line by the client (§6). Tests use `tower`'s `oneshot` to drive the router without a real socket.

## 5. apiserver binary (`src/bin/apiserver.rs`)

Plumbing. Light.

**Did:** clap args `--listen` (`SocketAddr`, default `0.0.0.0:8080`) and `--db` (sled path, default `/var/lib/my-k8s/etcd-like`). Creates the DB's parent dir, opens sled, builds the router with shared `AppState { store: Arc<PodStore> }`, and serves via `axum::serve(...).with_graceful_shutdown(...)`.

**Concept — graceful shutdown via axum's hook.** `with_graceful_shutdown(future)` makes the server stop accepting new connections and drain in-flight ones once `future` resolves. We feed it the same SIGTERM/SIGINT `wait_for_shutdown_signal` pattern the kubelet uses (Phase 1 §11) — consistent shutdown semantics across both binaries.

**Gotcha — the sled DB is the new "do not wipe" state.** `/var/lib/my-k8s/etcd-like/` now holds all persisted desired state. Wiping it is the apiserver equivalent of dropping etcd — it's on the destructive-actions list in `CLAUDE.md`.

## 6. Client (`src/client.rs`)

The typed Rust client the kubelet (and future controllers) use instead of hand-rolling HTTP. Medium.

**Did:** A `Client` wrapping `reqwest`, with methods mirroring the REST surface: `list_pods`, `get_pod`, `create_pod`, `replace_pod_spec`, `replace_pod_status`, `delete_pod`, and `watch_pods` → a `Stream` of `WatchEvent`.

**Concept — absence is `Ok(None)`, failures are typed.** `get_pod` maps a 404 to `Ok(None)` — a missing Pod isn't an error, it's a valid answer. Other failures map through the `Status` envelope into typed `ClientError` variants (`NotFound`, `AlreadyExists`, `Conflict`, …) via `map_envelope`, so callers `match` on Rust enums, never on raw HTTP codes. This is the client-side mirror of §4's error chain.

**Concept — decoding the watch stream (the adapter chain).** The server sends NDJSON. Turning an HTTP byte-stream into a typed event stream is a four-link chain, each link a standard tokio adapter:
```
   res.bytes_stream()                Stream<Result<Bytes>>      raw HTTP body chunks
     → StreamReader::new(..)         AsyncRead                  bytes-as-a-reader
     → FramedRead::new(.., Lines)    Stream<Result<String>>     split on '\n'
     → .map(parse as WatchEvent)     Stream<Result<WatchEvent>> typed events
```
Worth remembering: `StreamReader` (Stream→AsyncRead) and `FramedRead + LinesCodec` (AsyncRead→line Stream) are the canonical way to line-frame any async byte source.

**Decision — client tests hit a REAL apiserver.** `spawn_test_apiserver()` binds `127.0.0.1:0` (OS-assigned port), serves the actual router on a background task, and returns a `Client` pointed at it. So these are true end-to-end HTTP round-trips, not mocks — the watch test even drives a store write *after* opening the watch and asserts the event arrives over the wire. This catches serialization/route/status-mapping bugs a mock would hide.

## 7. Informer-style reconciler loop (`src/reconciler.rs`)

The kubelet's brain, rebuilt around the apiserver. Heavy.

**The shift from Phase 1.** Phase 1's reconciler polled a directory every 2s and did one combined diff. The Phase 2 reconciler is an **informer**: it opens a long-lived watch stream and reacts to events as they arrive, backed by a periodic resync, with container health handled on its own clock.
```
   run() loop — tokio::select! { biased }
   ┌────────────────────────────────────────────────────────────────────┐
   │ cancel.cancelled()      → break → shutdown() (destroy all sandboxes) │
   │ watch_stream.next()     → apply_watch_event()   react NOW to spec    │
   │ resync_interval (30s)   → resync()              relist + full diff   │
   │ liveness_interval (2s)  → tick_liveness()       restart crashed      │
   └────────────────────────────────────────────────────────────────────┘
```

**Concept — three clocks, three concerns.** The power of the informer shape is separating these:
- **watch** — react to *desired-state* changes with near-zero latency (a Pod was created/modified/deleted in the apiserver).
- **resync (30s)** — the *correctness backstop*. Watch streams can drop, lag, or close; a periodic full relist + diff heals any divergence no matter what was missed. (Real K8s informers resync on a ~10-minute default; 30s is a dev-friendly choice.)
- **liveness (2s)** — *actual-state* health. Containers crash for reasons unrelated to any spec change, so health polling + CrashLoopBackOff (carried over from Phase 1 §10) runs on its own tick against the cache.

This watch-for-latency + resync-for-safety pairing *is* the K8s informer pattern. Watch alone is fast but lossy; resync alone is safe but slow; together they're both.

**Concept — the cache.** A `HashMap<PodName, Pod>` mirroring the apiserver's desired state — updated incrementally by watch events, replaced wholesale on resync. `tick_liveness` iterates the *cache* (not the store) because the cache is the authoritative "what should be running"; the store holds the live sandboxes (actual).

**Concept — startup recovery (closes the Phase 1 gap).** Phase 1 lost all sandbox knowledge on kubelet restart — in-memory only, so a restart orphaned every running container. `startup()` now reconciles live reality against desired state:
```
   1. runtime.recover_all()  → walk libcontainer's state dir, rebuild a handle for
                               every container still alive → Vec<RecoveredContainer{id,state,pid}>
   2. client.list_pods()     → desired state from the apiserver
   3. for each desired pod:
        recovered pause exists?  YES → REATTACH: PodSandbox::from_recovered(pause_pid, app_names)
                                       (reuse the live pause PID — containers keep running, no restart)
                                 NO  → create a fresh sandbox
   4. recovered containers with no matching desired pod → ORPHANS → destroy
```
This is how a real kubelet survives its own restart without disturbing running Pods: it adopts what's alive and reconciles, instead of assuming a blank slate. The new primitives are `RecoveredContainer` (on the `RuntimeClient` trait, via `recover_all`) and `PodSandbox::from_recovered`, which reconstructs the same `{pod}__pause` id and app-name list the original `create()` produced, so ids line up for future calls. Pinned by `from_recovered_populates_fields_without_touching_runtime`.

**Gotcha — `RuntimeClient` grew `recover_all`.** Adding a trait method means every impl must provide it. `YoukiRuntime` walks the state dir for real; `MockRuntime` returns an empty `Vec`, so the Phase 1 mock-based reconciler/sandbox tests still pass unchanged (they never recover).

**Resolved since first draft.** The kubelet entrypoint is now migrated (see §8) — `Reconciler::new` is fed an `Arc<Client>`, and `cargo check --all-targets` is clean. This section's "reads desired state" half is complete; §8 adds the "reports observed state back" half.

## 8. Kubelet as a full client — reporting status back (`src/bin/kubelet.rs` + `src/reconciler.rs`)

This closes the loop the whole phase has been building toward. Heavy.

**The bin migration (the gap from earlier, now closed).** `src/bin/kubelet.rs` dropped `--manifests-dir` and gained `--api-server-url` (default `http://127.0.0.1:8080`). It constructs `Arc::new(Client::new(url))` and hands that to `Reconciler::new`. The kubelet is now a *true apiserver client*: it no longer reads any local directory — desired state arrives over HTTP via the watch (§7), and observed state goes back over HTTP via status writes (below).

**Concept — the two halves of a control loop.** Reconciliation isn't only "make actual match desired." It's also "publish what actual *is*, so everyone else can see it." Phase 1 only ever had the local `debug.json` snapshot (§14) — observed state that never left the node. Now the kubelet writes a real `PodStatus` back to the apiserver:
```
        ┌─────────────── apiserver (source of truth) ───────────────┐
        │  spec (desired)                         status (observed)  │
        └───────▲───────────────────────────────────────▲───────────┘
                │ watch / list (§7)                       │ PUT /status (§8)
        desired │  flows DOWN                    observed │  flows UP
                │                                         │
        ┌───────┴─────────────────────────────────────────┴─────────┐
        │  kubelet:  run containers to match spec  →  observe  →  report│
        └───────────────────────────────────────────────────────────┘
```
With status flowing up, a `kubectl get pods` (future) — or any other client — can see whether a Pod is actually Running without touching the node. This is why K8s status is a first-class, separately-written subresource.

**Concept — `compute_pod_status`: rolling container states up into one phase.** For each container the kubelet reads its runtime state and maps it to a `ContainerStatusState` + `ready` flag, then rolls the set up into a single Pod `phase` by **precedence**:
```
   any container Waiting (Created / NotFound)   → Pending    ← checked FIRST
   else all containers Stopped                  → Failed
   else any container Running                   → Running
   else                                         → Unknown
```
Order matters: a Pod with one container still starting is `Pending` *even if* a sibling is already `Running` — "not fully up yet" dominates. `observed_generation` is set to the spec's `metadata.generation`, so a reader can tell *which* spec revision this status reflects (the §2 generation-vs-rv idea, now consumed).

**Gotcha — two honest placeholders.** `started_at` is the literal string `"unknown"` (we don't track real start timestamps yet), and a `Stopped` container reports `exit_code: 0` unconditionally (we don't capture real exit codes yet). Both are visible Phase 2 shortcuts, not finished behavior — flagged here so a future reader doesn't trust those two fields.

**Concept — level-triggered dedup (`last_pushed_status`).** The liveness tick fires every 2s, but a Pod's status rarely changes that often. Re-PUTting an identical status each tick would: bump `resourceVersion` for nothing, wake every watcher, and spam the apiserver. So `tick_liveness` now returns a *dirty set* — only the `(name, status)` pairs where the freshly-computed status differs from `last_pushed_status[name]`:
```
   tick: compute status for each cached pod
         computed == last_pushed?  → skip (not dirty)
         computed != last_pushed?  → add to dirty set, will push
   (last_pushed_status[name] cleared when the pod is removed, so the map can't grow unbounded)
```
This is *level-triggered* reporting: push on **change**, not on schedule. Same instinct as the reconcile loop itself (act on the diff, not the clock). Pinned by `tick_liveness_marks_dirty_then_dedups_after_push`.

**Concept — `push_status`: optimistic concurrency from the client side.** The `/status` endpoint requires `?resourceVersion=` (§4), so a status write is a read-modify-write against the version the kubelet last saw:
```
   1. rv = cache[name].resourceVersion
   2. PUT /pods/name/status?resourceVersion=rv  with the computed status
        ├─ Ok(updated)        → cache[name] = updated; last_pushed_status[name] = status
        └─ Err(Conflict)      → someone advanced rv first (a spec edit, or our own resync
                                replaced the cache). Refetch the pod, take its fresh rv,
                                retry the PUT ONCE with the new rv.
```
This is the §2 optimistic-concurrency dance, now driven from the client. The conflict is expected, not exceptional: the cached rv goes stale any time the apiserver advances it. The retry is a *single* bounded refetch-and-reapply — if it conflicts again, we give up and let the next 2s tick try fresh. (A retry *loop* would risk livelock against a hot-edited Pod; one shot keeps it simple and the tick provides natural backoff.)

**Decision — why the push lives in the liveness arm, split sync/async.** `tick_liveness` is synchronous and runs inside `block_in_place` (it makes blocking libcontainer calls, §7/§10). But `push_status` is async (HTTP). So the run loop does:
```rust
_ = liveness_interval.tick() => {
    let dirty = block_in_place(|| self.tick_liveness());   // sync: compute under block_in_place
    for (name, status) in dirty {
        self.push_status(&name, &status).await;            // async: network writes outside it
    }
}
```
Compute synchronously (where the blocking runtime calls are), then `await` the network writes *outside* `block_in_place`. Mixing an `.await` inside `block_in_place` would be wrong — `block_in_place` is for blocking *sync* work, not for parking on a future. This clean split is the idiomatic way to bridge the sync runtime layer and the async client layer.

**Validation.** `cargo check --all-targets` clean; full suite **76 tests, all passing** (up from Phase 1's 36) — including the four new `compute_status_*` phase-rollup tests and the `tick_liveness` dirty/dedup test.
