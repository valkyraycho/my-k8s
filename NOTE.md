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
>
> **Philosophy call-outs** (the *thought process*, so you can re-derive the design yourself, not just recognize it) appear as blockquotes:
> - **⚙ Principle** — a transferable, non-K8s-specific engineering law (collected in the [[#Engineering principles, by example|index]]).
> - **🧭 Design rationale** — the *derivation chain*: problem → the forces in tension → the nearly-forced moves → a reproducible takeaway. "How would I arrive at this design from scratch?"
> - **🔧 Implementation choice** — why *this* structure/algorithm/ordering among the working alternatives (the engineering judgment one level below architecture).
> - **🦀 Rust pattern** — why this *type / trait / ownership / lifetime* shape, and the cue for when to reach for it in your own Rust.

---

## Contents

> Obsidian: click any entry to jump. (Links are `[[#heading]]` wikilinks — they resolve in Obsidian, not on GitHub.)

- [[#Project at a glance]]
- [[#Engineering principles, by example|⚙ Engineering principles index]]
- [[#How validation works in this project|✅ Validation & testing (all phases)]]
- [[#Phase 0 — Dev environment]]
  - [[#OrbStack + Linux VM + VirtioFS]]
  - [[#libcontainer smoke test]]
  - [[#Devcontainer wrap]]
- [[#Phase 1 — Mini-kubelet]]
  - [[#1. CLI skeleton (`src/bin/kubelet.rs`)|1 · CLI skeleton]]
  - [[#2. Pod schema (`src/pod.rs`)|2 · Pod schema]]
  - [[#3. `RuntimeClient` trait — the mini-CRI (`src/runtime.rs`)|3 · RuntimeClient trait (mini-CRI)]]
  - [[#4. Rootfs preparation (`scripts/prepare-rootfs.sh`)|4 · Rootfs preparation]]
  - [[#5. Bundle construction (`src/runtime/bundle.rs`)|5 · Bundle construction]]
  - [[#6. `YoukiRuntime` — libcontainer behind the trait (`src/runtime/youki.rs`)|6 · YoukiRuntime]]
  - [[#7. Pod sandbox — the pause-container pattern (`src/runtime/sandbox.rs`)|7 · Pod sandbox (pause container)]]
  - [[#8. In-memory pod store (`src/store.rs`)|8 · In-memory pod store]]
  - [[#9. Manifest watcher (`src/watcher.rs`)|9 · Manifest watcher]]
  - [[#10. Reconciler loop (`src/reconciler.rs`)|10 · Reconciler loop]]
  - [[#11. Graceful shutdown (`src/bin/kubelet.rs`)|11 · Graceful shutdown]]
  - [[#12. Mock-runtime integration test (`src/reconciler.rs` test module)|12 · Mock-runtime integration test]]
  - [[#13. End-to-end demo|13 · End-to-end demo]]
  - [[#14. Debug snapshot (`src/reconciler.rs` + `scripts/myctl.sh`)|14 · Debug snapshot]]
  - [[#Phase 1 wrap — what this earned us|Phase 1 wrap]]
- [[#Phase 2 — API server, watch streams, persistent store]]
  - [[#1. Wire format — Pod gains status + apiserver metadata (`src/pod.rs`)|1 · Wire format (status + metadata)]]
  - [[#2. PodStore — persistent storage + optimistic concurrency (`src/apiserver/storage.rs`)|2 · PodStore + optimistic concurrency]]
  - [[#3. Watch streaming (`src/apiserver/watch.rs`)|3 · Watch streaming]]
  - [[#4. HTTP surface — routes + handlers (`src/apiserver/{routes,handlers}.rs`)|4 · HTTP surface (routes + handlers)]]
  - [[#5. apiserver binary (`src/bin/apiserver.rs`)|5 · apiserver binary]]
  - [[#6. Client (`src/client.rs`)|6 · Client]]
  - [[#7. Informer-style reconciler loop (`src/reconciler.rs`)|7 · Informer-style reconciler loop]]
  - [[#8. Kubelet as a full client — reporting status back (`src/bin/kubelet.rs` + `src/reconciler.rs`)|8 · Kubelet status reporting]]
  - [[#9. `mykubectl` — the command-line client (`src/bin/mykubectl.rs`)|9 · mykubectl CLI]]
  - [[#10. End-to-end demo — the whole stack, live (verified 2026-06-01)|10 · End-to-end demo]]
  - [[#Phase 2 wrap — what this earned us|Phase 2 wrap]]
- [[#Phase 3 — Controllers: the ReplicaSet controller]]
  - [[#1. Generalize the store to `ResourceStore<T>` + the ReplicaSet schema (`storage.rs`, `replicaset.rs`, `meta.rs`)|1 · Generic ResourceStore + RS schema]]
  - [[#2. `ObjectMeta` gains labels + ownerReferences (`meta.rs`)|2 · ObjectMeta: labels + ownerReferences]]
  - [[#3. The work queue (`controller/workqueue.rs`)|3 · The work queue]]
  - [[#4. The reconcile function (`controller/replicaset.rs`)|4 · The reconcile function]]
  - [[#5. The manager: composing the loops (`controller/manager.rs`)|5 · The manager]]
  - [[#Phase 3 wrap — what this earned us|Phase 3 wrap]]
- [[#Phase 4 — The scheduler & multi-node]]
  - [[#1. `spec.nodeName` + the `Node` resource (`pod.rs`, `node.rs`)|1 · spec.nodeName + Node resource]]
  - [[#2. `Node` in the apiserver (3rd store) + the binding subresource (`storage.rs`, `handlers.rs`, `routes.rs`)|2 · Node in apiserver + binding]]
  - [[#3. Server-side `fieldSelector` — the centerpiece (`watch.rs`, `handlers.rs`)|3 · Server-side fieldSelector]]
  - [[#4. The node-aware kubelet (`reconciler.rs`, `bin/kubelet.rs`)|4 · Node-aware kubelet]]
  - [[#5. The scheduler — just another controller (`scheduler.rs`, `bin/scheduler.rs`)|5 · The scheduler]]
  - [[#6. Multi-node demo (verified on the VM)|6 · Multi-node demo]]
  - [[#Phase 4 wrap — what this earned us|Phase 4 wrap]]
- [[#Phase 5a — Real pod networking]]
  - [[#1. Schema: `PodStatus.pod_ip` + `NodeSpec.pod_cidr` (`pod.rs`, `node.rs`)|1 · Schema: podIP + podCIDR]]
  - [[#2. Pure IPAM allocator (`src/ipam.rs`)|2 · Pure IPAM allocator]]
  - [[#3. apiserver assigns each Node a /24 (`handlers.rs`/`storage.rs`)|3 · apiserver assigns /24]]
  - [[#4. Sandbox: veth + bridge wiring (`runtime/sandbox.rs`)|4 · Sandbox: veth + bridge]]
  - [[#5. Reconciler: IPAM lifecycle + recovery (`reconciler.rs`)|5 · Reconciler: IPAM + recovery]]
  - [[#6. mykubectl IP column|6 · mykubectl IP column]]
  - [[#7. e2e validation (on the VM)|7 · e2e validation]]
  - [[#Phase 5a wrap — what this earned us|Phase 5a wrap]]
- [[#Phase 5b — Services (ClusterIP, Endpoints, kube-proxy)]]
  - [[#1. Schema: `Service` + `Endpoints` as two objects (`service.rs`, `endpoints.rs`)|1 · Schema: Service + Endpoints]]
  - [[#2. apiserver assigns the ClusterIP VIP (`handlers.rs`)|2 · apiserver assigns ClusterIP]]
  - [[#3. The endpoints controller (`controller/endpoints.rs`)|3 · Endpoints controller]]
  - [[#4. kube-proxy: the iptables planner (`kube_proxy.rs`)|4 · kube-proxy iptables planner]]
  - [[#5. mykubectl: services + endpoints|5 · mykubectl svc/endpoints]]
  - [[#6. e2e validation (on the VM)|6 · e2e validation]]
  - [[#Phase 5b wrap — what this earned us|Phase 5b wrap]]
- [[#Phase 6a — Raft, from scratch]]
  - [[#0. The mental model: what problem Raft actually solves|0 · The mental model]]
  - [[#1. The log and the two IDs that rule everything (`raft/log.rs`)|1 · Log, terms, indices]]
  - [[#2. The two RPCs (`raft/message.rs`)|2 · The two RPCs]]
  - [[#3. Persistence: the three things that must survive (`raft/storage.rs`)|3 · Persistence (HardState + log)]]
  - [[#4. The pure core: events in, effects out (`raft/core.rs`)|4 · The pure core (step)]]
  - [[#5. Elections, traced (`raft/core.rs`)|5 · Elections, traced]]
  - [[#6. Log replication and repair, traced (`raft/core.rs`)|6 · Replication + repair, traced]]
  - [[#7. The commit rule and Figure 8 — the hardest idea in Raft|7 · Commit rule / Figure 8]]
  - [[#8. The async shell (`raft/node.rs`) and transport|8 · The shell + transport]]
  - [[#9. The simulation harness (`raft/sim_tests.rs`)|9 · Simulation harness]]
  - [[#10. e2e: three processes, one kill -9 (`bin/raft-demo.rs`)|10 · e2e raft-demo]]
  - [[#Phase 6a wrap — what this earned us|Phase 6a wrap]]

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
| 2 | API server v1 | kubelet talks to API server over HTTP; multiple kubelets possible ✅ |
| 3 | Controllers (ReplicaSet) | Kill a pod, controller recreates it ✅ |
| 4 | Scheduler | Pods distributed across 2+ kubelets ✅ |
| 5a | Real pod networking | cross-node `wget <pod-ip>:8080` works; IPs survive restart ✅ |
| 5b | Services & networking | `curl` a Service VIP, traffic load-balances ✅ |
| 6a | Raft consensus, from scratch | 3-node cluster elects, replicates, survives leader `kill -9` with zero loss ✅ |
| 6b | Raft under the apiserver | 3 apiserver replicas agree on one store ⬅ **next** |

Phase 6 explicitly **dropped** "write your own runtime" — already done that in the prior Docker project. Replaced with distributed-systems content where the marginal learning is highest.

---

# Engineering principles, by example

> **Why this section exists.** Writing code that works makes you a *developer*; knowing **what shape the code should take, and why** makes you a *software engineer*. The difference is judgment — about abstraction boundaries, trade-offs, failure modes, and when *not* to build something. This index collects the transferable engineering principles this project forced us to confront, each with a **judgment cue** (when to reach for it) and where you implemented it. None of these are K8s-specific; they travel to any system you build. Throughout the notes, a **⚙ Principle —** call-out flags where one shows up in context. Re-read this table periodically; the goal is to internalize the cues until they fire automatically.

| Principle | Judgment cue — when to reach for it | Where you built it |
|---|---|---|
| **Program to an interface (a seam)** | You'll want to swap implementations *or* test a layer in isolation. One real impl + one mock already pays for the trait. | `RuntimeClient` (P1 §3); CRI analogy |
| **Separate mechanism from policy** | A lower layer should expose *capabilities*; a higher layer decides *how to use them*. Keeps policy swappable without touching mechanism. | runtime primitives vs sandbox's graceful-term ladder (P1 §7) |
| **Level-triggered, not edge-triggered** | React to the *difference* between desired and actual, recomputed from full state — not to the stream of events. Survives missed/duplicated events. | reconcile loops (P1 §10, P2 §7, P3 reconcile) |
| **Idempotency & convergence** | Design an operation so running it twice is a no-op. Then you can retry freely and trigger on anything. | RS reconcile (P3); sandbox teardown |
| **Optimistic concurrency for shared mutable state** | Prevent lost updates *without locks*: read a version token, write only if it hasn't changed; loser retries. | `resourceVersion` compare-and-set (P2 §2, §8) |
| **Design for testability = design for modularity** | If something is painful to test, it's usually too coupled. A seam that enables a fast test almost always improves the structure too. | trait + mock (P1 §12); in-process apiserver tests (P2 §6, P3) |
| **Defer complexity / rule of three** | Build the simplest thing that works. Add the abstraction when a *second* concrete case forces it — not in anticipation. | no `Arc<Mutex>` till shared (P2 §8); generic store added when RS arrived (P3) |
| **Failure-mode-first thinking** | Ask "what happens on crash / partial failure / disconnect?" *before* the happy path — that's where real systems live. | restart recovery (P2 §7), partial-create rollback (P1 §10), watch lag→410 + reconnect (P2 §3, P3) |
| **Avoid feedback loops in reactive systems** | When an action emits an event that re-triggers the same action, guard it with a "did anything actually change?" check. | status-write dedup (P2 §8, P3 reconcile) |
| **Decouple producers from consumers with a queue** | A dedup queue absorbs bursts, collapses duplicates, and separates *arrival rate* from *processing rate*. | controller work queue (P3) |
| **Model identity & ownership explicitly** | Names get reused; use a stable id (uid) and explicit ownership links for lifecycle, cascade-delete, "is this mine?". | `uid` (P2 §1), `ownerReferences` (P3) |
| **Refactor without churn via aliases** | Generalize the implementation, keep the old name as a type alias so call sites don't all change at once. | `PodMetadata = ObjectMeta`, `PodStore = ResourceStore<Pod>` (P3) |
| **Separate decision from execution (policy as data)** | Split "what to do" (a decision, written as plain data) from "carry it out." The decider and the doer become independent, separately-testable, separately-scalable processes. | scheduler decides node → writes a *binding* → kubelet executes (P4) |
| **Filter at the source (predicate pushdown)** | Push the filter to where the data lives instead of shipping everything and filtering at the consumer. Less data on the wire; each consumer subscribes to only its slice. | server-side `fieldSelector` on list/watch (P4) |
| **Fail safe — default to the safe state** | When input is missing, stale, or unparseable, fall back to the choice that *can't* cause harm — not the permissive one. | stale/absent heartbeat ⇒ node NOT schedulable; bad selector ⇒ no filter (P4) |
| **Liveness via freshness, not a trusted flag** | Don't believe a cached "I'm healthy" bit; require a recent heartbeat and treat staleness as down. A crashed reporter can't un-set its own flag. | scheduler checks `lastHeartbeatTime` age, ignores stale `ready` (P4) |

---

# How validation works in this project

> **Why this section exists.** "How do I know it's correct?" is as much an engineering skill as writing the code. This project proves correctness in **four layers**, cheapest/fastest first, each catching what the layer below can't. Knowing which layer to reach for — and that the slow on-VM e2e is the *last* resort, not the first — is the point. The per-phase e2e catalog at the end is the recall list: what each phase's end-to-end run actually demonstrated.

## The four layers

```
   layer                 runs where     speed     catches
   ─────                 ──────────     ─────     ───────
   1 pure unit tests     anywhere       µs–ms     logic/algebra (no I/O, no root)
   2 in-process integ.   anywhere       ms        component wiring over real HTTP (no containers)
   3 mock-runtime tests  anywhere       ms        orchestration logic w/o libcontainer/root
   4 on-VM e2e (manual)  mykube-dev VM  seconds   the real Linux kernel: ns, veth, cgroups, signals
```

**Layer 1 — pure unit tests.** Functions with no I/O are tested directly. The design *enables* this: pure functions are carved out precisely so they're testable without a world. Examples: `IpAllocator` (allocate/reserve/release/exhaustion, P5a), `matches_selector` + `is_schedulable` (table-driven predicates, P3/P4), `compute_backoff`, bundle namespace wiring (P1 §5), all the serde round-trips (wire-format fields, externally-tagged enums). *Cue: if it's pure, it has no excuse not to be unit-tested.*

**Layer 2 — in-process integration tests.** Spin up the **real apiserver router** on an OS-assigned port (`TcpListener::bind("127.0.0.1:0")` + `axum::serve` on a background task), point a real `Client` at it, and drive actual HTTP. This proves serialization, routing, status-envelope mapping, optimistic-concurrency conflicts, and **watch streams** end-to-end — *without a single container*. The RS controller and scheduler tests go further: they spawn the apiserver **and** the full `manager::run()` / scheduler `run()`, then poll until the cluster converges (e.g. `wait_for_pod_count`). This is why `manager::run()` lives in the **lib, not the bin** — so the whole control loop is integration-testable in-process. Examples: `create_then_list_roundtrip`, `watch_stream_receives_added_event`, `controller_recreates_a_deleted_pod`, `run_schedules_unscheduled_pods_end_to_end`.

**Layer 3 — mock-runtime tests.** The kubelet's reconciler is generic over `RuntimeClient`; tests substitute a `MockRuntime` that records calls and serves canned states. This exercises sandbox-lifecycle ordering, CrashLoopBackOff, partial-create rollback, status dedup, and restart recovery — all the orchestration logic — **with no libcontainer, no root, no Linux**. This is the entire payoff of the trait seam (P1 §3): the hard-to-run dependency is mocked, the logic is tested everywhere.

**Layer 4 — on-VM e2e (manual).** The first three layers run on any machine in `cargo nextest run` (currently **229 passing**). But mocks and in-process servers cannot prove that the *real Linux kernel* does what we think — that `setns` actually shares a netns, that veth+bridge actually carries cross-node packets, that SIGTERM actually propagates, that `pivot_root` lands on our rootfs. So each phase ends with a **hand-run end-to-end test on the OrbStack VM** (`mykube-dev`), driving the real binaries against real `libcontainer` containers. These are the assumptions layers 1–3 take on faith.

## Running the automated layers (1–3)

All `cargo` runs happen **inside the VM** (`libcontainer` + its `procfs` dep are Linux-only and won't build on macOS), via SSH against the VirtioFS-mounted source:
```bash
ssh mykube-dev@orb 'source $HOME/.cargo/env && \
  cargo nextest run --manifest-path=/Users/raycho/solo-leveling/rust/my-k8s/Cargo.toml'
```
> **⚙ Principle — make the slow/privileged dependency the thinnest possible layer, then mock or isolate it.** Root + Linux + real containers are the expensive part of testing an orchestrator, so the design pushes *all* of that behind one trait (`RuntimeClient`) and one set of shell-outs (`#[cfg(not(test))]` in `sandbox.rs`). Everything above can then be tested fast and anywhere; only the irreducible kernel-truth checks need the VM. Cue: *identify the dependency that makes tests slow/privileged/flaky, quarantine it behind a seam, and you convert most of your test suite from "needs the world" to "runs anywhere."*
>
> **⚙ Principle — scale time down in tests with `#[cfg(test)]` constants.** Production intervals (10s heartbeat, 30s staleness, 5min backoff cap) would make tests sleep for minutes. Each is a `#[cfg(test)]` constant set to milliseconds, so the *same code path* runs but the suite stays in the ms range. Cue: *when behavior depends on real durations, make the durations constants you can shrink under test — don't `sleep` real seconds in a unit test, and don't fake the clock if a smaller constant will do.*

## Per-phase e2e catalog (what each on-VM run proved)

Each entry: the scenario, the key command(s), and the **property** it demonstrated that the automated layers couldn't.

**Phase 0 — runtime works at all.** `sudo target/debug/scratch` → a busybox container runs end-to-end via libcontainer, cgroup v2 auto-detected, clean exit. *Proved: the runtime layer functions in our VM before any orchestration exists (the tracer bullet).*

**Phase 1 — the Pod sandbox & self-healing.** Against a manifests dir watched by the kubelet:
- `cp web.yml manifests/active/` → sandbox + httpd come up; `curl <pod-ip>:8080` serves. *Proved: pause-container sandbox + a container actually run and serve.*
- `cp sidecar.yml …` → `readlink /proc/<httpd-pid>/ns/net == /proc/<sidecar-pid>/ns/net`. *Proved: shared-netns is real at the kernel level (setns works).*
- `kill -9 <httpd-pid>` → within ~2s the reconciler restarts it; sandbox + sidecar untouched, **pod IP unchanged**. *Proved: liveness reconciliation + the pause holds the namespace across a container restart.*
- `rm web.yml` → full teardown; `kill -TERM <kubelet>` → graceful shutdown, no orphans (`pgrep -f /var/lib/my-k8s` empty). *Proved: deletion propagation + graceful SIGTERM teardown.*

**Phase 2 — the two-tier control plane.** apiserver (non-root) + kubelet (root) + mykubectl:
- `mykubectl apply -f web.yml` → watch fires ADDED → kubelet runs it → `mykubectl get pods` shows `Running 1/1` → `curl` serves. *Proved: the full desired→watch→run→status→observe loop over HTTP.*
- `kill -9` the kubelet, restart → `recover_all` count=2 → `from_recovered` reattaches → **httpd PID unchanged, no dup containers**. *Proved: kubelet restart recovery doesn't disturb running pods.*
- `kill -9` the apiserver, restart → a previously-applied pod is still there. *Proved: sled persistence survives an apiserver crash.*
- Operational gotchas learned here: `with_graceful_shutdown` hangs while a watch is open (needs `kill -9`); sled is single-writer (2nd apiserver fails to open — fail-safe); apiserver DB dir must be pre-created + `chown`ed to the non-root run user; `pkill -f kubelet` over SSH self-matches your shell → use a bracket pattern `[k]ubelet`.

**Phase 3 — the ReplicaSet controller.** apiserver + controller-manager + kubelet:
- `mykubectl apply -f rs.yml` (replicas: 3) → 3 pods created and Running. *Proved: controller create-to-match.*
- delete one pod → controller recreates it (the canonical "kill a pod, it comes back"). *Proved: Pod DELETED event → ownerRef → enqueue RS → reconcile → recreate, the self-heal loop end-to-end.*
- scale 3→1 → surplus (oldest-first) deleted; delete the RS → cascade teardown to 0 containers, `state/pods` empty. *Proved: scale-down + cascade-delete via ownerReferences.*

**Phase 4 — scheduler & multi-node.** Two kubelets (`--node-name node-a`/`node-b`, separate `--state-dir`) + scheduler:
- RS replicas=4 → scheduler spreads least-loaded → 2 on node-a, 2 on node-b, all Running; `mykubectl get nodes` shows both Ready. *Proved: binding-subresource placement + least-loaded spread + per-node fieldSelector watch each kubelet only runs its own pods.*
- `kill` node-b's kubelet → its heartbeat goes stale (>30s) → scale RS to 6 → **both** new pods land on node-a only; node-b's existing pods left alone. *Proved: heartbeat-freshness liveness gate excludes the dead node; the scheduler places only the unscheduled (no eviction).*

**Phase 5a — real pod networking.** Two kubelets, each assigned a distinct /24 by the apiserver:
- RS replicas=3 spread across both nodes → each pod gets an IP from **its node's** /24 (`mykubectl get pods` IP column). *Proved: apiserver per-node PodCIDR assignment + per-node IPAM hand out non-overlapping addresses.*
- **cross-node** `wget <pod-ip>:8080` from a pod on node-a to a pod on node-b → succeeds. *Proved: veth+bridge puts every pod on one flat L2 segment (`mykube0`), so cross-(logical-)node traffic works with no routing/overlay.*
- `kill` a kubelet, restart → its pods' **IPs survive** (recovered from apiserver `status.podIP`, re-`reserve()`d into the rebuilt allocator before any fresh allocate). *Proved: IP persistence across kubelet restart — and WHY pod IP lives in `status` (it's the recovery source of truth).*
- Two bugs this e2e caught (both invisible to layers 1–3): the `ensure_bridge` idempotency race (losing kubelet's `ip addr add` says "already assigned", not "File exists" → crash on startup), and `resync()` using cluster-wide `list_pods()` instead of `list_pods_on_node` (surviving node tried to adopt a dead node's pods). *Lesson: a scoping filter must be threaded through **every** read path — watch, startup, AND resync — not just the obvious ones.*

**Phase 5b — Services (ClusterIP load-balancing).** apiserver + controller-manager (now with the endpoints controller) + kube-proxy + 2 kubelets:
- RS replicas=3 + a Service selecting them → `mykubectl get endpoints` shows all 3 pod IPs; `iptables -t nat -L` shows the KUBE-SVC/KUBE-SEP chains with `0.33333` / `0.50000` / catch-all probabilities + DNAT targets. *Proved: endpoints controller derives membership from selector+phase+IP, and kube-proxy translates it to the correct netfilter ruleset.*
- **30/30 `curl 10.96.0.0:80`** → spread across all 3 backends, including pods on the *other* node. *Proved: ClusterIP DNAT load-balances end-to-end over the flat bridge (cross-node), with masquerade so replies route back.*
- scale RS 3→5 → Endpoints + iptables update live → 20/20 curls spread across 5. *Proved: the service→endpoints→iptables pipeline is reactive, not one-shot.*
- Bugs/env caught only here: `iptables` not installed on the VM (`apt-get install iptables`); the ClusterIP double-guard (re-apply was burning VIPs with only the request-side check); an E0428 `run` vs `run_cmd` name clash; and the reassuring negative result that a correctly-scoped kube-proxy does NOT break SSH (DNAT only matches `10.96.x` VIPs, MASQ only mark `0x4000`). *Lesson: a new privileged dataplane component needs its blast radius reasoned about explicitly — "does my rule match traffic it shouldn't?" is a design question, not a runtime surprise.*

**Phase 6a — Raft (consensus).** Three `raft-demo` processes over HTTP transport, each with its own sled at `/tmp/raft-demo-N`. Note this phase added a **new validation layer between 1 and 4**: the deterministic simulation harness (real cores + real storages, fake network/time; the paper's three safety invariants checked after every step; partitions/crashes/restarts as test operations) — it caught a real *liveness* bug (fixed-timeout candidate starvation) that no single-node unit test could express, while all safety invariants held.
- 3 processes start → node 3 elected (term 1) → leader proposes every 2s → all three print `APPLIED from-3-#1…#10` in lockstep. *Proved: real election + replication over real HTTP, state machines in lockstep.*
- `kill -9` the leader → node 2 wins term 2 → **the proposal stream continues automatically** (#11…#16) because the proposer is gated on the `leader_watch` channel. *Proved: failover with zero committed-entry loss, and leadership-aware clients following the handoff.*
- restart node 3 → logs `recovered id=3 term=1 last=10` (from sled) → catches up to #25 → all three APPLIED sequences **byte-identical** (`from-3-#1..10, from-2-#1..15` — the handoff visible in the log itself). *Proved: fsync'd persistence + restart recovery + log repair converge a rejoining node.*
- Caught during the build (sim + tests, not e2e): the starving-candidate liveness phenomenon (fix: re-randomize timeout per candidacy); a follower commit path that never emitted Apply effects (dead code caught by decision-table test design); zero-padded log keys (the >10-entries ordering bug). *Lesson: for distributed algorithms the simulator IS the primary test; e2e only confirms the shell's plumbing.*

> **⚙ Principle — the test pyramid: many fast tests, few slow ones, and know what only the top can prove.** 229 automated tests (layers 1–3) run in seconds and catch logic/wiring regressions on every change; the handful of manual on-VM e2e runs (layer 4) are reserved for the things only the real kernel can confirm. Note what the e2e caught that nothing else could: a bridge-setup race between two real processes, and a cross-node adoption bug that only surfaces when one real node fails while another watches across a resync. Cue: *push coverage down the pyramid (fast, deterministic, runs-anywhere), but never assume the fast layers prove integration with the real world — keep a thin top layer for the kernel/network/hardware truths your mocks asserted on faith.*

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

**The working libcontainer construction** (it survives almost verbatim into `youki.rs::create_container`):
```rust
let container = ContainerBuilder::new(id, SyscallType::default()) // ① builder pattern
    .with_root_path(state_dir)?     // ② where libcontainer keeps per-container state.json
    .with_executor(DefaultExecutor {})
    .as_init(bundle_path)           // ③ this bundle dir holds config.json + rootfs
    .with_systemd(false)            // ④ cgroupfs driver, not systemd (we manage cgroups directly)
    .build()?;                      // ⑤ each builder step can fail → Result, hence `?`
```
> **① `ContainerBuilder`** is `derive_builder`-style: chained setters then `.build()`. **②** `with_root_path` is libcontainer's `--root` equivalent — the on-disk state dir. **⑤** the whole chain is fallible, so the smoke test was also where we learned every builder call wants `?`.

**Key facts that shaped Phase 1:**
- `Container` exposes `start()`, `refresh_status()`, `status()`, `delete(force)`, `pid()` — but **no `wait()`**. No blocking wait means we *poll* `refresh_status()` to detect crashes (→ the 2s liveness tick).
- `oci_spec::runtime::LinuxNamespace` has an optional `path` field — `None` = create a new namespace, `Some("/proc/PID/ns/X")` = join an existing one. **This single `Option` is the entire pause-container mechanism** (§5/§7).
- Root is required for namespace creation. The kubelet runs as root, like real K8s.

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

**Did — the actual trait** (`src/runtime.rs`):
```rust
pub trait RuntimeClient {
    fn create_container(&mut self, id: &str, bundle_path: &Path) -> Result<()>; // build, don't run
    fn start_container(&mut self, id: &str) -> Result<()>;                       // run init proc
    fn kill_container(&mut self, id: &str, signal: i32) -> Result<()>;           // SIGTERM/SIGKILL
    fn delete_container(&mut self, id: &str, force: bool) -> Result<()>;         // free state
    fn container_state(&mut self, id: &str) -> Result<ContainerState>;           // POLL this
    fn container_pid(&mut self, id: &str) -> Result<Option<u32>>;                // ① for §7
    fn recover_all(&mut self) -> Result<Vec<RecoveredContainer>>;                // ② Phase 2
}
```
> **`&mut self` everywhere** (see Decision below) — the honest constraint, surfaced not hidden. **`Result<T>`** is a crate-local alias defaulting the error to `RuntimeError`, so signatures stay terse. **①** `container_pid` looks odd on a generic runtime API; it exists *only* so the sandbox can read the pause PID for `/proc/{pid}/ns/net`. **②** `recover_all` was added in Phase 2 — adding a trait method forces every impl (real + mock) to provide it.

`RuntimeError` is a `thiserror` enum (`NotFound`, `AlreadyExists`, `InvalidBundle`, `Other` via `#[from] anyhow::Error`); `ContainerState` flattens libcontainer's five statuses into four (`Created`/`Running`/`Stopped`/`NotFound`).

**Why a trait when there's only one real impl.** Two reasons, both load-bearing:
1. **Testability.** Everything above the seam is tested with `MockRuntime`, which just appends each call to a `Vec<String>`. No root, no libcontainer, no OCI bundle. The reconciler's whole behavior (restart logic, backoff, teardown ordering) is verified by asserting on that recorded call list (§12). Without the seam, every test would need a real Linux VM.
2. **The abstraction *is* the lesson.** Real K8s has CRI (Container Runtime Interface) for exactly this reason: separate *what* the orchestrator wants from *how* a runtime delivers it, and you can swap containerd ↔ CRI-O without touching the kubelet. We rebuilt the tiny version, so the design pressure that produced CRI is now something you've felt, not just read about.

**Comparison to Go.** This is the same move as a Go interface — `type RuntimeClient interface { CreateContainer(...) error; ... }` with a real impl and a mock impl. The difference: in Go any type satisfies the interface implicitly if it has the methods; in Rust you write `impl RuntimeClient for YoukiRuntime` explicitly. Rust's version is checked at compile time and the intent is visible at the impl site.

> **⚙ Principle — program to an interface (a seam), and let testability drive design.** Introducing a trait with *one* real implementation looks like over-engineering until you notice the second consumer it serves: the test suite. The same seam that would let you swap libcontainer for containerd is what lets `MockRuntime` stand in so the entire reconciler is testable without root or Linux. That's not a coincidence — *a boundary clean enough to mock is a boundary clean enough to swap*. Cue: when a layer is hard to test, the design is usually too coupled; reach for a seam, and you'll often find it improves modularity for free. (This is exactly the design pressure that produced real K8s's CRI.)
>
> **🧭 Design rationale — how you'd arrive at the trait without knowing CRI exists.** Start from a constraint, not a pattern: *the thing that runs containers needs root + Linux + libcontainer, but I want to test the orchestration logic on my laptop in milliseconds.* Those two facts are in direct tension. The only way to satisfy both is to make the orchestrator depend on an **abstraction** it can talk to, with two implementations behind it — the real one (needs the world) and a fake one (needs nothing). The moment you write that sentence, you've invented the trait; CRI is just the name K8s gives the same forced move. Reproducible takeaway: *when "the real dependency is expensive/privileged" collides with "I want fast, local tests," the resolution is almost always a trait/interface with a real impl + a test double — derive the seam from the tension, don't reach for it as dogma.*
>
> **🦀 Rust pattern — trait + generic (`Reconciler<R: RuntimeClient>`) vs `Box<dyn RuntimeClient>`.** Two ways to be polymorphic over the runtime: a generic type parameter (static dispatch, monomorphized per impl) or a trait object (dynamic dispatch through a vtable). We use the **generic** because the runtime is chosen *once at construction* and never changes per call — so there's no need to pay vtable indirection, and monomorphization lets the compiler inline. Reach for `Box<dyn Trait>` instead when you need a *heterogeneous collection* (a `Vec` of different impls) or to *erase the type* across an API boundary. Cue: *one impl chosen at startup → generic; many impls mixed at runtime → `dyn`.*

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

This is *exactly* what real K8s does. And it's startlingly little code — the entire pause-vs-app distinction is one `if let Some`:
```rust
// per-container: PID + mount always get a FRESH namespace (never a path)
for ty in [LinuxNamespaceType::Pid, LinuxNamespaceType::Mount] {
    namespaces.push(LinuxNamespaceBuilder::default().typ(ty).build()?);
}
// shared-from-pause: net/ipc/uts get a path ONLY when we were given a pid
for (ty, ns_name) in [(Network, "net"), (Ipc, "ipc"), (Uts, "uts")] {
    let mut b = LinuxNamespaceBuilder::default().typ(ty);
    if let Some(pid) = share_namespaces_from_pid {          // ← the whole mechanism
        b = b.path(PathBuf::from(format!("/proc/{pid}/ns/{ns_name}")));
    }
    namespaces.push(b.build()?);
}
```
> `share_namespaces_from_pid` is `None` for the pause (all five fresh) and `Some(pause_pid)` for app containers (the three shared ones get a `path`, so libcontainer `setns`es into them). `if let Some` is the idiomatic "set this only when present" — no null, no unwrap. The four `bundle.rs::tests` pin the contract — e.g. `app_container_keeps_pid_and_mount_per_container` asserts PID/mount stay `path: None`.

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

> **🧭 Design rationale — how the pause container is forced by "Pod IP must survive restarts."** Walk it forward. (1) Containers in a Pod must share a network identity → they must share a network namespace. (2) A Linux namespace lives exactly as long as *some process* is in it. (3) So *which* process anchors the shared namespace? If it's one of the app containers, then when that container crashes (and they do — that's why we have restart logic), its namespace dies, and the Pod's IP changes out from under every other container. Contradiction with the requirement. (4) Therefore the anchor must be a process that *never* exits for app reasons — a dedicated do-nothing container. That's the pause. It wasn't a clever trick someone thought up; it's the unique resolution of "shared, restart-stable namespace" against "namespaces die with their last process." Reproducible takeaway: *when a resource's lifetime must outlive the things that use it, give it a dedicated owner whose only job is to stay alive — don't let a volatile participant double as the anchor.*
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

**Did:** A `PodSandbox` owning one Pod's lifecycle — `create` (build+run pause, capture pid, bring `lo` up), `add_container` (join the pause), `remove_container`, `destroy`. The graceful-termination ladder in `remove_container` shows several idioms at once:
```rust
match runtime.kill_container(&id, libc::SIGTERM) {
    Ok(()) => {
        let deadline = Instant::now() + TERMINATION_GRACE;   // 5s
        while Instant::now() < deadline {
            let s = runtime.container_state(&id)?;
            if matches!(s, ContainerState::Stopped | ContainerState::NotFound) { break; }
            std::thread::sleep(POLLING_INTERVAL);             // ① sync sleep — see note
        }
    }
    Err(e) => warn!(?e, "SIGTERM failed; proceeding to delete"), // ② not fatal: fall through
}
match runtime.delete_container(&id, true) {
    Ok(()) | Err(RuntimeError::NotFound(_)) => {}             // ③ or-pattern → idempotent
    Err(e) => return Err(e).context(format!("delete container {name}")),
}
let _ = std::fs::remove_dir_all(&bundle_dir);                // ④ best-effort, discard Result
```
> **①** a blocking `thread::sleep` is fine because the reconciler runs this inside `block_in_place`. **②** SIGTERM failing (already-gone) isn't fatal — delete is what actually frees state. **③** treating "deleted" and "already NotFound" the same makes delete idempotent (an or-pattern in one match arm). **④** `let _ =` deliberately discards a Result we don't care about — a leftover dir is harmless.

And `destroy` tears down in **reverse dependency order** — all app containers first, pause last — so an app never loses its shared netns mid-cleanup (pinned by `destroy_removes_app_containers_before_pause`).

**Decision — pause runs `/bin/busybox sleep infinity`.** Real K8s ships a purpose-built `pause` binary that ignores signals and reaps zombie processes. Ours just needs to hold namespaces and stay alive, which `sleep infinity` does. Good enough for Phase 1; the zombie-reaping nicety can come later if we ever share the PID namespace.

**Decision — container ID convention `{pod}__{container}` (double underscore).** A single `_` could collide with a pod or container name that legitimately contains `_`; the double underscore is far less likely in real input. Pause is `{pod}__pause`. This id is what's passed to every `RuntimeClient` call.

**Decision — `destroy()` removes app containers BEFORE the pause** (the ordering the diagram demands, in reverse). If the pause died first, every app container's shared net/ipc/uts namespace would be yanked out from under it mid-cleanup — the kernel could unbind `lo`, drop `/dev/shm`, etc., and the app teardown would hit undefined behavior. Tear down in reverse-dependency order: apps first (they're the dependents), pause last (it's the anchor). The `destroy_removes_app_containers_before_pause` test locks this in by asserting the delete-call ordering.

**Decision — graceful-term polling lives here, not in `RuntimeClient`.** "SIGTERM, wait up to 5s, then force-delete" is an *orchestrator policy*, not a *runtime primitive*. The trait (§3) exposes the primitives (`kill_container`, `container_state`); the sandbox *composes* them into the policy.

> **⚙ Principle — separate mechanism from policy.** The runtime layer knows *how* to kill and query a container (mechanism); it has no opinion on *how long to wait* before force-killing (policy). Push the "how" down and keep the "how to use it" up, and you can change the grace period — or write a totally different teardown policy — without touching the layer that does the work. Cue: when a low-level module starts encoding decisions ("wait 5s", "retry 3×"), that's policy leaking downward; lift it to the caller and leave the module a clean set of verbs. It's the same split as `kill(2)` (mechanism) vs an init system's shutdown sequence (policy).

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

> **⚙ Principle — level-triggered beats edge-triggered for reliability.** An *edge-triggered* design reacts to events ("a Pod was deleted → recreate it"); if it ever misses an event, it's permanently wrong. A *level-triggered* design reacts to the *current gap between desired and actual* ("I want 3, I see 2 → make 1"), recomputed from full state every tick — so a missed or duplicated trigger costs you nothing but a little latency. This single choice is why K8s self-heals, and it recurs everywhere: the kubelet here, the status reporter (P2 §8), the ReplicaSet controller (P3). Cue: *whenever you're tempted to handle "the event," ask instead "what's the difference between what I want and what is?" and drive that to zero — events become mere hints to recheck, not facts you must not miss.*
>
> **🧭 Design rationale — how you'd arrive at the reconcile loop from a reliability requirement.** Suppose you started naive: "on each watch event, do the matching action." Now ask the failure question *first* (the engineer's habit): what if an event is dropped, duplicated, or arrives out of order? Networks guarantee none of those won't happen. The edge-triggered design is then *unfixable* — a lost "deleted" event means a pod never gets recreated, forever. The only robust escape is to stop trusting events as facts and instead treat them as *hints to go re-observe reality*, then act on the diff. That reframing — from "apply this delta" to "observe everything, compute the gap, close it" — **is** the reconcile loop; it wasn't designed, it was forced by taking unreliable delivery seriously. Reproducible takeaway: *design the failure path first; if "what if I miss a message?" has no good answer, you're edge-triggered and need to flip to level-triggered.*

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

**Decision — disjoint mutable borrows via destructuring.** Inside `reconcile_liveness` we need `&mut` on three fields of `self` *at once*. The naive form fails the borrow checker; destructuring fixes it:
```rust
// ✗ self.store.get_mut(name) borrows ALL of *self mutably; the later
//   self.runtime / self.restart_state uses then conflict.
let Self { store, runtime, restart_state, .. } = self;  // ✓ three INDEPENDENT field borrows
let state = match store.get_mut(name) { Some(s) => s, None => return Ok(()) };
// now `store`, `runtime`, `restart_state` are usable together:
let s = runtime.container_state(&id)?;
let tracker = restart_state.entry(id.clone()).or_insert_with(/* ... */); // upsert idiom
```
> **🦀 Rust pattern — destructure `self` for disjoint field borrows.** The borrow checker tracks borrows *per field* once you name the fields via `let Self { store, runtime, restart_state, .. } = self` — but it CANNOT see through a method call like `self.store.get_mut()`, which conservatively borrows all of `*self`. So the fix isn't `unsafe` or `RefCell`; it's giving the checker the field-level view it needs by destructuring up front. `..` ignores untouched fields. Cue: *when you need `&mut` to several fields of one struct at once and the checker complains, destructure `self` into named field bindings rather than reaching for interior mutability — it's the zero-cost, idiomatic resolution.*

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

## 12. Mock-runtime integration test (`src/reconciler.rs` test module)

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

**Phase 2 is shipped.** ✅ The full vertical slice compiles, passes **76 tests**, and was **verified live end-to-end on the VM** (§10): `apiserver` (axum + sled) ← HTTP → `kubelet` (informer-style client) plus a `mykubectl` CLI. `cargo check --all-targets` clean. The kubelet reads desired state from the apiserver (informer loop, §7) *and* reports observed state back (status loop, §8); `mykubectl` (§9) drives the whole thing from the command line. `src/watcher.rs` is gone — the apiserver replaces the directory watch.

The order below mirrors the dependency order: wire types → storage → watch → HTTP surface → server bin → client → the kubelet's informer loop → the kubelet's status loop → the `mykubectl` CLI → the end-to-end demo.

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

> **⚙ Principle — optimistic concurrency for contended shared state.** Two ways to stop concurrent writers from clobbering each other: *pessimistic* (take a lock, block everyone else) or *optimistic* (let everyone proceed, attach a version, reject the write whose version is stale, make the loser retry). Optimistic wins when conflicts are *rare* — no lock to hold, no deadlocks, no blocking, and it works across a network where you can't hold a lock anyway. The cost is that callers must handle a Conflict and retry (P2 §8). Cue: *for shared state with infrequent contention — especially over a network — prefer a compare-and-set on a version token over a lock; reserve locks for genuinely hot, in-process contention.*
>
> **🧭 Design rationale — why a lock was never even an option here.** Don't start from "optimistic vs pessimistic" as a menu; start from the constraint. The writers are *separate processes over HTTP* (kubelet, scheduler, controllers, mykubectl). A lock requires a holder that everyone trusts and that releases on crash — but a client can die mid-write holding the lock, and there's no shared memory to put a mutex in anyway. So pessimistic locking is *structurally unavailable* across a network of independent clients. That leaves "let writes race, detect the conflict after the fact" — which forces a per-object version you compare on write. The `resourceVersion` field isn't a clever choice; it's the only thing that *can* work once you accept "many independent network clients, any of which may vanish." Reproducible takeaway: *when mutators are distributed and crash-prone, lock-based coordination is off the table — reach for versioned compare-and-set, and the version field is mandatory, not optional.*

**Concept — the atomic transaction, in code.** The rv-check and counter-bump must be one indivisible step, or two writers could both pass the check before either bumps. `sled`'s `transaction(closure)` gives that (`replace_spec`):
```rust
let updated = self.db.transaction(|tx| {
    let current = load_required_pod(tx, &key, name)?;   // ① read
    check_rv(&current, provided_rv.as_deref())?;        // ② guard: stale rv → Abort(Conflict)
    let rv = bump_rv(tx)?;                               // ③ bump the global counter
    let mut p = new_pod.clone();
    p.metadata.resource_version = Some(rv.to_string());
    tx.insert(key.as_bytes(), to_json(&p)?)?;           // ④ write — all inside one txn
    Ok(p)
}).map_err(unwrap_txn)?;                                 // ⑤ flatten the 2-layer error
emit(&self.watch_tx, WatchEventType::Modified, updated.clone()); // ⑥ fan out AFTER commit
```
> **🦀 Rust pattern — two-layer transaction error, collapsed at the boundary.** Inside the closure, `?`/`Abort` produce `ConflictableTransactionError<StoreError>`; sled wraps that as `TransactionError<StoreError>` (our `Abort` vs sled's `Storage` I/O error). `unwrap_txn` collapses both back into one flat `StoreError` so callers see a single error type. Cue: *when a library hands you a nested/wrapped error type at an internal boundary, flatten it to your own domain error at that boundary — don't leak the library's error taxonomy up to every caller.*
> **🔧 Implementation choice — emit the watch event AFTER the transaction commits, not inside it.** ⑥ is outside the `transaction(...)` closure on purpose. If you fired the event inside and the transaction then *retried* (sled may re-run the closure under contention) or *aborted*, watchers would see an event for a write that never happened — phantom state. Emitting only on the committed result guarantees every watch event corresponds to a durable change. Cue: *publish a change notification only after the change is durably committed; a notification inside the not-yet-committed critical section can describe a write that gets rolled back.*

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
The whole thing is one generator (`src/apiserver/watch.rs`):
```rust
pub fn stream_events(store: Arc<PodStore>, from_rv: u64)
    -> impl Stream<Item = Result<WatchEvent, WatchError>>     // ① opaque return type
{
    try_stream! {                                             // ② generator macro
        let mut rx = store.subscribe();                       // ③ subscribe BEFORE snapshot
        let (snapshot, snapshot_rv) = store.list()?;
        for pod in snapshot {                                 // catch-up
            if pod_rv(&pod) > from_rv { yield added(pod) }
        }
        loop {                                                // live
            match rx.recv().await {
                Ok(ev) => if pod_rv(&ev.object) > snapshot_rv { yield ev },
                Err(RecvError::Lagged(n)) => Err(WatchError::Lagged(n))?, // ④ `?` ENDS the stream
                Err(RecvError::Closed) => break,
            }
        }
    }
}
```
> **①** `impl Stream` — `try_stream!`'s concrete type is unnameable, so return an opaque `impl Trait`. **②** write the stream as straight-line async with `yield` instead of a hand-rolled `poll_next`. **③** subscribing first means a write between subscribe and `list()` is buffered, not lost. **④** inside `try_stream!`, `?` on an `Err` *yields it as the final item and terminates the stream* — that's how a `Lagged` receiver becomes a clean close → HTTP 410 → client re-lists.

**Gotcha — subscribe BEFORE snapshotting (the correctness lynchpin).** The code does `store.subscribe()` *then* `store.list()`. Reverse that order and a write landing between list and subscribe would vanish — absent from the snapshot, and not yet subscribed. Subscribing first means any such write is buffered in the broadcast channel and replayed in the live phase; the `rv > snapshot_rv` filter discards it only if the snapshot already contained it. This subscribe-then-list ordering is the classic watch-cache argument, and `live_events_after_catch_up_are_delivered` exercises it.

**Concept — `Lagged` → 410 Gone → client must re-list.** The broadcast channel is bounded (256). A client that falls more than 256 events behind gets `RecvError::Lagged`. We *cannot* silently skip — the client's local cache would be permanently wrong. So we terminate the stream with `WatchError::Lagged`; the HTTP layer closes the connection; the client re-lists from scratch and starts a fresh watch. Real K8s returns `410 Gone` with identical meaning: "your resourceVersion is too old to resume from — start over." Pinned by `lagged_receiver_terminates_stream_with_error`.

**Why `async_stream::try_stream!`.** Implementing `Stream`/`poll_next` by hand is fiddly state-machine code. `try_stream!` lets us write the catch-up loop and the live loop as ordinary straight-line async with `yield`; a `?` inside cleanly ends the stream on error. The result is an `impl Stream<Item = Result<WatchEvent, WatchError>>`.

> **🦀 Rust pattern — `try_stream!` + `impl Stream` return for a hand-rolled async iterator.** A `Stream` is the async analogue of `Iterator`; implementing one by hand means writing a `poll_next` state machine that manually tracks "am I in catch-up or live mode?" — error-prone and unreadable. The `try_stream!` macro turns that inside out: you write *normal sequential async code* with `yield` to emit items and `?` to short-circuit, and the macro generates the state machine. The catch: the generated type is **unnameable**, so the function must return `impl Stream<...>` (return-position `impl Trait`), not a concrete type. Cue: *to produce a stream with non-trivial internal control flow, write it as a `try_stream!`/`async_stream!` generator returning `impl Stream`, rather than hand-implementing `poll_next` — reserve the manual impl for when you need a nameable type or zero macro magic.* The `?`-terminates-the-stream behavior is the macro-level mirror of `?` short-circuiting a `Result`-returning fn.

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
In code, the loop is a single `select!` over the four arms:
```rust
loop {
    tokio::select! {
        biased;                                          // ① check arms top-down, not random
        _ = cancel.cancelled() => break,                 //    so cancel beats a pending tick
        event = watch_stream.next() => match event {
            Some(Ok(ev)) => block_in_place(|| self.apply_watch_event(ev)), // ② sync work
            Some(Err(e)) => warn!(?e, "watch error; resync will reseed"),
            None => warn!("watch closed; resync will reseed"),
        },
        _ = resync_interval.tick() => { self.resync().await?; }
        _ = liveness_interval.tick() => {
            let dirty = block_in_place(|| self.tick_liveness());  // ③ compute sync...
            for (name, status) in dirty { self.push_status(&name, &status).await; } // ...push async
        },
    }
}
```
> **①** `biased` makes `select!` poll arms in written order instead of randomly, so cancel always wins over doing one more tick. **②/③** the sync/async boundary: libcontainer work runs in `block_in_place` (tells tokio "this thread is going blocking, move other tasks off it"), but the HTTP `push_status` is `.await`ed *outside* it — you must never `.await` inside `block_in_place`.

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

> **⚙ Principle — design the failure path first, not last.** The "developer" version of a kubelet assumes a clean boot and only handles the happy path; the "engineer" version asks *"what's already running when I start, and what happened to the thing I was managing when I died?"* — and treats restart-into-existing-state as the normal case, not an exception. Notice the pattern is *the same reconcile loop again*: observe actual (live containers), compare to desired (apiserver Pods), converge (reattach / create / destroy-orphan). Level-triggered reconciliation is *why* crash recovery is almost free here — there's no special "recovery mode," just the normal loop run against whatever state it finds. Cue: *for any long-lived process, ask "what does correct behavior look like after a crash mid-operation?" before you write the happy path — and prefer a design where recovery is just the steady-state loop meeting reality, not a separate codepath.*

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

**Concept — `push_status`: optimistic concurrency from the client side.** The `/status` endpoint requires `?resourceVersion=` (§4), so a status write is a read-modify-write against the version the kubelet last saw. Matching on the specific error *variant* drives the retry:
```rust
match self.client.replace_pod_status(name, status, &rv).await {
    Ok(updated) => {                              // server echoes the NEW rv
        self.cache.insert(name.clone(), updated); // refresh cache so next push is current
        self.last_pushed_status.insert(name.clone(), status.clone());
    }
    Err(ClientError::Conflict { .. }) => {        // ← EXPECTED, not exceptional
        let latest = self.client.get_pod(name).await?;        // refetch fresh rv
        let Some(latest) = latest else { return Ok(()) };     // let-else: gone? done.
        let new_rv = latest.metadata.resource_version.clone()
            .ok_or_else(|| anyhow!("refetched pod missing rv"))?;
        self.cache.insert(name.clone(), latest);
        self.client.replace_pod_status(name, status, &new_rv).await?; // retry ONCE
        /* update cache + last_pushed_status again */
    }
    Err(e) => return Err(anyhow!(e)).context("status push"), // any other error propagates
}
```
> Only the `Conflict` variant triggers the refetch-retry; everything else propagates. A *single* retry (not a loop) avoids livelock against a hot-edited Pod — if it conflicts again, the next 2s tick tries fresh. This is the §2 optimistic-concurrency dance, now driven from the client side. The conflict is expected, not exceptional: the cached rv goes stale any time the apiserver advances it. The retry is a *single* bounded refetch-and-reapply — if it conflicts again, we give up and let the next 2s tick try fresh. (A retry *loop* would risk livelock against a hot-edited Pod; one shot keeps it simple and the tick provides natural backoff.)

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

## 9. `mykubectl` — the command-line client (`src/bin/mykubectl.rs`)

The human-facing front door. Medium.

**What it is.** A `kubectl`-shaped CLI that is a *thin* UX layer over the §6 `Client` — no new protocol, no new types, just ergonomics. Three subcommands plus a global `--server` (env `MY_K8S_SERVER`, default `http://127.0.0.1:8080`):
```
   mykubectl apply -f web.yml          create-or-update a Pod from YAML
   mykubectl get pods                  table of all Pods
   mykubectl get pod web               one Pod as YAML
   mykubectl get pods -w               stream live changes as a table
   mykubectl delete pod web            delete by name
```

**Concept — `apply` is an upsert (get → branch → create-or-replace).** This is the most interesting command. `kubectl apply` means "make the cluster match this file" whether or not the object exists yet. Ours does the minimal version:
```
   read YAML → Pod
   client.get_pod(name)?
     Some(existing) → copy existing.resourceVersion into our Pod → replace_pod_spec()   ("replaced")
     None           → create_pod()                                                       ("created")
```
The `resource_version` copy is the key line: a PUT needs the *current* rv (§2 optimistic concurrency), and the user's YAML file doesn't carry one — so `apply` fetches it first. This is a deliberately simplified apply: real `kubectl` does a three-way merge (last-applied vs live vs desired); we just last-writer-wins with the freshly-read rv. Honest shortcut, same user-facing shape.

**Concept — `get` renders status, not just spec.** Listing prints the familiar table by reading the **status** subresource the kubelet reports (§8):
```
   NAME                 PHASE      READY    RESTARTS   AGE
   web                  Running    1/1      0          12s
```
- `PHASE` ← `status.phase` (or `Pending` if status absent yet)
- `READY` ← count of `container_statuses` with `ready == true` / total containers
- `RESTARTS` ← sum of per-container `restart_count`
- `AGE` ← now − `metadata.creationTimestamp`, humanized (`12s`, `5m`, `3h`, `2d`)

This is the payoff of the §8 status loop: the kubelet writes observed state up, and now a *different* process — `mykubectl`, talking only to the apiserver, never touching the node — renders it. Single-pod `get pod web` prints full YAML instead (serde round-trips the Pod back out via `serde_yaml_ng`).

**Concept — `get -w` reuses the watch stream.** With `-w`, `get` opens `client.watch_pods(None)` and prints each event as a row (`EVENT  PHASE  NAME`) as it arrives — the same NDJSON stream the kubelet consumes (§6), now driving a human display. Optional name filter just skips non-matching events. Watching the apiserver from the CLI and from the kubelet are literally the same API call.

**Concept — `delete` is read-rv-then-delete.** Like `apply`, delete needs the current rv (the DELETE endpoint requires `?resourceVersion=`, §4). So it `get_pod` first, pulls the rv, then `delete_pod(name, rv)`. A missing Pod is a clean error (`bail!`), not a panic.

**Decision — `mykubectl` replaces the throwaway `scripts/myctl.sh`.** Phase 1 §14's `myctl.sh` (cat the debug.json through `jq`) was always labeled "Phase 2 will replace this with a real client." It now has: `myctl.sh` reads a node-local debug file; `mykubectl` talks to the apiserver like a real client. The debug snapshot still exists as a kubelet-local introspection aid, but the *cluster* view now comes from `mykubectl`.

## 10. End-to-end demo — the whole stack, live (verified 2026-06-01)

This is the Phase 2 capstone, the analogue of Phase 1 §13 — but now spanning three processes (`apiserver`, `kubelet`, `mykubectl`) instead of one. Verified by hand on the VM; the sequence below is what was actually observed.

**Runbook — bring the stack up:**
```
   # 0. one-time: rootfs (Phase 1) + the apiserver's DB dir, owned by the run user
   sudo bash scripts/prepare-rootfs.sh
   sudo mkdir -p /var/lib/my-k8s/etcd-like && sudo chown raycho /var/lib/my-k8s/etcd-like

   # 1. apiserver (NON-root) — owns desired state in sled
   cargo run --bin apiserver                       # listens 0.0.0.0:8080, db /var/lib/my-k8s/etcd-like

   # 2. kubelet (root — needs namespaces) — informer client of the apiserver
   sudo target/debug/kubelet --api-server-url http://127.0.0.1:8080

   # 3. drive it from the CLI
   mykubectl apply -f manifests/examples/web.yml
   mykubectl get pods           # → web  Running  1/1  0  <age>
```

**What was verified (each step proves a Phase 2 concept):**
1. **apply → watch → run → serve.** `mykubectl apply web.yml` → the kubelet's watch fires `ADDED` almost instantly → it builds the sandbox + httpd container → `mykubectl get pods` shows `Running 1/1` once status is pushed back → `curl` against the pod IP returns the served body. *Proves: the full desired→watch→run→status→observe loop.*
2. **kubelet crash + restart → reattach, no disruption.** `kill -9` the kubelet, restart it → logs show `recover_all` count=2 (pause + httpd) → `from_recovered` reattaches the existing sandbox → **the httpd PID is unchanged and no duplicate containers spawn**. *Proves: §7 startup recovery — a kubelet restart doesn't disturb running Pods.*
3. **delete → graceful teardown.** `mykubectl delete pod web` → the watch fires `DELETED` → the sandbox tears down after the 5s SIGTERM grace (Phase 1 §7). *Proves: deletion propagates apiserver → watch → runtime.*
4. **apiserver crash + restart → state persists.** `kill -9` the apiserver, restart it → a previously-applied `sidecar-demo` Pod is **still there**, read back from sled. *Proves: §2 persistence — desired state outlives the apiserver process.*

**Gotcha — `with_graceful_shutdown` hangs while a watch is open.** A watch is an infinite HTTP response; axum's graceful shutdown waits for in-flight requests to drain, and that one never does. So stopping the apiserver while a kubelet is watching needs `kill -9`. Real K8s sends a stream-close frame to watchers on shutdown; we don't (yet). Known limitation.

**Gotcha — sled is single-writer.** Starting a second apiserver on the same DB fails cleanly with `could not acquire lock` — sled holds an exclusive lock, which *prevents* DB corruption from two writers. (Good: it fails safe. Implication: only one apiserver per DB, as expected for Phase 2.)

**Gotcha — the apiserver runs non-root, the kubelet runs as root.** The kubelet needs root for namespace creation; the apiserver doesn't need it and shouldn't have it. But the default DB path lives under root-owned `/var/lib/my-k8s/`, so its parent must be pre-created and `chown`ed to the run user (see runbook step 0). Two processes, two privilege levels, two state dirs: apiserver owns `/var/lib/my-k8s/etcd-like/`, kubelet owns `/var/lib/my-k8s/state/`.

**Gotcha — manifest examples are `.yml`, and `pkill` can shoot your own shell.** The example files are `web.yml` / `sidecar.yml` (not `.yaml`). And over SSH, `pkill -f "target/debug/kubelet"` matches the *running command line that contains that string* — including your own — so it can kill your shell; use a bracket pattern like `pkill -f "[k]ubelet --api-server"` to avoid self-matching.

## Phase 2 wrap — what this earned us

A working two-tier control plane. Desired state now lives in a persistent, HTTP-served **apiserver** (axum + sled) instead of a local directory; the **kubelet** is an informer-style *client* that watches for spec changes, runs containers to match, and reports observed status back; and **`mykubectl`** drives the whole thing the way `kubectl` drives real K8s. The deep concepts we built rather than read about: **optimistic concurrency** (resourceVersion, read-modify-write, Conflict-retry), the **watch pattern** (list-then-watch, catch-up→live, Lagged→410→relist), the **informer** (watch for latency + resync for safety), the **spec/status split** with level-triggered status reporting, and **persistence + restart recovery** (a kubelet adopts live containers; an apiserver reloads from sled).

What we explicitly did NOT build (deferred): real container start timestamps and exit codes (status carries placeholders), graceful watch-stream close on apiserver shutdown, image pull (still busybox-only), probes, volumes/env/limits. And there's still only one node.

**Phase 3 (next) is controllers.** With a watchable API in place, the natural next layer is the controller pattern: a separate process that watches a higher-level resource (ReplicaSet) and reconciles Pods to match a desired count — recreating one when it's deleted. It's the same list-watch-reconcile loop the kubelet already runs, but operating on *Pods as its output* instead of containers. The watch API we just built is exactly what makes it possible.

---

# Phase 3 — Controllers: the ReplicaSet controller

**What a controller is, and why it's the heart of K8s.** A *controller* is an independent process that watches a high-level resource and drives reality toward it. A **ReplicaSet** says "I want N Pods matching this label selector"; the ReplicaSet controller makes that true — creating Pods when there are too few, deleting when too many, recreating one the instant it's deleted. It is the *same* list-watch-reconcile loop the kubelet runs (P2 §7), but its "output" is **API objects (Pods)**, not containers. This shape — watch a desired-state resource, reconcile the world to match — is the **single most important pattern in Kubernetes**: every Deployment, Job, StatefulSet, and every custom operator is just another controller of this form. Build this one well and you understand all of them.

```
   ┌── apiserver ──┐   watch RS + watch Pods    ┌──── controller-manager ────┐
   │ ReplicaSet    │ ─────────────────────────► │ informers → work queue      │
   │ Pods          │ ◄───────────────────────── │ worker: reconcile(rs_name)  │
   └───────────────┘   create / delete Pods      └─────────────────────────────┘
        the controller is just another CLIENT of the apiserver (P2 §6) — no special access
```

New this phase: `replicaset.rs` (schema), a *generalized* `ResourceStore<T>`, `ObjectMeta` gains labels + ownerReferences, and a whole `controller/` module (`workqueue`, `replicaset`, `manager`) plus a `controller-manager` binary. Construction order below.

## 1. Generalize the store to `ResourceStore<T>` + the ReplicaSet schema (`storage.rs`, `replicaset.rs`, `meta.rs`)

Phase 2's store was hard-wired to `Pod`. A second resource (ReplicaSet) needs the *exact same* machinery — JSON-in-sled, resourceVersion, optimistic concurrency, watch events. So we made the store generic over a trait, and pointed `PodStore` at it:
```rust
pub trait ResourceMeta: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    const KIND_PREFIX: &'static str;          // "pods/" or "replicasets/" — the sled key namespace
    fn meta(&self) -> &ObjectMeta;            // every resource embeds the shared metadata...
    fn meta_mut(&mut self) -> &mut ObjectMeta;// ...and exposes it through these
    fn clear_status(&mut self) {}             // create strips status (default: no-op)
    fn inherit_status(&mut self, _cur: &Self) {} // spec-replace preserves status
}

pub struct ResourceStore<T: ResourceMeta> { db: sled::Db, rv_tree: sled::Tree, watch_tx: broadcast::Sender<WatchEvent<T>> }
pub type PodStore = ResourceStore<Pod>;       // the alias: old call sites keep compiling
```
> **⚙ Principle — defer generalization until the second case (rule of three).** We did *not* build a generic store in Phase 2, even though we "knew" more resources were coming. Speculative generality is a classic trap: you abstract over a future you're guessing at and get the shape wrong. Waiting until ReplicaSet actually arrived meant the trait was shaped by *two real cases*, so `ResourceMeta` carves exactly the seams both need (`KIND_PREFIX`, status hooks) and nothing speculative. Cue: *generalize when a second concrete user forces it, not when you anticipate one.*
>
> **⚙ Principle — refactor without churn via type aliases.** `pub type PodStore = ResourceStore<Pod>;` means every `PodStore::...` call site, every test, every handler kept compiling untouched while the type underneath became generic. Cue: *when you generalize an implementation, keep the old name as an alias so the blast radius of the refactor stays near zero.*

The **ReplicaSet schema** (`replicaset.rs`) is "desired count + how to find my Pods + the Pod blueprint":
```rust
pub struct ReplicaSetSpec {
    pub replicas: u32,
    pub selector: LabelSelector,        // matchLabels — how the RS identifies its Pods
    pub template: PodTemplateSpec,      // the Pod blueprint to stamp out (labels + spec only)
}
```
> **⚙ Principle — couple by data, not by reference.** An RS doesn't hold a list of Pod IDs; it holds a *label selector* and finds its Pods by querying. This is loose coupling: Pods can be created, adopted, or deleted by anyone, and the RS re-derives its set each reconcile. Cue: *prefer "describe what I want and query for it" over "hold hard pointers to specific instances" — the former self-heals, the latter goes stale.*

## 2. `ObjectMeta` gains labels + ownerReferences (`meta.rs`)

Two fields turn flat objects into a graph the controller can navigate:
```rust
pub struct ObjectMeta {
    /* name, uid, resourceVersion, generation, creationTimestamp ... */
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,        // selectors match on these
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owner_references: Vec<OwnerReference>,    // "who controls me"
}
pub struct OwnerReference { /* apiVersion, kind, name, uid, */ pub controller: bool } // exactly one controller:true
```
> **⚙ Principle — model identity and ownership explicitly.** `labels` answer "which set am I part of?" (a *many-to-many, value-based* relationship); `ownerReferences` answer "who is responsible for my lifecycle?" (a *direct, identity-based* link, with `controller: true` marking the one managing owner). Keeping these separate is deliberate: selectors are for *discovery/grouping*, owner refs are for *lifecycle/cascade-delete*. Cue: *don't conflate "is similar to / grouped with" (labels) with "is owned by / cascades from" (references) — they have different change rates and different consumers.*
>
> **⚙ Principle — make the wire format forward/backward compatible.** `skip_serializing_if` omits empty labels/owners entirely, so JSON written before these fields existed still deserializes, and the common no-labels case stays clean. `BTreeMap` (not `HashMap`) gives deterministic key order, so the same labels always serialize to the same bytes. Cue: *additive schema changes should be invisible to old data; deterministic serialization avoids spurious diffs.*

## 3. The work queue (`controller/workqueue.rs`)

Controllers never reconcile straight from watch events. They enqueue a **key** (a resource *name*), and a worker drains keys. The queue is three sets working together:
```rust
struct Inner {
    queue: VecDeque<String>,      // ready to hand to a worker, FIFO
    dirty: HashSet<String>,       // needs processing (queued OR in-flight) — the DEDUP set
    processing: HashSet<String>,  // currently checked out by a worker
}
// add(key):    if already dirty → drop it (dedup). else mark dirty; if not processing, push to queue.
// get()  → key: pop from queue, remove from dirty, insert into processing.
// done(key):   remove from processing; if it went dirty again while in flight, re-queue it ONCE.
```
> **⚙ Principle — decouple producers from consumers with a queue.** Watch events (producers) arrive in unpredictable bursts; reconciles (consumer) take variable time. A queue between them absorbs the burst and lets each side run at its own rate. Cue: *whenever arrival rate ≠ processing rate, put a buffer between them.*
>
> **⚙ Principle — dedup on identity, carry no payload.** The queue holds *names*, not event data. Ten events for `web` collapse to one `web` key → one reconcile that reads *current* full state. This is what makes the controller level-triggered (§4) and cheap under load. Cue: *enqueue "what changed" (a key), not "what happened" (an event); then re-derive the truth when you process it.*
>
> **⚙ Principle — make concurrency invariants explicit in the data model.** The three-set design guarantees, by construction, "never two workers on one key" and "a key re-added mid-flight is reprocessed exactly once, never lost." Those are the hard bugs in concurrent systems, and they're prevented here by *structure*, not by careful timing. Cue: *encode your concurrency guarantees in the types/state, so they hold regardless of scheduling.* (Pinned by `readd_during_processing_requeues_once_on_done`.)
>
> **🧭 Design rationale — how you'd arrive at *three* sets.** Start with the naive queue: a `VecDeque` of keys. Now stress it with the real questions. (1) "The same Service fires 50 events in a burst — do I reconcile 50 times?" No → I need to know "is this key already waiting?" → add a `dirty` set, skip enqueue if present. (2) "A key is being processed and a new event for it arrives — if I requeue now, a second worker could grab it concurrently." Bad → I need to know "is this key in flight?" → add a `processing` set; while processing, mark `dirty` but DON'T enqueue. (3) "When processing finishes, did something arrive meanwhile?" → on `done`, if still `dirty`, requeue once. Each set was forced by one concurrency question; the trio is the minimal state that answers all three. Reproducible takeaway: *don't design a concurrent structure top-down from a pattern — interrogate the naive version with "what if two things race here?" and add exactly the state each answer requires.* (This is client-go's `workqueue` rederived from first principles.)
>
> **🦀 Rust pattern — `Arc<Mutex<Inner>>` + `tokio::sync::Notify` for a shared async queue.** The queue is shared across informer tasks (producers) and the worker (consumer), so it's `Arc` (shared ownership) wrapping a `Mutex` (the three sets mutate together — one lock over the whole `Inner`, not a lock per set, so the invariants stay atomic). The blocking-`get` uses `Notify`: a worker `await`s `notified()` when the queue is empty, and `add` calls `notify_one()` — async parking without busy-polling. Note it's a `std::sync::Mutex`, not `tokio::sync::Mutex`: the critical section is tiny and never `.await`s while held, so the cheaper sync mutex is correct. Cue: *share-across-tasks = `Arc`; mutate-together = one `Mutex` over the whole struct; wake-without-polling = `Notify`; and prefer `std::sync::Mutex` unless you must hold the lock across an `.await`.*

A separate `RateLimiter` (per-key failure count) + `backoff_for(n)` give **exponential backoff on reconcile errors** — the same saturating-shift math as the kubelet's CrashLoopBackOff (P1 §10), kept independent of queue position.

## 4. The reconcile function (`controller/replicaset.rs`)

The heart. Keyed by RS name, it re-reads full state and converges:
```rust
pub async fn reconcile(rs_name: &str, client: &Client) -> Result<()> {
    let rs = match client.get_replicaset(rs_name).await? {
        Some(rs) => rs,
        None => return cascade_delete(rs_name, client).await,   // RS gone → delete its Pods
    };
    // gather owned Pods, adopting matching orphans
    let mut owned = /* list_pods filtered by ownerRef, + adopt() any matching unowned Pod */;
    let desired = rs.spec.replicas as usize;
    if owned.len() < desired {                 // deficit → create from template
        for _ in 0..(desired - owned.len()) { client.create_pod(&pod_from_template(&rs)).await?; }
    } else if owned.len() > desired {          // surplus → delete OLDEST first
        owned.sort_by(|a,b| a.meta.creation_timestamp.cmp(&b.meta.creation_timestamp));
        for pod in owned.iter().take(owned.len() - desired) { client.delete_pod(...).await?; }
    }
    update_status(rs_name, &rs, client).await  // recompute + PUT status — only if changed
}
```
> **⚙ Principle — level-triggered reconciliation.** `reconcile` doesn't ask "what event fired?"; it reads *current* desired (`rs.spec.replicas`) and *current* actual (owned Pods) and computes the difference. So it doesn't matter whether it was triggered by a watch event, a 30s resync, or a retry — the outcome is identical, and a missed event just means the next trigger fixes it. Cue: *make the unit of work "observe everything, converge," not "handle this delta" — it's the difference between a system that self-heals and one that drifts.*
>
> **⚙ Principle — idempotency is what makes retries safe.** Run `reconcile("web")` once or five times against `replicas: 3` and you get exactly 3 Pods (pinned by `reconcile_is_idempotent`). Because re-running is harmless, the controller can trigger aggressively and retry on error without fear of double-creating. Cue: *if an operation is idempotent, you get retry-safety and trigger-freedom for free; design for it.*
>
> **⚙ Principle — guard invariants others depend on (don't steal).** Adoption only takes a Pod that matches the selector **and has no controller owner yet** — a Pod owned by *another* RS is off-limits (`reconcile_does_not_adopt_pod_owned_by_another_rs`). Cue: *before mutating shared state, check the invariants other actors rely on; "it matches my filter" is not the same as "it's mine to take."*
>
> **⚙ Principle — break feedback loops with a change check.** `update_status` PUTs only when the computed status *differs* from the stored one. Without that guard, the status write would emit an RS MODIFIED watch event → re-enqueue → reconcile → identical status write → event → … forever. Cue: *whenever your write produces an event that re-triggers your own logic, gate the write on "did anything actually change?" — this is the #1 way reactive systems spin.*
>
> **⚙ Principle — prefer deterministic behavior.** Scale-down deletes the *oldest* Pods first, not a random set. Deterministic choices make the system predictable, debuggable, and testable. Cue: *when you must pick among equivalent items, pick by a stable rule (age, name) rather than leaving it to map/iteration order.*

## 5. The manager: composing the loops (`controller/manager.rs`)

Four tasks, all funneling RS *names* into one queue; one worker drains it:
```rust
tokio::spawn(rs_informer(...));    // watch ReplicaSets → enqueue the RS's own name
tokio::spawn(pod_informer(...));   // watch Pods → map each to its owning RS (via ownerRef) → enqueue that
tokio::spawn(resync_loop(...));    // every 30s: enqueue EVERY RS (safety net for missed events)
tokio::spawn(worker_loop(...));    // get() a key → reconcile → on Ok forget+done; on Err done + add_after(backoff)
```
> **⚙ Principle — watch for latency, resync for safety.** The informers react in milliseconds; the 30s resync is the backstop that heals anything the watches dropped (a connection blip, a missed event). Neither alone is enough — watch is fast but lossy, resync is reliable but slow. Cue: *for eventually-consistent reactive systems, pair a fast-but-unreliable signal with a slow-but-complete sweep.* (Same shape as the kubelet's informer, P2 §7.)
>
> **⚙ Principle — funnel many event sources to one unit of work.** A *Pod* event and an *RS* event both reduce to "reconcile this RS name." The pod_informer maps a Pod back to its owner via `rs_key_for_pod` (reads the ownerRef) before enqueuing. So the worker has exactly one kind of job, and all the triggering complexity lives at the edges. Cue: *normalize diverse triggers into a single canonical work item as early as possible.*
>
> **⚙ Principle — design for disconnection.** Each informer wraps its watch in a reconnect loop: on stream error or close, it logs, waits `RECONNECT_DELAY`, and re-opens — and the next resync re-seeds anything missed during the gap. Cue: *any network stream WILL drop; the question is whether your code treats reconnect as the normal case (it should) or a crash (it shouldn't).*

This is exactly how you'd kill a Pod and watch it come back: `delete_pod` → apiserver emits DELETED (carrying ownerRefs) → pod_informer maps it to the RS → enqueue → worker reconciles → recreates the missing Pod. Pinned end-to-end by `controller_recreates_a_deleted_pod` (apiserver + full manager in-process).

## Phase 3 wrap — what this earned us

The **controller pattern**, the mechanism that makes Kubernetes *extensible*: an independent process that watches a desired-state resource and reconciles the world to match, built entirely as an ordinary apiserver client (no privileged access). Concretely: a generic `ResourceStore<T>` serving multiple kinds, label selectors + ownerReferences turning flat objects into an ownership graph, a deduplicating work queue, and a level-triggered reconcile that creates/deletes/adopts Pods and self-heals on deletion.

But the durable takeaway is the **engineering judgment**, not the K8s trivia. This phase alone demonstrates: defer-generalization (rule of three), refactor-by-alias, couple-by-data, model-ownership-explicitly, decouple-via-queue, level-triggered reconciliation, idempotency-for-retry-safety, guard-others'-invariants, break-feedback-loops, deterministic-behavior, watch-for-latency/resync-for-safety, and design-for-disconnection. Every one of those (see the [[#Engineering principles, by example|Engineering principles index]]) transfers to systems that have nothing to do with containers.

What we did NOT build: Deployments (rolling updates over ReplicaSets), multi-controller coordination, leader election (only one controller-manager may run safely today), and real readiness beyond the kubelet's phase report.

**Phase 4 (next) is the scheduler.** Right now a Pod created by the RS controller has no node assignment — in a multi-node world, *something* must decide which kubelet runs it. The scheduler is yet another controller: watch for Pods with no node, score the candidates, write the binding. Same loop, new decision.

---

# Phase 4 — The scheduler & multi-node

**The one new idea: placement is a decision, and a decision is just data.** Until now every Pod ran wherever the single kubelet was. With more than one node, *something* must decide *which* node runs each Pod. That something is the **scheduler** — and the beautiful part is how little it touches: it watches for Pods with no node assigned, picks one, and writes a single field (`spec.nodeName`) back through the apiserver. It never starts a container. The kubelet on the chosen node notices "this Pod is mine" and runs it. The scheduler *decides*; the kubelet *executes*; they never talk to each other.

```
   scheduler                 apiserver                 kubelet (node-a)
   ─────────                 ─────────                 ────────────────
   watch pods nodeName=∅ ───► (unscheduled pod)
   pick node-a (filter+score)
   POST .../binding {node-a}─► spec.nodeName = node-a
                              └ watch pods nodeName=node-a ──► sees it → runs it
```
> **⚙ Principle — separate decision from execution; encode the decision as data.** The scheduler's entire output is one written field. Because the *decision* (where) is decoupled from the *execution* (how), they're independent processes — you can test the scheduler with no kubelet, swap the scheduling algorithm without touching the runtime, and run many kubelets that each only execute their own slice. Cue: *when a system both "chooses" and "does," split them — make the choice a piece of persisted data, and let the doer react to it. Decider and doer then evolve, fail, and scale independently.*
>
> **🧭 Design rationale — how you'd arrive at "the scheduler writes a field" instead of "the scheduler starts the container."** The tempting first design: the scheduler picks a node and *tells that node's kubelet* to run the pod (an RPC). Now ask the hard questions. (1) "What if the kubelet is briefly down when the scheduler calls?" → the scheduler must retry, track delivery, handle the kubelet coming back — it's now coupled to kubelet liveness. (2) "What if I want two schedulers, or to swap the algorithm?" → every kubelet now needs to know about schedulers. (3) "How do I test placement?" → I need a fake kubelet to receive the call. Every problem traces to the *direct call*. Remove it: the scheduler instead writes its decision (`nodeName`) into the shared store, and each kubelet — already watching for its own pods (P4 §3) — picks it up. The RPC dissolves; delivery, retry, multi-consumer, and testability all become free properties of the watch you already built. Reproducible takeaway: *when component A needs B to act, prefer "A records the desired outcome in shared state, B reacts via its existing watch" over "A calls B directly" — the indirection through data buys you decoupling, retry-for-free, and testability that a direct call costs you.*

New this phase: `spec.nodeName` (the binding target), a `Node` resource + heartbeat, the **binding subresource**, a server-side **`fieldSelector`** (the ambitious centerpiece), a node-aware kubelet, and a `scheduler` binary. Construction order below.

## 1. `spec.nodeName` + the `Node` resource (`pod.rs`, `node.rs`)

Two tiny additions carry the whole phase. First, the binding target on the Pod:
```rust
pub struct PodSpec {
    pub containers: Vec<Container>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node_name: Option<String>,   // None = unscheduled; Some = bound to that node
}
```
`Option<String>` *is* the scheduled/unscheduled distinction — the scheduler watches for `None`, the kubelet filters for *its* `Some`. Then the `Node` itself — a machine that can run Pods, which self-registers and heartbeats:
```rust
pub struct NodeSpec { pub unschedulable: bool }          // cordon flag (default false)
pub struct NodeStatus {
    pub ready: bool,
    pub last_heartbeat_time: Option<String>,             // RFC3339; freshness = liveness
}
impl ResourceMeta for Node { const KIND_PREFIX: &'static str = "nodes/"; /* meta()/status hooks */ }
```
> **⚙ Principle — model liveness as freshness, not a flag.** `NodeStatus` has a `ready` *bool*, but the truth lives in `last_heartbeat_time`. A node that crashes can't flip its own `ready` to false — but it *stops heartbeating*, and staleness is observable from outside. The flag is a hint; the timestamp is the fact. Cue: *to know if a remote component is alive, require it to keep proving so (a heartbeat) and treat silence as death — never trust a self-reported "I'm fine" bit that a dead process leaves frozen at true.* (Real K8s uses a richer `conditions` list; we kept a flat status because we only ever ask the one Ready+freshness question — see [[#Engineering principles, by example|defer-complexity]].)

## 2. `Node` in the apiserver (3rd store) + the binding subresource (`storage.rs`, `handlers.rs`, `routes.rs`)

Adding a third resource was almost free — the Phase 3 generic store just gets a third instantiation, all three sharing **one** sled DB (one global resourceVersion counter):
```rust
// AppState now carries three stores, one per kind, over the same Db:
pub struct AppState {
    pub store:      Arc<ResourceStore<Pod>>,        // = PodStore
    pub rs_store:   Arc<ResourceStore<ReplicaSet>>,
    pub node_store: Arc<ResourceStore<Node>>,       // NEW — zero new storage code
}
```
> **⚙ Principle — a good abstraction makes the 3rd case free.** ReplicaSet (P3) was the case that *forced* the generic `ResourceStore<T>`; Node is the payoff — it reused every line. That's the signal a generalization was correct: the *next* user costs almost nothing. Cue: *judge an abstraction by the marginal cost of the case after the one that motivated it — near-zero means you cut the seam right; still-painful means you abstracted the wrong axis.*

The **binding subresource** is how placement happens — a narrow, single-purpose endpoint:
```rust
// POST /api/v1/pods/{name}/binding   body: { "nodeName": "node-a" }
pub async fn bind_pod(State(state), Path(name), Json(binding): Json<Binding>) -> Result<Response, ApiError> {
    if binding.node_name.is_empty() { return Err(ApiError::BadRequest(...)); }
    let mut pod = state.store.get(&name)?.ok_or(ApiError::NotFound(...))?;
    pod.spec.node_name = Some(binding.node_name);     // stamp the one field
    let updated = state.store.replace_spec(&name, pod)?;   // reuses rv-checked write (P2 §2)
    Ok((StatusCode::OK, Json(updated)).into_response())
}
```
> **⚙ Principle — a subresource is interface segregation for a write.** Binding could have been "just PUT the whole Pod with nodeName set." A dedicated `/binding` endpoint that touches *only* placement means the scheduler needs no permission to edit anything else, the intent is explicit in the API, and the surface a future RBAC layer must guard is tiny. Cue: *when one actor should change only one aspect of an object, give it a narrow endpoint for exactly that, rather than handing it the whole object and trusting it to touch nothing else.*

## 3. Server-side `fieldSelector` — the centerpiece (`watch.rs`, `handlers.rs`)

The ambitious choice this phase: rather than ship every Pod to every kubelet and filter client-side, the apiserver filters **server-side, per subscriber**. A kubelet asks for `?fieldSelector=spec.nodeName=node-a` and its watch only ever sees *its* Pods. This threads a predicate all the way into the generic watch stream:
```rust
pub fn stream_events<T, F>(store: Arc<ResourceStore<T>>, from_rv: u64, filter: F)
    -> impl Stream<Item = Result<WatchEvent<T>, WatchError>>
where T: ResourceMeta, F: Fn(&T) -> bool + Send + 'static {   // ① the bound that matters
    try_stream! {
        /* catch-up */ for obj in snapshot { if rv(&obj) > from_rv && filter(&obj) { yield added(obj) } }
        /* live      */ loop { match rx.recv().await { Ok(ev) => if rv > snap && filter(&ev.object) { yield ev }, ... } }
    }
}
```
And the Pod handler builds the predicate as a single owned closure:
```rust
let node_filter = parse_node_name_selector(params.field_selector.as_deref()); // Option<String>
let filter = move |p: &Pod| match &node_filter {          // ② ONE closure, branch on captured data
    Some(node) => p.spec.node_name.as_deref() == Some(node.as_str()),
    None => true,                                          // no selector ⇒ match all
};
stream_events(state.store.clone(), from_rv, filter)
```
> **⚙ Principle — filter at the source (predicate pushdown).** The same instinct as a SQL `WHERE` running in the database, not in your app: move the predicate to where the data already is, so you transmit only what's wanted. Here it also means each kubelet's watch is isolated to its own node's Pods — less wire traffic, and a natural security/blast-radius boundary. Cue: *when a consumer wants a subset, push the filter toward the producer; shipping everything "and filtering later" wastes bandwidth and leaks data the consumer shouldn't see.*
>
> **🦀 Rust pattern — capture owned data in a `'static` closure, branch inside.** `F: Fn(&T) -> bool + Send + 'static` is the crux: the closure outlives the request (it lives inside the `try_stream!` generator, on its own task), so it can't borrow — it must `move` and own its captures (the `Option<String>`). And note the design choice: rather than *return one of two different closure types* (which would need `Box<dyn Fn>`), we make **one** closure that captures the `Option` and branches on it — a single concrete type, no allocation, no dynamic dispatch. Cue: *a closure that escapes to another task must own everything it touches (`move` + `'static`); to keep "filter or don't" as one type, capture the choice as data and branch inside, instead of picking between closures.*
>
> **⚙ Principle — be lenient at the boundary, safe by default.** `parse_node_name_selector` understands exactly one selector (`spec.nodeName=`); anything else returns `None` → no filtering. An unrecognized selector degrades to "show everything," never to an error or an empty result. Cue: *a parser at a system boundary should fall back to a safe, obvious default on input it doesn't understand, rather than failing hard — but make sure the default is the safe one (here, "no filter" can't hide a Pod from a kubelet that needs it).*

## 4. The node-aware kubelet (`reconciler.rs`, `bin/kubelet.rs`)

The kubelet gains a `--node-name` (clap `env = "NODE_NAME"`, default `node-0`) and three new behaviors. First it **registers itself** — idempotently:
```rust
match self.client.create_node(&node).await {
    Ok(_) => info!("registered node"),
    Err(ClientError::AlreadyExists) => info!("node already registered"), // ← restart/race = success
    Err(e) => return Err(...),
}
```
> **⚙ Principle — make registration idempotent.** A kubelet restarts, or two start at once — "already exists" is not an error, it's the steady state. Treating `AlreadyExists` as success means registration is safe to run every boot with no "have I registered?" bookkeeping. Cue: *operations that establish state ("ensure X exists") should succeed whether or not X was already there — idempotency turns "did I already do this?" from a question you must answer into one you never ask.*

Then a **heartbeat loop** on its own task, refreshing `lastHeartbeatTime` every 10s:
```rust
let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);   // first tick fires IMMEDIATELY
loop {
    tokio::select! { _ = cancel.cancelled() => return, _ = interval.tick() => {} }
    // get node → take its rv → PUT fresh status (ready:true, lastHeartbeatTime: now)
}
```
> **⚙ Gotcha turned principle — know your timer's first-tick semantics.** `tokio::time::interval`'s **first `.tick()` returns immediately**, so the node is marked Ready the instant the kubelet starts — no 10s window where a freshly-booted node looks dead. (Elsewhere — the reconciler's resync — we *await-and-discard* the first tick on purpose, to avoid acting before startup finishes.) Same API, opposite need. Cue: *before using any periodic timer, check whether tick #1 fires now or after one period — it's a perennial off-by-one-interval bug (Go's `time.Ticker` does NOT fire immediately; tokio's `interval` does).* (This bug prompted a Rust-vault note on tick scheduling.)

Finally, the kubelet switches its Pod watch to `watch_pods_on_node(&node_name)` / `list_pods_on_node` — so it only ever sees Pods bound to it. Because the server already filtered (§3), `apply_watch_event` needs **no** node guard: every Pod that arrives is, by construction, this node's to run.

## 5. The scheduler — just another controller (`scheduler.rs`, `bin/scheduler.rs`)

Structurally identical to the controller-manager (P3 §5): informer + resync + worker over one queue. Only the *unit of work* differs — it's `schedule(pod_name)` instead of `reconcile(rs_name)`, and the informer enqueues Pods with **no** `nodeName`:
```rust
pub async fn schedule(pod_name: &str, client: &Client) -> Result<()> {
    let pod = client.get_pod(pod_name).await?;            // re-read FRESH (level-triggered)
    if pod.spec.node_name.is_some() { return Ok(()); }    // someone already placed it → done

    let now = Utc::now();
    let nodes = client.list_nodes().await?;
    let candidates: Vec<&Node> = nodes.iter().filter(|n| is_schedulable(n, now)).collect(); // FILTER
    if candidates.is_empty() { bail!("no Ready node available for pod {pod_name}"); }

    // SCORE: least-loaded. Count each candidate's current pods, pick the min.
    let all_pods = client.list_pods().await?;
    let mut load: HashMap<&str, usize> = candidates.iter().map(|n| (n.name(), 0)).collect();
    for p in &all_pods { if let Some(n) = &p.spec.node_name { load.entry(n).and_modify(|v| *v += 1); } }
    let chosen = candidates.iter().min_by_key(|n| load[n.name()]).unwrap();

    client.bind_pod(pod_name, chosen.name()).await        // WRITE the decision
}
```
The filter is the liveness gate:
```rust
fn is_schedulable(node: &Node, now: DateTime<Utc>) -> bool {
    if node.spec.unschedulable { return false; }                 // cordoned
    let Some(status) = &node.status else { return false; };       // never heartbeated
    if !status.ready { return false; }
    match &status.last_heartbeat_time {
        Some(ts) => parse(ts).map_or(false, |hb| (now - hb).num_seconds() < STALENESS_WINDOW_SECS),
        None => false,                                            // ← every unknown ⇒ NOT schedulable
    }
}
```
> **⚙ Principle — filter → score → act, and make the filter fail-safe.** Scheduling is two phases: *filter* to feasible candidates (hard constraints — Ready, fresh, not cordoned), then *score* the survivors (soft preference — least-loaded) and pick the best. Crucially, every uncertain case in `is_schedulable` returns **false**: no status, no heartbeat, unparseable timestamp, stale → all "not a candidate." Cue: *for a placement/selection decision, separate must-haves (filter) from nice-to-haves (score); and when a candidate's fitness is unknown, exclude it — binding a Pod to a maybe-dead node is worse than waiting for a definitely-live one.*
>
> **⚙ Principle — reuse the loop, change the verb.** The scheduler is not a new kind of program — it's the controller skeleton (P3) with `schedule` swapped in for `reconcile`. Once you have "watch → enqueue key → worker reconciles," every new control plane component is *that shape with a different decision function*. Cue: *recognize when a "new" requirement is an instance of a pattern you already have; the scheduler, the RS controller, and the kubelet are one loop wearing three hats.*

## 6. Multi-node demo (verified on the VM)

Two kubelets on the one VM host, made logically distinct by `--node-name` + separate `--state-dir` (same Linux kernel runs all the containers; the *node* identity is what differs):
```
   apiserver + controller-manager + scheduler, then:
   kubelet --node-name node-a --state-dir /var/lib/my-k8s/state-a
   kubelet --node-name node-b --state-dir /var/lib/my-k8s/state-b
   mykubectl apply -f rs.yml   (replicas: 4)
   → RS controller creates 4 Pods (nodeName=∅)
   → scheduler spreads them least-loaded → 2 on node-a, 2 on node-b → all Running
   → mykubectl get nodes   shows node-a, node-b Ready
```
**The liveness test:** `kill` node-b's kubelet → its heartbeat goes stale (>30s) → scale the RS to 6 → the scheduler places **both** new Pods on node-a only (node-b excluded as stale), while node-b's *existing* Pods are left untouched (the scheduler only places the unscheduled; it doesn't evict). That's the heartbeat-freshness liveness gate (§1, §5) working end-to-end.

> **⚙ Gotcha — `mykubectl get nodes` READY is a display wart.** The READY column just echoes `status.ready`, which a dead node leaves frozen at `true` (nothing flips it). The *scheduler* ignores that bool and checks heartbeat freshness, so placement is correct — but the table lies until you'd compute READY from heartbeat age too. Worth knowing: the authoritative liveness signal is `lastHeartbeatTime`, not `ready`.

## Phase 4 wrap — what this earned us

A real **scheduler** and a **multi-node** cluster: Pods get placed onto live nodes by an independent component whose entire job is to write one field, kubelets self-register and prove liveness by heartbeat, and the apiserver serves each kubelet a server-filtered slice of Pods via `fieldSelector`. The control plane is now four cooperating loops — apiserver (state), controller-manager (replicas), scheduler (placement), kubelet (execution) — none aware of the others, all coordinating through watched, versioned state.

The transferable engineering haul (all in the [[#Engineering principles, by example|index]]): separate-decision-from-execution (policy as data), filter-at-the-source (predicate pushdown), fail-safe defaults, liveness-via-freshness, idempotent registration, a-good-abstraction-makes-the-3rd-case-free, subresource-as-interface-segregation, and the recurring reuse-the-loop-change-the-verb. Plus a sharp Rust lesson: escaping closures must own their captures (`move + Send + 'static`), and "filter or not" stays one type by capturing the choice as data.

What we did NOT build: scheduling beyond least-loaded (no resource requests/limits, affinity, taints/tolerations), Pod eviction/rescheduling off a dead node (we only place the unscheduled), and leader election (one scheduler instance only).

**Phase 5 (next) is Services & networking.** Pods now spread across nodes — but they have per-Pod IPs that come and go. A Service gives a stable virtual IP that load-balances across a changing set of Pods, which means programming the dataplane (iptables/netfilter, the kube-proxy model). The control-loop muscle carries over; the new frontier is the Linux network stack.

> **Phase 5 was split** into **5a (real pod networking)** — build the IPs first, ship independently — and **5b (Services)** — re-plan once pods are actually reachable. Driver: before 5a, pods had only loopback; there were no pod IPs for a Service VIP to target. This section is 5a.

---

# Phase 5a — Real pod networking

**The shift: the new frontier is the Linux network stack, not another control loop.** Every phase so far added a control-plane loop; 5a is different — the orchestration is familiar, but pods finally get **routable IPs** and can talk to each other across (logical) nodes. Before this, a pod had only `lo`. After it, each pod has an `eth0` on a shared bridge with a cluster-unique address, and `wget <pod-ip>:8080` works from any pod to any other.

The three locked design decisions that make this simple:
1. **One flat host bridge** `mykube0` (cluster CIDR `10.244.0.0/16`, gateway `.0.1`). All kubelets share ONE Linux host, so a single bridge puts every pod on one L2 segment — cross-(logical-)node reachability for free, no overlay, no routing.
2. **Per-node /24 PodCIDR.** The apiserver hands each Node a disjoint `10.244.{n}.0/24` on registration. Disjoint slices mean two kubelets never hand out the same IP **without any coordination** — the partition *is* the coordination.
3. **No new crates** — shell out to `ip`/`nsenter` (the same tools a real CNI plugin drives).

```
                         host: mykube0 bridge  10.244.0.1/16  (one L2 segment)
        ┌───────────── veth ─────────────┐         ┌───────────── veth ─────────────┐
   node-a kubelet (/24 = 10.244.1.0/24)            node-b kubelet (/24 = 10.244.2.0/24)
   pod p1  eth0 10.244.1.2/16 ───┐                 pod p3  eth0 10.244.2.2/16 ───┐
   pod p2  eth0 10.244.1.3/16 ───┴── all on mykube0 ── p1 can wget p3 directly ──┘
```

## 1. Schema: `PodStatus.pod_ip` + `NodeSpec.pod_cidr` (`pod.rs`, `node.rs`)

Two `Option<String>` fields carry the whole feature. The placement of `pod_ip` is the deepest decision:
```rust
pub struct PodStatus { /* phase, container_statuses, ... */
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,        // serde: "podIP" — OBSERVED, so it lives in status
}
pub struct NodeSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,      // serde: "podCIDR" — the node's assigned /24
}
```
> **⚙ Principle — put a fact where its writer and its recovery path live.** `pod_ip` goes in **status**, not spec, because the *kubelet* assigns it (it's observed reality, not desired intent) — and crucially, it becomes the **recovery source of truth**: a restarted kubelet reads each pod's `status.podIP` back from the apiserver and re-reserves it, so IPs survive restarts. Spec is "what I want"; status is "what is" — an assigned IP is emphatically the latter. Cue: *decide who owns a field by asking "who writes it, and who needs to read it back after a crash?" — that usually tells you spec vs status (desired vs observed).*

## 2. Pure IPAM allocator (`src/ipam.rs`)

A self-contained allocator over one /24 — the CNI `host-local` plugin in miniature. Pure (no I/O), so it's exhaustively unit-tested (Layer 1):
```rust
pub struct IpAllocator {
    prefix: [u8; 3],          // e.g. [10,244,1] for 10.244.1.0/24
    next: u16,                // cursor: next 4th-octet to try
    released: BTreeSet<u8>,   // freed addrs, REUSED before advancing (compact pool, smallest-first)
    allocated: BTreeSet<u8>,  // in-use; lets reserve() claim out of order, allocate() skip it
}
// allocate(): reuse a released addr first, else advance the cursor skipping `allocated`. .2..=.254 → 253 usable.
// reserve(ip): mark in-use out-of-band — the recovery path, idempotent, runs BEFORE any fresh allocate.
// release(ip): return to pool; no-op if not ours (best-effort teardown).
```
> **⚙ Principle — separate the *policy* (which IP) from the *mechanism* (wiring it up).** `IpAllocator` decides addresses with zero knowledge of veth, bridges, or netns — it's just integer-set bookkeeping. The sandbox (§4) does the kernel wiring with zero knowledge of allocation strategy. That seam is why the allocation logic is 100% unit-testable without root, and why you could swap the strategy (random, ranges) without touching networking. Cue: *a "decide" step and a "do" step almost always want to be different functions/modules — the decider stays pure and testable, the doer stays thin.*
>
> **⚙ Principle — partition to avoid coordination.** Two kubelets allocate IPs concurrently with **no locks, no consensus, no chatter** — because each owns a disjoint /24. The hard distributed problem (don't double-assign) is dissolved by carving the space so overlaps are structurally impossible. Cue: *before reaching for a lock or a consensus protocol to coordinate writers, ask whether you can partition the resource so they never contend in the first place — disjoint ownership is the cheapest concurrency control there is.*

## 3. apiserver assigns each Node a /24 (`handlers.rs`/`storage.rs`)

On `create_node`, the apiserver stamps the next slice via a persistent sled counter (`node_cidr_counter`): node *n* → `10.244.{n}.0/24`. The assignment is guarded by a get()-check so a **re-registering** kubelet doesn't burn a fresh slice each restart.
> **⚙ Principle — make resource assignment idempotent across retries.** Registration is create-or-already-exists (P4); the CIDR assignment rides the *create* path and is skipped when the Node already exists — so a kubelet can restart-and-re-register all day and keep its same /24. Cue: *any "assign a scarce resource on first contact" step must detect "already assigned" and return the existing value, or restarts silently leak the resource (here: exhaust the /16 one reboot at a time).*

## 4. Sandbox: veth + bridge wiring (`runtime/sandbox.rs`)

The kernel mechanism. `create(runtime, pod_ip: Option<String>)` now branches: an IP → full networking; `None` → loopback-only fallback (the Phase-1 behavior). `ensure_bridge()` (idempotent, run once at kubelet startup) makes the shared `mykube0`; `setup_pod_network` wires each pod:
```rust
// create the veth "cable", host end onto the bridge:
run(&["ip","link","add", &host_veth, "type","veth","peer","name", &peer])?;
run(&["ip","link","set", &host_veth, "master", BRIDGE_NAME])?;   // mykube0
run(&["ip","link","set", &host_veth, "up"])?;
run(&["ip","link","set", &peer, "netns", &pause_pid])?;          // peer INTO the pod netns
// inside the netns (via nsenter -t <pause_pid> -n): become eth0, address /16, default route:
nsenter(&pid, &["ip","link","set", &peer, "name","eth0"])?;
nsenter(&pid, &["ip","addr","add", &format!("{pod_ip}/16"), "dev","eth0"])?;
nsenter(&pid, &["ip","link","set","eth0","up"])?;
nsenter(&pid, &["ip","route","add","default","via", BRIDGE_GATEWAY])?;
```
> **⚙ Concept — a veth pair is a virtual patch cable.** One end stays in the host and plugs into the bridge; the other end is *moved* into the pod's network namespace (targeted by the pause PID) and renamed `eth0`. The `/16` address (not `/24`!) is deliberate: it tells the pod "the whole cluster is on my local segment," so cross-node pods are reachable directly over the bridge with just a default route — no inter-node routing needed. This is exactly what a CNI plugin does; we're hand-rolling the `bridge` + `host-local` plugins.
>
> **⚙ Principle — idempotent setup must tolerate the *exact* failure wording, and races have more than one loser-shape.** `ensure_bridge` runs in every kubelet; the second one to start must treat "already there" as success. The bug caught in e2e: `ip link add` says `"File exists"` but `ip addr add` says `"Address already assigned"` — so tolerating only the first phrasing crashed the losing kubelet on the *address* step. Fix: `run_tolerate` takes a **slice** of acceptable substrings. Cue: *when making an operation idempotent by swallowing "already exists" errors, enumerate every distinct way the underlying tool can phrase it — one tolerated string is rarely enough.*
>
> **⚙ Decision — a stable, length-bounded veth name from the pod name.** `veth_name` = `'v'`/`'p'` + 14 hex of `DefaultHasher(pod_name)` — deterministic (so restart finds the same interface) and ≤15 chars (the kernel `IFNAMSIZ` limit). Cue: *when an external system caps an identifier's length, hash a stable input down to fit rather than truncating (which collides) or counting (which isn't restart-stable).*

## 5. Reconciler: IPAM lifecycle + recovery (`reconciler.rs`)

The kubelet gains `ipam: Option<IpAllocator>`, built in `startup()` from its own Node's `podCIDR` (learned from the apiserver — the source of truth for CIDR). The recovery ordering is the subtle, important part:
```rust
// startup(): for every pod already on this node (from the apiserver),
//   RESERVE its status.podIP into the allocator BEFORE any fresh create_pod() can allocate:
if let Some(ip) = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref())
    && let (Some(ipam), Ok(addr)) = (self.ipam.as_mut(), ip.parse()) {
    let _ = ipam.reserve(addr);   // survivor's IP claimed first → no fresh pod can collide
}
// ensure_pod_ip(): reuse the pod's existing status.podIP if set, else allocate() a new one.
// release_ip(): on pod removal, return the IP to the pool.
// compute_pod_status(): report pod_ip up so it persists + drives the next recovery.
```
> **⚙ Principle — on recovery, claim known state before minting new state.** The reservation pass runs *before* any fresh allocation, so a restarted kubelet can never hand a live survivor's IP to a new pod. Order matters: reserve-then-allocate is safe; allocate-then-reserve would let a new pod grab `.2` before the survivor's `.2` is reclaimed. Cue: *crash recovery should first re-establish what's already true (reserve existing assignments) and only then make new decisions — rebuild the world before you add to it.* This is the same shape as the kubelet reattaching sandboxes (P2 §7); IP recovery is that pattern applied to addresses.
>
> **🔧 Implementation choice — in-memory allocator rebuilt from the apiserver, not persisted to local disk.** The `IpAllocator` keeps its `allocated`/`released` sets purely in memory; on restart they're *reconstructed* by reading each pod's `status.podIP` back from the apiserver and `reserve()`-ing it. We could instead have persisted the allocator state to a local file — but that introduces a second source of truth that can drift from the apiserver (the real record of which pod has which IP). Rebuilding from the apiserver means there's exactly one authority, and the allocator is a derived cache of it. Cue: *don't persist derived state to a second store if you can cheaply rebuild it from the authoritative one on startup — two stores of the same fact will eventually disagree, and reconciling them is its own bug farm.*

## 6. mykubectl IP column

`mykubectl get pods` gained an `IP` column reading `status.podIP` — the observable proof, from a pure apiserver client, that the kubelet assigned and reported each address.

## 7. e2e validation (on the VM)

See the [[#How validation works in this project|Validation catalog]] → "Phase 5a" for the full run. In short, two kubelets with distinct /24s; an RS of 3 spread across both; each pod got an IP from its node's slice; **cross-node `wget <pod-ip>:8080` succeeded** (flat bridge = one L2 segment); and pod IPs **survived a kubelet kill+restart** (recovered from `status.podIP` and re-reserved). Two latent bugs surfaced only here — the `ensure_bridge` wording race and `resync()` using cluster-wide `list_pods()` instead of `list_pods_on_node` — both documented in the catalog.

## Phase 5a wrap — what this earned us

Real pod networking: every pod gets a routable, cluster-unique IPv4 via veth+bridge, reported up to the apiserver and surviving kubelet restarts. The control plane is unchanged in shape; what's new is that the *data plane* exists — pods can finally reach each other. The transferable haul ([[#Engineering principles, by example|index]]): policy-vs-mechanism (allocator vs wiring), partition-to-avoid-coordination (disjoint /24s = lock-free IPAM), idempotent-resource-assignment, claim-known-state-before-minting-new (recovery ordering), and the hard-won idempotency lesson that you must tolerate every wording of "already exists."

What we did NOT build (deferred to 5b): Services (a stable virtual IP), an endpoints controller (tracking which pod IPs back a Service), and the iptables/netfilter DNAT dataplane (the kube-proxy analog). 5a gave Services something real to point at.

**Phase 5b (next) is Services.** Pod IPs are real now but ephemeral — they change as pods come and go. A Service is a stable ClusterIP that load-balances across the current backing pods, which means an endpoints controller (a familiar reconcile loop) feeding iptables DNAT rules (the unfamiliar netfilter part).

---

# Phase 5b — Services (ClusterIP, Endpoints, kube-proxy)

The capstone of Phase 5: a **stable virtual IP** that load-balances across a churning set of backend pods. This section leans hard into the *thought process* — design derivation, implementation judgment, and Rust patterns — because the architecture here is almost entirely re-derivable from forces you've already met.

> **🧭 Design rationale — how you'd arrive at "Service + a separate Endpoints object" from scratch.**
> **Problem, stated honestly:** pods have IPs now (5a) but they're *ephemeral* — a pod dies, the RS makes a replacement with a new IP. A client can't hold a pod IP. We need a *stable address fronting a changing set of pods*.
> **Force 1 — decouple the stable from the volatile.** The name (VIP + selector) is durable; the membership (which pod IPs back it right now) churns on every pod birth/death. When one thing is stable and another churns underneath it, the move is **split them into two objects**: `Service` (stable) and `Endpoints` (volatile). Fuse them and every pod churn rewrites the Service — conflating durable intent with disposable fact.
> **Force 2 — who computes the volatile part?** The endpoint list is *derived* state ("Running pods matching this selector, that have an IP"). Derived state in this system is always produced by a **reconcile loop** — you've built three. So you don't invent a mechanism; the endpoints controller is the controller pattern wearing a fifth hat.
> **Force 3 — separate the decision from the dataplane.** Something must turn "VIP → these IPs" into actual packet redirection. Not the endpoints controller — same split as scheduler (decide) vs kubelet (execute): the control decision and the kernel packet-rewriting have different privileges, failure modes, and scaling. So a *third* component, **kube-proxy**, watches Endpoints and owns iptables.
> **Reproducible takeaway:** faced with "stable front / churning back," the moves are nearly forced — *split stable from volatile into two objects; derive the volatile one with the reconcile loop you already have; separate the decision from the dataplane.* You could re-derive the whole Service architecture from those three forces without ever having seen K8s.

```
   Service (stable: selector, port, clusterIP)         kube-proxy (watches svc+ep)
        │ selector                                          │ rebuilds iptables nat
        ▼                                                   ▼
   endpoints controller ── writes ──► Endpoints ───►  KUBE-SERVICES ─► KUBE-SVC-<h> ─► KUBE-SEP-<h> ─► DNAT pod:port
   (Running+IP pods matching selector)  (volatile IP list)        (VIP:port)   (pick backend)   (rewrite dest)
```

## 1. Schema: `Service` + `Endpoints` as two objects (`service.rs`, `endpoints.rs`)

```rust
pub struct ServiceSpec {
    pub selector: BTreeMap<String, String>,   // pods matching ALL of these are backends
    pub port: u16,                            // the VIP listens here (clients hit this)
    pub target_port: u16,                     // forward to this port on the pod
    #[serde(rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,           // VIP, assigned by apiserver; None until then
}
// Endpoints is a SEPARATE top-level resource — the derived membership list:
pub struct Endpoints { /* metadata, */ pub addresses: Vec<EndpointAddress> } // { ip, port }
```
> **🦀 Rust pattern — `#[serde(rename = "clusterIP")]` vs `rename_all = "camelCase"`.** The struct uses `camelCase` globally (gives `targetPort` ✓), but `cluster_ip` would camelCase to `clusterIp` — wrong; K8s wire key is `clusterIP` (capital P). A per-field `rename` *overrides* the container rule for the one field that doesn't fit the pattern. Cue: *reach for a field-level `rename` when one field is an exception to an otherwise-correct blanket rule — don't abandon the blanket rule for a hand-written name on every field.*
> **🔧 Implementation choice — `BTreeMap`, not `HashMap`, for the selector.** Deterministic iteration order → the same selector always serializes to identical bytes, which keeps the dedup-if-unchanged checks (controller §3, proxy §4) honest. A `HashMap`'s random order would produce spurious "changed" diffs. Cue: *if a map's serialized form is ever compared for equality, use a `BTreeMap`.*

## 2. apiserver assigns the ClusterIP VIP (`handlers.rs`)

On `create_service`, the apiserver mints the next VIP from `10.96.0.0/16` via a persistent `svc_clusterip_counter` — *exactly* mirroring `create_node`'s /24 assignment (5a §3).
> **🧭 Design rationale — the apiserver assigns scarce cluster-unique resources, always.** Both PodCIDRs (per node) and ClusterIPs (per service) must be globally unique. The single component that sees *all* creates of a kind, with a persistent counter, is the apiserver — so uniqueness falls out of "one assigner, one counter" with no coordination. Recognizing this is the same shape as 5a means you write *zero* new design: copy the pattern. Cue: *cluster-wide uniqueness → assign at the one chokepoint that serializes creation (the apiserver), backed by durable state.*
> **🔧 Implementation choice — the double guard `cluster_ip.is_none() && get().is_none()`.** Re-applying a Service must NOT burn a fresh VIP. The first clause skips if the *incoming* object already carries a VIP; the second skips if one is *already stored*. (A bug caught in testing: with only the first guard, `mykubectl apply` of a VIP-less manifest re-assigned every time — the stored-state check is the load-bearing one.) Cue: *an "assign on first create" path must check persisted state, not just the request, or idempotent re-apply silently leaks the resource.*

## 3. The endpoints controller (`controller/endpoints.rs` + `endpoints_manager.rs`)

A reconcile loop, keyed by Service name: list pods, keep those matching the selector that are **Running with an IP**, write the sorted IP list as the Service's `Endpoints`.
```rust
pub async fn reconcile(svc_name: &str, client: &Client) -> Result<()> {
    let Some(svc) = client.get_service(svc_name).await? else {
        // Service gone → delete its Endpoints (cascade). Then done.
        /* delete endpoints if present */ return Ok(());
    };
    let mut addresses = client.list_pods().await?.into_iter()
        .filter(|p| pod_matches(p, &svc.spec.selector))
        .filter_map(|p| backend_ip(&p).map(|ip| EndpointAddress { ip, port: svc.spec.target_port }))
        .collect::<Vec<_>>();
    addresses.sort_by(|a, b| a.ip.cmp(&b.ip));      // deterministic order
    write_endpoints(svc_name, addresses, client).await   // dedup: skip write if unchanged
}
fn backend_ip(pod: &Pod) -> Option<String> {       // Running AND has an IP, else None
    let s = pod.status.as_ref()?;
    (s.phase == PodPhase::Running).then(|| s.pod_ip.clone()).flatten()
}
```
> **🦀 Rust pattern — `filter_map` + `Option`-returning helper = "select and transform, dropping misses" in one pass.** `backend_ip` returns `Option<String>` (a pod is a backend only if Running *and* it reported an IP — a Running-but-IP-less pod would be a DNAT black hole). `filter_map` keeps the `Some`s and discards the `None`s, fusing the "is it eligible?" filter and the "extract its IP" map into one iterator step. Cue: *when a predicate and an extraction share logic ("is it valid? and if so, give me the value"), express it as one `Option`-returning fn + `filter_map`, not a `.filter().map()` that recomputes.*
> **⚙ Principle — break the feedback loop with dedup (again).** `write_endpoints` reads the current Endpoints and *returns without writing if the address list is unchanged*. Writing unconditionally would emit an Endpoints MODIFIED event → kube-proxy resync → (and if anything re-enqueued the controller) an endless churn. Same guard as the RS status-write (P3) and kubelet status push (P2 §8). Cue: *any reconcile that writes a derived object must compare-before-write, or it feeds its own watch.*
> **🧭 Design rationale — why pods carry no back-pointer to Services (and what that costs).** A Pod has `ownerReferences` to its ReplicaSet (a direct link) but *nothing* pointing at Services — because Service membership is by *label selector*, a late-bound query, not ownership. Cost: to map a Pod event → affected Services, `services_for_pod` must scan *all* Services and test each selector (no direct lookup). That's the deliberate trade of label-selection: maximal flexibility (a pod can join any service by wearing the right label, even retroactively) paid for with O(services) fan-out per pod event. Cue: *selector-based association buys loose coupling and late binding; the bill is a scan to invert it — fine at small scale, the thing real K8s adds indexes for at large scale.*

## 4. kube-proxy: the iptables planner (`kube_proxy.rs`)

The dataplane. A new root binary that watches Services + Endpoints and programs `iptables` so packets to a ClusterIP get DNAT'd to a backend pod. The heart is a **pure** planner:
```rust
pub fn plan_rules(services: &[Service], endpoints: &[Endpoints]) -> Vec<Vec<String>> {
    let mut rules = vec![args(&["-F", KUBE_SERVICES])];      // flush + rebuild from scratch
    for svc in services {
        let Some(vip) = svc.spec.cluster_ip.as_deref() else { continue };   // no VIP → skip
        let addrs = /* this service's Endpoints addresses */;
        if addrs.is_empty() { continue; }                   // no backends → no DNAT
        let n = addrs.len();
        for (i, ep) in addrs.iter().enumerate() {
            // SEP chain: mark-for-masq, then DNAT to the pod
            rules.push(args(&["-A", &sep, "-j", KUBE_MARK_MASQ]));
            rules.push(args(&["-A", &sep, "-p","tcp","-j","DNAT","--to-destination", &fmt(ep)]));
            // SVC chain: probabilistic jump to each SEP
            if i < n - 1 {
                let prob = format!("{:.5}", 1.0 / (n - i) as f64);          // 1/N, 1/(N-1), ...
                rules.push(args(&["-A", &svc, "-m","statistic","--mode","random","--probability",&prob,"-j",&sep]));
            } else {
                rules.push(args(&["-A", &svc, "-j", &sep]));                // last = catch-all
            }
        }
        rules.push(args(&["-A", KUBE_SERVICES, "-d", vip, "--dport", &port, "-j", &svc]));
    }
    rules
}
```
> **🧭 Design rationale — declining probabilities `1/(N-i)`, not uniform `1/N`.** iptables rules are evaluated *in order*; a `--probability p` rule fires with chance `p`, else falls through to the next. To get a uniform 1/N split across N backends from sequential coin-flips, each rule must be conditioned on "we got here, so the earlier ones didn't fire." The math: 1st rule 1/N, 2nd 1/(N-1) (of the remaining N-1), … last is an unconditional catch-all (probability 1). Multiply it out and every backend gets exactly 1/N. Cue: *when you turn a parallel "pick one of N uniformly" into a sequential fall-through chain, the per-step probability must be conditional — `1/(remaining)`, not `1/N` — and the final step is deterministic.*
> **🔧 Implementation choice — rebuild the entire ruleset every sync; collapse all events to one `SYNC_KEY`.** Every Service/Endpoints change enqueues the same constant key, and the worker flushes `KUBE-SERVICES` and re-plans everything. Why not surgically patch the one changed rule? Because *idempotent full-rebuild* is dramatically simpler to get right than incremental diffing of kernel state — no "which rules are stale?" bookkeeping, the desired state is always computed fresh from the API objects, and a missed event self-heals on the next sync. The cost (re-applying all rules) is negligible at this scale. Cue: *prefer "recompute the whole desired state and apply it" over "compute and apply a delta" until the full rebuild actually hurts — level-triggered beats edge-triggered for the dataplane too.*
> **🦀 Rust pattern — a pure `plan_rules(...) -> Vec<Vec<String>>` split from the `#[cfg(not(test))]` executor.** The planner returns iptables invocations *as data* (argv vectors); a separate `apply_rules` (compiled out of tests) actually shells out. So the entire load-balancing logic — chain structure, probabilities, skip-empty rules — is unit-tested with plain `assert!`s on strings, no root, no iptables. Cue: *to test code whose effect is a side-effecting command, have the logic RETURN the command as data and let a thin, separately-compiled layer execute it — "functional core, imperative shell."* (This is the same seam as `RuntimeClient` and the 5a allocator, applied to shell-outs.)
> **⚙ Gotcha — chain names are capped at 28 chars.** `KUBE-SVC-<16 hex of hash>` = 25 chars, under the limit; same `short_hash` trick as veth names (5a §4). And base chains use the `iptables -C` "check" idiom for idempotent hooking (add the jump only if not already present). Env note: the VM needed `apt-get install iptables` — it wasn't present.

## 5. mykubectl: services + endpoints

`mykubectl apply` detects `kind` (Service vs Pod vs ReplicaSet); `get svc` prints a table (NAME/CLUSTER-IP/PORT/TARGET); `get endpoints` lists the backing IPs; Service apply **preserves the ClusterIP** across re-apply (reads existing, carries the VIP forward — the client-side half of §2's stability guarantee).

## 6. e2e validation (on the VM)

See the [[#How validation works in this project|Validation catalog]] → "Phase 5b" for the full run. In short: an RS of 3 + a Service → the endpoints controller populated Endpoints with all 3 pod IPs → kube-proxy built the iptables chains (verified the `0.33333` / `0.50000` / catch-all probabilities and the DNAT targets) → **30/30 `curl 10.96.0.0:80` load-balanced across all 3 backends** (including cross-node pods) → scaling the RS 3→5 updated Endpoints and iptables live, and 20/20 curls then spread across 5. Bugs/env caught only here: iptables absent on the VM; the ClusterIP double-guard; a `run`-vs-`run_cmd` name clash (E0428); and that a correctly-scoped kube-proxy does *not* break SSH (its DNAT only matches `10.96.x` VIPs, its MASQ only mark `0x4000`).

## Phase 5b wrap — what this earned us

Services: a stable ClusterIP that load-balances across a live-updating set of backend pods, via three cooperating pieces — a two-object schema (stable `Service` / derived `Endpoints`), an endpoints **controller** (the reconcile loop, fifth instance), and **kube-proxy** programming the iptables DNAT dataplane. Phase 5 (networking) is complete: pods have real IPs (5a) *and* a stable way to reach a service of them (5b).

The transferable haul, now framed as *re-derivable moves* ([[#Engineering principles, by example|index]]): split-stable-from-volatile, derive-state-with-the-loop-you-have, separate-decision-from-dataplane, assign-scarce-resources-at-the-chokepoint, functional-core/imperative-shell (pure planner + thin executor), conditional-probability for sequential uniform choice, and (again) dedup-before-write to break feedback loops. The Rust patterns: field-level serde `rename` as a blanket-rule exception, `filter_map` for select-and-transform, and returning commands-as-data for testability.

What we did NOT build: Service types beyond ClusterIP (no NodePort/LoadBalancer), kube-proxy IPVS mode (iptables only), readiness gating of endpoints beyond phase+IP, and DNS (you hit the VIP directly, not a name).

**Phase 6 (next) is the distributed-systems track** — leader election, Raft-backed storage, admission webhooks, a real CNI plugin, or RBAC. The single-instance assumption baked into the controller-manager, scheduler, and kube-proxy (each must run exactly once) is the thread Phase 6's leader-election option would pull.

---

# Phase 6a — Raft, from scratch

The deepest dive of the project: a complete Raft consensus implementation per the paper's Figure 2 — leader election, log replication, the safety rules, persistence, an async shell, an HTTP transport, and a deterministic simulation harness that checks the paper's invariants after *every step*. **229 tests.** This section is deliberately the heaviest in the note, because Raft is famous for being graspable in outline and bewildering in its edge cases. Every "but WHY?" that came up during the build gets a worked example here.

## 0. The mental model: what problem Raft actually solves

Strip everything away and Raft answers one question: **how do N machines agree on the contents of a list, when any of them can crash and the network can drop or reorder messages?** That list is the *replicated log*. If every machine ends up with the same log and applies it in order to its own copy of some state machine (a KV store, our sled store), every machine computes the same state. Same log → same state. That's the entire game: **agree on the log, and state agreement falls out for free.**

```
   client ──propose──►  ┌─ leader ─┐      replicate      ┌─ follower ─┐
                        │ log: a b c │ ──────────────────► │ log: a b c │
                        │ KV  ↑apply │                     │ KV  ↑apply │
                        └───────────┘                      └────────────┘
        every node applies the SAME log in the SAME order → identical KV state
```

The two sub-problems Raft splits this into:
1. **Election** — exactly one node (the leader) may extend the log at a time. (Two writers = divergent logs = the thing we're trying to prevent.)
2. **Replication** — the leader gets its entries onto a *majority* of nodes before declaring them permanent ("committed").

Why a **majority**? Because any two majorities of the same cluster *overlap in at least one node*. That single overlapping node is the thread of continuity: a new leader elected by majority B must share a member with the old majority A that accepted entries — so the new leader's election can be forced (via the vote rules, §5) to go through someone who *has* the committed data. Every safety argument in Raft bottoms out at "two majorities intersect."

> **🧭 Design rationale — why this is the problem our apiserver has.** Our apiserver (P2) is one process over one sled DB — a single point of failure. Run three apiservers naively and they'd each have their own sled, instantly diverging. To run three replicas that *act like one store*, every write must become a log entry that all three agree on, applied in the same order to each sled. That's exactly the replicated log. 6a builds the agreement machine; 6b will put the apiserver's writes inside it. Reproducible takeaway: *replication without agreement = divergence; the canonical fix is to serialize all writes through one agreed-upon log — and consensus is the machinery that keeps that log identical everywhere despite crashes.*

## 1. The log and the two IDs that rule everything (`raft/log.rs`)

```rust
pub type Term = u64;       // logical clock: bumps on every election attempt
pub type LogIndex = u64;   // 1-BASED position in the log; 0 = "empty log"
pub struct LogEntry { pub term: Term, pub index: LogIndex, pub command: Vec<u8> }
```

**Concept — the term is a logical clock, and it's the key to EVERYTHING.** A term is "the reign of (at most) one leader." Every election attempt bumps it. Every message carries the sender's term, and every node enforces one universal rule: *see a higher term → adopt it and become a follower; see a lower term → the sender is stale, refuse/correct it.* This one rule is what makes crashed-and-returned old leaders harmless — the moment a deposed leader (term 3) hears from anyone in term 5, it steps down. Terms turn "who is in charge?" — a question about *time*, which distributed nodes can't agree on — into a question about *numbers*, which they can compare.

```
   term:     1111 222 4444444          ← each node tracks current_term
              ↑    ↑   ↑
            leader leader leader      (term 3 existed but elected nobody —
             A      B     C            a failed election still burns a term)
```

**Concept — an entry's identity is the `(index, term)` PAIR, not the index.** Two nodes can both have "entry 5" with *different contents* — if they got them from different leaders (different terms). The pair `(5, t2)` vs `(5, t3)` distinguishes them. Raft's **Log Matching property** says: if two logs have the same `(index, term)`, then they hold the *same command* — and so do all earlier entries. The whole repair machinery (§6) exists to enforce this.

> **🔧 Implementation choice — 1-based indices with a `(0,0)` sentinel.** The paper is 1-based; translating to 0-based Rust would invite ±1 bugs in every Figure 2 formula, so `RaftLog` stays 1-based (entry *i* lives at `entries[i-1]`). The payoff is `term_at(0) == Some(0)`: the "entry before the first entry" is the sentinel `(index 0, term 0)`, which makes the very first AppendEntries consistency check (`prev=(0,0)`) *vacuously true on every node* — no special "empty log" branch anywhere. Cue: *when implementing from a paper, keep the paper's indexing and add a sentinel for the boundary, rather than shifting the math and hoping you got every formula right.*

## 2. The two RPCs (`raft/message.rs`)

Raft needs exactly two message types (plus their replies):

| RPC | Sent by | Asks | Carries |
|---|---|---|---|
| `RequestVote` | candidate | "elect me for term T?" | candidate's term + its log's `(last_index, last_term)` |
| `AppendEntries` | leader | "append these after `(prev_index, prev_term)`" | new entries (empty = **heartbeat**) + `leader_commit` |

```rust
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,   // "I now match you through HERE"
}
```

> **🔧 Implementation choice — the ack says *what I have*, not *yes to that request*.** Our `AppendEntriesResp` carries `match_index` ("I match through index 8") instead of the paper's bare `success` bool. Why: messages can be reordered or duplicated. A bare "yes" is only meaningful if you know *which request* it answers — which means correlating requests to replies. A self-describing "I hold through 8" needs no correlation: the leader just takes `max(current, reply)` and stale/duplicate acks are harmless by construction. Cue: *in async protocols, prefer absolute-state acks ("my offset is X") over relative acks ("OK") — they're idempotent and reorder-proof, which deletes a whole class of correlation bookkeeping.* (Same trick TCP uses: ACK numbers are cumulative positions, not per-segment yes/nos.)
>
> **🦀 Rust pattern — one `Message` enum as the wire envelope.** All four message types wrap into `enum Message { RequestVote(..), RequestVoteResp(..), AppendEntries(..), AppendEntriesResp(..) }`, deriving serde. One channel/endpoint carries everything; the receiver `match`es once and the compiler guarantees no variant is forgotten. Replies are *standalone messages* sent later — never a blocking request/response — which is what lets the core stay synchronous and the transport fire-and-forget. Cue: *for a protocol over one pipe, model the message set as a single serde enum (externally tagged JSON gives you the discriminator for free), and make replies first-class messages instead of return values.*

## 3. Persistence: the three things that must survive (`raft/storage.rs`)

Figure 2 marks exactly three things "persistent": **`current_term`**, **`voted_for`**, and **the log**. Everything else (commit_index, who's leader, next/match) is safely recomputed after a restart.

**Worked example — why `voted_for` on disk is non-negotiable.** Suppose node 2 votes for candidate A in term 5, then crashes and restarts *without* remembering the vote. Candidate B asks for term 5; node 2, amnesiac, votes again. Now term 5 has **two votes from node 2** → A and B can *both* assemble "majorities" sharing node 2 → **two leaders in one term** → divergent logs. The entire single-leader guarantee rests on "one node, one vote per term," and that promise must outlive a crash:

```rust
pub fn save_hard_state(&self, hs: &HardState) -> Result<()> {
    self.tree.insert(HARD_STATE_KEY, serde_json::to_vec(hs)?)?;
    self.tree.flush()?;   // ← fsync. A buffered vote is a forgettable vote.
    Ok(())
}
```

> **🔧 Implementation choice — `flush()` (= fsync) on every hard-state and log write.** sled buffers; a crash between `insert` and flush loses the write. For most apps that's a perf knob — here it's a *correctness cliff* (the double-vote above). So every persistence call flushes before returning, and the core's effect order (§4) ensures we never *send* a message claiming state that isn't yet durable. Cue: *find the writes whose loss breaks an externally-made promise (a vote sent, an ack sent) and fsync exactly those — durability is required precisely where someone else will act on your word.*
>
> **🦀 Rust pattern — zero-padded keys make byte order = numeric order.** Log entries live at keys `log/{index:020}` (20-digit zero-padded). sled iterates keys in *byte* order: unpadded, `"log/10"` sorts before `"log/2"` and `load_log` would return entries misordered — silently corrupting the log on restart. `{:020}` makes lexicographic = numeric. (Same idiom as the apiserver's rv index.) The test that catches it uses 25 entries — you *must* cross 10 to expose the bug. Cue: *any time numbers become string keys in an ordered store, zero-pad to fixed width — and test past a digit boundary.*

`truncate_from` has its own trap: when a follower overwrites a conflicting suffix, the *disk* must also drop everything from the conflict point — entries past the rewrite that survive on disk are **zombies** that would resurrect on restart (the `truncate_from_kills_the_zombie_suffix` test pins the scenario: entries 3,4 on disk; truncate-at-3 then write a single new 3; without the truncate, old entry 4 reappears after reboot next to new entry 3).

## 4. The pure core: events in, effects out (`raft/core.rs`)

The architectural centerpiece. The entire Raft brain is one synchronous, side-effect-free function:

```rust
pub fn step(&mut self, event: Event) -> Vec<Effect>

pub enum Event {                      pub enum Effect {
    Tick,                                 Send(NodeId, Message),
    Message(NodeId, Message),             Persist,                  // hard state → disk
    Propose(Vec<u8>),                     PersistTruncate(LogIndex),
}                                         PersistEntries(Vec<LogEntry>),
                                          Apply(LogEntry),          // hand to state machine
                                          ProposeRejected { leader_hint: Option<NodeId> },
                                      }
```

No I/O, no clocks, no randomness inside. Time arrives as `Event::Tick` (the shell ticks every 50ms; the core just counts them). The network arrives as `Event::Message`. Everything the node *wants done* leaves as `Effect`s, which the shell executes **in order** — and that order IS the safety contract:

```
   step() returns:  [ Persist,  Send(2, RequestVote), Send(3, RequestVote) ]
                       ↑ MUST hit disk before ───────► these leave the machine
```

**Worked example — persist-before-send.** When a node becomes a candidate it sets `voted_for = me` and emits `[Persist, Send, Send]`. If the shell sent first and crashed before persisting, the node could wake up, forget it voted for itself, and grant its term-5 vote to someone else — the double-vote disaster from §3 again, self-inflicted. The core can't *enforce* the ordering (it's pure); it *encodes* it in the Vec, and the shell's one job is to honor it. The tests assert positions: `persist_pos < send_pos`.

> **🧭 Design rationale — how you'd arrive at the pure core (sans-I/O) architecture.** Start from the question "how do I *test* consensus?" The bugs that matter live in vanishingly rare interleavings: a vote arriving during a candidacy, a crash between persist and send, a partition healing mid-election. Real networks and timers produce those interleavings *occasionally and unreproducibly* — a test that fails once a week is worse than none. The only way to make the interleavings *choosable* is to evict everything nondeterministic (time, network, disk, randomness) from the logic. What's left is a function of `(state, event) → (state', effects)` — and now a test can hand-feed ANY sequence of events and assert on exact effects, and a simulator (§9) can interleave thousands of orderings deterministically. The async shell becomes a dumb executor with almost nothing to get wrong. Reproducible takeaway: *when correctness depends on event orderings you can't control, restructure so the logic is a pure event→effects function and the orderings become test INPUT. (This is "functional core, imperative shell" — P5b's kube-proxy planner — escalated from testing-convenience to the only viable strategy.)*
>
> **🦀 Rust pattern — leader bookkeeping lives INSIDE the `Role::Leader` variant.**
> ```rust
> pub enum Role {
>     Follower,
>     Candidate { votes: HashSet<NodeId> },
>     Leader { next_index: HashMap<NodeId, LogIndex>, match_index: HashMap<NodeId, LogIndex> },
> }
> ```
> `next_index`/`match_index` only mean anything while leading; `votes` only while campaigning. Putting them *in the variant* (rather than as `Option` fields on the struct) makes illegal states unrepresentable: you literally cannot read leader bookkeeping as a follower (the pattern match won't let you), and stepping down (`role = Follower`) *destroys* it — no "stale match_index from last reign" bug class, no manual cleanup to forget. The compiler enforces what would otherwise be a comment in Go (`// only valid when state == Leader`). Cue: *when fields are only valid in one mode, move them into that mode's enum variant — state transitions then automatically create/destroy exactly the right data.*

## 5. Elections, traced (`raft/core.rs`)

The follower's clock: every `Tick` bumps `ticks_since_reset`; hitting `election_timeout_ticks` (randomized 10–20) with no word from a leader → become candidate. Trace a 3-node election:

```
  node 1 (timeout 12)          node 2 (timeout 15)          node 3 (timeout 18)
  ─────────────────            ─────────────────            ─────────────────
  tick 12: TIMEOUT
    term 0→1, vote self
    [Persist, Send RV→2, Send RV→3]
                               receives RequestVote(t1):
                                 t1 > t0 → adopt term 1
                                 log_ok? yes; can_vote? yes
                                 → grant, reset timer
                                 [Persist, Send grant→1]
  receives grant from 2:
    votes {1,2} ≥ majority(2)
    → LEADER. next=[last+1,last+1]
    [Send heartbeat→2, Send heartbeat→3] ◄── announces + suppresses 3's timeout
                                              (3 never reaches tick 18)
```

The vote rule, straight from the code (`on_request_vote`):
```rust
let log_ok = (req.last_log_term, req.last_log_index)      // §5.4.1: lexicographic
    >= (self.log.last_term(), self.log.last_index());     //   tuple comparison
let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
let grant = req.term == self.current_term && log_ok && can_vote;
```

**Concept — the §5.4.1 "up-to-date" check is what protects committed data.** A candidate must show its log's `(last_term, last_index)`; voters refuse anyone *behind* them. Why this matters: a committed entry lives on a majority. A candidate missing it cannot win, because it would need votes from a majority — and every majority contains at least one holder of that entry, who will vote "your log is behind mine, no." The election itself filters out leaders who'd lose data. Higher last-*term* wins outright (a longer log from a dead term is still older news); same term → longer log wins.

> **🦀 Rust pattern — tuple comparison IS the lexicographic rule.** `(a_term, a_idx) >= (b_term, b_idx)` compares term first, index only on ties — exactly §5.4.1, in one expression, with no nested if/else to get backwards. Cue: *for "compare by A, then by B" rules, build tuples and use the built-in `Ord` — it's both shorter and harder to get wrong than hand-rolled precedence logic.*
>
> **🔧 Implementation choice — granting a vote resets YOUR election timer.** Subtle but load-bearing: when node 2 grants its vote, it also resets its own timeout (`ticks_since_reset = 0`). Otherwise this happens: 2 grants to 1, then 2's own timeout fires a few ticks later, 2 starts a *competing* election in term 2, and dethrones the leader it just elected. A grant means "I believe a leader is being born — I'll hold off." Same for hearing any live leader's AppendEntries. The things that reset the timer are precisely the things that mean *the cluster is functioning without me stepping up*.

**Split votes and the randomized timeout.** Two nodes timing out simultaneously → each votes for itself → neither gets a majority → term burned, try again. The fix is jittered timeouts (10–20 ticks, re-randomized by the shell on *every* new candidacy) so a retry rarely collides twice. And **the sim harness caught the real failure mode here** (§9): with *fixed* timeouts, a lagging candidate (log behind, can never win) can time out perpetually *first*, and each hopeless candidacy bumps the term — which makes everyone else `maybe_step_down` and reset, starving the node that *could* win. Liveness, not safety: no invariant fires, the cluster just never elects anyone. Re-randomizing per candidacy breaks the lockstep. (Production systems add PreVote; we documented rather than built it.)

## 6. Log replication and repair, traced (`raft/core.rs`)

The leader keeps two numbers per peer — and one mechanism serves heartbeat, replication, *and* retransmission:

```
   next_index[p]  = the next entry I'll SEND p        (optimistic, walks back on reject)
   match_index[p] = the highest entry p CONFIRMED      (pessimistic, only moves up)

   append_for(p): send entries[next_index[p]..] with prev = (next-1, term_at(next-1))
                  └── caught-up peer → empty entries = pure heartbeat
```

**The consistency check.** Every AppendEntries says: "these entries go after `(prev_index, prev_term)`." The follower checks it *holds* that exact pair (`term_at(prev_index) == prev_term`); if not, it rejects, and the leader walks `next_index` back one and re-probes. Induction: if the follower matched at `prev`, and every earlier append also matched, the logs are identical up to `prev` — appending preserves Log Matching.

**Worked example — repairing a diverged follower.** Leader (term 3) has `[t1, t1, t3, t3]`; follower diverged with `[t1, t1, t2, t2]` (entries 3–4 from a deposed term-2 leader that never committed them):

```
 leader sends: prev=(4,t3), entries=[]            follower: term_at(4)=t2 ≠ t3 → REJECT
 leader: next[f] 5→4; sends prev=(3,t3), [e4]     follower: term_at(3)=t2 ≠ t3 → REJECT
 leader: next[f] 4→3; sends prev=(2,t1), [e3,e4]  follower: term_at(2)=t1 ✓ MATCH
                                                   → entry 3: have t2, incoming t3 → CONFLICT
                                                     truncate_from(3)  [+PersistTruncate]
                                                     append [e3,e4]    [+PersistEntries]
                                                   → log now [t1, t1, t3, t3]  ✓ repaired
```

The follower's three-way scan per incoming entry (from `on_append_entries`):
```rust
match self.log.term_at(entry.index) {
    Some(t) if t == entry.term => continue,        // already have it → skip (idempotent!)
    Some(_) => {                                    // CONFLICT → truncate from here, then take
        self.log.truncate_from(entry.index);
        effects.push(Effect::PersistTruncate(entry.index));
        to_append.push(entry);
    }
    None => to_append.push(entry),                  // new territory → take
}
```
The `continue` arm is why a duplicated/retransmitted AppendEntries is **harmless**: re-delivery finds every entry already present, appends nothing, re-persists nothing, and just re-acks (pinned by `duplicate_delivery_is_idempotent`).

> **🧭 Design rationale — why the leader never asks "what do you have?"** You might expect a negotiation: leader asks, follower answers, leader sends the diff. Raft instead has the leader *assert optimistically* (`next = my last+1`) and walk back on rejection — converging on the divergence point by probing. Why this shape? It makes every message **self-contained and stateless**: an AppendEntries means the same thing no matter what was lost before it, so retransmission is trivially safe and there's no conversation state to corrupt. The cost is O(divergence) round trips — fine, because big divergences are rare. Reproducible takeaway: *in unreliable networks, prefer stateless probe-and-converge over stateful negotiation — every message idempotent and self-describing beats fewer-but-fragile round trips.*

## 7. The commit rule and Figure 8 — the hardest idea in Raft

**Committed** = "this entry can never be lost, no matter who crashes" = it's on a majority *and* the leader has declared it so. The leader advances `commit_index` to the highest `n` where a majority's `match_index ≥ n` — **but only counts entries from its OWN term**:

```rust
for n in (self.commit_index + 1..=self.log.last_index()).rev() {
    if self.log.term_at(n) != Some(self.current_term) {
        continue;            // ← the Figure 8 guard: NEVER count old-term entries
    }
    let holders = 1 + peers.filter(|p| match_index[p] >= n).count();
    if holders >= self.majority() { found = n; break; }
}
```

**Why?! The Figure 8 scenario, slowly.** The paper's most subtle trap. Suppose a new leader (term 4) finds an *old* entry (term 2, index 1) already sitting on a majority. "Majority = committed", right? **No.** Here's the disaster if it commits it:

```
   Setup: node 1's log has [idx1:t2]. Node 1 wins term 4. Peers 2,3 ack idx1.
          A rival (node 5) holds [idx1:t3] — a term-3 entry from a failed reign.

   If leader counts the t2 entry: majority holds idx1 → "commit!" → APPLIED. 💥
   Then leader 1 crashes. Node 5 runs for term 5: its last_term=3 BEATS the
   others' last_term=2 (§5.4.1!) → node 5 WINS, and its log says idx1 is the
   t3 entry → it overwrites idx1 on everyone. The "committed" t2 entry is gone
   — but we already applied it. State machines have now diverged. Game over.
```

The killer detail: node 5's *higher last term* legitimately wins elections even though the t2 entry sat on a majority — "on a majority" does NOT imply "safe" for old-term entries, because newer-term logs beat longer ones in voting. **Only own-term entries are safe to commit by counting** — when the term-4 leader replicates a term-4 entry to a majority, any future winner must (by §5.4.1) have last_term ≥ 4, and the only term-4 entries in existence came from this leader's log — so the winner provably contains everything up to that entry. And that's also how old entries DO commit: **transitively**. Commit a term-4 entry at index 2, and index 1 beneath it is implicitly committed too (the `figure8_old_term_entry_commits_only_transitively` test walks exactly this: acks on the old entry alone do nothing; one own-term entry on a majority commits both, applying `[1, 2]` in order).

> **🧭 Design rationale — the meta-lesson of Figure 8.** The seductive-but-wrong rule ("majority holds it → committed") fails because of an interaction between two *separately correct* rules: the vote rule prefers higher last-*term*, and replication can spread old entries widely. Neither rule is wrong; their composition has a hole. The fix isn't more machinery — it's *narrowing a claim* ("I only declare MY OWN entries committed") until composition is provably safe. Reproducible takeaway: *in distributed protocols, audit the INTERACTIONS between individually-sound rules — and when a claim is unsafe in general, restrict the claimant rather than adding mechanism. Also: this is why you implement consensus from a paper and not from intuition.*

## 8. The async shell (`raft/node.rs`) and transport

The shell is everything the core refused to be — and it's *boring*, by design:

```rust
loop {
    let event = tokio::select! {
        biased;
        _ = cancel.cancelled() => break,
        _ = tick.tick() => Event::Tick,                       // 50ms → Event::Tick
        Some((from, msg)) = self.inbox.recv() => Event::Message(from, msg),
        Some(p) = self.proposals.recv() => Event::Propose(p),
    };
    let effects = self.node.step(event);     // ← ALL the thinking
    /* re-randomize timeout on Follower→Candidate edge */
    self.execute(effects).await;             // ← in order: persist ⟶ send ⟶ apply
    self.leader_watch.send_if_modified(/* publish leader_hint on change */);
}
```

The channel topology around it:
```
   HTTP POST /raft/message ──► inbox(mpsc) ──┐
   ticker 50ms ─────────────────────────────►│ select! → step() → effects
   client proposals ───────► proposals(mpsc)─┘            │
                                                          ├─ storage (sled, fsync)
   state machine ◄─(mpsc, backpressure)── Apply ◄─────────┤
   anyone curious ◄─(watch)── leader_hint ◄───────────────┘
```

> **🦀 Rust pattern — the right channel for each job.** Three different tokio primitives, each matched to its semantics: **`mpsc`** for the inbox/proposals (queued work, many producers, must not be lost once accepted); **`mpsc` with `.send().await`** for Apply — *backpressure*: if the state machine falls behind, the shell awaits rather than buffering unboundedly; **`watch`** for `leader_hint` — observers only care about the *latest* value, not the history, and `send_if_modified` dedups so watchers wake only on real changes (the same latest-value-only semantics as a K8s status). Cue: *pick channels by data semantics — queue of jobs = mpsc; latest-value-wins = watch; broadcast of events = broadcast — rather than defaulting to mpsc everywhere and reimplementing the others badly.*
>
> **🔧 Implementation choice — no `rand` crate: a 10-line xorshift in the shell.** The only randomness Raft needs is election-timeout jitter. Pulling in `rand` for that is a whole dependency tree for one `% 10`; a seeded xorshift (`x ^= x<<13; x ^= x>>7; x ^= x<<17`) is plenty — and *seedable*, which keeps even the shell deterministic when tests want it (`0xC0FFEE + id`). The core never sees randomness at all (it takes the timeout as a parameter), preserving its purity. Cue: *for non-cryptographic jitter, a seeded xorshift beats a dependency; and keep randomness OUT of the logic — generate it at the edge, pass it in as data.*

The `Transport` trait is one fire-and-forget method — `fn send(&self, to: NodeId, msg: Message)` — with the contract that **losing a message is fine** (Raft's heartbeat retransmission and election retries are the recovery story; reliability machinery in the transport would be redundant). `HttpTransport` spawns a task per send and ignores the result; replies arrive later as ordinary inbox messages via the peer's own POST.

## 9. The simulation harness (`raft/sim_tests.rs`)

The payoff of the pure core: a **deterministic cluster simulator** — real `RaftNode`s, real `RaftStorage`s (sled), and a fake everything-else: messages go into a FIFO `VecDeque` instead of a network; partitions are a `HashSet<(from,to)>` of blocked links; crash = mark dead, **restart = rebuild the node from its sled storage** (the true recovery path). After *every single step*, the paper's three safety invariants are checked:

```
   Election Safety     at most one leader per term, ever          (leaders_by_term map)
   Log Matching        same (index,term) on two nodes → same command
   State-Machine Safety  every node's apply stream is a prefix of every other's
```

Seven scenarios run on this rig — election + stability, split-vote resolution, **committed entries survive leader crash**, stale leader steps down + uncommitted entries vanish, restart-recovers-from-sled + catches up, 5-node cluster survives 2 crashes but halts at 3 (majority arithmetic made visible), and duplicate-delivery harmlessness.

> **⚙ Principle — check invariants continuously, not outcomes at the end.** The harness doesn't just assert the final state looks right; it validates all three safety properties after *every step of every scenario*. An invariant violated transiently-then-masked would pass an end-state assertion but is still a real bug (something observed the broken state). Continuous checking turns every scenario into hundreds of assertions and catches the violation at the exact step it appears. Cue: *when a system claims invariants, assert them at every state transition in tests — "ends correct" is far weaker than "never wrong."*
>
> **🧭 Design rationale — the sim caught a bug class the unit tests structurally couldn't.** Every unit test in core.rs drives ONE node and asserts its effects. The starving-candidate liveness failure (§5) only exists in the *interaction* of multiple nodes' timers — node A's hopeless candidacies resetting node B's progress, forever. No single-node test can express that; the sim found it within seconds of existing, deterministically. Reproducible takeaway: *unit tests verify components, simulators verify emergent behavior; for distributed algorithms the interesting bugs are ALL emergent, so the simulator isn't a nice-to-have — it's the primary test.* (Also note: the sim found a LIVENESS bug while all SAFETY invariants held — the two failure classes are independent, and you must watch for both.)

## 10. e2e: three processes, one kill -9 (`bin/raft-demo.rs`)

The demo binary assembles the real thing: one `RaftShell` per process, `HttpTransport`, axum endpoint for the inbox, a proposer that submits a numbered command every 2s **gated on `leader_watch`** (only the current leader proposes), and an APPLIED printer. On the VM:

```
  terminal 1-3:  raft-demo --id N --listen 127.0.0.1:700N --peers "..." --db /tmp/raft-demo-N
  → node 3 elected (term 1); all three print APPLIED from-3-#1 … #10 in lockstep
  kill -9 <node 3>
  → node 2 times out, wins term 2; proposal stream CONTINUES (applied #11…#16)
    — the proposer is leader-gated, so the new leader picks up proposing automatically
  restart node 3
  → "recovered id=3 term=1 last=10" from sled → catches up to #25
  → all three APPLIED sequences byte-identical: from-3-#1..10, from-2-#1..15
                                                ↑ the leadership handoff, visible in the log
```

What only this run could prove: real fsync'd recovery across a process kill, real HTTP message loss tolerance, and the leader-gated proposer following the `watch` channel across a failover. (Validation catalog has the full entry.)

## Phase 6a wrap — what this earned us

A from-scratch, paper-faithful Raft: elections with the §5.4.1 log check, replication with probe-and-converge repair, the Figure-8-safe commit rule, fsync'd persistence of exactly the three things that must survive, a pure event→effects core under an async shell, and a deterministic simulator enforcing the paper's invariants at every step — 229 tests, plus a 3-process e2e surviving `kill -9` with zero committed-entry loss.

The transferable haul: **pure-core/imperative-shell as the only viable testing strategy for interleaving-sensitive logic**; absolute-state acks over correlated yes/nos; persist-before-promise (and fsync exactly where promises are made); logical clocks (terms) turning time disputes into number comparisons; majority-intersection as the bedrock safety argument; narrow-the-claim when sound rules compose unsafely (Figure 8); invariant-checking-every-step; and simulators for emergent behavior. Rust-wise: state-dependent data inside enum variants, tuple-`Ord` for lexicographic rules, channel selection by semantics (mpsc/watch/backpressure), and seeded xorshift over a `rand` dependency.

What we did NOT build (scoped out, documented): log compaction/snapshots (the log grows forever), cluster membership changes (fixed 3 nodes), PreVote (we re-randomize timeouts instead), and read-index/lease reads (reads aren't linearizable through the log).

**Phase 6b (next): put the apiserver on top of it.** Writes become `StoreCommand` log entries; three apiserver replicas apply the same log to their own sleds (deterministic rv across replicas — same-log-same-state in action); followers redirect writes to the leader (307 + the leader hint); `--standalone` keeps the single-node path. The agreement machine is built; 6b makes it carry real cargo.
