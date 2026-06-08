use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::client::Client;
use crate::controller::workqueue::{RateLimiter, WorkQueue, backoff_for};
use crate::endpoints::Endpoints;
use crate::service::Service;

const RESYNC_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

const SYNC_KEY: &str = "sync";

const KUBE_SERVICES: &str = "KUBE-SERVICES";
const KUBE_MARK_MASQ: &str = "KUBE-MARK-MASQ";

/// Entry point: set up base chains, then run the informers + resync + worker
/// until `cancel` fires. All sync triggers collapse to one SYNC_KEY — we always
/// rebuild the entire ruleset, so which Service changed doesn't matter.
pub async fn run(client: Arc<Client>, cancel: CancellationToken) {
    let queue = WorkQueue::new();
    info!("kube-proxy started");

    if let Err(e) = ensure_base_chains() {
        error!(error = ?e, "failed to set up base iptables chains");
    }

    let tasks = vec![
        tokio::spawn(service_informer(
            client.clone(),
            queue.clone(),
            cancel.clone(),
        )),
        tokio::spawn(endpoints_informer(
            client.clone(),
            queue.clone(),
            cancel.clone(),
        )),
        tokio::spawn(resync_loop(queue.clone(), cancel.clone())),
        tokio::spawn(worker_loop(client.clone(), queue.clone(), cancel.clone())),
    ];
    for t in tasks {
        let _ = t.await;
    }
    info!("kube-proxy stopped");
}

async fn service_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_services(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                                    _ = cancel.cancelled() => return,
                                    ev = stream.next() => match ev {
                                        Some(Ok(_)) => queue.add(SYNC_KEY.to_string()),
                                        Some(Err(e)) => { warn!(error = ?e, "service watch error;
                reconnecting"); break; }
                                        None => { warn!("service watch closed; reconnecting");
                break; }
                                    }
                                }
            },
            Err(e) => warn!(error = ?e, "service watch open failed; retrying"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

async fn endpoints_informer(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        match client.watch_endpoints(Some("0")).await {
            Ok(mut stream) => loop {
                tokio::select! {
                                    _ = cancel.cancelled() => return,
                                    ev = stream.next() => match ev {
                                        Some(Ok(_)) => queue.add(SYNC_KEY.to_string()),
                                        Some(Err(e)) => { warn!(error = ?e, "endpoints watch error; reconnecting"); break; }
                                        None => { warn!("endpoints watch closed; reconnecting");
                break; }
                                    }
                                }
            },
            Err(e) => warn!(error = ?e, "endpoints watch open failed; retrying"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

async fn resync_loop(queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let mut tick = interval(RESYNC_INTERVAL);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => queue.add(SYNC_KEY.to_string()),
        }
    }
}

async fn worker_loop(client: Arc<Client>, queue: Arc<WorkQueue>, cancel: CancellationToken) {
    let rl = RateLimiter::new();
    loop {
        let key = tokio::select! {
            _ = cancel.cancelled() => return,
            k = queue.get() => k,
        };
        match sync(&client).await {
            Ok(()) => {
                rl.forget(&key);
                queue.done(&key);
            }
            Err(e) => {
                let attempt = rl.failure(&key);
                let delay = backoff_for(attempt);
                error!(error = ?e, attempt, "iptables sync failed; retrying after backoff");
                queue.done(&key);
                queue.add_after(key, delay);
            }
        }
    }
}

async fn sync(client: &Client) -> anyhow::Result<()> {
    let services = client.list_services().await?;
    let endpoints = client.list_endpoints().await?;
    let rules = plan_rules(&services, &endpoints);
    apply_rules(&rules)?;
    info!(services = services.len(), "iptables synced");
    Ok(())
}

pub fn plan_rules(services: &[Service], endpoints: &[Endpoints]) -> Vec<Vec<String>> {
    let mut rules: Vec<Vec<String>> = Vec::new();

    // 1. Flush our top-level chain so we rebuild from scratch each sync.
    rules.push(args(&["-F", KUBE_SERVICES]));

    for svc in services {
        let Some(cluster_ip) = svc.spec.cluster_ip.as_deref() else {
            continue; // no VIP yet → nothing to program
        };
        // Find this service's endpoints (same name).
        let eps = endpoints
            .iter()
            .find(|e| e.metadata.name == svc.metadata.name);
        let addrs = eps.map(|e| e.addresses.as_slice()).unwrap_or(&[]);
        if addrs.is_empty() {
            continue; // no backends → no DNAT (traffic to the VIP just fails)
        }

        let svc_chain = svc_chain_name(&svc.metadata.name);
        // (Re)create + flush the per-service chain.
        rules.push(args(&["-N", &svc_chain])); // create (tolerated if exists at apply)
        rules.push(args(&["-F", &svc_chain]));

        let n = addrs.len();
        for (i, ep) in addrs.iter().enumerate() {
            let sep_chain = sep_chain_name(&svc.metadata.name, &ep.ip, ep.port);
            rules.push(args(&["-N", &sep_chain]));
            rules.push(args(&["-F", &sep_chain]));
            // SEP: mark-for-masquerade, then DNAT to the pod.
            rules.push(args(&["-A", &sep_chain, "-j", KUBE_MARK_MASQ]));
            rules.push(args(&[
                "-A",
                &sep_chain,
                "-p",
                "tcp",
                "-j",
                "DNAT",
                "--to-destination",
                &format!("{}:{}", ep.ip, ep.port),
            ]));

            // SVC: jump to this SEP. Declining probability for all but the last.
            if i < n - 1 {
                let prob = format!("{:.5}", 1.0 / (n - i) as f64);
                rules.push(args(&[
                    "-A",
                    &svc_chain,
                    "-m",
                    "statistic",
                    "--mode",
                    "random",
                    "--probability",
                    &prob,
                    "-j",
                    &sep_chain,
                ]));
            } else {
                rules.push(args(&["-A", &svc_chain, "-j", &sep_chain]));
            }
        }

        // SERVICES: clusterIP:port → the per-service chain.
        rules.push(args(&[
            "-A",
            KUBE_SERVICES,
            "-d",
            cluster_ip,
            "-p",
            "tcp",
            "--dport",
            &svc.spec.port.to_string(),
            "-j",
            &svc_chain,
        ]));
    }

    rules
}

fn args(a: &[&str]) -> Vec<String> {
    a.iter().map(|s| s.to_string()).collect()
}

/// Chain names must be <= 28 chars (iptables limit). Hash the inputs to a short,
/// stable suffix — deterministic so re-syncs target the same chains.
fn svc_chain_name(svc: &str) -> String {
    format!("KUBE-SVC-{}", short_hash(svc))
}
fn sep_chain_name(svc: &str, ip: &str, port: u16) -> String {
    format!("KUBE-SEP-{}", short_hash(&format!("{svc}/{ip}:{port}")))
}
fn short_hash(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    // 16 hex chars; with the "KUBE-SEP-" prefix (9) → 25, under the 28 limit.
    format!("{:016X}", h.finish())
}

// ---- everything below shells out; compiled out of tests ----

#[cfg(not(test))]
fn ensure_base_chains() -> anyhow::Result<()> {
    // Our top-level chain + the mark-masq helper, and the masquerade rule.
    run_tolerate(
        &["iptables", "-t", "nat", "-N", KUBE_SERVICES],
        "Chain already exists",
    );
    run_tolerate(
        &["iptables", "-t", "nat", "-N", KUBE_MARK_MASQ],
        "Chain already exists",
    );
    // MARK packets (set 0x4000) so POSTROUTING can masquerade them.
    run_tolerate(
        &[
            "iptables",
            "-t",
            "nat",
            "-A",
            KUBE_MARK_MASQ,
            "-j",
            "MARK",
            "--set-xmark",
            "0x4000/0x4000",
        ],
        "",
    );
    // Hook KUBE-SERVICES from PREROUTING + OUTPUT (idempotent via -C check).
    ensure_jump("PREROUTING")?;
    ensure_jump("OUTPUT")?;
    // Masquerade marked packets in POSTROUTING.
    ensure_masquerade()?;
    Ok(())
}

#[cfg(not(test))]
fn ensure_jump(parent: &str) -> anyhow::Result<()> {
    let check = std::process::Command::new("iptables")
        .args(["-t", "nat", "-C", parent, "-j", KUBE_SERVICES])
        .output()?;
    if !check.status.success() {
        run_cmd(&["iptables", "-t", "nat", "-A", parent, "-j", KUBE_SERVICES])?;
    }
    Ok(())
}

#[cfg(not(test))]
fn ensure_masquerade() -> anyhow::Result<()> {
    let check = std::process::Command::new("iptables")
        .args([
            "-t",
            "nat",
            "-C",
            "POSTROUTING",
            "-m",
            "mark",
            "--mark",
            "0x4000/0x4000",
            "-j",
            "MASQUERADE",
        ])
        .output()?;
    if !check.status.success() {
        run_cmd(&[
            "iptables",
            "-t",
            "nat",
            "-A",
            "POSTROUTING",
            "-m",
            "mark",
            "--mark",
            "0x4000/0x4000",
            "-j",
            "MASQUERADE",
        ])?;
    }
    Ok(())
}

/// Apply the planned ruleset. Each line runs in the nat table. `-N` (create
/// chain) is tolerated when the chain already exists; everything else must
/// succeed.
#[cfg(not(test))]
fn apply_rules(rules: &[Vec<String>]) -> anyhow::Result<()> {
    for rule in rules {
        let mut argv = vec!["iptables".to_string(), "-t".into(), "nat".into()];
        argv.extend(rule.iter().cloned());
        let argv_ref: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        if rule.first().map(|s| s == "-N").unwrap_or(false) {
            run_tolerate(&argv_ref, "Chain already exists");
        } else {
            run_cmd(&argv_ref)?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn ensure_base_chains() -> anyhow::Result<()> {
    Ok(())
}
#[cfg(test)]
fn apply_rules(_rules: &[Vec<String>]) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(not(test))]
fn run_cmd(args: &[&str]) -> anyhow::Result<()> {
    let output = std::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        .map_err(|e| anyhow::anyhow!("running {args:?}: {e}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "command {args:?} failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(())
}

#[cfg(not(test))]
fn run_tolerate(args: &[&str], tolerate: &str) {
    if let Ok(output) = std::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        && !output.status.success()
    {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !tolerate.is_empty() && !stderr.contains(tolerate) {
            warn!(?args, stderr = %stderr.trim(), "iptables command failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::EndpointAddress;
    use crate::meta::ObjectMeta;
    use crate::service::ServiceSpec;
    use std::collections::BTreeMap;

    fn svc(name: &str, cluster_ip: &str, port: u16, target_port: u16) -> Service {
        Service {
            api_version: "v1".into(),
            kind: "Service".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: ServiceSpec {
                selector: BTreeMap::new(),
                port,
                target_port,
                cluster_ip: Some(cluster_ip.into()),
            },
        }
    }

    fn eps(name: &str, ips: &[&str], port: u16) -> Endpoints {
        Endpoints {
            api_version: "v1".into(),
            kind: "Endpoints".into(),
            metadata: ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            addresses: ips
                .iter()
                .map(|ip| EndpointAddress {
                    ip: (*ip).into(),
                    port,
                })
                .collect(),
        }
    }

    /// Render each argv line back to a single space-joined string for easy
    /// substring assertions.
    fn lines(rules: &[Vec<String>]) -> Vec<String> {
        rules.iter().map(|r| r.join(" ")).collect()
    }

    #[test]
    fn always_flushes_kube_services_first() {
        let rules = plan_rules(&[], &[]);
        assert_eq!(rules[0], vec!["-F", "KUBE-SERVICES"]);
    }

    #[test]
    fn service_with_no_endpoints_is_skipped() {
        let rules = plan_rules(&[svc("web", "10.96.0.1", 80, 8080)], &[]);
        // Only the top-level flush — no SVC/SEP chains, no DNAT.
        let text = lines(&rules).join("\n");
        assert!(!text.contains("KUBE-SVC-"), "got: {text}");
        assert!(!text.contains("DNAT"), "got: {text}");
    }

    #[test]
    fn service_without_clusterip_is_skipped() {
        let mut s = svc("web", "10.96.0.1", 80, 8080);
        s.spec.cluster_ip = None;
        let rules = plan_rules(&[s], &[eps("web", &["10.244.0.2"], 8080)]);
        let text = lines(&rules).join("\n");
        assert!(!text.contains("KUBE-SVC-"), "no VIP → no rules: {text}");
    }

    #[test]
    fn single_endpoint_uses_catch_all_dnat_no_statistic() {
        let rules = plan_rules(
            &[svc("web", "10.96.0.1", 80, 8080)],
            &[eps("web", &["10.244.0.2"], 8080)],
        );
        let text = lines(&rules);
        let joined = text.join("\n");

        // The SEP DNATs to the pod's ip:targetPort.
        assert!(
            joined.contains("DNAT --to-destination 10.244.0.2:8080"),
            "got: {joined}"
        );
        // The single SEP is marked for masquerade.
        assert!(joined.contains("-j KUBE-MARK-MASQ"), "got: {joined}");
        // One backend → NO statistic rule (the lone jump is the catch-all).
        assert!(
            !joined.contains("statistic"),
            "single endpoint needs no probability: {joined}"
        );
        // KUBE-SERVICES routes the VIP:port to the per-service chain.
        assert!(
            text.iter().any(|l| l.contains("-A KUBE-SERVICES")
                && l.contains("-d 10.96.0.1")
                && l.contains("--dport 80")
                && l.contains("-j KUBE-SVC-")),
            "got: {joined}"
        );
    }

    #[test]
    fn three_endpoints_get_declining_probabilities_and_catch_all() {
        let rules = plan_rules(
            &[svc("web", "10.96.0.1", 80, 8080)],
            &[eps("web", &["10.244.0.2", "10.244.0.3", "10.244.0.4"], 8080)],
        );
        let joined = lines(&rules).join("\n");

        // Declining probabilities 1/3, 1/2 for the first two; last is catch-all.
        assert!(
            joined.contains("--probability 0.33333"),
            "first endpoint should be 1/3: {joined}"
        );
        assert!(
            joined.contains("--probability 0.50000"),
            "second endpoint should be 1/2: {joined}"
        );
        // Exactly two statistic rules for three endpoints (the 3rd is catch-all).
        let stat_count = lines(&rules)
            .iter()
            .filter(|l| l.contains("statistic"))
            .count();
        assert_eq!(stat_count, 2, "N endpoints → N-1 statistic rules");

        // All three pods get a DNAT.
        for ip in ["10.244.0.2", "10.244.0.3", "10.244.0.4"] {
            assert!(
                joined.contains(&format!("DNAT --to-destination {ip}:8080")),
                "missing DNAT for {ip}: {joined}"
            );
        }
    }

    #[test]
    fn chain_names_stay_within_iptables_limit() {
        // 28-char iptables chain-name cap; ours are prefix(9) + 16 hex = 25.
        let s = svc_chain_name("a-very-long-service-name-that-would-overflow");
        let e = sep_chain_name("a-very-long-service-name", "10.244.255.254", 65535);
        assert!(s.len() <= 28, "svc chain {s:?} too long ({})", s.len());
        assert!(e.len() <= 28, "sep chain {e:?} too long ({})", e.len());
        // Deterministic: same inputs → same name (so re-syncs target same chains).
        assert_eq!(s, svc_chain_name("a-very-long-service-name-that-would-overflow"));
    }
}
