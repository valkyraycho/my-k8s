//! Host-local IP allocator — the CNI `host-local` IPAM plugin in miniature.
//! Hands out pod IPs from ONE /24 slice of the cluster pod CIDR. Each kubelet
//! owns a disjoint /24 (the apiserver assigns it as `node.spec.podCIDR`), so
//! two kubelets never hand out the same address without coordinating — the
//! partition IS the coordination.

use std::{collections::BTreeSet, net::Ipv4Addr};

use anyhow::{Result, bail};

// Host octets we never hand out, in every slice: .0 = network, .1 = gateway,
// .255 = broadcast. (.1 is only truly the bridge gateway in the slice that
// contains it; skipping it uniformly costs one address per node but keeps the
// allocator dead simple.) Usable range is .2 ..= .254 → 253 addresses per /24.
const FIRST_HOST: u16 = 2;
const BROADCAST_HOST: u16 = 255;

/// Allocates IPv4 addresses from a single /24. State is in-memory only; the
/// kubelet rebuilds it on restart by `reserve()`-ing IPs read back from the
/// apiserver (see the reconciler's recovery path).
pub struct IpAllocator {
    /// First three octets of the /24, e.g. [10, 244, 1] for 10.244.1.0/24.
    prefix: [u8; 3],
    /// Cursor: next 4th-octet to try when no released address is waiting.
    next: u16,
    /// Freed addresses, reused before advancing the cursor (keeps the pool
    /// compact). BTreeSet → deterministic smallest-first reuse.
    released: BTreeSet<u8>,
    /// Currently handed-out addresses. Lets `reserve` claim a host out of order
    /// (e.g. recovered .50 while the cursor is at .2) and have `allocate` skip
    /// it when the cursor eventually reaches it.
    allocated: BTreeSet<u8>,
}

impl IpAllocator {
    pub fn from_cidr(cidr: &str) -> Result<Self> {
        let (addr, mask) = cidr
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("missing /prefix in {cidr:?}"))?;

        if mask != "24" {
            return Err(anyhow::anyhow!(
                "only /24 CIDRs supported, got /{mask} in {cidr:?}"
            ));
        }

        let ip: Ipv4Addr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("bad CIDR address {addr:?}: {e}"))?;

        let o = ip.octets();
        if o[3] != 0 {
            bail!("/24 base must end in .0, got {addr}");
        }

        Ok(Self {
            prefix: [o[0], o[1], o[2]],
            next: FIRST_HOST,
            released: BTreeSet::new(),
            allocated: BTreeSet::new(),
        })
    }

    /// Hand out the next free address. Reuses a released one first, else
    /// advances the cursor (skipping anything reserved out-of-order). Errors
    /// when the /24 is exhausted.
    pub fn allocate(&mut self) -> Result<Ipv4Addr> {
        if let Some(&host) = self.released.iter().next() {
            self.released.remove(&host);
            self.allocated.insert(host);
            return Ok(self.make_ip(host));
        }
        while self.next < BROADCAST_HOST {
            let host = self.next as u8;
            self.next += 1;
            if !self.allocated.contains(&host) {
                self.allocated.insert(host);
                return Ok(self.make_ip(host));
            }
        }
        bail!(
            "IP pool exhausted for {}.{}.{}.0/24",
            self.prefix[0],
            self.prefix[1],
            self.prefix[2]
        );
    }

    /// Mark a specific address in-use — the recovery path: a surviving pod's IP
    /// read back from the apiserver, reserved BEFORE any fresh `allocate` so a
    /// restarted kubelet can't re-hand-out a live pod's address. Idempotent.
    pub fn reserve(&mut self, ip: Ipv4Addr) -> Result<()> {
        let host = self.host_in_range(ip)?;
        self.released.remove(&host);
        self.allocated.insert(host);
        Ok(())
    }

    /// Return an address to the pool for reuse. No-op if it wasn't allocated
    /// (or isn't in this slice) — release is best-effort teardown.
    pub fn release(&mut self, ip: Ipv4Addr) {
        if let Ok(host) = self.host_in_range(ip)
            && self.allocated.remove(&host)
        {
            self.released.insert(host);
        }
    }
    fn make_ip(&self, host: u8) -> Ipv4Addr {
        Ipv4Addr::new(self.prefix[0], self.prefix[1], self.prefix[2], host)
    }

    /// Validate `ip` belongs to this /24 and isn't a reserved host, returning
    /// its 4th octet.
    fn host_in_range(&self, ip: Ipv4Addr) -> Result<u8> {
        let o = ip.octets();
        if o[0..3] != self.prefix {
            bail!(
                "{ip} not in {}.{}.{}.0/24",
                self.prefix[0],
                self.prefix[1],
                self.prefix[2]
            );
        }
        match o[3] {
            0 | 1 | 255 => bail!("{ip} is a reserved host (.0/.1/.255)"),
            host => Ok(host),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn alloc(a: &mut IpAllocator) -> Ipv4Addr {
        a.allocate().expect("allocate should succeed")
    }

    #[test]
    fn from_cidr_accepts_valid_and_rejects_malformed() {
        assert!(IpAllocator::from_cidr("10.244.1.0/24").is_ok());
        // Wrong mask, non-.0 base, missing slash, garbage → all errors.
        assert!(IpAllocator::from_cidr("10.244.1.0/16").is_err());
        assert!(IpAllocator::from_cidr("10.244.1.5/24").is_err());
        assert!(IpAllocator::from_cidr("10.244.1.0").is_err());
        assert!(IpAllocator::from_cidr("not-an-ip/24").is_err());
    }

    #[test]
    fn allocate_is_sequential_starting_at_dot_two() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        // .0 (network) and .1 (gateway) are skipped — first handout is .2.
        assert_eq!(alloc(&mut a), ip("10.244.1.2"));
        assert_eq!(alloc(&mut a), ip("10.244.1.3"));
        assert_eq!(alloc(&mut a), ip("10.244.1.4"));
    }

    #[test]
    fn reserve_blocks_that_address_from_allocation() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        // Reserve a host AHEAD of the cursor (recovery of a survivor at .3).
        a.reserve(ip("10.244.1.3")).unwrap();
        // Sequential allocation must step over the reserved .3.
        assert_eq!(alloc(&mut a), ip("10.244.1.2"));
        assert_eq!(alloc(&mut a), ip("10.244.1.4"));
        assert!(
            std::iter::repeat_with(|| alloc(&mut a))
                .take(20)
                .all(|got| got != ip("10.244.1.3")),
            "reserved .3 must never be handed out",
        );
    }

    #[test]
    fn released_addresses_are_reused_before_advancing() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        let _two = alloc(&mut a); // .2
        let three = alloc(&mut a); // .3
        let _four = alloc(&mut a); // .4

        a.release(three); // .3 back to the pool
        // Reuse-first: the next allocation returns .3, not .5.
        assert_eq!(alloc(&mut a), ip("10.244.1.3"));
        assert_eq!(alloc(&mut a), ip("10.244.1.5"));
    }

    #[test]
    fn release_of_unallocated_is_a_noop() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        // Releasing an address we never handed out must NOT seed the reuse bin;
        // the next allocate still starts at .2 (not the released .9).
        a.release(ip("10.244.1.9"));
        assert_eq!(alloc(&mut a), ip("10.244.1.2"));
    }

    #[test]
    fn reserve_rejects_out_of_range_and_reserved_hosts() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        assert!(a.reserve(ip("10.244.2.5")).is_err(), "different /24");
        assert!(a.reserve(ip("10.244.1.0")).is_err(), ".0 network");
        assert!(a.reserve(ip("10.244.1.1")).is_err(), ".1 gateway");
        assert!(a.reserve(ip("10.244.1.255")).is_err(), ".255 broadcast");
    }

    #[test]
    fn pool_exhausts_after_253_addresses() {
        let mut a = IpAllocator::from_cidr("10.244.1.0/24").unwrap();
        // Usable hosts are .2 ..= .254 → exactly 253 addresses.
        for _ in 0..253 {
            a.allocate().expect("within capacity");
        }
        let err = a.allocate().expect_err("254th must fail");
        assert!(err.to_string().contains("exhausted"), "got: {err}");
    }
}
