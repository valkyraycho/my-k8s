//! Durable Raft state. Three things MUST survive a crash (Figure 2's
//! "persistent state"): current_term, voted_for, the log. Everything else is
//! safely volatile. Lives in its own sled tree — one per Raft node.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::raft::log::{LogEntry, LogIndex, NodeId, Term};

const HARD_STATE_KEY: &[u8] = b"hard_state";
const LOG_KEY_PREFIX: &str = "log/";

#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

pub struct RaftStorage {
    tree: sled::Tree,
}

impl RaftStorage {
    pub fn open(db: &sled::Db) -> Result<Self> {
        let tree = db.open_tree("raft").context("open raft tree")?;
        Ok(Self { tree })
    }

    pub fn load_hard_state(&self) -> Result<HardState> {
        match self.tree.get(HARD_STATE_KEY)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(HardState::default()),
        }
    }

    pub fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        self.tree
            .insert(HARD_STATE_KEY, serde_json::to_vec(hs)?)
            .context("save hard state")?;
        // flush = fsync. A buffered vote is a vote you can forget after a
        // crash — and a re-vote in the same term means two leaders.
        self.tree.flush().context("flush hard state")?;
        Ok(())
    }

    // Zero-padded so byte order == numeric order ("log/…10" after "log/…2"
    // would corrupt load_log's ordering). Same idiom as the rv_index tree.
    fn log_key(index: LogIndex) -> Vec<u8> {
        format!("{LOG_KEY_PREFIX}{index:020}").into_bytes()
    }

    pub fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            self.tree
                .insert(Self::log_key(entry.index), serde_json::to_vec(entry)?)
                .context("append log entry")?;
        }
        self.tree.flush().context("flush log entries")?;
        Ok(())
    }

    /// Remove every entry at `from` and beyond — kills the zombie suffix a
    /// pure overwrite would leave behind (stale entries past the rewrite
    /// would resurrect on restart). Collect-then-remove: don't mutate while
    /// iterating.
    pub fn truncate_from(&self, from: LogIndex) -> Result<()> {
        let doomed: Vec<sled::IVec> = self
            .tree
            .scan_prefix(LOG_KEY_PREFIX)
            .keys()
            .filter_map(|k| k.ok())
            .filter(|k| k.as_ref() >= Self::log_key(from.max(1)).as_slice())
            .collect();
        for key in doomed {
            self.tree.remove(key)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    pub fn load_log(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();
        for item in self.tree.scan_prefix(LOG_KEY_PREFIX) {
            let (_, value) = item?;
            entries.push(serde_json::from_slice(&value)?);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_storage() -> (sled::Db, RaftStorage) {
        let db = sled::Config::default()
            .temporary(true)
            .open()
            .expect("temp db");
        let storage = RaftStorage::open(&db).expect("open storage");
        (db, storage)
    }

    fn entry(index: LogIndex, term: Term) -> LogEntry {
        LogEntry {
            term,
            index,
            command: format!("cmd-{index}").into_bytes(),
        }
    }

    #[test]
    fn first_boot_yields_default_hard_state() {
        let (_db, s) = temp_storage();
        let hs = s.load_hard_state().unwrap();
        assert_eq!(hs, HardState::default()); // term 0, no vote
    }

    #[test]
    fn hard_state_roundtrips_and_overwrites() {
        let (_db, s) = temp_storage();
        s.save_hard_state(&HardState {
            current_term: 1,
            voted_for: Some(2),
        })
        .unwrap();
        assert_eq!(s.load_hard_state().unwrap().voted_for, Some(2));

        // Later term overwrites in place (single key).
        s.save_hard_state(&HardState {
            current_term: 5,
            voted_for: None,
        })
        .unwrap();
        let hs = s.load_hard_state().unwrap();
        assert_eq!(hs.current_term, 5);
        assert_eq!(hs.voted_for, None);
    }

    /// The vote-twice guard: a "restart" (reopen over the same tree) must
    /// still know the vote.
    #[test]
    fn hard_state_survives_restart() {
        let (db, s) = temp_storage();
        s.save_hard_state(&HardState {
            current_term: 3,
            voted_for: Some(1),
        })
        .unwrap();
        drop(s);

        let reopened = RaftStorage::open(&db).unwrap();
        let hs = reopened.load_hard_state().unwrap();
        assert_eq!(hs.current_term, 3);
        assert_eq!(hs.voted_for, Some(1), "must remember the vote after restart");
    }

    /// 25 entries: with unpadded keys "log/10" would sort before "log/2" and
    /// load_log would come back misordered. Padding makes byte order = numeric.
    #[test]
    fn load_log_returns_entries_in_index_order_past_ten() {
        let (_db, s) = temp_storage();
        let entries: Vec<LogEntry> = (1..=25).map(|i| entry(i, 1)).collect();
        s.append_entries(&entries).unwrap();

        let loaded = s.load_log().unwrap();
        let indices: Vec<LogIndex> = loaded.iter().map(|e| e.index).collect();
        assert_eq!(indices, (1..=25).collect::<Vec<_>>());
    }

    #[test]
    fn truncate_from_kills_the_zombie_suffix() {
        let (_db, s) = temp_storage();
        s.append_entries(&[entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2)])
            .unwrap();

        // Conflict at 3: truncate 3+, then the new leader's SINGLE entry 3.
        s.truncate_from(3).unwrap();
        s.append_entries(&[entry(3, 3)]).unwrap();

        let loaded = s.load_log().unwrap();
        let view: Vec<(LogIndex, Term)> = loaded.iter().map(|e| (e.index, e.term)).collect();
        // Old entry 4 must NOT resurrect — that's the zombie the truncate kills.
        assert_eq!(view, vec![(1, 1), (2, 1), (3, 3)]);
    }

    #[test]
    fn truncate_from_zero_clears_log_but_not_hard_state() {
        let (_db, s) = temp_storage();
        s.save_hard_state(&HardState {
            current_term: 2,
            voted_for: Some(3),
        })
        .unwrap();
        s.append_entries(&[entry(1, 1), entry(2, 1)]).unwrap();

        s.truncate_from(0).unwrap();
        assert!(s.load_log().unwrap().is_empty());
        // hard_state lives under a different key — untouched by log truncation.
        assert_eq!(s.load_hard_state().unwrap().current_term, 2);
    }

    /// Full restart simulation: log + hard state written, process "dies"
    /// (drop), reopen, rebuild — the exact startup path the shell will run.
    #[test]
    fn full_state_survives_restart() {
        let (db, s) = temp_storage();
        s.save_hard_state(&HardState {
            current_term: 4,
            voted_for: Some(2),
        })
        .unwrap();
        s.append_entries(&[entry(1, 1), entry(2, 4)]).unwrap();
        drop(s);

        let reopened = RaftStorage::open(&db).unwrap();
        let log = crate::raft::log::RaftLog::from_entries(reopened.load_log().unwrap());
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 4);
        assert_eq!(reopened.load_hard_state().unwrap().current_term, 4);
    }
}
