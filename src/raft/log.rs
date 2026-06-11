//! The replicated log — the thing Raft exists to agree on. Each entry carries
//! the TERM it was created in; (index, term) pairs are how all of Raft's
//! consistency checks work.

use serde::{Deserialize, Serialize};

/// Logical clock: increments on every election. Term 0 = "before any leader".
pub type Term = u64;
/// 1-BASED (paper convention; avoids ±1 bugs translating Figure 2). Index 0 =
/// "the empty log" — a real entry never has index 0.
pub type LogIndex = u64;
pub type NodeId = u64;
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }

    pub fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(0, |entry| entry.term)
    }

    // Index 0 → Some(0): the empty-log sentinel makes AppendEntries'
    // prev_log check uniform (prev=(0,0) is vacuously true for every node).
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entries
            .get((index - 1) as usize)
            .map(|entry| entry.term)
    }

    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    pub fn entries_from(&self, from: LogIndex) -> Vec<LogEntry> {
        if from == 0 || from > self.last_index() {
            return Vec::new();
        }
        self.entries[(from - 1) as usize..].to_vec()
    }

    pub fn append(&mut self, term: Term, command: Vec<u8>) -> LogIndex {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry {
            term,
            index,
            command,
        });
        index
    }

    pub fn truncate_from(&mut self, from: LogIndex) {
        if from == 0 {
            self.entries.clear();
        } else {
            self.entries.truncate((from - 1) as usize);
        }
    }

    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        self.entries.extend(entries);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a log with entries of the given terms (commands are the index).
    fn log_with_terms(terms: &[Term]) -> RaftLog {
        let mut log = RaftLog::new();
        for t in terms {
            log.append(*t, vec![]);
        }
        log
    }

    #[test]
    fn empty_log_sentinels() {
        let log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        // The (0,0) sentinel: term_at(0) is Some(0) even on an empty log —
        // this is what lets the FIRST AppendEntries consistency check pass.
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);
        assert!(log.get(0).is_none());
        assert!(log.entries_from(1).is_empty());
    }

    #[test]
    fn append_assigns_one_based_contiguous_indices() {
        let mut log = RaftLog::new();
        assert_eq!(log.append(1, b"a".to_vec()), 1);
        assert_eq!(log.append(1, b"b".to_vec()), 2);
        assert_eq!(log.append(2, b"c".to_vec()), 3);

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        // 1-based lookup returns the right entries.
        assert_eq!(log.get(1).unwrap().command, b"a");
        assert_eq!(log.get(3).unwrap().command, b"c");
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), None);
    }

    #[test]
    fn entries_from_slices_inclusive_suffix() {
        let log = log_with_terms(&[1, 1, 2, 3]);
        // from=1 → everything; from=3 → last two; past-end / zero → empty.
        assert_eq!(log.entries_from(1).len(), 4);
        let tail = log.entries_from(3);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].index, 3);
        assert_eq!(tail[1].index, 4);
        assert!(log.entries_from(5).is_empty());
        assert!(log.entries_from(0).is_empty());
    }

    #[test]
    fn truncate_from_drops_suffix_inclusive() {
        // Conflict resolution: drop the divergent suffix STARTING AT `from`.
        let mut log = log_with_terms(&[1, 1, 2, 3]);
        log.truncate_from(3);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.term_at(3), None);

        // from=0 clears everything; truncating past the end is a no-op.
        log.truncate_from(0);
        assert_eq!(log.last_index(), 0);
        let mut log = log_with_terms(&[1]);
        log.truncate_from(9);
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn append_entries_extends_and_roundtrips_from_entries() {
        let mut log = log_with_terms(&[1]);
        log.append_entries(vec![
            LogEntry {
                term: 2,
                index: 2,
                command: b"x".to_vec(),
            },
            LogEntry {
                term: 2,
                index: 3,
                command: b"y".to_vec(),
            },
        ]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);

        // Restart path: rebuild from the same entries → identical view.
        let rebuilt = RaftLog::from_entries(log.entries_from(1));
        assert_eq!(rebuilt.last_index(), 3);
        assert_eq!(rebuilt.term_at(2), Some(2));
        assert_eq!(rebuilt.get(3).unwrap().command, b"y");
    }
}
