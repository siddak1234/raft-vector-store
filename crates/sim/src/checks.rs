//! Safety checks (`docs/RESEARCH.md` §9).
//!
//! The simulator calls these checks as the run progresses: after every applied entry,
//! every storage write, and every input a server processes. A violation is recorded with the
//! simulated time it was detected, so the run can be replayed from its seed and inspected.

use std::collections::{BTreeMap, HashMap};

use consensus::{LogIndex, MemStorage, NodeId, Payload, Role, Status, Storage, Term};

use crate::SimTime;

/// The properties the simulator checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Check {
    /// At most one leader per term.
    ElectionSafety,
    /// No two servers apply different entries at the same index.
    StateMachineSafety,
    /// Each server applies entries in index order, starting at 1, without gaps.
    ApplyOrder,
    /// An index and a term identify at most one entry, across all servers.
    UniqueEntries,
    /// By the end of a run, every server has applied every acknowledged command.
    Durability,
    /// After faults stop, commands commit again within the deadline.
    Liveness,
    /// Raft only: two logs that hold the same entry at an index are identical up to it.
    LogMatching,
    /// Raft only: a new leader's log holds every entry any server has already applied.
    LeaderCompleteness,
}

impl Check {
    /// Returns the check's name, as used in reports.
    pub fn name(self) -> &'static str {
        match self {
            Check::ElectionSafety => "election safety",
            Check::StateMachineSafety => "state machine safety",
            Check::ApplyOrder => "apply order",
            Check::UniqueEntries => "unique entries",
            Check::Durability => "durability",
            Check::Liveness => "liveness",
            Check::LogMatching => "log matching",
            Check::LeaderCompleteness => "leader completeness",
        }
    }
}

/// A broken property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// When the violation was detected.
    pub time: SimTime,
    /// The property that failed.
    pub check: Check,
    /// What was observed.
    pub detail: String,
}

/// Violations kept in full; later ones are only counted.
const MAX_RECORDED: usize = 32;

pub(crate) struct Checker {
    raft_checks: bool,
    /// The leader of each term seen so far.
    leaders: BTreeMap<Term, NodeId>,
    /// The first payload any server applied at each index; index `i` is at position `i - 1`.
    applied: Vec<Payload>,
    /// The first payload written under each index and term.
    entries: HashMap<(LogIndex, Term), Payload>,
    /// For each server slot, the term it currently leads, if any.
    leading: Vec<Option<Term>>,
    violations: Vec<Violation>,
    total: u64,
}

impl Checker {
    pub(crate) fn new(servers: usize, raft_checks: bool) -> Self {
        Checker {
            raft_checks,
            leaders: BTreeMap::new(),
            applied: Vec::new(),
            entries: HashMap::new(),
            leading: vec![None; servers],
            violations: Vec::new(),
            total: 0,
        }
    }

    pub(crate) fn report(&mut self, time: SimTime, check: Check, detail: String) {
        self.total += 1;
        if self.violations.len() < MAX_RECORDED {
            self.violations.push(Violation {
                time,
                check,
                detail,
            });
        }
    }

    pub(crate) fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Records that `server` applied `payload` at `index`. `next_apply` is the index the
    /// server is expected to apply next; it is advanced here.
    pub(crate) fn on_apply(
        &mut self,
        time: SimTime,
        server: NodeId,
        index: LogIndex,
        payload: &Payload,
        next_apply: &mut u64,
    ) {
        if index.0 != *next_apply {
            self.report(
                time,
                Check::ApplyOrder,
                format!(
                    "server {} applied index {} but was expected to apply {}",
                    server.0, index.0, next_apply
                ),
            );
        }
        *next_apply = index.0 + 1;
        if index.0 == 0 {
            return;
        }
        let position = (index.0 - 1) as usize;
        match self.applied.get(position) {
            Some(first) if first != payload => self.report(
                time,
                Check::StateMachineSafety,
                format!(
                    "server {} applied a different entry at index {} than an earlier server",
                    server.0, index.0
                ),
            ),
            Some(_) => {}
            None => {
                if position == self.applied.len() {
                    self.applied.push(payload.clone());
                }
            }
        }
    }

    /// Checks an entry `server` has just written at `index`.
    pub(crate) fn on_write(
        &mut self,
        time: SimTime,
        server: NodeId,
        index: LogIndex,
        storage: &MemStorage,
    ) {
        let Some(entry) = storage.entry(index) else {
            return;
        };
        match self.entries.get(&(index, entry.term)) {
            Some(first) if *first != entry.payload => self.report(
                time,
                Check::UniqueEntries,
                format!(
                    "server {} wrote a different entry at index {} in term {} than another server",
                    server.0, index.0, entry.term.0
                ),
            ),
            Some(_) => {}
            None => {
                self.entries
                    .insert((index, entry.term), entry.payload.clone());
            }
        }
    }

    /// Checks a server's status after it processed an input.
    pub(crate) fn on_status(
        &mut self,
        time: SimTime,
        slot: usize,
        server: NodeId,
        status: Status,
        storage: &MemStorage,
    ) {
        if status.role != Role::Leader {
            self.leading[slot] = None;
            return;
        }
        match self.leaders.get(&status.term) {
            Some(&other) if other != server => self.report(
                time,
                Check::ElectionSafety,
                format!(
                    "servers {} and {} both led term {}",
                    other.0, server.0, status.term.0
                ),
            ),
            Some(_) => {}
            None => {
                self.leaders.insert(status.term, server);
            }
        }
        if self.leading[slot] != Some(status.term) {
            self.leading[slot] = Some(status.term);
            if self.raft_checks {
                self.check_leader_completeness(time, server, status.term, storage);
            }
        }
    }

    /// Called when a server stops.
    pub(crate) fn on_stop(&mut self, slot: usize) {
        self.leading[slot] = None;
    }

    fn check_leader_completeness(
        &mut self,
        time: SimTime,
        server: NodeId,
        term: Term,
        storage: &MemStorage,
    ) {
        let missing = self
            .applied
            .iter()
            .enumerate()
            .find(|(position, payload)| {
                let index = LogIndex(*position as u64 + 1);
                storage.entry(index).map(|e| &e.payload) != Some(*payload)
            })
            .map(|(position, _)| position as u64 + 1);
        if let Some(index) = missing {
            self.report(
                time,
                Check::LeaderCompleteness,
                format!(
                    "server {} became leader of term {} without the applied entry at index {}",
                    server.0, term.0, index
                ),
            );
        }
    }

    /// Raft only: checks every pair of logs for the Log Matching property (Raft Figure 3).
    pub(crate) fn check_log_matching(&mut self, time: SimTime, logs: &[(NodeId, &MemStorage)]) {
        for (i, (a, log_a)) in logs.iter().enumerate() {
            for (b, log_b) in &logs[i + 1..] {
                if let Some(detail) = log_matching_failure(log_a, log_b) {
                    self.report(
                        time,
                        Check::LogMatching,
                        format!("servers {} and {}: {detail}", a.0, b.0),
                    );
                }
            }
        }
    }

    /// Checks that every server has applied every acknowledged command.
    pub(crate) fn check_durability(
        &mut self,
        time: SimTime,
        acknowledged: &[(LogIndex, Payload)],
        applied: &[(NodeId, u64)],
    ) {
        for (index, payload) in acknowledged {
            let position = index.0.saturating_sub(1) as usize;
            if self.applied.get(position) != Some(payload) {
                self.report(
                    time,
                    Check::Durability,
                    format!(
                        "the command acknowledged at index {} is not the entry applied there",
                        index.0
                    ),
                );
            }
        }
        let Some(highest) = acknowledged.iter().map(|(index, _)| index.0).max() else {
            return;
        };
        for &(server, count) in applied {
            if count < highest {
                self.report(
                    time,
                    Check::Durability,
                    format!(
                        "server {} applied up to index {count}, but a command was acknowledged at index {highest}",
                        server.0
                    ),
                );
            }
        }
    }
}

/// Returns a description of how two logs break Log Matching, or `None` if they do not.
fn log_matching_failure(a: &MemStorage, b: &MemStorage) -> Option<String> {
    let shared = a.last_index().min(b.last_index()).0;
    let mut first_difference = None;
    for i in 1..=shared {
        let index = LogIndex(i);
        let (x, y) = (a.entry(index), b.entry(index));
        if let (Some(x), Some(y), Some(diff)) = (x, y, first_difference)
            && x.term == y.term
        {
            return Some(format!(
                "both hold term {} at index {i} but differ at index {diff}",
                x.term.0
            ));
        }
        if x != y && first_difference.is_none() {
            first_difference = Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use consensus::{
        Command, Entry, LogIndex, MemStorage, NodeId, Payload, Role, Status, Storage, Term,
    };

    use super::{Check, Checker};
    use crate::SimTime;

    fn payload(byte: u8) -> Payload {
        Payload::Command(Command(vec![byte]))
    }

    fn log(entries: &[(u64, u8)]) -> MemStorage {
        let mut storage = MemStorage::new();
        for (i, (term, byte)) in entries.iter().enumerate() {
            storage.put(
                LogIndex(i as u64 + 1),
                Entry {
                    term: Term(*term),
                    payload: payload(*byte),
                },
            );
        }
        storage
    }

    fn leader(term: u64) -> Status {
        Status {
            role: Role::Leader,
            term: Term(term),
            leader: None,
            commit_index: LogIndex::ZERO,
        }
    }

    fn checks(checker: &Checker) -> Vec<Check> {
        checker.violations().iter().map(|v| v.check).collect()
    }

    #[test]
    fn two_leaders_in_one_term() {
        let mut checker = Checker::new(2, false);
        let empty = MemStorage::new();
        checker.on_status(SimTime::ZERO, 0, NodeId(1), leader(3), &empty);
        checker.on_status(SimTime::ZERO, 1, NodeId(2), leader(4), &empty);
        assert!(checker.violations().is_empty());
        checker.on_status(SimTime::ZERO, 1, NodeId(2), leader(3), &empty);
        assert_eq!(checks(&checker), [Check::ElectionSafety]);
    }

    #[test]
    fn different_entries_applied_at_one_index() {
        let mut checker = Checker::new(2, false);
        let (mut next_a, mut next_b) = (1, 1);
        checker.on_apply(
            SimTime::ZERO,
            NodeId(1),
            LogIndex(1),
            &payload(1),
            &mut next_a,
        );
        checker.on_apply(
            SimTime::ZERO,
            NodeId(2),
            LogIndex(1),
            &payload(1),
            &mut next_b,
        );
        assert!(checker.violations().is_empty());
        checker.on_apply(
            SimTime::ZERO,
            NodeId(1),
            LogIndex(2),
            &payload(2),
            &mut next_a,
        );
        checker.on_apply(
            SimTime::ZERO,
            NodeId(2),
            LogIndex(2),
            &payload(9),
            &mut next_b,
        );
        assert_eq!(checks(&checker), [Check::StateMachineSafety]);
    }

    #[test]
    fn applying_out_of_order() {
        let mut checker = Checker::new(1, false);
        let mut next = 1;
        checker.on_apply(
            SimTime::ZERO,
            NodeId(1),
            LogIndex(2),
            &payload(2),
            &mut next,
        );
        assert_eq!(checks(&checker), [Check::ApplyOrder]);
    }

    #[test]
    fn two_entries_under_one_index_and_term() {
        let mut checker = Checker::new(2, false);
        let a = log(&[(1, 1)]);
        let b = log(&[(1, 2)]);
        checker.on_write(SimTime::ZERO, NodeId(1), LogIndex(1), &a);
        checker.on_write(SimTime::ZERO, NodeId(2), LogIndex(1), &b);
        assert_eq!(checks(&checker), [Check::UniqueEntries]);
    }

    #[test]
    fn logs_that_break_log_matching() {
        let mut checker = Checker::new(2, true);
        // Same term at index 3, but the logs differ at index 2.
        let a = log(&[(1, 1), (1, 2), (2, 3)]);
        let b = log(&[(1, 1), (1, 7), (2, 3)]);
        checker.check_log_matching(SimTime::ZERO, &[(NodeId(1), &a), (NodeId(2), &b)]);
        assert_eq!(checks(&checker), [Check::LogMatching]);

        // Logs that diverge and never share a later term are fine.
        let mut checker = Checker::new(2, true);
        let c = log(&[(1, 1), (2, 2), (2, 3)]);
        let d = log(&[(1, 1), (3, 5)]);
        checker.check_log_matching(SimTime::ZERO, &[(NodeId(1), &c), (NodeId(2), &d)]);
        assert!(checker.violations().is_empty());
    }

    #[test]
    fn new_leader_missing_an_applied_entry() {
        let mut checker = Checker::new(1, true);
        let mut next = 1;
        checker.on_apply(
            SimTime::ZERO,
            NodeId(1),
            LogIndex(1),
            &payload(1),
            &mut next,
        );
        let stale = log(&[(1, 5)]);
        checker.on_status(SimTime::ZERO, 0, NodeId(1), leader(2), &stale);
        assert_eq!(checks(&checker), [Check::LeaderCompleteness]);
    }

    #[test]
    fn acknowledged_command_not_applied_everywhere() {
        let mut checker = Checker::new(2, false);
        let mut next = 1;
        checker.on_apply(
            SimTime::ZERO,
            NodeId(1),
            LogIndex(1),
            &payload(1),
            &mut next,
        );
        let acknowledged = [(LogIndex(1), payload(1))];
        checker.check_durability(
            SimTime::ZERO,
            &acknowledged,
            &[(NodeId(1), 1), (NodeId(2), 1)],
        );
        assert!(checker.violations().is_empty());
        checker.check_durability(
            SimTime::ZERO,
            &acknowledged,
            &[(NodeId(1), 1), (NodeId(2), 0)],
        );
        assert_eq!(checks(&checker), [Check::Durability]);
    }
}
