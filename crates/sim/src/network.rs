//! The simulated network between servers.

use std::collections::BTreeSet;
use std::time::Duration;

use consensus::{NodeId, Rng};

use crate::time::nanos;

/// Denominator for probabilities given in parts per million. Probabilities are integers so
/// that the same seed gives the same run on every machine.
pub const PPM: u32 = 1_000_000;

/// A rule that drops matching messages. Experiment procedures use it, for example, to
/// deliver a leader's new entries to only some followers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropRule {
    /// The sending server.
    pub from: NodeId,
    /// The receiving server.
    pub to: NodeId,
    /// If `true`, only messages that carry log entries are dropped, so heartbeats without
    /// entries still get through. If `false`, every message on the link is dropped.
    pub only_with_entries: bool,
}

/// What happens to a message.
pub(crate) enum Route {
    Dropped,
    Delivered {
        delay: Duration,
        /// Set if the network delivers a second copy, with its own delay.
        duplicate: Option<Duration>,
    },
}

/// Network conditions between servers.
///
/// One-way delay is uniform in `[0.5 d, 1.5 d]` (`docs/RESEARCH.md` §3), multiplied by
/// `delay_factor`. Because each message gets its own delay, messages are reordered
/// naturally.
pub(crate) struct Network {
    base_delay: u64,
    pub(crate) loss_ppm: u32,
    pub(crate) duplicate_ppm: u32,
    pub(crate) delay_factor: u64,
    blocked: BTreeSet<(NodeId, NodeId)>,
    rules: Vec<DropRule>,
    rng: Rng,
}

impl Network {
    pub(crate) fn new(one_way_delay: Duration, rng: Rng) -> Self {
        Network {
            base_delay: nanos(one_way_delay),
            loss_ppm: 0,
            duplicate_ppm: 0,
            delay_factor: 1,
            blocked: BTreeSet::new(),
            rules: Vec::new(),
            rng,
        }
    }

    /// Draws one message delay.
    pub(crate) fn delay(&mut self) -> Duration {
        let d = self.base_delay;
        let sample = d / 2 + self.rng.below(d + 1);
        Duration::from_nanos(sample.saturating_mul(self.delay_factor))
    }

    /// Decides the fate of a message from `from` to `to`.
    pub(crate) fn route(&mut self, from: NodeId, to: NodeId, has_entries: bool) -> Route {
        let ruled_out = self.rules.iter().any(|rule| {
            rule.from == from && rule.to == to && (!rule.only_with_entries || has_entries)
        });
        if ruled_out || self.blocked.contains(&(from, to)) || self.chance(self.loss_ppm) {
            return Route::Dropped;
        }
        let delay = self.delay();
        let duplicate = if self.chance(self.duplicate_ppm) {
            Some(self.delay())
        } else {
            None
        };
        Route::Delivered { delay, duplicate }
    }

    /// Blocks every link between servers in different groups, in both directions.
    pub(crate) fn partition(&mut self, groups: &[Vec<NodeId>]) {
        for (i, group) in groups.iter().enumerate() {
            for other in groups.iter().skip(i + 1) {
                for &a in group {
                    for &b in other {
                        self.blocked.insert((a, b));
                        self.blocked.insert((b, a));
                    }
                }
            }
        }
    }

    pub(crate) fn block(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from, to));
    }

    pub(crate) fn unblock(&mut self, from: NodeId, to: NodeId) {
        self.blocked.remove(&(from, to));
    }

    pub(crate) fn heal_links(&mut self) {
        self.blocked.clear();
    }

    pub(crate) fn add_rule(&mut self, rule: DropRule) {
        self.rules.push(rule);
    }

    pub(crate) fn clear_rules(&mut self) {
        self.rules.clear();
    }

    fn chance(&mut self, ppm: u32) -> bool {
        ppm > 0 && self.rng.below(u64::from(PPM)) < u64::from(ppm)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use consensus::{NodeId, Rng};

    use super::{DropRule, Network, Route};

    fn network() -> Network {
        Network::new(Duration::from_millis(10), Rng::new(1))
    }

    #[test]
    fn delays_stay_in_range() {
        let mut net = network();
        for _ in 0..10_000 {
            let d = net.delay();
            assert!(d >= Duration::from_millis(5) && d <= Duration::from_millis(15));
        }
    }

    #[test]
    fn partitions_and_rules_drop_messages() {
        let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
        let mut net = network();
        net.partition(&[vec![a], vec![b, c]]);
        assert!(matches!(net.route(a, b, false), Route::Dropped));
        assert!(matches!(net.route(c, a, false), Route::Dropped));
        assert!(matches!(net.route(b, c, false), Route::Delivered { .. }));
        net.heal_links();

        net.add_rule(DropRule {
            from: a,
            to: b,
            only_with_entries: true,
        });
        assert!(matches!(net.route(a, b, true), Route::Dropped));
        assert!(matches!(net.route(a, b, false), Route::Delivered { .. }));
    }

    #[test]
    fn full_loss_drops_everything() {
        let mut net = network();
        net.loss_ppm = super::PPM;
        for _ in 0..100 {
            assert!(matches!(
                net.route(NodeId(1), NodeId(2), false),
                Route::Dropped
            ));
        }
    }
}
