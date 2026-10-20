//! Faults: scheduled changes to servers and the network.

use std::time::Duration;

use consensus::{NodeId, Rng};

use crate::SimTime;
use crate::time::nanos;

/// A change to the simulated environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Stop a server between inputs. Its timers stop, and messages sent to it are lost
    /// until it restarts.
    Crash(NodeId),
    /// Stop a server in the middle of its next input. The input's storage writes are lost
    /// because they were never synced, and its outputs are never sent.
    CrashDuringNextInput(NodeId),
    /// Restart a stopped server from its storage.
    Restart(NodeId),
    /// Block every link between servers in different groups, in both directions.
    Partition(Vec<Vec<NodeId>>),
    /// Block the link from one server to another, in that direction only.
    Block {
        /// The sending server.
        from: NodeId,
        /// The receiving server.
        to: NodeId,
    },
    /// Unblock the link from one server to another.
    Unblock {
        /// The sending server.
        from: NodeId,
        /// The receiving server.
        to: NodeId,
    },
    /// Unblock every link.
    HealLinks,
    /// Drop each message with this probability, in parts per million.
    SetLoss(u32),
    /// Deliver a second copy of each message with this probability, in parts per million.
    SetDuplication(u32),
    /// Multiply every message delay by this factor.
    SetDelayFactor(u64),
    /// End every fault: unblock all links, stop loss and duplication, restore normal
    /// delays, and restart every stopped server.
    HealAll,
}

/// Settings for a random fault schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChaosConfig {
    /// The earliest time a fault may start.
    pub start: Duration,
    /// The time every fault ends. A [`Fault::HealAll`] is scheduled at this time.
    pub end: Duration,
    /// Average time between the starts of consecutive faults. Each gap is uniform in
    /// `[0, 2 × mean_gap]`.
    pub mean_gap: Duration,
    /// Longest duration of one fault. Each duration is uniform in `[100 ms, max_duration]`.
    pub max_duration: Duration,
}

/// Loss probabilities to choose from, in parts per million: 5%, 20%, and 50%.
const LOSS_LEVELS: [u32; 3] = [50_000, 200_000, 500_000];

/// Delay multipliers to choose from.
const DELAY_FACTORS: [u64; 2] = [3, 10];

/// Generates a random fault schedule for `servers`.
///
/// Each fault is one of: a crash between inputs, a crash during an input, a two-sided
/// partition, a one-way link failure, message loss, message duplication, or slower
/// delivery. Each fault is undone after its duration, and every fault ends at
/// `config.end`. Only integer arithmetic is used, so the same generator state gives the
/// same schedule on every machine.
pub fn chaos_schedule(
    servers: &[NodeId],
    config: &ChaosConfig,
    rng: &mut Rng,
) -> Vec<(SimTime, Fault)> {
    let end = SimTime::from_duration(config.end);
    let mut schedule = Vec::new();
    let mut time = SimTime::from_duration(config.start);
    let multi = servers.len() > 1;

    loop {
        time = time + uniform(rng, Duration::ZERO, config.mean_gap * 2);
        if time >= end {
            break;
        }
        let duration = uniform(rng, Duration::from_millis(100), config.max_duration);
        let until = (time + duration).min(end);
        let server = pick(rng, servers);

        match rng.below(7) {
            0 => {
                schedule.push((time, Fault::Crash(server)));
                schedule.push((until, Fault::Restart(server)));
            }
            1 => {
                schedule.push((time, Fault::CrashDuringNextInput(server)));
                schedule.push((until, Fault::Restart(server)));
            }
            2 if multi => {
                schedule.push((time, Fault::Partition(split(rng, servers))));
                schedule.push((until, Fault::HealLinks));
            }
            3 if multi => {
                let mut other = pick(rng, servers);
                while other == server {
                    other = pick(rng, servers);
                }
                schedule.push((
                    time,
                    Fault::Block {
                        from: server,
                        to: other,
                    },
                ));
                schedule.push((
                    until,
                    Fault::Unblock {
                        from: server,
                        to: other,
                    },
                ));
            }
            4 => {
                let level = LOSS_LEVELS[rng.below(LOSS_LEVELS.len() as u64) as usize];
                schedule.push((time, Fault::SetLoss(level)));
                schedule.push((until, Fault::SetLoss(0)));
            }
            5 => {
                schedule.push((time, Fault::SetDuplication(100_000)));
                schedule.push((until, Fault::SetDuplication(0)));
            }
            _ => {
                let factor = DELAY_FACTORS[rng.below(DELAY_FACTORS.len() as u64) as usize];
                schedule.push((time, Fault::SetDelayFactor(factor)));
                schedule.push((until, Fault::SetDelayFactor(1)));
            }
        }
    }
    schedule.push((end, Fault::HealAll));
    // Stable sort: faults scheduled for the same time keep the order they were generated.
    schedule.sort_by_key(|(time, _)| *time);
    schedule
}

fn uniform(rng: &mut Rng, low: Duration, high: Duration) -> Duration {
    let span = nanos(high).saturating_sub(nanos(low));
    low + Duration::from_nanos(rng.below(span.saturating_add(1)))
}

fn pick(rng: &mut Rng, servers: &[NodeId]) -> NodeId {
    servers[rng.below(servers.len() as u64) as usize]
}

/// Splits `servers` into two non-empty groups at random.
fn split(rng: &mut Rng, servers: &[NodeId]) -> Vec<Vec<NodeId>> {
    let mut groups = vec![Vec::new(), Vec::new()];
    for &server in servers {
        groups[rng.below(2) as usize].push(server);
    }
    if groups[0].is_empty() || groups[1].is_empty() {
        let (full, empty) = if groups[0].is_empty() { (1, 0) } else { (0, 1) };
        let moved = groups[full].pop().expect("at least two servers");
        groups[empty].push(moved);
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use consensus::{NodeId, Rng};

    use super::{ChaosConfig, Fault, chaos_schedule};
    use crate::SimTime;

    fn config() -> ChaosConfig {
        ChaosConfig {
            start: Duration::from_secs(1),
            end: Duration::from_secs(30),
            mean_gap: Duration::from_millis(500),
            max_duration: Duration::from_secs(3),
        }
    }

    #[test]
    fn same_seed_same_schedule() {
        let servers: Vec<NodeId> = (1..=5).map(NodeId).collect();
        let a = chaos_schedule(&servers, &config(), &mut Rng::new(3));
        let b = chaos_schedule(&servers, &config(), &mut Rng::new(3));
        assert_eq!(a, b);
        assert!(a.len() > 10);
    }

    #[test]
    fn schedule_is_ordered_and_ends_with_heal_all() {
        let servers: Vec<NodeId> = (1..=3).map(NodeId).collect();
        let schedule = chaos_schedule(&servers, &config(), &mut Rng::new(9));
        assert!(schedule.windows(2).all(|w| w[0].0 <= w[1].0));
        let end = SimTime::from_duration(Duration::from_secs(30));
        assert_eq!(schedule.last(), Some(&(end, Fault::HealAll)));
        assert!(schedule.iter().all(|(time, _)| *time <= end));
    }

    #[test]
    fn partitions_have_two_nonempty_groups() {
        let servers: Vec<NodeId> = (1..=5).map(NodeId).collect();
        for seed in 0..50 {
            for (_, fault) in chaos_schedule(&servers, &config(), &mut Rng::new(seed)) {
                if let Fault::Partition(groups) = fault {
                    assert_eq!(groups.len(), 2);
                    assert!(groups.iter().all(|g| !g.is_empty()));
                }
            }
        }
    }
}
