//! Independent random streams derived from one seed.

use consensus::{NodeId, Rng};

/// A part of the simulation that draws random numbers.
///
/// Each part gets its own stream, so a change in how many numbers one part draws never
/// shifts the numbers another part sees. Two algorithms that send different numbers of
/// messages therefore still face the same fault schedule under the same seed, which is
/// what makes paired comparisons possible (`docs/PLAN.md` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    /// Fault schedules.
    Faults,
    /// Message delays, loss, and duplication.
    Network,
    /// Client choices, such as which server to retry.
    Workload,
    /// Choices made by an experiment's procedure, such as which followers receive an entry.
    Scenario,
    /// The random numbers of one server in one incarnation (its election timeouts).
    Server {
        /// The server.
        id: NodeId,
        /// How many times the server has been restarted.
        incarnation: u32,
    },
}

impl Stream {
    fn tag(self) -> u64 {
        match self {
            Stream::Faults => 1,
            Stream::Network => 2,
            Stream::Workload => 3,
            Stream::Scenario => 4,
            Stream::Server { id, incarnation } => {
                (5 << 56) | (u64::from(incarnation) << 32) | (id.0 & 0xFFFF_FFFF)
            }
        }
    }
}

/// Returns the seed of `stream` in a simulation seeded with `seed`.
pub fn stream_seed(seed: u64, stream: Stream) -> u64 {
    let base = Rng::new(seed).next_u64();
    Rng::new(base ^ stream.tag().wrapping_mul(0x9E37_79B9_7F4A_7C15)).next_u64()
}

/// Returns a generator for `stream` in a simulation seeded with `seed`.
pub fn stream_rng(seed: u64, stream: Stream) -> Rng {
    Rng::new(stream_seed(seed, stream))
}

#[cfg(test)]
mod tests {
    use consensus::NodeId;

    use super::{Stream, stream_seed};

    #[test]
    fn streams_differ_and_repeat() {
        let streams = [
            Stream::Faults,
            Stream::Network,
            Stream::Workload,
            Stream::Scenario,
            Stream::Server {
                id: NodeId(1),
                incarnation: 0,
            },
            Stream::Server {
                id: NodeId(1),
                incarnation: 1,
            },
            Stream::Server {
                id: NodeId(2),
                incarnation: 0,
            },
        ];
        let seeds: Vec<u64> = streams.iter().map(|s| stream_seed(7, *s)).collect();
        for (i, a) in seeds.iter().enumerate() {
            for b in &seeds[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert_eq!(seeds[0], stream_seed(7, Stream::Faults));
        assert_ne!(seeds[0], stream_seed(8, Stream::Faults));
    }
}
