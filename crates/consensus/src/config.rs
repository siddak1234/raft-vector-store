//! Timing and batching settings.
//!
//! Both algorithms take the same [`Config`], so the matched settings in `docs/PLAN.md` §5
//! are enforced by construction rather than by convention.

use std::time::Duration;

use crate::Rng;

/// Settings shared by both algorithms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Config {
    /// How often a leader sends heartbeats.
    pub heartbeat_interval: Duration,
    /// Shortest election timeout.
    pub election_timeout_min: Duration,
    /// Longest election timeout. Each timeout is drawn uniformly from
    /// `election_timeout_min..=election_timeout_max`.
    pub election_timeout_max: Duration,
    /// Most entries a leader sends in one message.
    pub max_entries_per_message: usize,
    /// Most entries a leader sends to one follower before that follower acknowledges them.
    pub max_entries_in_flight: usize,
}

impl Default for Config {
    /// Election timeouts of 150–300 ms, as the Raft paper recommends (§9.3), and a heartbeat
    /// at half the minimum timeout, as in the paper's Figure 16 setup.
    fn default() -> Self {
        Config {
            heartbeat_interval: Duration::from_millis(75),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            max_entries_per_message: 64,
            max_entries_in_flight: 256,
        }
    }
}

impl Config {
    /// Checks that the settings can be used together.
    ///
    /// # Errors
    ///
    /// Returns a description of the first problem found.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min.is_zero() {
            return Err("election_timeout_min must be positive".to_string());
        }
        if self.election_timeout_min > self.election_timeout_max {
            return Err("election_timeout_min must not exceed election_timeout_max".to_string());
        }
        if self.heartbeat_interval.is_zero() || self.heartbeat_interval >= self.election_timeout_min
        {
            return Err(
                "heartbeat_interval must be positive and shorter than election_timeout_min"
                    .to_string(),
            );
        }
        if self.max_entries_per_message == 0 {
            return Err("max_entries_per_message must be positive".to_string());
        }
        if self.max_entries_in_flight < self.max_entries_per_message {
            return Err(
                "max_entries_in_flight must be at least max_entries_per_message".to_string(),
            );
        }
        Ok(())
    }

    /// Draws an election timeout uniformly from the configured range, to the nanosecond.
    pub fn election_timeout(&self, rng: &mut Rng) -> Duration {
        let min = nanos(self.election_timeout_min);
        let span = nanos(self.election_timeout_max).saturating_sub(min);
        Duration::from_nanos(min + rng.below(span.saturating_add(1)))
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Config;
    use crate::Rng;

    #[test]
    fn default_is_valid() {
        assert_eq!(Config::default().validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_inconsistent_settings() {
        let invalid = [
            Config {
                election_timeout_min: Duration::ZERO,
                ..Config::default()
            },
            Config {
                election_timeout_max: Duration::from_millis(100),
                ..Config::default()
            },
            Config {
                heartbeat_interval: Duration::from_millis(150),
                ..Config::default()
            },
            Config {
                max_entries_per_message: 0,
                ..Config::default()
            },
            Config {
                max_entries_in_flight: 10,
                ..Config::default()
            },
        ];
        for config in invalid {
            assert!(config.validate().is_err(), "{config:?} should be invalid");
        }
    }

    #[test]
    fn election_timeouts_stay_in_range() {
        let config = Config::default();
        let mut rng = Rng::new(1);
        for _ in 0..10_000 {
            let timeout = config.election_timeout(&mut rng);
            assert!(timeout >= config.election_timeout_min);
            assert!(timeout <= config.election_timeout_max);
        }
    }

    #[test]
    fn fixed_timeout_has_no_randomness() {
        let config = Config {
            election_timeout_max: Duration::from_millis(150),
            ..Config::default()
        };
        let mut rng = Rng::new(1);
        assert_eq!(
            config.election_timeout(&mut rng),
            Duration::from_millis(150)
        );
    }
}
