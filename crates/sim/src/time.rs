//! Simulated time.

use std::fmt;
use std::ops::{Add, Sub};
use std::time::Duration;

/// A point in simulated time, stored as nanoseconds since the simulation started.
///
/// Simulated time moves only when the simulator processes an event, so a run that covers
/// minutes of simulated time can finish in milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimTime(u64);

impl SimTime {
    /// The start of the simulation.
    pub const ZERO: SimTime = SimTime(0);

    /// Returns the time `duration` after the start of the simulation.
    pub fn from_duration(duration: Duration) -> SimTime {
        SimTime(nanos(duration))
    }

    /// Returns the time elapsed since the start of the simulation.
    pub fn since_start(self) -> Duration {
        Duration::from_nanos(self.0)
    }

    /// Returns the number of nanoseconds since the start of the simulation.
    pub fn as_nanos(self) -> u64 {
        self.0
    }
}

impl Add<Duration> for SimTime {
    type Output = SimTime;

    fn add(self, duration: Duration) -> SimTime {
        SimTime(self.0.saturating_add(nanos(duration)))
    }
}

impl Sub for SimTime {
    type Output = Duration;

    /// Returns the time from `earlier` to `self`, or zero if `earlier` is later.
    fn sub(self, earlier: SimTime) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl fmt::Display for SimTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.3} ms", self.0 as f64 / 1e6)
    }
}

pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SimTime;

    #[test]
    fn arithmetic() {
        let t = SimTime::from_duration(Duration::from_millis(5));
        assert_eq!((t + Duration::from_millis(2)) - t, Duration::from_millis(2));
        assert_eq!(SimTime::ZERO - t, Duration::ZERO);
        assert_eq!(t.since_start(), Duration::from_millis(5));
    }
}
