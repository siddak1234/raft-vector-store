//! Deterministic random numbers.
//!
//! Each server gets its own generator, seeded by the driver, so a simulation run with the
//! same seeds replays exactly. The generator is SplitMix64 (Steele, Lea, and Flood, *Fast
//! Splittable Pseudorandom Number Generators*, OOPSLA 2014). It is small, fast, and does
//! not change between dependency versions.

/// A seeded SplitMix64 random number generator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Creates a generator from a seed.
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Returns the next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a number in `0..bound`. The bias is at most `bound / 2^64`, which is
    /// negligible for timeout ranges.
    ///
    /// # Panics
    ///
    /// Panics if `bound` is zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "bound must be positive");
        let wide = u128::from(self.next_u64()) * u128::from(bound);
        // The high 64 bits of the product are always less than `bound`.
        (wide >> 64) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn matches_reference_splitmix64_output() {
        // Reference values for seed 0, computed independently of this implementation.
        let mut rng = Rng::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn same_seed_gives_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn below_stays_in_range() {
        let mut rng = Rng::new(7);
        for bound in [1, 2, 3, 150, 1_000_000] {
            for _ in 0..1000 {
                assert!(rng.below(bound) < bound);
            }
        }
    }
}
