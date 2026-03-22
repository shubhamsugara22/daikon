use crate::error::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const MIN_PRECISION: u8 = 4;
const MAX_PRECISION: u8 = 16;
const DEFAULT_PRECISION: u8 = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new(DEFAULT_PRECISION)
    }
}

impl HyperLogLog {
    pub fn new(precision: u8) -> Self {
        let precision = precision.clamp(MIN_PRECISION, MAX_PRECISION);
        let registers = vec![0; 1usize << precision];
        Self {
            precision,
            registers,
        }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn register_count(&self) -> usize {
        self.registers.len()
    }

    pub fn add(&mut self, value: &str) -> bool {
        let hash = hash_value(value);
        let index = (hash >> (64 - self.precision)) as usize;
        let shifted = hash << self.precision;
        let max_rank = 64 - self.precision as u32;
        let rank = (shifted.leading_zeros() + 1).min(max_rank + 1) as u8;

        if rank > self.registers[index] {
            self.registers[index] = rank;
            return true;
        }

        false
    }

    pub fn merge(&mut self, other: &HyperLogLog) -> Result<()> {
        if self.precision != other.precision {
            return Err(KvStoreError::InvalidValue(format!(
                "Cannot merge HyperLogLogs with different precisions: {} vs {}",
                self.precision, other.precision
            )));
        }

        for (target, source) in self.registers.iter_mut().zip(&other.registers) {
            *target = (*target).max(*source);
        }

        Ok(())
    }

    pub fn count(&self) -> u64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let sum: f64 = self
            .registers
            .iter()
            .map(|register| 2f64.powi(-(*register as i32)))
            .sum();
        let raw_estimate = alpha * m * m / sum;
        let zero_registers = self.registers.iter().filter(|&&r| r == 0).count() as f64;

        let estimate = if raw_estimate <= 2.5 * m && zero_registers > 0.0 {
            m * (m / zero_registers).ln()
        } else {
            raw_estimate
        };

        estimate.round().max(0.0) as u64
    }

    pub fn memory_bytes(&self) -> usize {
        self.registers.len()
    }
}

fn hash_value(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_add_and_count_non_zero() {
        let mut hll = HyperLogLog::default();
        for value in ["a", "b", "c", "d", "e"] {
            hll.add(value);
        }

        let count = hll.count();
        assert!(count >= 3, "expected count >= 3, got {}", count);
        assert!(count <= 10, "expected count <= 10, got {}", count);
    }

    #[test]
    fn test_hll_duplicate_values_do_not_blow_up_count() {
        let mut hll = HyperLogLog::default();
        for _ in 0..100 {
            hll.add("same-user");
        }

        let count = hll.count();
        assert!(
            count <= 5,
            "duplicate-heavy count should stay small, got {}",
            count
        );
    }

    #[test]
    fn test_hll_merge_combines_estimates() {
        let mut left = HyperLogLog::default();
        let mut right = HyperLogLog::default();

        for value in ["u1", "u2", "u3", "u4"] {
            left.add(value);
        }
        for value in ["u3", "u4", "u5", "u6", "u7"] {
            right.add(value);
        }

        let before = left.count();
        left.merge(&right).unwrap();
        let after = left.count();

        assert!(after >= before);
        assert!(
            after >= 5,
            "expected merged count to reflect union, got {}",
            after
        );
    }

    #[test]
    fn test_hll_merge_precision_mismatch_fails() {
        let mut left = HyperLogLog::new(8);
        let right = HyperLogLog::new(10);
        assert!(left.merge(&right).is_err());
    }
}
