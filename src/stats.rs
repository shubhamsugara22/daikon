use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Thread-safe statistics with atomic operations
/// Allows lock-free concurrent reads while tracking metrics
#[derive(Debug, Serialize, Deserialize)]
pub struct AtomicStoreStats {
    #[serde(skip)]
    pub total_reads: AtomicU64,
    #[serde(skip)]
    pub total_writes: AtomicU64,
    #[serde(skip)]
    pub total_deletes: AtomicU64,
    #[serde(skip)]
    pub hits: AtomicU64,
    #[serde(skip)]
    pub misses: AtomicU64,
    #[serde(skip)]
    pub memory_bytes: AtomicUsize,

    // Non-atomic fields (updated less frequently, require write lock)
    pub total_keys: usize,
    pub expired_keys: usize,
    pub evictions: u64,
}

impl AtomicStoreStats {
    pub fn new() -> Self {
        Self {
            total_reads: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            memory_bytes: AtomicUsize::new(0),
            total_keys: 0,
            expired_keys: 0,
            evictions: 0,
        }
    }

    /// Increment total reads (lock-free)
    pub fn inc_reads(&self) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment total writes (lock-free)
    pub fn inc_writes(&self) {
        self.total_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment total deletes (lock-free)
    pub fn inc_deletes(&self) {
        self.total_deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache hits (lock-free)
    pub fn inc_hits(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache misses (lock-free)
    pub fn inc_misses(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to memory usage (lock-free)
    pub fn add_memory(&self, bytes: usize) {
        self.memory_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtract from memory usage (lock-free)
    pub fn sub_memory(&self, bytes: usize) {
        self.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get current memory usage
    pub fn get_memory(&self) -> usize {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Get total reads count
    pub fn get_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Relaxed)
    }

    /// Get total writes count
    pub fn get_writes(&self) -> u64 {
        self.total_writes.load(Ordering::Relaxed)
    }

    /// Get total deletes count
    pub fn get_deletes(&self) -> u64 {
        self.total_deletes.load(Ordering::Relaxed)
    }

    /// Get cache hits count
    pub fn get_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get cache misses count
    pub fn get_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Calculate hit rate percentage
    pub fn hit_rate(&self) -> f64 {
        let total = self.get_hits() + self.get_misses();
        if total == 0 {
            0.0
        } else {
            (self.get_hits() as f64 / total as f64) * 100.0
        }
    }
}

impl Default for AtomicStoreStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_stats_lock_free() {
        let stats = AtomicStoreStats::new();

        // Test lock-free operations
        stats.inc_reads();
        stats.inc_writes();
        stats.inc_hits();
        stats.add_memory(1024);

        assert_eq!(stats.get_reads(), 1);
        assert_eq!(stats.get_writes(), 1);
        assert_eq!(stats.get_hits(), 1);
        assert_eq!(stats.get_memory(), 1024);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let stats = AtomicStoreStats::new();

        for _ in 0..90 {
            stats.inc_hits();
        }
        for _ in 0..10 {
            stats.inc_misses();
        }

        assert_eq!(stats.hit_rate(), 90.0);
    }
}
