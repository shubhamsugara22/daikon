use crate::error::{KvStoreError, Result};
use crate::kv_store::KvStore;
use crate::wal::{Wal, WalEntry, WalOperation};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Snapshot metadata for Point-in-Time Recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub timestamp: u64,
    pub num_keys: usize,
    pub num_operations: u64,
    pub snapshot_file: String,
}

/// Point-in-Time Recovery manager
/// Enables restoring the database to any point in time using snapshots and WAL replay
pub struct Pitr {
    snapshots_dir: PathBuf,
    wal: std::sync::Arc<Wal>,
}

impl Pitr {
    fn extract_snapshot_timestamp(filename: &str) -> Option<u64> {
        let core = filename.strip_prefix("snapshot_")?.strip_suffix(".json")?;
        let ts_part = core.split('_').next()?;
        ts_part.parse::<u64>().ok()
    }

    /// Create a new PITR manager
    pub fn new<P: AsRef<Path>>(snapshots_dir: P, wal: std::sync::Arc<Wal>) -> Result<Self> {
        let snapshots_dir = snapshots_dir.as_ref().to_path_buf();
        fs::create_dir_all(&snapshots_dir)?;
        info!(
            "Initialized PITR with snapshots directory: {}",
            snapshots_dir.display()
        );
        Ok(Pitr { snapshots_dir, wal })
    }

    /// Create a snapshot of the current database state
    /// Returns the snapshot metadata
    pub fn create_snapshot(&self, store: &KvStore) -> Result<SnapshotMetadata> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut snapshot_filename = format!("snapshot_{}.json", timestamp);
        let mut snapshot_path = self.snapshots_dir.join(&snapshot_filename);
        let mut suffix = 1u64;
        while snapshot_path.exists() {
            snapshot_filename = format!("snapshot_{}_{}.json", timestamp, suffix);
            snapshot_path = self.snapshots_dir.join(&snapshot_filename);
            suffix += 1;
        }

        // Serialize the store state
        let store_json = serde_json::to_string_pretty(&store)
            .map_err(|e| KvStoreError::SerializationError(e))?;

        fs::write(&snapshot_path, store_json).map_err(|e| KvStoreError::IoError(e))?;

        let num_keys = store.len();
        let num_operations = self.wal.read_all()?.len() as u64;

        let metadata = SnapshotMetadata {
            timestamp,
            num_keys,
            num_operations,
            snapshot_file: snapshot_filename,
        };

        info!(
            "Created snapshot at {} with {} keys and {} operations",
            timestamp, num_keys, num_operations
        );

        Ok(metadata)
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        let mut snapshots = Vec::new();

        for entry in fs::read_dir(&self.snapshots_dir).map_err(|e| KvStoreError::IoError(e))? {
            let entry = entry.map_err(|e| KvStoreError::IoError(e))?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("snapshot_") {
                        if let Some(timestamp) = Self::extract_snapshot_timestamp(filename) {
                            snapshots.push(SnapshotMetadata {
                                timestamp,
                                num_keys: 0,       // Could read from metadata file
                                num_operations: 0, // Could track separately
                                snapshot_file: filename.to_string(),
                            });
                        }
                    }
                }
            }
        }

        snapshots.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        info!("Found {} snapshots", snapshots.len());
        Ok(snapshots)
    }

    /// Recover to a specific timestamp
    /// Returns the restored store state
    pub fn recover_to_timestamp(&self, target_timestamp: u64) -> Result<KvStore> {
        let snapshots = self.list_snapshots()?;

        // Find the most recent snapshot before target_timestamp
        let base_snapshot = snapshots.iter().find(|s| s.timestamp <= target_timestamp);

        let mut store = if let Some(snapshot) = base_snapshot {
            // Load from snapshot
            let snapshot_path = self.snapshots_dir.join(&snapshot.snapshot_file);
            let snapshot_data =
                fs::read_to_string(&snapshot_path).map_err(|e| KvStoreError::IoError(e))?;

            serde_json::from_str::<KvStore>(&snapshot_data)
                .map_err(|e| KvStoreError::SerializationError(e))?
        } else {
            // No snapshot found, start from empty store
            KvStore::new()
        };

        // Replay WAL entries up to target_timestamp
        let wal_entries = self.wal.read_all()?;
        let mut replayed = 0;

        for entry in wal_entries {
            if entry.timestamp > target_timestamp {
                break; // Stop when we reach the target time
            }

            if let Err(e) = self.apply_wal_entry(&mut store, entry) {
                warn!("Failed to apply WAL entry during PITR: {}", e);
                // Continue with next entry for resilience
            }
            replayed += 1;
        }

        info!(
            "Recovered to timestamp {} by replaying {} WAL entries",
            target_timestamp, replayed
        );
        Ok(store)
    }

    /// Recover to the most recent snapshot
    pub fn recover_to_latest_snapshot(&self) -> Result<KvStore> {
        let snapshots = self.list_snapshots()?;

        if snapshots.is_empty() {
            return Err(KvStoreError::OperationFailed(
                "No snapshots available for recovery".to_string(),
            ));
        }

        let latest_snapshot = &snapshots[0]; // Already sorted in descending order
        self.recover_to_timestamp(latest_snapshot.timestamp)
    }

    /// Delete snapshots older than the specified age (in seconds)
    pub fn cleanup_old_snapshots(&self, max_age_secs: u64) -> Result<usize> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cutoff_time = now.saturating_sub(max_age_secs);
        let mut deleted_count = 0;

        for entry in fs::read_dir(&self.snapshots_dir).map_err(|e| KvStoreError::IoError(e))? {
            let entry = entry.map_err(|e| KvStoreError::IoError(e))?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("snapshot_") {
                        if let Some(timestamp) = Self::extract_snapshot_timestamp(filename) {
                            if timestamp < cutoff_time {
                                fs::remove_file(path)?;
                                deleted_count += 1;
                            }
                        }
                    }
                }
            }
        }

        info!("Cleaned up {} old snapshots", deleted_count);
        Ok(deleted_count)
    }

    /// Apply a single WAL entry to the store
    fn apply_wal_entry(&self, store: &mut KvStore, entry: WalEntry) -> Result<()> {
        match entry.operation {
            WalOperation::Set {
                key,
                value,
                ttl_secs,
            } => {
                if let Some(ttl) = ttl_secs {
                    store.set_with_ttl(key, value, std::time::Duration::from_secs(ttl))?;
                } else {
                    store.set(key, value)?;
                }
            }
            WalOperation::Delete { key } => {
                store.delete(&key);
            }
            WalOperation::Incr { key } => {
                store.incr(&key)?;
            }
            WalOperation::Decr { key } => {
                store.decr(&key)?;
            }
            WalOperation::IncrBy { key, amount } => {
                store.incrby(&key, amount)?;
            }
            WalOperation::Append { key, value } => {
                store.append(&key, &value)?;
            }
            WalOperation::GetSet { key, value } => {
                store.getset(key, value)?;
            }
            WalOperation::Mset { pairs } => {
                store.mset(pairs)?;
            }
        }
        Ok(())
    }

    /// Get recovery point statistics
    pub fn get_recovery_stats(&self) -> Result<RecoveryStats> {
        let snapshots = self.list_snapshots()?;
        let wal_entries = self.wal.read_all()?;

        let earliest_timestamp = wal_entries.first().map(|e| e.timestamp);
        let latest_timestamp = wal_entries.last().map(|e| e.timestamp);

        Ok(RecoveryStats {
            total_snapshots: snapshots.len(),
            total_wal_entries: wal_entries.len() as u64,
            earliest_recovery_point: earliest_timestamp,
            latest_recovery_point: latest_timestamp,
            wal_file_size: self.wal.size()?,
        })
    }
}

/// Statistics about available recovery points
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryStats {
    pub total_snapshots: usize,
    pub total_wal_entries: u64,
    pub earliest_recovery_point: Option<u64>,
    pub latest_recovery_point: Option<u64>,
    pub wal_file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_pitr_create_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let snapshots_dir = temp_dir.path().join("snapshots");
        let wal_dir = temp_dir.path().join("wal");
        let wal = std::sync::Arc::new(Wal::new(wal_dir.join("test.wal")).unwrap());

        let pitr = Pitr::new(snapshots_dir.clone(), wal).unwrap();
        let store = KvStore::new();

        let metadata = pitr.create_snapshot(&store).unwrap();
        assert_eq!(metadata.num_keys, 0);

        let snapshots = pitr.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
    }

    #[test]
    fn test_pitr_list_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let snapshots_dir = temp_dir.path().join("snapshots");
        let wal_dir = temp_dir.path().join("wal");
        let wal = std::sync::Arc::new(Wal::new(wal_dir.join("test.wal")).unwrap());

        let pitr = Pitr::new(snapshots_dir, wal).unwrap();
        let store = KvStore::new();

        pitr.create_snapshot(&store).unwrap();
        pitr.create_snapshot(&store).unwrap();

        let snapshots = pitr.list_snapshots().unwrap();
        assert!(snapshots.len() >= 2);
    }

    #[test]
    fn test_pitr_recovery_stats() {
        let temp_dir = TempDir::new().unwrap();
        let snapshots_dir = temp_dir.path().join("snapshots");
        let wal_dir = temp_dir.path().join("wal");
        let wal = std::sync::Arc::new(Wal::new(wal_dir.join("test.wal")).unwrap());

        let pitr = Pitr::new(snapshots_dir, wal).unwrap();
        let stats = pitr.get_recovery_stats().unwrap();
        assert_eq!(stats.total_snapshots, 0);
        assert_eq!(stats.total_wal_entries, 0);
    }
}
