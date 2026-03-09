use crate::error::{KvStoreError, Result};
use crate::kv_store::KvStore;
use crate::wal::{Wal, WalEntry, WalOperation};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Replication role for a KV store node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationRole {
    Master,
    Replica,
}

/// Information about a replica node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub last_applied_index: u64,
    pub last_heartbeat: u64,
    pub lag: u64,
    pub status: ReplicaStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaStatus {
    Healthy,
    Lagging,
    Disconnected,
}

/// Master replication manager
/// Tracks connected replicas and serves WAL entries for replication
pub struct ReplicationMaster {
    wal: Arc<Wal>,
    replicas: Arc<RwLock<HashMap<String, ReplicaInfo>>>,
    heartbeat_timeout_secs: u64,
}

impl ReplicationMaster {
    /// Create a new replication master
    pub fn new(wal: Arc<Wal>, heartbeat_timeout_secs: u64) -> Self {
        info!("Initializing replication master");
        ReplicationMaster {
            wal,
            replicas: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout_secs,
        }
    }

    /// Register or update a replica's heartbeat
    pub fn register_replica(&self, replica_id: String, last_applied_index: u64) -> Result<()> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut replicas = self.replicas.write();

        let total_entries = self.wal.read_all()?.len() as u64;
        let lag = total_entries.saturating_sub(last_applied_index);

        let status = if lag > 100 {
            ReplicaStatus::Lagging
        } else {
            ReplicaStatus::Healthy
        };

        replicas.insert(
            replica_id.clone(),
            ReplicaInfo {
                replica_id,
                last_applied_index,
                last_heartbeat: current_time,
                lag,
                status,
            },
        );

        Ok(())
    }

    /// Get WAL entries for replication starting from the given index
    pub fn get_wal_entries(&self, from_index: u64, limit: usize) -> Result<Vec<WalEntry>> {
        let all_entries = self.wal.read_all()?;

        let start = from_index as usize;
        if start >= all_entries.len() {
            return Ok(Vec::new());
        }

        let end = std::cmp::min(start + limit, all_entries.len());
        Ok(all_entries[start..end].to_vec())
    }

    /// Get information about all connected replicas
    pub fn get_replicas_info(&self) -> Vec<ReplicaInfo> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut replicas = self.replicas.write();

        // Update status based on heartbeat timeout
        for replica in replicas.values_mut() {
            let time_since_heartbeat = current_time.saturating_sub(replica.last_heartbeat);
            if time_since_heartbeat > self.heartbeat_timeout_secs {
                replica.status = ReplicaStatus::Disconnected;
            }
        }

        replicas.values().cloned().collect()
    }

    /// Remove a replica from tracking
    pub fn remove_replica(&self, replica_id: &str) -> Result<()> {
        let mut replicas = self.replicas.write();
        replicas.remove(replica_id);
        info!("Removed replica: {}", replica_id);
        Ok(())
    }

    /// Get total WAL entry count
    pub fn get_total_entries(&self) -> Result<u64> {
        Ok(self.wal.read_all()?.len() as u64)
    }
}

/// Replica replication manager
/// Pulls WAL entries from master and applies them to local store
pub struct ReplicationReplica {
    replica_id: String,
    master_url: String,
    store: Arc<RwLock<KvStore>>,
    wal: Arc<Wal>,
    last_applied_index: Arc<RwLock<u64>>,
    client: reqwest::blocking::Client,
}

impl ReplicationReplica {
    /// Create a new replication replica
    pub fn new(
        replica_id: String,
        master_url: String,
        store: Arc<RwLock<KvStore>>,
        wal: Arc<Wal>,
    ) -> Result<Self> {
        info!(
            "Initializing replication replica: id={}, master={}",
            replica_id, master_url
        );

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                KvStoreError::ReplicationError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Determine last applied index from local WAL
        let entries = wal.read_all()?;
        let last_applied_index = entries.len() as u64;

        Ok(ReplicationReplica {
            replica_id,
            master_url,
            store,
            wal,
            last_applied_index: Arc::new(RwLock::new(last_applied_index)),
            client,
        })
    }

    /// Sync with master: pull and apply new WAL entries
    pub fn sync(&self) -> Result<usize> {
        let last_applied = *self.last_applied_index.read();

        // Register/send heartbeat to master
        self.send_heartbeat(last_applied)?;

        // Fetch new WAL entries from master
        let entries = self.fetch_wal_entries(last_applied)?;

        if entries.is_empty() {
            debug!("No new entries to replicate");
            return Ok(0);
        }

        info!("Replicating {} entries from master", entries.len());

        // Apply each entry to local store
        let mut applied_count: usize = 0;
        for entry in entries {
            self.apply_wal_entry(&entry)?;
            applied_count += 1;
        }

        // Update last applied index
        let mut last_applied = self.last_applied_index.write();
        *last_applied += applied_count as u64;

        Ok(applied_count)
    }

    /// Send heartbeat to master and register replica
    fn send_heartbeat(&self, last_applied_index: u64) -> Result<()> {
        let url = format!("{}/api/replication/heartbeat", self.master_url);

        #[derive(Serialize)]
        struct HeartbeatRequest {
            replica_id: String,
            last_applied_index: u64,
        }

        let request = HeartbeatRequest {
            replica_id: self.replica_id.clone(),
            last_applied_index,
        };

        let response = self.client.post(&url).json(&request).send().map_err(|e| {
            KvStoreError::ReplicationError(format!("Failed to send heartbeat: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(KvStoreError::ReplicationError(format!(
                "Heartbeat failed with status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    /// Fetch WAL entries from master starting from given index
    fn fetch_wal_entries(&self, from_index: u64) -> Result<Vec<WalEntry>> {
        let url = format!(
            "{}/api/replication/wal?from_index={}&limit=100",
            self.master_url, from_index
        );

        let response = self.client.get(&url).send().map_err(|e| {
            KvStoreError::ReplicationError(format!("Failed to fetch WAL entries: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(KvStoreError::ReplicationError(format!(
                "Fetch WAL failed with status: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct WalResponse {
            entries: Vec<WalEntry>,
        }

        let wal_response: WalResponse = response.json().map_err(|e| {
            KvStoreError::ReplicationError(format!("Failed to parse WAL response: {}", e))
        })?;

        Ok(wal_response.entries)
    }

    /// Apply a WAL entry to the local store
    fn apply_wal_entry(&self, entry: &WalEntry) -> Result<()> {
        let mut store = self.store.write();

        match &entry.operation {
            WalOperation::Set {
                key,
                value,
                ttl_secs,
            } => {
                if let Some(ttl) = ttl_secs {
                    let ttl_duration = std::time::Duration::from_secs(*ttl);
                    store.set_with_ttl(key.clone(), value.clone(), ttl_duration)?;
                } else {
                    store.set(key.clone(), value.clone())?;
                }
            }
            WalOperation::Delete { key } => {
                let _ = store.delete(key); // Returns Option<Value>
            }
            WalOperation::Incr { key } => {
                store.incr(key)?;
            }
            WalOperation::Decr { key } => {
                store.decr(key)?;
            }
            WalOperation::IncrBy { key, amount } => {
                store.incrby(key, *amount)?;
            }
            WalOperation::Append { key, value } => {
                store.append(key, value)?;
            }
            WalOperation::GetSet { key, value } => {
                let _ = store.getset(key.clone(), value.clone())?;
            }
            WalOperation::Mset { pairs } => {
                store.mset(pairs.clone())?;
            }
        }

        // Also append to local WAL for persistence
        self.wal.append(entry)?;

        Ok(())
    }

    /// Get current replication status
    pub fn get_status(&self) -> ReplicationStatus {
        let last_applied = *self.last_applied_index.read();

        ReplicationStatus {
            replica_id: self.replica_id.clone(),
            master_url: self.master_url.clone(),
            last_applied_index: last_applied,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub replica_id: String,
    pub master_url: String,
    pub last_applied_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_replication_master_register_replica() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_wal.log");
        let wal = Arc::new(Wal::new(&wal_path).unwrap());

        let master = ReplicationMaster::new(wal, 30);

        // Register a replica
        master.register_replica("replica-1".to_string(), 0).unwrap();

        // Check replicas info
        let replicas = master.get_replicas_info();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].replica_id, "replica-1");
        assert_eq!(replicas[0].last_applied_index, 0);
    }

    #[test]
    fn test_replication_master_get_wal_entries() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_wal.log");
        let wal = Arc::new(Wal::new(&wal_path).unwrap());

        // Add some WAL entries
        for i in 0..10 {
            let entry = WalEntry::new(WalOperation::Set {
                key: format!("key{}", i),
                value: format!("value{}", i),
                ttl_secs: None,
            });
            wal.append(&entry).unwrap();
        }

        let master = ReplicationMaster::new(wal, 30);

        // Get entries from index 3, limit 5
        let entries = master.get_wal_entries(3, 5).unwrap();
        assert_eq!(entries.len(), 5);

        // Get entries from index 8, limit 5
        let entries = master.get_wal_entries(8, 5).unwrap();
        assert_eq!(entries.len(), 2); // Only 2 entries left

        // Get entries from index 100 (out of bounds)
        let entries = master.get_wal_entries(100, 5).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_replication_replica_status() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let store = Arc::new(RwLock::new(KvStore::new()));
        let wal = Arc::new(Wal::new(&wal_path).unwrap());

        let replica = ReplicationReplica::new(
            "replica-1".to_string(),
            "http://localhost:8080".to_string(),
            store,
            wal,
        )
        .unwrap();

        let status = replica.get_status();
        assert_eq!(status.replica_id, "replica-1");
        assert_eq!(status.master_url, "http://localhost:8080");
        assert_eq!(status.last_applied_index, 0);
    }
}
