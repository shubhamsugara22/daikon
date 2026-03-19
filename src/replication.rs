use crate::error::{KvStoreError, Result};
use crate::kv_store::KvStore;
use crate::wal::{Wal, WalEntry, WalOperation};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

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

/// Master replication manager.
/// Tracks connected replicas and serves WAL entries for replication.
pub struct ReplicationMaster {
    wal: Arc<Wal>,
    replicas: Arc<RwLock<HashMap<String, ReplicaInfo>>>,
    heartbeat_timeout_secs: u64,
    /// Optional shared-secret. Replicas must send `Authorization: Bearer <token>`.
    auth_token: Option<String>,
}

impl ReplicationMaster {
    /// Create a new replication master.
    ///
    /// `auth_token` – if `Some(secret)`, inbound replica requests must carry
    /// `Authorization: Bearer <secret>`. Pass `None` to disable auth.
    pub fn new(wal: Arc<Wal>, heartbeat_timeout_secs: u64, auth_token: Option<String>) -> Self {
        if auth_token.is_some() {
            info!("Initializing replication master (auth enabled)");
        } else {
            info!("Initializing replication master (no auth)");
        }
        ReplicationMaster {
            wal,
            replicas: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout_secs,
            auth_token,
        }
    }

    /// Verify an auth token from an inbound request.
    /// Returns `true` if no auth is configured, or if `provided` matches the secret.
    pub fn verify_auth(&self, provided: Option<&str>) -> bool {
        match (&self.auth_token, provided) {
            (None, _) => true,
            (Some(expected), Some(token)) => token == expected.as_str(),
            (Some(_), None) => false,
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

/// Replica replication manager.
/// Pulls WAL entries from master and applies them to the local store.
pub struct ReplicationReplica {
    replica_id: String,
    master_url: String,
    store: Arc<RwLock<KvStore>>,
    wal: Arc<Wal>,
    last_applied_index: Arc<RwLock<u64>>,
    client: reqwest::blocking::Client,
    /// Must match the master's `auth_token` if auth is enabled.
    auth_token: Option<String>,
    /// Timestamps of entries applied this process lifetime — dedup guard.
    applied_entry_timestamps: Arc<RwLock<HashSet<u64>>>,
    /// Last successful sync completion time (unix secs).
    last_successful_sync_unix_secs: Arc<RwLock<Option<u64>>>,
    /// Last successful sync duration in milliseconds.
    last_sync_duration_ms: Arc<RwLock<Option<u64>>>,
    /// Last observed replica lag in entries.
    lag_entries: Arc<RwLock<u64>>,
}

impl ReplicationReplica {
    /// Create a new replication replica.
    ///
    /// `auth_token` – must match the master's secret (or `None` if auth is disabled).
    pub fn new(
        replica_id: String,
        master_url: String,
        store: Arc<RwLock<KvStore>>,
        wal: Arc<Wal>,
        auth_token: Option<String>,
    ) -> Result<Self> {
        info!(
            "Initializing replication replica: id={}, master={}, auth={}",
            replica_id,
            master_url,
            auth_token.is_some()
        );

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                KvStoreError::ReplicationError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Seed last_applied_index from local WAL so restarts are idempotent.
        let entries = wal.read_all()?;
        let last_applied_index = entries.len() as u64;

        Ok(ReplicationReplica {
            replica_id,
            master_url,
            store,
            wal,
            last_applied_index: Arc::new(RwLock::new(last_applied_index)),
            client,
            auth_token,
            applied_entry_timestamps: Arc::new(RwLock::new(HashSet::new())),
            last_successful_sync_unix_secs: Arc::new(RwLock::new(None)),
            last_sync_duration_ms: Arc::new(RwLock::new(None)),
            lag_entries: Arc::new(RwLock::new(0)),
        })
    }

    /// Sync with master: pull and apply new WAL entries.
    ///
    /// Idempotency guarantees:
    /// 1. Index-based protocol — only entries beyond `last_applied_index` are fetched.
    /// 2. Per-batch dedup — duplicate timestamps within one HTTP response are skipped.
    /// 3. Cross-sync dedup — entries whose timestamp was already applied this
    ///    process lifetime are skipped even if the master resends them.
    pub fn sync(&self) -> Result<usize> {
        let sync_started = Instant::now();
        let last_applied = *self.last_applied_index.read();

        // Register / heartbeat so the master can track our lag.
        self.send_heartbeat(last_applied)?;

        // Pull only entries we haven't applied yet.
        let (entries, total_entries) = self.fetch_wal_entries(last_applied)?;

        if entries.is_empty() {
            {
                let mut lag = self.lag_entries.write();
                *lag = total_entries.saturating_sub(last_applied);
            }

            let sync_finished_unix_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let duration_ms = sync_started.elapsed().as_millis() as u64;

            {
                let mut last_sync = self.last_successful_sync_unix_secs.write();
                *last_sync = Some(sync_finished_unix_secs);
            }
            {
                let mut last_duration = self.last_sync_duration_ms.write();
                *last_duration = Some(duration_ms);
            }

            debug!("No new entries to replicate");
            return Ok(0);
        }

        info!("Replicating {} entries from master", entries.len());

        let mut batch_seen: HashSet<u64> = HashSet::new();
        let mut applied_count: usize = 0;

        for entry in &entries {
            // Guard 1: intra-batch duplicates.
            if !batch_seen.insert(entry.timestamp) {
                warn!(
                    "Skipping intra-batch duplicate entry (timestamp={})",
                    entry.timestamp
                );
                continue;
            }

            // Guard 2: cross-sync duplicates.
            {
                let applied = self.applied_entry_timestamps.read();
                if applied.contains(&entry.timestamp) {
                    warn!(
                        "Skipping already-applied entry (timestamp={})",
                        entry.timestamp
                    );
                    continue;
                }
            }

            self.apply_wal_entry(entry)?;

            {
                let mut applied = self.applied_entry_timestamps.write();
                applied.insert(entry.timestamp);
            }

            applied_count += 1;
        }

        {
            let mut last = self.last_applied_index.write();
            *last += applied_count as u64;
        }

        let latest_applied = *self.last_applied_index.read();
        {
            let mut lag = self.lag_entries.write();
            *lag = total_entries.saturating_sub(latest_applied);
        }

        let sync_finished_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let duration_ms = sync_started.elapsed().as_millis() as u64;

        {
            let mut last_sync = self.last_successful_sync_unix_secs.write();
            *last_sync = Some(sync_finished_unix_secs);
        }
        {
            let mut last_duration = self.last_sync_duration_ms.write();
            *last_duration = Some(duration_ms);
        }

        Ok(applied_count)
    }

    // ── Auth-aware HTTP helpers ───────────────────────────────────────────────

    fn with_auth_post(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        let builder = self.client.post(url);
        if let Some(ref token) = self.auth_token {
            builder.header("Authorization", format!("Bearer {}", token))
        } else {
            builder
        }
    }

    fn with_auth_get(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        let builder = self.client.get(url);
        if let Some(ref token) = self.auth_token {
            builder.header("Authorization", format!("Bearer {}", token))
        } else {
            builder
        }
    }

    /// Send heartbeat to master and register replica.
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

        let response = self
            .with_auth_post(&url)
            .json(&request)
            .send()
            .map_err(|e| {
                KvStoreError::ReplicationError(format!("Failed to send heartbeat: {}", e))
            })?;

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(KvStoreError::ReplicationError(
                "Heartbeat rejected: authentication failed. Check KV_REPLICATION_SECRET."
                    .to_string(),
            ));
        }

        if !response.status().is_success() {
            return Err(KvStoreError::ReplicationError(format!(
                "Heartbeat failed with status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    /// Fetch WAL entries from master starting from the given index.
    fn fetch_wal_entries(&self, from_index: u64) -> Result<(Vec<WalEntry>, u64)> {
        let url = format!(
            "{}/api/replication/wal?from_index={}&limit=100",
            self.master_url, from_index
        );

        let response = self.with_auth_get(&url).send().map_err(|e| {
            KvStoreError::ReplicationError(format!("Failed to fetch WAL entries: {}", e))
        })?;

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(KvStoreError::ReplicationError(
                "WAL fetch rejected: authentication failed. Check KV_REPLICATION_SECRET."
                    .to_string(),
            ));
        }

        if !response.status().is_success() {
            return Err(KvStoreError::ReplicationError(format!(
                "Fetch WAL failed with status: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct WalResponse {
            entries: Vec<WalEntry>,
            total_entries: u64,
        }

        let wal_response: WalResponse = response.json().map_err(|e| {
            KvStoreError::ReplicationError(format!("Failed to parse WAL response: {}", e))
        })?;

        Ok((wal_response.entries, wal_response.total_entries))
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
            WalOperation::PfAdd { key, values } => {
                let _ = store.pfadd(key.clone(), values.clone());
            }
            WalOperation::PfMerge {
                destination,
                sources,
            } => {
                let _ = store.pfmerge(destination.clone(), sources);
            }
        }

        // Also append to local WAL for persistence
        self.wal.append(entry)?;

        Ok(())
    }

    /// Get current replication status
    pub fn get_status(&self) -> ReplicationStatus {
        let last_applied = *self.last_applied_index.read();
        let lag_entries = *self.lag_entries.read();
        let last_successful_sync_unix_secs = *self.last_successful_sync_unix_secs.read();
        let last_sync_duration_ms = *self.last_sync_duration_ms.read();

        ReplicationStatus {
            replica_id: self.replica_id.clone(),
            master_url: self.master_url.clone(),
            last_applied_index: last_applied,
            lag_entries,
            last_successful_sync_unix_secs,
            last_sync_duration_ms,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub replica_id: String,
    pub master_url: String,
    pub last_applied_index: u64,
    pub lag_entries: u64,
    pub last_successful_sync_unix_secs: Option<u64>,
    pub last_sync_duration_ms: Option<u64>,
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

        let master = ReplicationMaster::new(wal, 30, None);

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

        let master = ReplicationMaster::new(wal, 30, None);

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
            None,
        )
        .unwrap();

        let status = replica.get_status();
        assert_eq!(status.replica_id, "replica-1");
        assert_eq!(status.master_url, "http://localhost:8080");
        assert_eq!(status.last_applied_index, 0);
        assert_eq!(status.lag_entries, 0);
        assert_eq!(status.last_successful_sync_unix_secs, None);
        assert_eq!(status.last_sync_duration_ms, None);
    }

    // ── Auth tests ────────────────────────────────────────────────────────────

    #[test]
    fn test_master_verify_auth_no_token() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&temp_dir.path().join("wal.log")).unwrap());
        let master = ReplicationMaster::new(wal, 30, None);

        // No auth configured → all callers pass
        assert!(master.verify_auth(None));
        assert!(master.verify_auth(Some("anything")));
    }

    #[test]
    fn test_master_verify_auth_with_token() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&temp_dir.path().join("wal.log")).unwrap());
        let master = ReplicationMaster::new(wal, 30, Some("supersecret".to_string()));

        assert!(master.verify_auth(Some("supersecret")));
        assert!(!master.verify_auth(Some("wrongtoken")));
        assert!(!master.verify_auth(None));
    }

    // ── Idempotency / dedup tests ─────────────────────────────────────────────

    #[test]
    fn test_replica_apply_wal_entry_set() {
        let temp_dir = tempdir().unwrap();
        let store = Arc::new(RwLock::new(KvStore::new()));
        let wal = Arc::new(Wal::new(&temp_dir.path().join("wal.log")).unwrap());

        let replica = ReplicationReplica::new(
            "replica-1".to_string(),
            "http://localhost:8080".to_string(),
            Arc::clone(&store),
            Arc::clone(&wal),
            None,
        )
        .unwrap();

        // Apply a SET entry directly
        let entry = WalEntry::new(WalOperation::Set {
            key: "hello".to_string(),
            value: "\"world\"".to_string(),
            ttl_secs: None,
        });
        replica.apply_wal_entry(&entry).unwrap();

        // Key should exist in the store
        let store_guard = store.read();
        assert!(store_guard.get("hello").is_some());
    }

    #[test]
    fn test_replica_dedup_cross_sync_guard() {
        let temp_dir = tempdir().unwrap();
        let store = Arc::new(RwLock::new(KvStore::new()));
        let wal = Arc::new(Wal::new(&temp_dir.path().join("wal.log")).unwrap());

        let replica = ReplicationReplica::new(
            "replica-1".to_string(),
            "http://localhost:8080".to_string(),
            Arc::clone(&store),
            wal,
            None,
        )
        .unwrap();

        // Simulate a previously applied entry by inserting its timestamp
        let ts = 123_456_u64;
        {
            let mut applied = replica.applied_entry_timestamps.write();
            applied.insert(ts);
        }

        // That timestamp should be detected as a duplicate
        let is_dup = {
            let applied = replica.applied_entry_timestamps.read();
            applied.contains(&ts)
        };
        assert!(is_dup);

        // A fresh timestamp should not be flagged
        let is_fresh = {
            let applied = replica.applied_entry_timestamps.read();
            !applied.contains(&999_999_u64)
        };
        assert!(is_fresh);
    }
}
