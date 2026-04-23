use crate::error::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Write-Ahead Log entry for durability
/// Every write operation is logged before being applied
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub timestamp: u64,
    pub operation: WalOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum WalOperation {
    #[serde(rename = "SET")]
    Set {
        key: String,
        value: String,
        ttl_secs: Option<u64>,
    },
    #[serde(rename = "DELETE")]
    Delete { key: String },
    #[serde(rename = "INCR")]
    Incr { key: String },
    #[serde(rename = "DECR")]
    Decr { key: String },
    #[serde(rename = "INCRBY")]
    IncrBy { key: String, amount: i64 },
    #[serde(rename = "APPEND")]
    Append { key: String, value: String },
    #[serde(rename = "GETSET")]
    GetSet { key: String, value: String },
    #[serde(rename = "MSET")]
    Mset { pairs: Vec<(String, String)> },
    #[serde(rename = "PFADD")]
    PfAdd { key: String, values: Vec<String> },
    #[serde(rename = "PFMERGE")]
    PfMerge {
        destination: String,
        sources: Vec<String>,
    },
    #[serde(rename = "LPUSH")]
    LPush { key: String, values: Vec<String> },
    #[serde(rename = "RPUSH")]
    RPush { key: String, values: Vec<String> },
    #[serde(rename = "LPOP")]
    LPop { key: String },
    #[serde(rename = "RPOP")]
    RPop { key: String },
    #[serde(rename = "EXPIRE")]
    Expire { key: String, ttl_secs: u64 },
    #[serde(rename = "PERSIST")]
    Persist { key: String },
}

impl WalEntry {
    pub fn new(operation: WalOperation) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        WalEntry {
            timestamp,
            operation,
        }
    }
}

/// Write-Ahead Logger: logs all write operations before applying them
pub struct Wal {
    path: std::path::PathBuf,
}

impl Wal {
    /// Create or open a WAL at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Ensure the file exists
        if !path.exists() {
            std::fs::File::create(&path)?;
        }

        info!("Initialized WAL at: {}", path.display());
        Ok(Wal { path })
    }

    /// Append a WAL entry to the log file
    pub fn append(&self, entry: &WalEntry) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;

        let json_line = serde_json::to_string(entry).map_err(KvStoreError::SerializationError)?;

        writeln!(file, "{}", json_line).map_err(KvStoreError::IoError)?;

        debug!("WAL entry logged: {:?}", entry.operation);
        Ok(())
    }

    /// Read and return all WAL entries from the log
    pub fn read_all(&self) -> Result<Vec<WalEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(&self.path).map_err(KvStoreError::IoError)?;

        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for (line_no, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| {
                KvStoreError::OperationFailed(format!("Failed to read WAL line {}: {}", line_no, e))
            })?;

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("Failed to parse WAL entry at line {}: {}", line_no, e);
                    continue; // Skip malformed entries
                }
            }
        }

        info!("Read {} WAL entries from log", entries.len());
        Ok(entries)
    }

    /// Clear the WAL (called after taking a snapshot)
    pub fn clear(&self) -> Result<()> {
        std::fs::File::create(&self.path)
            .map_err(|e| KvStoreError::OperationFailed(format!("Failed to clear WAL: {}", e)))?;
        info!("WAL cleared");
        Ok(())
    }

    /// Get the file size in bytes
    pub fn size(&self) -> Result<u64> {
        std::fs::metadata(&self.path)
            .map(|m| m.len())
            .map_err(|e| KvStoreError::OperationFailed(format!("Failed to get WAL size: {}", e)))
    }

    /// Get the path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let wal = Wal::new(&wal_path).unwrap();

        // Append entries
        let entry1 = WalEntry::new(WalOperation::Set {
            key: "key1".to_string(),
            value: "value1".to_string(),
            ttl_secs: None,
        });
        wal.append(&entry1).unwrap();

        let entry2 = WalEntry::new(WalOperation::Delete {
            key: "key2".to_string(),
        });
        wal.append(&entry2).unwrap();

        // Read back
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);

        if let WalOperation::Set { key, value, .. } = &entries[0].operation {
            assert_eq!(key, "key1");
            assert_eq!(value, "value1");
        } else {
            panic!("Expected SET operation");
        }

        if let WalOperation::Delete { key } = &entries[1].operation {
            assert_eq!(key, "key2");
        } else {
            panic!("Expected DELETE operation");
        }
    }

    #[test]
    fn test_wal_clear() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let wal = Wal::new(&wal_path).unwrap();

        // Add entries
        let entry = WalEntry::new(WalOperation::Set {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_secs: None,
        });
        wal.append(&entry).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);

        // Clear
        wal.clear().unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_wal_size() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let wal = Wal::new(&wal_path).unwrap();

        let size_before = wal.size().unwrap();

        let entry = WalEntry::new(WalOperation::Set {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_secs: None,
        });
        wal.append(&entry).unwrap();

        let size_after = wal.size().unwrap();
        assert!(size_after > size_before);
    }
}
