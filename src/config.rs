use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the KV Store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Maximum key size in bytes
    #[serde(default = "default_max_key_size")]
    pub max_key_size: usize,

    /// Maximum value size in bytes
    #[serde(default = "default_max_value_size")]
    pub max_value_size: usize,

    /// Maximum total memory in bytes (0 = unlimited)
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: usize,

    /// Maximum number of keys (0 = unlimited)
    #[serde(default = "default_max_keys")]
    pub max_keys: usize,

    /// Enable LRU eviction when memory limit is reached
    #[serde(default = "default_lru_enabled")]
    pub lru_eviction_enabled: bool,

    /// TTL cleanup interval in seconds
    #[serde(default = "default_ttl_cleanup_interval")]
    pub ttl_cleanup_interval_secs: u64,

    /// Path for persistence
    #[serde(default = "default_persist_path")]
    pub persist_path: PathBuf,

    /// Automatic snapshot interval in seconds (0 = disabled)
    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,

    /// Number of backup versions to keep
    #[serde(default = "default_backup_versions")]
    pub backup_versions: usize,

    /// Enable detailed logging
    #[serde(default = "default_enable_logging")]
    pub enable_logging: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            max_key_size: default_max_key_size(),
            max_value_size: default_max_value_size(),
            max_memory_bytes: default_max_memory(),
            max_keys: default_max_keys(),
            lru_eviction_enabled: default_lru_enabled(),
            ttl_cleanup_interval_secs: default_ttl_cleanup_interval(),
            persist_path: default_persist_path(),
            snapshot_interval_secs: default_snapshot_interval_secs(),
            backup_versions: default_backup_versions(),
            enable_logging: default_enable_logging(),
        }
    }
}

// Default value functions
fn default_max_key_size() -> usize {
    1024 // 1KB
}

fn default_max_value_size() -> usize {
    10 * 1024 * 1024 // 10MB
}

fn default_max_memory() -> usize {
    1024 * 1024 * 1024 // 1GB
}

fn default_max_keys() -> usize {
    1_000_000 // 1 million keys
}

fn default_lru_enabled() -> bool {
    true
}

fn default_ttl_cleanup_interval() -> u64 {
    60 // 60 seconds
}

fn default_persist_path() -> PathBuf {
    PathBuf::from("store.json")
}

fn default_snapshot_interval_secs() -> u64 {
    0
}

fn default_backup_versions() -> usize {
    3
}

fn default_enable_logging() -> bool {
    true
}

impl StoreConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from a file
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: StoreConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to a file
    pub fn save_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), String> {
        if self.max_key_size == 0 {
            return Err("max_key_size must be greater than 0".to_string());
        }
        if self.max_value_size == 0 {
            return Err("max_value_size must be greater than 0".to_string());
        }
        if self.max_key_size > self.max_value_size {
            return Err("max_key_size cannot be larger than max_value_size".to_string());
        }
        if self.backup_versions == 0 {
            return Err("backup_versions must be at least 1".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StoreConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.max_key_size, 1024);
        assert_eq!(config.snapshot_interval_secs, 0);
        assert_eq!(config.backup_versions, 3);
    }

    #[test]
    fn test_invalid_config() {
        let config = StoreConfig {
            max_key_size: 0,
            ..StoreConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
