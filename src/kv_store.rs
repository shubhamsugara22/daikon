use crate::config::StoreConfig;
use crate::error::{KvStoreError, Result};
use crate::hyperloglog::HyperLogLog;
use flate2::Compression;
use flate2::{read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCompression {
    None,
    Gzip,
    Zstd,
}

fn compression_for_path(path: &Path) -> FileCompression {
    match path.extension().and_then(|e| e.to_str()) {
        Some("gz") => FileCompression::Gzip,
        Some("zst") => FileCompression::Zstd,
        _ => FileCompression::None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StoreStats {
    pub total_keys: usize,
    pub expired_keys: usize,
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
    pub hits: u64,
    pub misses: u64,
    pub memory_bytes: usize,
    pub evictions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HllInfo {
    pub precision: u8,
    pub registers: usize,
    pub memory_bytes: usize,
    pub estimated_count: u64,
}

/// Detailed memory usage breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfile {
    pub total_bytes: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub string_values: usize,
    pub int_values: usize,
    pub float_values: usize,
    pub bool_values: usize,
    pub json_values: usize,
    pub hyperloglog_values: usize,
    pub ttl_entries: usize,
    pub heap_fragmentation_ratio: f64,
}

impl Default for MemoryProfile {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            key_bytes: 0,
            value_bytes: 0,
            string_values: 0,
            int_values: 0,
            float_values: 0,
            bool_values: 0,
            json_values: 0,
            hyperloglog_values: 0,
            ttl_entries: 0,
            heap_fragmentation_ratio: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueWithTTL {
    pub value: Value,
    pub expires_at: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Json(JsonValue),
    HyperLogLog(HyperLogLog),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Str(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(x) => write!(f, "{}", x),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Json(j) => write!(f, "{}", j),
            Value::HyperLogLog(hll) => write!(f, "HyperLogLog(count≈{})", hll.count()),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(s)
    }
}
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.to_string())
    }
}
impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}
impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}
impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}
impl From<JsonValue> for Value {
    fn from(j: JsonValue) -> Self {
        Value::Json(j)
    }
}

/// Transaction operation for MULTI/EXEC support
#[derive(Debug, Clone)]
pub enum TransactionOp {
    Set(String, Value, Option<u64>), // key, value, expires_at
    Delete(String),                  // key
    Incr(String),
    Decr(String),
    IncrBy(String, i64),
    Append(String, String),
}

#[derive(Serialize, Deserialize)]
pub struct KvStore {
    store: HashMap<String, ValueWithTTL>,
    #[serde(default)]
    stats: StoreStats,
    #[serde(skip)]
    config: StoreConfig,
    #[serde(skip)]
    lru_order: Vec<String>,
    #[serde(skip)]
    transaction_queue: Option<Vec<TransactionOp>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self::with_config(StoreConfig::default())
    }

    pub fn with_config(config: StoreConfig) -> Self {
        info!("Initializing KvStore with config: {:?}", config);
        KvStore {
            store: HashMap::new(),
            stats: StoreStats::default(),
            config,
            lru_order: Vec::new(),
            transaction_queue: None,
        }
    }
    pub fn set_with_ttl<V>(&mut self, key: String, value: V, ttl: Duration) -> Result<()>
    where
        V: Into<Value>,
    {
        self.validate_key(&key)?;
        let value = value.into();
        self.validate_value(&value)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now.checked_add(ttl.as_secs());

        if self.in_transaction() {
            self.queue_operation(TransactionOp::Set(key, value, expires_at))?;
            return Ok(());
        }

        let value_size = self.estimate_value_size(&value);
        self.store
            .insert(key.clone(), ValueWithTTL { value, expires_at });

        self.update_lru(&key);
        self.stats.total_writes += 1;
        self.stats.total_keys = self.store.len();
        self.stats.memory_bytes += value_size;

        // Enforce memory limits after updating memory usage
        if self.config.max_memory_bytes > 0 && self.config.lru_eviction_enabled {
            self.enforce_memory_limit()?;
        }

        debug!("Set key '{}' with TTL {:?}", key, ttl);
        Ok(())
    }

    pub fn set<V>(&mut self, key: String, value: V) -> Result<()>
    where
        V: Into<Value>,
    {
        self.validate_key(&key)?;
        let value = value.into();
        self.validate_value(&value)?;

        if self.in_transaction() {
            self.queue_operation(TransactionOp::Set(key, value, None))?;
            return Ok(());
        }

        let value_size = self.estimate_value_size(&value);
        self.store.insert(
            key.clone(),
            ValueWithTTL {
                value,
                expires_at: None,
            },
        );

        self.update_lru(&key);
        self.stats.total_writes += 1;
        self.stats.total_keys = self.store.len();
        self.stats.memory_bytes += value_size;

        // Enforce memory limits after updating memory usage
        if self.config.max_memory_bytes > 0 && self.config.lru_eviction_enabled {
            self.enforce_memory_limit()?;
        }

        debug!("Set key '{}'", key);
        Ok(())
    }
    /// Generic set: accepts any type convertible into Value
    /// Pure read operation with no side effects (for concurrent read access)
    /// Does NOT update statistics - this allows the operation to be lock-free
    pub fn get(&self, key: &str) -> Option<&Value> {
        let result = self.store.get(key).and_then(|v| {
            if let Some(exp) = v.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > exp {
                    return None;
                }
            }
            Some(&v.value)
        });
        result
    }

    /// Convenience getters...
    pub fn get_string(&mut self, key: &str) -> Option<String> {
        match self.get(key) {
            Some(Value::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }
    pub fn get_i64(&mut self, key: &str) -> Option<i64> {
        match self.get(key) {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        }
    }
    pub fn get_f64(&mut self, key: &str) -> Option<f64> {
        match self.get(key) {
            Some(Value::Float(x)) => Some(*x),
            _ => None,
        }
    }
    pub fn get_bool(&mut self, key: &str) -> Option<bool> {
        match self.get(key) {
            Some(Value::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    pub fn delete(&mut self, key: &str) -> Option<Value> {
        if self.in_transaction() {
            let prior = self.store.get(key).map(|v| v.value.clone());
            let _ = self.queue_operation(TransactionOp::Delete(key.to_string()));
            return prior;
        }

        let result = self.store.remove(key).map(|v| v.value);
        if result.is_some() {
            self.stats.total_deletes += 1;
        }
        result
    }

    // Atomic Operations

    /// Increment an integer value atomically. Returns new value or error if key doesn't exist or isn't an integer.
    pub fn incr(&mut self, key: &str) -> Result<i64> {
        let result = match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val += 1;
                    self.stats.total_writes += 1;
                    let new_val = *val;
                    debug!("Incremented key '{}' to {}", key, new_val);
                    Ok(new_val)
                } else {
                    let type_name = match &entry.value {
                        Value::Str(_) => "Str",
                        Value::Int(_) => "Int",
                        Value::Float(_) => "Float",
                        Value::Bool(_) => "Bool",
                        Value::Json(_) => "Json",
                        Value::HyperLogLog(_) => "HyperLogLog",
                    };
                    Err(KvStoreError::type_mismatch(key, "Int", type_name))
                }
            }
            None => Err(KvStoreError::KeyNotFound(key.to_string())),
        };

        if result.is_ok() {
            self.update_lru(&key.to_string());
        }
        result
    }

    /// Decrement an integer value atomically. Returns new value or error.
    pub fn decr(&mut self, key: &str) -> Result<i64> {
        let result = match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val -= 1;
                    self.stats.total_writes += 1;
                    let new_val = *val;
                    debug!("Decremented key '{}' to {}", key, new_val);
                    Ok(new_val)
                } else {
                    let type_name = match &entry.value {
                        Value::Str(_) => "Str",
                        Value::Int(_) => "Int",
                        Value::Float(_) => "Float",
                        Value::Bool(_) => "Bool",
                        Value::Json(_) => "Json",
                        Value::HyperLogLog(_) => "HyperLogLog",
                    };
                    Err(KvStoreError::type_mismatch(key, "Int", type_name))
                }
            }
            None => Err(KvStoreError::KeyNotFound(key.to_string())),
        };

        if result.is_ok() {
            self.update_lru(&key.to_string());
        }
        result
    }

    /// Increment by a specific amount. Returns new value or error.
    pub fn incrby(&mut self, key: &str, amount: i64) -> Result<i64> {
        let result = match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val += amount;
                    self.stats.total_writes += 1;
                    let new_val = *val;
                    debug!("Incremented key '{}' by {} to {}", key, amount, new_val);
                    Ok(new_val)
                } else {
                    let type_name = match &entry.value {
                        Value::Str(_) => "Str",
                        Value::Int(_) => "Int",
                        Value::Float(_) => "Float",
                        Value::Bool(_) => "Bool",
                        Value::Json(_) => "Json",
                        Value::HyperLogLog(_) => "HyperLogLog",
                    };
                    Err(KvStoreError::type_mismatch(key, "Int", type_name))
                }
            }
            None => Err(KvStoreError::KeyNotFound(key.to_string())),
        };

        if result.is_ok() {
            self.update_lru(&key.to_string());
        }
        result
    }

    /// Append to a string value. Returns new length or error.
    pub fn append(&mut self, key: &str, value: &str) -> Result<usize> {
        let result = match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Str(ref mut s) = entry.value {
                    s.push_str(value);
                    self.stats.total_writes += 1;
                    let new_len = s.len();
                    debug!("Appended to key '{}', new length: {}", key, new_len);
                    Ok(new_len)
                } else {
                    let type_name = match &entry.value {
                        Value::Str(_) => "Str",
                        Value::Int(_) => "Int",
                        Value::Float(_) => "Float",
                        Value::Bool(_) => "Bool",
                        Value::Json(_) => "Json",
                        Value::HyperLogLog(_) => "HyperLogLog",
                    };
                    Err(KvStoreError::type_mismatch(key, "Str", type_name))
                }
            }
            None => {
                // If key doesn't exist, create it with the value
                self.set(key.to_string(), value.to_string())?;
                return Ok(value.len());
            }
        };

        if result.is_ok() {
            self.update_lru(&key.to_string());
        }
        result
    }

    /// Get old value and set new value atomically.
    pub fn getset(&mut self, key: String, value: impl Into<Value>) -> Result<Option<Value>> {
        let old_value = self.store.get(&key).map(|v| v.value.clone());
        self.set(key, value)?;
        Ok(old_value)
    }

    // Batch Operations

    /// Get multiple values at once. Returns Vec of Option<Value>.
    pub fn mget(&mut self, keys: &[String]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get(k).cloned()).collect()
    }

    /// Set multiple key-value pairs at once.
    pub fn mset(&mut self, pairs: Vec<(String, String)>) -> Result<()> {
        if self.in_transaction() {
            for (key, value) in pairs {
                self.validate_key(&key)?;
                self.validate_value(&Value::Str(value.clone()))?;
                self.queue_operation(TransactionOp::Set(key, Value::Str(value), None))?;
            }
            return Ok(());
        }

        for (key, value) in pairs {
            self.set(key, value)?;
        }
        Ok(())
    }

    /// Check if key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Check if multiple keys exist. Returns count of existing keys.
    pub fn exists_many(&self, keys: &[String]) -> usize {
        keys.iter().filter(|k| self.exists(k)).count()
    }

    /// Add one or more values to a HyperLogLog under `key`.
    ///
    /// Returns the approximate cardinality after the add.
    pub fn pfreserve(&mut self, key: String, precision: u8) -> Result<()> {
        self.validate_key(&key)?;

        if self.store.contains_key(&key) {
            return Err(KvStoreError::OperationFailed(format!(
                "Key '{}' already exists",
                key
            )));
        }

        let hll = HyperLogLog::new(precision);
        self.stats.memory_bytes += self.estimate_value_size(&Value::HyperLogLog(hll.clone()));
        self.store.insert(
            key.clone(),
            ValueWithTTL {
                value: Value::HyperLogLog(hll),
                expires_at: None,
            },
        );
        self.stats.total_keys = self.store.len();
        self.stats.total_writes += 1;
        self.update_lru(&key);
        Ok(())
    }

    pub fn pfadd(&mut self, key: String, values: Vec<String>) -> Result<u64> {
        self.validate_key(&key)?;
        if values.is_empty() {
            return Err(KvStoreError::InvalidValue(
                "PFADD requires at least one value".to_string(),
            ));
        }

        let mut created = false;
        let approximate_count = match self.store.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                match &mut occupied.get_mut().value {
                    Value::HyperLogLog(hll) => {
                        for value in &values {
                            hll.add(value);
                        }
                        hll.count()
                    }
                    other => {
                        let got = match other {
                            Value::Str(_) => "Str",
                            Value::Int(_) => "Int",
                            Value::Float(_) => "Float",
                            Value::Bool(_) => "Bool",
                            Value::Json(_) => "Json",
                            Value::HyperLogLog(_) => "HyperLogLog",
                        };
                        return Err(KvStoreError::type_mismatch(&key, "HyperLogLog", got));
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let mut hll = HyperLogLog::default();
                for value in &values {
                    hll.add(value);
                }
                let count = hll.count();
                vacant.insert(ValueWithTTL {
                    value: Value::HyperLogLog(hll),
                    expires_at: None,
                });
                created = true;
                count
            }
        };

        if created {
            self.stats.memory_bytes +=
                self.estimate_value_size(&Value::HyperLogLog(HyperLogLog::default()));
            self.stats.total_keys = self.store.len();
        }
        self.stats.total_writes += 1;
        self.update_lru(&key);
        Ok(approximate_count)
    }

    /// Get the approximate cardinality for a HyperLogLog key.
    pub fn pfcount(&self, key: &str) -> Result<u64> {
        match self.get(key) {
            Some(Value::HyperLogLog(hll)) => Ok(hll.count()),
            Some(other) => {
                let got = match other {
                    Value::Str(_) => "Str",
                    Value::Int(_) => "Int",
                    Value::Float(_) => "Float",
                    Value::Bool(_) => "Bool",
                    Value::Json(_) => "Json",
                    Value::HyperLogLog(_) => "HyperLogLog",
                };
                Err(KvStoreError::type_mismatch(key, "HyperLogLog", got))
            }
            None => Err(KvStoreError::KeyNotFound(key.to_string())),
        }
    }

    pub fn hll_info(&self, key: &str) -> Result<HllInfo> {
        match self.get(key) {
            Some(Value::HyperLogLog(hll)) => Ok(HllInfo {
                precision: hll.precision(),
                registers: hll.register_count(),
                memory_bytes: hll.memory_bytes(),
                estimated_count: hll.count(),
            }),
            Some(other) => {
                let got = match other {
                    Value::Str(_) => "Str",
                    Value::Int(_) => "Int",
                    Value::Float(_) => "Float",
                    Value::Bool(_) => "Bool",
                    Value::Json(_) => "Json",
                    Value::HyperLogLog(_) => "HyperLogLog",
                };
                Err(KvStoreError::type_mismatch(key, "HyperLogLog", got))
            }
            None => Err(KvStoreError::KeyNotFound(key.to_string())),
        }
    }

    /// Merge one or more HyperLogLog source keys into a destination key.
    ///
    /// Missing source keys are treated as empty sketches.
    pub fn pfmerge(&mut self, destination: String, source_keys: &[String]) -> Result<u64> {
        self.validate_key(&destination)?;
        if source_keys.is_empty() {
            return Err(KvStoreError::InvalidValue(
                "PFMERGE requires at least one source key".to_string(),
            ));
        }

        let mut merged = match self.store.get(&destination) {
            Some(ValueWithTTL {
                value: Value::HyperLogLog(hll),
                ..
            }) => hll.clone(),
            Some(ValueWithTTL { value, .. }) => {
                let got = match value {
                    Value::Str(_) => "Str",
                    Value::Int(_) => "Int",
                    Value::Float(_) => "Float",
                    Value::Bool(_) => "Bool",
                    Value::Json(_) => "Json",
                    Value::HyperLogLog(_) => "HyperLogLog",
                };
                return Err(KvStoreError::type_mismatch(
                    &destination,
                    "HyperLogLog",
                    got,
                ));
            }
            None => HyperLogLog::default(),
        };

        for source_key in source_keys {
            if source_key == &destination {
                continue;
            }
            if let Some(entry) = self.store.get(source_key) {
                match &entry.value {
                    Value::HyperLogLog(hll) => merged.merge(hll)?,
                    other => {
                        let got = match other {
                            Value::Str(_) => "Str",
                            Value::Int(_) => "Int",
                            Value::Float(_) => "Float",
                            Value::Bool(_) => "Bool",
                            Value::Json(_) => "Json",
                            Value::HyperLogLog(_) => "HyperLogLog",
                        };
                        return Err(KvStoreError::type_mismatch(source_key, "HyperLogLog", got));
                    }
                }
            }
        }

        let created = !self.store.contains_key(&destination);
        self.store.insert(
            destination.clone(),
            ValueWithTTL {
                value: Value::HyperLogLog(merged.clone()),
                expires_at: None,
            },
        );

        if created {
            self.stats.memory_bytes +=
                self.estimate_value_size(&Value::HyperLogLog(merged.clone()));
            self.stats.total_keys = self.store.len();
        }
        self.stats.total_writes += 1;
        self.update_lru(&destination);

        Ok(merged.count())
    }

    // Pattern Matching

    /// Find keys matching a glob pattern (*, ?).
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.store
            .keys()
            .filter(|key| {
                // Check if key is expired
                if let Some(entry) = self.store.get(*key) {
                    if let Some(exp) = entry.expires_at {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        if now > exp {
                            return false;
                        }
                    }
                }

                // Simple glob matching
                matches_glob(key, pattern)
            })
            .cloned()
            .collect()
    }

    /// Get store statistics.
    pub fn stats(&self) -> &StoreStats {
        &self.stats
    }

    /// Reset statistics.
    pub fn reset_stats(&mut self) {
        self.stats = StoreStats::default();
    }

    /// Clean up expired keys manually.
    pub fn cleanup_expired(&mut self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let expired: Vec<String> = self
            .store
            .iter()
            .filter_map(|(k, v)| {
                if let Some(exp) = v.expires_at {
                    if now > exp {
                        return Some(k.clone());
                    }
                }
                None
            })
            .collect();

        let count = expired.len();
        for key in expired {
            self.store.remove(&key);
        }

        self.stats.expired_keys += count;
        count
    }

    /// Get detailed memory profile showing breakdown by value type
    pub fn memory_profile(&self) -> MemoryProfile {
        let mut profile = MemoryProfile::default();

        for (key, entry) in &self.store {
            // Count key memory
            profile.key_bytes += key.len();

            // Count value memory by type
            match &entry.value {
                Value::Str(s) => {
                    profile.value_bytes += s.len();
                    profile.string_values += 1;
                }
                Value::Int(_) => {
                    profile.value_bytes += 8; // 64-bit integer
                    profile.int_values += 1;
                }
                Value::Float(_) => {
                    profile.value_bytes += 8; // 64-bit float
                    profile.float_values += 1;
                }
                Value::Bool(_) => {
                    profile.value_bytes += 1;
                    profile.bool_values += 1;
                }
                Value::Json(j) => {
                    profile.value_bytes += j.to_string().len();
                    profile.json_values += 1;
                }
                Value::HyperLogLog(hll) => {
                    profile.value_bytes += hll.memory_bytes();
                    profile.hyperloglog_values += 1;
                }
            }

            // Count TTL entries
            if entry.expires_at.is_some() {
                profile.ttl_entries += 8; // 64-bit timestamp
            }
        }

        // Calculate total and fragmentation ratio
        profile.total_bytes = profile.key_bytes + profile.value_bytes + profile.ttl_entries;
        // Simple fragmentation estimate: empty slots in HashMap capacity
        let capacity = self.store.capacity();
        let len = self.store.len();
        if capacity > 0 {
            profile.heap_fragmentation_ratio = (capacity - len) as f64 / capacity as f64;
        }
        profile.total_bytes = self.stats.memory_bytes; // Use actual tracked memory

        profile
    }

    /// Persist store to JSON file (overwrites).
    pub fn save_to_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let file = File::create(path)?;
        match compression_for_path(path) {
            FileCompression::None => {
                let writer = BufWriter::new(file);
                serde_json::to_writer_pretty(writer, &self)?;
            }
            FileCompression::Gzip => {
                let writer = BufWriter::new(file);
                let encoder = GzEncoder::new(writer, Compression::default());
                serde_json::to_writer_pretty(encoder, &self)?;
            }
            FileCompression::Zstd => {
                let writer = BufWriter::new(file);
                let mut encoder = zstd::Encoder::new(writer, 3)?;
                serde_json::to_writer_pretty(&mut encoder, &self)?;
                encoder.finish()?;
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.store.iter().filter_map(|(k, v)| {
            if let Some(exp) = v.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > exp {
                    return None;
                }
            }
            Some((k, &v.value))
        })
    }

    /// Load store from JSON file.
    pub fn load_from_file<P: AsRef<Path>>(
        path: P,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let mut store: KvStore = match compression_for_path(path) {
            FileCompression::None => {
                let reader = BufReader::new(file);
                serde_json::from_reader(reader)?
            }
            FileCompression::Gzip => {
                let reader = BufReader::new(file);
                let decoder = GzDecoder::new(reader);
                serde_json::from_reader(decoder)?
            }
            FileCompression::Zstd => {
                let reader = BufReader::new(file);
                let decoder = zstd::Decoder::new(reader)?;
                serde_json::from_reader(decoder)?
            }
        };
        // Reinitialize transient fields
        store.lru_order = store.store.keys().cloned().collect();
        Ok(store)
    }
    /// Save with backup/versioning:
    /// - If the target file exists, it is copied to `<filename>.bak.<epoch_secs>`
    /// - The file is written atomically (write temp -> rename)
    /// - Keeps at most `max_versions` backups (oldest removed)
    pub fn save_with_version<P: AsRef<Path>>(
        &self,
        path: P,
        max_versions: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let compression = compression_for_path(path);

        // If existing file present, create a timestamped backup next to it
        if path.exists() {
            let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let file_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or("invalid filename")?;
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            // Handle empty parent path (for relative files like "store.json")
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let backup_name = format!("{}.bak.{}", file_name, epoch);
            let backup_path = parent.join(backup_name);

            fs::copy(path, &backup_path).map_err(|e| format!("Failed to create backup: {}", e))?;

            // Prune old backups matching "<file>.bak.*" keeping newest `max_versions`
            let prefix = format!("{}{}", file_name, ".bak.");
            let mut backups: Vec<_> = fs::read_dir(parent)
                .map_err(|e| format!("Failed to read directory {:?}: {}", parent, e))?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().map(|t| t.is_file()).unwrap_or(false)
                        && e.file_name()
                            .to_str()
                            .map(|s| s.starts_with(&prefix))
                            .unwrap_or(false)
                })
                .collect();

            // Sort by modified time (oldest first)
            backups.sort_by_key(|e| {
                e.metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH)
            });

            while backups.len() > max_versions {
                if let Some(oldest) = backups.first() {
                    let _ = fs::remove_file(oldest.path());
                    backups.remove(0);
                }
            }
        }

        // Atomic write: write to temp file then rename
        let tmp_path = if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            path.with_extension(format!("{}.tmp", ext))
        } else {
            path.with_extension("tmp")
        };
        {
            let file = File::create(&tmp_path)
                .map_err(|e| format!("Failed to create temp file {:?}: {}", tmp_path, e))?;
            match compression {
                FileCompression::None => {
                    let writer = BufWriter::new(file);
                    serde_json::to_writer_pretty(writer, &self)?;
                }
                FileCompression::Gzip => {
                    let writer = BufWriter::new(file);
                    let encoder = GzEncoder::new(writer, Compression::default());
                    serde_json::to_writer_pretty(encoder, &self)?;
                }
                FileCompression::Zstd => {
                    let writer = BufWriter::new(file);
                    let mut encoder = zstd::Encoder::new(writer, 3)?;
                    serde_json::to_writer_pretty(&mut encoder, &self)?;
                    encoder.finish()?;
                }
            }
        }
        fs::rename(&tmp_path, path)
            .map_err(|e| format!("Failed to rename {:?} to {:?}: {}", tmp_path, path, e))?;

        Ok(())
    }

    // ===== Validation Methods =====

    fn validate_key(&self, key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(KvStoreError::InvalidKey("Key cannot be empty".to_string()));
        }

        let key_size = key.len();
        if key_size > self.config.max_key_size {
            return Err(KvStoreError::KeyTooLarge {
                size: key_size,
                max: self.config.max_key_size,
            });
        }

        Ok(())
    }

    fn validate_value(&self, value: &Value) -> Result<()> {
        let value_size = self.estimate_value_size(value);
        if value_size > self.config.max_value_size {
            return Err(KvStoreError::ValueTooLarge {
                size: value_size,
                max: self.config.max_value_size,
            });
        }
        Ok(())
    }

    fn estimate_value_size(&self, value: &Value) -> usize {
        match value {
            Value::Str(s) => s.len(),
            Value::Int(_) => 8,
            Value::Float(_) => 8,
            Value::Bool(_) => 1,
            Value::Json(j) => j.to_string().len(),
            Value::HyperLogLog(hll) => hll.memory_bytes(),
        }
    }

    // ===== LRU Management =====

    fn update_lru(&mut self, key: &str) {
        // Remove key if it exists in LRU order
        self.lru_order.retain(|k| k != key);
        // Add to end (most recently used)
        self.lru_order.push(key.to_string());
    }

    fn enforce_memory_limit(&mut self) -> Result<()> {
        if self.config.max_memory_bytes == 0 {
            return Ok(());
        }

        while self.stats.memory_bytes > self.config.max_memory_bytes {
            if let Some(lru_key) = self.lru_order.first().cloned() {
                warn!("Memory limit exceeded, evicting key: {}", lru_key);
                if let Some(entry) = self.store.remove(&lru_key) {
                    let value_size = self.estimate_value_size(&entry.value);
                    self.stats.memory_bytes = self.stats.memory_bytes.saturating_sub(value_size);
                    self.stats.evictions += 1;
                    self.stats.total_keys = self.store.len();
                }
                self.lru_order.remove(0);
            } else {
                break; // No more keys to evict
            }
        }
        Ok(())
    }

    /// Get configuration
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Update configuration (some settings may not apply retroactively)
    pub fn set_config(&mut self, config: StoreConfig) {
        info!("Updating KvStore configuration");
        self.config = config;
    }

    /// Start a transaction (MULTI command)
    pub fn multi(&mut self) -> Result<()> {
        if self.transaction_queue.is_some() {
            return Err(KvStoreError::OperationFailed(
                "Transaction already in progress".to_string(),
            ));
        }
        self.transaction_queue = Some(Vec::new());
        info!("Transaction started");
        Ok(())
    }

    /// Execute all queued operations atomically (EXEC command)
    pub fn exec(&mut self) -> Result<Vec<String>> {
        let queue = match self.transaction_queue.take() {
            Some(q) => q,
            None => {
                return Err(KvStoreError::OperationFailed(
                    "No transaction in progress".to_string(),
                ))
            }
        };

        let mut results = Vec::new();
        for op in queue {
            match op {
                TransactionOp::Set(key, value, expires_at) => {
                    let value_with_ttl = ValueWithTTL {
                        value: value.clone(),
                        expires_at,
                    };
                    self.store.insert(key.clone(), value_with_ttl);
                    self.stats.total_writes += 1;
                    results.push(format!("OK"));
                    self.update_lru(&key);
                }
                TransactionOp::Delete(key) => {
                    self.store.remove(&key);
                    self.stats.total_deletes += 1;
                    results.push(format!("OK"));
                }
                TransactionOp::Incr(key) => {
                    if let Some(entry) = self.store.get_mut(&key) {
                        if let Value::Int(i) = &mut entry.value {
                            *i += 1;
                            results.push(format!("{}", i));
                        } else {
                            results.push(format!("ERR: Type mismatch"));
                        }
                    } else {
                        self.store.insert(
                            key.clone(),
                            ValueWithTTL {
                                value: Value::Int(1),
                                expires_at: None,
                            },
                        );
                        results.push(format!("1"));
                    }
                    self.stats.total_writes += 1;
                    self.update_lru(&key);
                }
                TransactionOp::Decr(key) => {
                    if let Some(entry) = self.store.get_mut(&key) {
                        if let Value::Int(i) = &mut entry.value {
                            *i -= 1;
                            results.push(format!("{}", i));
                        } else {
                            results.push(format!("ERR: Type mismatch"));
                        }
                    } else {
                        self.store.insert(
                            key.clone(),
                            ValueWithTTL {
                                value: Value::Int(-1),
                                expires_at: None,
                            },
                        );
                        results.push(format!("-1"));
                    }
                    self.stats.total_writes += 1;
                    self.update_lru(&key);
                }
                TransactionOp::IncrBy(key, amount) => {
                    if let Some(entry) = self.store.get_mut(&key) {
                        if let Value::Int(i) = &mut entry.value {
                            *i += amount;
                            results.push(format!("{}", i));
                        } else {
                            results.push(format!("ERR: Type mismatch"));
                        }
                    } else {
                        self.store.insert(
                            key.clone(),
                            ValueWithTTL {
                                value: Value::Int(amount),
                                expires_at: None,
                            },
                        );
                        results.push(format!("{}", amount));
                    }
                    self.stats.total_writes += 1;
                    self.update_lru(&key);
                }
                TransactionOp::Append(key, s) => {
                    if let Some(entry) = self.store.get_mut(&key) {
                        if let Value::Str(ref mut string) = &mut entry.value {
                            string.push_str(&s);
                            results.push(format!("OK"));
                        } else {
                            results.push(format!("ERR: Type mismatch"));
                        }
                    } else {
                        self.store.insert(
                            key.clone(),
                            ValueWithTTL {
                                value: Value::Str(s.clone()),
                                expires_at: None,
                            },
                        );
                        results.push(format!("OK"));
                    }
                    self.stats.total_writes += 1;
                    self.update_lru(&key);
                }
            }
        }
        info!("Transaction executed with {} operations", results.len());
        Ok(results)
    }

    /// Discard transaction (DISCARD command)
    pub fn discard(&mut self) -> Result<()> {
        if self.transaction_queue.is_none() {
            return Err(KvStoreError::OperationFailed(
                "No transaction in progress".to_string(),
            ));
        }
        self.transaction_queue = None;
        info!("Transaction discarded");
        Ok(())
    }

    /// Queue an operation in the current transaction
    pub fn queue_operation(&mut self, op: TransactionOp) -> Result<()> {
        match &mut self.transaction_queue {
            Some(queue) => {
                queue.push(op);
                Ok(())
            }
            None => Err(KvStoreError::OperationFailed(
                "No transaction in progress".to_string(),
            )),
        }
    }

    /// Check if currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.transaction_queue.is_some()
    }
}

// Helper function for simple glob pattern matching
fn matches_glob(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    fn match_recursive(text: &[char], pattern: &[char], ti: usize, pi: usize) -> bool {
        if pi == pattern.len() {
            return ti == text.len();
        }

        if pattern[pi] == '*' {
            // Match zero or more characters
            for i in ti..=text.len() {
                if match_recursive(text, pattern, i, pi + 1) {
                    return true;
                }
            }
            false
        } else if ti < text.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            match_recursive(text, pattern, ti + 1, pi + 1)
        } else {
            false
        }
    }

    match_recursive(&text_chars, &pattern_chars, 0, 0)
}
