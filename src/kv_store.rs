use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StoreStats {
    pub total_keys: usize,
    pub expired_keys: usize,
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
    pub hits: u64,
    pub misses: u64,
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
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Str(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(x) => write!(f, "{}", x),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Json(j) => write!(f, "{}", j),
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
#[derive(Serialize, Deserialize)]
pub struct KvStore {
    store: HashMap<String, ValueWithTTL>,
    #[serde(default)]
    stats: StoreStats,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
            stats: StoreStats::default(),
        }
    }
    pub fn set_with_ttl<V>(&mut self, key: String, value: V, ttl: Duration)
    where
        V: Into<Value>,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now.checked_add(ttl.as_secs());
        self.store.insert(
            key,
            ValueWithTTL {
                value: value.into(),
                expires_at,
            },
        );
        self.stats.total_writes += 1;
    }

    pub fn set<V>(&mut self, key: String, value: V)
    where
        V: Into<Value>,
    {
        self.store.insert(
            key,
            ValueWithTTL {
                value: value.into(),
                expires_at: None,
            },
        );
        self.stats.total_writes += 1;
    }
    /// Generic set: accepts any type convertible into Value
    pub fn get(&mut self, key: &str) -> Option<&Value> {
        self.stats.total_reads += 1;
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

        if result.is_some() {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
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
        let result = self.store.remove(key).map(|v| v.value);
        if result.is_some() {
            self.stats.total_deletes += 1;
        }
        result
    }

    // Atomic Operations

    /// Increment an integer value atomically. Returns new value or error if key doesn't exist or isn't an integer.
    pub fn incr(&mut self, key: &str) -> Result<i64, String> {
        match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val += 1;
                    self.stats.total_writes += 1;
                    Ok(*val)
                } else {
                    Err(format!("Key '{}' is not an integer", key))
                }
            }
            None => Err(format!("Key '{}' not found", key)),
        }
    }

    /// Decrement an integer value atomically. Returns new value or error.
    pub fn decr(&mut self, key: &str) -> Result<i64, String> {
        match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val -= 1;
                    self.stats.total_writes += 1;
                    Ok(*val)
                } else {
                    Err(format!("Key '{}' is not an integer", key))
                }
            }
            None => Err(format!("Key '{}' not found", key)),
        }
    }

    /// Increment by a specific amount. Returns new value or error.
    pub fn incrby(&mut self, key: &str, amount: i64) -> Result<i64, String> {
        match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Int(ref mut val) = entry.value {
                    *val += amount;
                    self.stats.total_writes += 1;
                    Ok(*val)
                } else {
                    Err(format!("Key '{}' is not an integer", key))
                }
            }
            None => Err(format!("Key '{}' not found", key)),
        }
    }

    /// Append to a string value. Returns new length or error.
    pub fn append(&mut self, key: &str, value: &str) -> Result<usize, String> {
        match self.store.get_mut(key) {
            Some(entry) => {
                if let Value::Str(ref mut s) = entry.value {
                    s.push_str(value);
                    self.stats.total_writes += 1;
                    Ok(s.len())
                } else {
                    Err(format!("Key '{}' is not a string", key))
                }
            }
            None => {
                // If key doesn't exist, create it with the value
                self.set(key.to_string(), value.to_string());
                Ok(value.len())
            }
        }
    }

    /// Get old value and set new value atomically.
    pub fn getset(&mut self, key: String, value: impl Into<Value>) -> Option<Value> {
        let old_value = self.store.get(&key).map(|v| v.value.clone());
        self.set(key, value);
        old_value
    }

    // Batch Operations

    /// Get multiple values at once. Returns Vec of Option<Value>.
    pub fn mget(&mut self, keys: &[String]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get(k).cloned()).collect()
    }

    /// Set multiple key-value pairs at once.
    pub fn mset(&mut self, pairs: Vec<(String, String)>) {
        for (key, value) in pairs {
            self.set(key, value);
        }
    }

    /// Check if key exists and is not expired.
    pub fn exists(&mut self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Check if multiple keys exist. Returns count of existing keys.
    pub fn exists_many(&mut self, keys: &[String]) -> usize {
        keys.iter().filter(|k| self.exists(k)).count()
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

    /// Persist store to JSON file (overwrites).
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, &self)?;
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
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let store: KvStore = serde_json::from_reader(reader)?;
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
    ) -> Result<(), Box<dyn Error>> {
        let path = path.as_ref();

        // If existing file present, create a timestamped backup next to it
        if path.exists() {
            let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let file_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or("invalid filename")?;
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let backup_name = format!("{}.bak.{}", file_name, epoch);
            let backup_path = parent.join(backup_name);

            fs::copy(path, &backup_path)?;

            // Prune old backups matching "<file>.bak.*" keeping newest `max_versions`
            let prefix = format!("{}{}", file_name, ".bak.");
            let mut backups: Vec<_> = fs::read_dir(parent)?
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
        let tmp_path = path.with_extension("tmp");
        {
            let file = File::create(&tmp_path)?;
            serde_json::to_writer_pretty(file, &self)?;
        }
        fs::rename(&tmp_path, path)?;

        Ok(())
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
