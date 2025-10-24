use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

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
    store: HashMap<String, Value>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Generic set: accepts any type convertible into Value
    pub fn set<VT>(&mut self, key: String, value: VT)
    where
        VT: Into<Value>,
    {
        self.store.insert(key, value.into());
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.store.get(key)
    }

    /// Convenience getters...
    pub fn get_string(&self, key: &str) -> Option<String> {
        match self.get(key) {
            Some(Value::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.get(key) {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        }
    }
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        match self.get(key) {
            Some(Value::Float(x)) => Some(*x),
            _ => None,
        }
    }
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key) {
            Some(Value::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    pub fn delete(&mut self, key: &str) -> Option<Value> {
        self.store.remove(key)
    }

    /// Persist store to JSON file (overwrites).
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, &self)?;
        Ok(())
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
