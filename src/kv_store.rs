use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]

pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Json(JsonValue),
}
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.store.get(key)
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.store.remove(key)
    }
}
