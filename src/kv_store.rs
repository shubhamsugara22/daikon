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
