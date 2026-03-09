use thiserror::Error;

/// Custom error types for KvStore operations
#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("Key '{0}' not found")]
    KeyNotFound(String),

    #[error("Type mismatch for key '{key}': expected {expected}, got {got}")]
    TypeMismatch {
        key: String,
        expected: String,
        got: String,
    },

    #[error("Key too large: {size} bytes (max: {max} bytes)")]
    KeyTooLarge { size: usize, max: usize },

    #[error("Value too large: {size} bytes (max: {max} bytes)")]
    ValueTooLarge { size: usize, max: usize },

    #[error("Memory limit exceeded: {current} bytes (max: {max} bytes)")]
    MemoryLimitExceeded { current: usize, max: usize },

    #[error("Invalid key: {0}")]
    InvalidKey(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Data corruption detected: {0}")]
    DataCorruption(String),

    #[error("Store is read-only")]
    ReadOnly,

    #[error("Replication error: {0}")]
    ReplicationError(String),

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

pub type Result<T> = std::result::Result<T, KvStoreError>;

impl KvStoreError {
    pub fn type_mismatch(
        key: impl Into<String>,
        expected: impl Into<String>,
        got: impl Into<String>,
    ) -> Self {
        KvStoreError::TypeMismatch {
            key: key.into(),
            expected: expected.into(),
            got: got.into(),
        }
    }
}
