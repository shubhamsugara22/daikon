use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, value_name = "FILE")]
    pub file: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set a key-value pair
    Set { key: String, value: String },
    SetTTL {
        key: String,
        value: String,
        #[arg(short, long)]
        ttl: u64,
    },
    /// Get a value by key
    Get { key: String },
    /// Delete a key-value pair
    Delete { key: String },
    /// List all keys
    List,
    /// Save the store to file
    Save {
        #[arg(short, long, default_value = "2")]
        versions: usize,
    },
    /// Load the store from file
    Load,
    /// Increment an integer value
    Incr { key: String },
    /// Decrement an integer value
    Decr { key: String },
    /// Increment by a specific amount
    IncrBy { key: String, amount: i64 },
    /// Append to a string value
    Append { key: String, value: String },
    /// Get and set value atomically
    GetSet { key: String, value: String },
    /// Get multiple values
    MGet { keys: Vec<String> },
    /// Set multiple key-value pairs
    MSet { pairs: Vec<String> },
    /// Check if key(s) exist
    Exists { keys: Vec<String> },
    /// Find keys matching a pattern
    Keys { pattern: String },
    /// Show store statistics
    Stats,
    /// Clean up expired keys
    Cleanup,
    /// Start a transaction (MULTI command)
    Multi,
    /// Execute all queued transaction operations (EXEC command)
    Exec,
    /// Discard an ongoing transaction (DISCARD command)
    Discard,
    /// Add values to a HyperLogLog key
    PfAdd { key: String, values: Vec<String> },
    /// Get approximate cardinality for a HyperLogLog key
    PfCount { key: String },
    /// Merge HyperLogLog source keys into a destination key
    PfMerge {
        destination: String,
        sources: Vec<String>,
    },
    /// Execute a Lua script string against the store
    Lua { script: String },
    /// Run performance benchmark suite
    Benchmark,
    /// Push values to the left (head) of a list
    LPush { key: String, values: Vec<String> },
    /// Push values to the right (tail) of a list
    RPush { key: String, values: Vec<String> },
    /// Pop a value from the left (head) of a list
    LPop { key: String },
    /// Pop a value from the right (tail) of a list
    RPop { key: String },
    /// Get a range of elements from a list
    LRange {
        key: String,
        #[arg(default_value = "0")]
        start: i64,
        #[arg(default_value = "-1")]
        stop: i64,
    },
    /// Get the length of a list
    LLen { key: String },
}
