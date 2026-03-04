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
    /// Run performance benchmark suite
    Benchmark,
}
