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
}
