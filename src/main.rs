// Import the kv_store module
use clap::Parser;
use rust_kv_store::{
    cli::{Cli, Commands},
    kv_store::KvStore,
};
use std::path::PathBuf;

fn main() {
    let mut store = KvStore::new();
    let cli = Cli::parse();

    let default_path = PathBuf::from("store.json");
    let file_path = cli.file.unwrap_or(default_path);

    // Load existing store if file exists
    if file_path.exists() {
        match KvStore::load_from_file(&file_path) {
            Ok(loaded_store) => store = loaded_store,
            Err(e) => eprintln!("Error loading store: {}", e),
        }
    }

    match cli.command {
        Commands::Set { key, value } => {
            store.set(key.clone(), value);
            println!("Set '{}' successfully", key);
        }
        Commands::Get { key } => match store.get(&key) {
            Some(value) => println!("{}: {}", key, value),
            None => println!("Key '{}' not found", key),
        },
        Commands::Delete { key } => match store.delete(&key) {
            Some(_) => println!("Deleted '{}' successfully", key),
            None => println!("Key '{}' not found", key),
        },
        Commands::List => {
            if store.is_empty() {
                println!("Store is empty");
            } else {
                for (key, value) in store.iter() {
                    println!("{}: {}", key, value);
                }
            }
        }
        Commands::Save { versions } => match store.save_with_version(&file_path, versions) {
            Ok(_) => println!("Store saved successfully to {:?}", file_path),
            Err(e) => eprintln!("Error saving store: {}", e),
        },
        Commands::Load => match KvStore::load_from_file(&file_path) {
            Ok(loaded_store) => {
                store = loaded_store;
                println!("Store loaded successfully from {:?}", file_path);
            }
            Err(e) => eprintln!("Error loading store: {}", e),
        },
    }

    // store.set("key1".to_string(), "value1".to_string());
    // store.set("key2".to_string(), "value2".to_string());

    // if let Some(value) = store.get("key1") {
    //     println!("key1: {}", value);
    // }

    // Delete the key
    // if let Some(removed) = store.delete("key1") {
    //     println!("Deleted key1, value was: {}", removed);
    // } else {
    //     println!("key1 not found for deletion");
    // }
}
