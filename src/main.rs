// Import the kv_store module
use clap::Parser;
use rust_kv_store::{
    cli::{Cli, Commands},
    kv_store::KvStore,
};
use std::path::PathBuf;
use std::time::Duration;

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
        Commands::SetTTL { key, value, ttl } => {
            store.set_with_ttl(key.clone(), value, Duration::from_secs(ttl));
            println!("Set '{}' successfully with {}s TTL", key, ttl);
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
        Commands::Incr { key } => match store.incr(&key) {
            Ok(new_val) => println!("{} = {}", key, new_val),
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::Decr { key } => match store.decr(&key) {
            Ok(new_val) => println!("{} = {}", key, new_val),
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::IncrBy { key, amount } => match store.incrby(&key, amount) {
            Ok(new_val) => println!("{} = {}", key, new_val),
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::Append { key, value } => match store.append(&key, &value) {
            Ok(len) => println!("Appended to '{}', new length: {}", key, len),
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::GetSet { key, value } => {
            match store.getset(key.clone(), value.clone()) {
                Some(old_val) => println!("Old value: {}, Set '{}' to '{}'", old_val, key, value),
                None => println!("Key '{}' was not set, now set to '{}'", key, value),
            }
        }
        Commands::MGet { keys } => {
            let values = store.mget(&keys);
            for (key, value) in keys.iter().zip(values.iter()) {
                match value {
                    Some(v) => println!("{}: {}", key, v),
                    None => println!("{}: (nil)", key),
                }
            }
        }
        Commands::MSet { pairs } => {
            if pairs.len() % 2 != 0 {
                eprintln!("Error: MSET requires an even number of arguments (key1 value1 key2 value2 ...)");
            } else {
                let kv_pairs: Vec<(String, String)> = pairs
                    .chunks(2)
                    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                    .collect();
                store.mset(kv_pairs);
                println!("Set {} key-value pairs", pairs.len() / 2);
            }
        }
        Commands::Exists { keys } => {
            let count = store.exists_many(&keys);
            println!("{} key(s) exist", count);
        }
        Commands::Keys { pattern } => {
            let matching_keys = store.keys(&pattern);
            if matching_keys.is_empty() {
                println!("No keys match pattern '{}'", pattern);
            } else {
                for key in matching_keys {
                    println!("{}", key);
                }
            }
        }
        Commands::Stats => {
            let stats = store.stats();
            println!("=== Store Statistics ===");
            println!("Total keys: {}", stats.total_keys);
            println!("Expired keys cleaned: {}", stats.expired_keys);
            println!("Total reads: {}", stats.total_reads);
            println!("Total writes: {}", stats.total_writes);
            println!("Total deletes: {}", stats.total_deletes);
            println!("Cache hits: {}", stats.hits);
            println!("Cache misses: {}", stats.misses);
            if stats.total_reads > 0 {
                let hit_rate = (stats.hits as f64 / stats.total_reads as f64) * 100.0;
                println!("Hit rate: {:.2}%", hit_rate);
            }
        }
        Commands::Cleanup => {
            let removed = store.cleanup_expired();
            println!("Cleaned up {} expired keys", removed);
        }
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
