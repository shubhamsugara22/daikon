// Import the kv_store module
mod kv_store;

fn main() {
    let mut store = kv_store::KvStore::new();
    store.set("key1".to_string(), "value1".to_string());
    store.set("key2".to_string(), "value2".to_string());

    if let Some(value) = store.get("key1") {
        println!("key1: {}", value);
    }

    // Delete the key
    if let Some(removed) = store.delete("key1") {
        println!("Deleted key1, value was: {}", removed);
    } else {
        println!("key1 not found for deletion");
    }
}
