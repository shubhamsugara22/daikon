# Import the kv_store module
mod kv_store;

fn main() {
    let mut store = kv.store::KvStore::new();
    store.set("key1".to_string(), "value1".to_string());
    if let Some(value) = store.get("key1"){
        println!("key1: {}", value);
    }
}
