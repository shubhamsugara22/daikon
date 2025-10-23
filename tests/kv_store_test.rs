use rust_kv_store::kv_store::{KvStore, Value};
use std::env;
use std::fs;

#[test]
fn test_save_and_load() {
    let mut store = KvStore::new();
    store.set("k1".to_string(), "v1".to_string());
    store.set("k2".to_string(), 123_i32);

    let mut path = env::temp_dir();
    path.push("kv_store_test.json");

    // save
    store.save_to_file(&path).expect("save failed");

    // load
    let loaded = KvStore::load_from_file(&path).expect("load failed");

    // assertions
    assert_eq!(loaded.get("k1"), Some(&Value::Str("v1".to_string())));
    assert_eq!(loaded.get("k2"), Some(&Value::Int(123)));

    // cleanup
    let _ = fs::remove_file(&path);
}

fn test_set_and_get() {
    let mut store = KvStore::new();
    store.set("alpha".to_string(), "beta".to_string());
    assert_eq!(store.get("alpha"), Some(&Value::Str("beta".to_string())));
}

#[test]
fn test_delete() {
    let mut store = KvStore::new();
    store.set("gamma".to_string(), "delta".to_string());
    let removed = store.delete("gamma");
    assert_eq!(removed, Some(Value::Str("delta".to_string())));
    assert_eq!(store.get("gamma"), None);
}
