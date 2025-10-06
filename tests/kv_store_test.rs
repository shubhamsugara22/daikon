use rust_kv_store::kv_store::KvStore;

#[test]
fn test_set_and_get() {
    let mut store = KvStore::new();
    store.set("alpha".to_string(), "beta".to_string());
    assert_eq!(store.get("alpha"), Some(&"beta".to_string()));
}

#[test]
fn test_delete() {
    let mut store = KvStore::new();
    store.set("gamma".to_string(), "delta".to_string());
    let removed = store.delete("gamma");
    assert_eq!(removed, Some("delta".to_string()));
    assert_eq!(store.get("gamma"), None);
}
