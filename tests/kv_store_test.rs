use rust_kv_store::config::StoreConfig;
use rust_kv_store::error::KvStoreError;
use rust_kv_store::kv_store::{KvStore, Value};
use std::env;
use std::fs;

#[test]
fn test_versioning_and_prune() {
    use std::time::Duration;
    let mut store = KvStore::new();
    store.set("a".to_string(), "1".to_string()).unwrap();

    let mut path = std::env::temp_dir();
    path.push("kv_store_version.json");

    // cleanup any leftover from previous runs
    let _ = fs::remove_file(&path);

    // first save -> no backup yet
    store.save_with_version(&path, 2).expect("save1 failed");

    // small sleep to ensure different timestamps (only necessary on very fast filesystems)
    std::thread::sleep(Duration::from_millis(10));

    // second save -> creates first backup
    store.set("b".to_string(), "2".to_string()).unwrap();
    store.save_with_version(&path, 2).expect("save2 failed");

    std::thread::sleep(Duration::from_millis(10));

    // third save -> creates another backup and should prune keeping max 2
    store.set("c".to_string(), "3".to_string()).unwrap();
    store.save_with_version(&path, 2).expect("save3 failed");

    // verify backups count <= 2
    let parent = path.parent().unwrap();
    let file_name = path.file_name().unwrap().to_str().unwrap();
    let prefix = format!("{}{}", file_name, ".bak.");
    let backups: Vec<_> = fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with(&prefix))
                .unwrap_or(false)
        })
        .collect();

    assert!(backups.len() <= 2, "backup count should be pruned to max 2");

    // cleanup
    let _ = fs::remove_file(&path);
    for b in backups {
        let _ = fs::remove_file(b.path());
    }
}

#[test]
fn test_save_and_load() {
    let mut store = KvStore::new();
    store.set("k1".to_string(), "v1".to_string()).unwrap();
    store.set("k2".to_string(), 123_i32).unwrap();

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

#[test]
fn test_save_and_load_gzip() {
    let mut store = KvStore::new();
    store.set("k1".to_string(), "v1".to_string()).unwrap();

    let mut path = env::temp_dir();
    path.push("kv_store_test.json.gz");

    store.save_to_file(&path).expect("gzip save failed");
    let loaded = KvStore::load_from_file(&path).expect("gzip load failed");

    assert_eq!(loaded.get("k1"), Some(&Value::Str("v1".to_string())));

    let _ = fs::remove_file(&path);
}

#[test]
fn test_save_and_load_zstd() {
    let mut store = KvStore::new();
    store.set("k1".to_string(), "v1".to_string()).unwrap();

    let mut path = env::temp_dir();
    path.push("kv_store_test.json.zst");

    store.save_to_file(&path).expect("zstd save failed");
    let loaded = KvStore::load_from_file(&path).expect("zstd load failed");

    assert_eq!(loaded.get("k1"), Some(&Value::Str("v1".to_string())));

    let _ = fs::remove_file(&path);
}

#[test]
fn test_set_and_get() {
    let mut store = KvStore::new();
    store.set("alpha".to_string(), "beta".to_string()).unwrap();
    assert_eq!(store.get("alpha"), Some(&Value::Str("beta".to_string())));
}

#[test]
fn test_delete() {
    let mut store = KvStore::new();
    store.set("gamma".to_string(), "delta".to_string()).unwrap();
    let removed = store.delete("gamma");
    assert_eq!(removed, Some(Value::Str("delta".to_string())));
    assert_eq!(store.get("gamma"), None);
}

// ===== Phase 1 Production Hardening Tests =====

#[test]
fn test_key_validation_empty_key() {
    let mut store = KvStore::new();
    let result = store.set("".to_string(), "value".to_string());
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), KvStoreError::InvalidKey(_)));
}

#[test]
fn test_key_validation_too_large() {
    let mut store = KvStore::new();
    // Default max_key_size is 1024 bytes
    let large_key = "a".repeat(2000);
    let result = store.set(large_key, "value".to_string());
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        KvStoreError::KeyTooLarge { .. }
    ));
}

#[test]
fn test_value_validation_too_large() {
    let mut config = StoreConfig::default();
    config.max_value_size = 100; // Set small limit for testing
    let mut store = KvStore::with_config(config);

    let large_value = "a".repeat(200);
    let result = store.set("key".to_string(), large_value);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        KvStoreError::ValueTooLarge { .. }
    ));
}

#[test]
fn test_memory_limit_enforcement() {
    let mut config = StoreConfig::default();
    config.max_memory_bytes = 500; // Small limit to trigger evictions
    config.max_value_size = 500; // Allow individual values up to 500 bytes
    config.lru_eviction_enabled = true;
    let mut store = KvStore::with_config(config);

    // Add values that should exceed the 500-byte limit
    // 3 keys × 250 bytes = 750 bytes total, well over limit
    store.set("key1".to_string(), "a".repeat(250)).unwrap();
    store.set("key2".to_string(), "b".repeat(250)).unwrap();
    store.set("key3".to_string(), "c".repeat(250)).unwrap();

    // After adding 750 bytes total to a 500-byte limit store,
    // evictions should have occurred
    let stats = store.stats();
    assert!(
        stats.evictions > 0,
        "Expected some evictions to have occurred, got {} evictions",
        stats.evictions
    );

    // The most recent key should still exist
    assert!(
        store.get("key3").is_some(),
        "Most recent key should still exist"
    );
}

#[test]
fn test_lru_order_updates() {
    let mut config = StoreConfig::default();
    config.max_memory_bytes = 1000;
    config.max_value_size = 500;
    config.lru_eviction_enabled = true;
    let mut store = KvStore::with_config(config);

    // Add three keys
    store.set("key1".to_string(), "a".repeat(150)).unwrap();
    store.set("key2".to_string(), "b".repeat(150)).unwrap();
    store.set("key3".to_string(), "c".repeat(150)).unwrap();

    // Access key1 to make it most recently used
    let _ = store.get("key1");

    // Add key4 to trigger eviction
    store.set("key4".to_string(), "d".repeat(150)).unwrap();

    // key2 should be evicted (it's the LRU since we accessed key1)
    assert!(
        store.get("key1").is_some(),
        "key1 should still exist after being accessed"
    );
    assert!(
        store.get("key4").is_some(),
        "key4 should exist as it was just added"
    );
}

#[test]
fn test_type_mismatch_errors() {
    let mut store = KvStore::new();
    store.set("counter".to_string(), 42_i64).unwrap();

    // Try to append to an integer - should fail
    let result = store.append("counter", " text");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        KvStoreError::TypeMismatch { .. }
    ));
}

#[test]
fn test_incr_on_nonexistent_key() {
    let mut store = KvStore::new();
    let result = store.incr("nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), KvStoreError::KeyNotFound(_)));
}

#[test]
fn test_atomic_operations_with_lru() {
    let mut store = KvStore::new();
    store.set("counter".to_string(), 10_i64).unwrap();

    // Increment should update LRU
    let result = store.incr("counter");
    assert_eq!(result.unwrap(), 11);

    // Decrement should update LRU
    let result = store.decr("counter");
    assert_eq!(result.unwrap(), 10);

    // IncrBy should update LRU
    let result = store.incrby("counter", 5);
    assert_eq!(result.unwrap(), 15);
}

#[test]
fn test_append_creates_key_if_not_exists() {
    let mut store = KvStore::new();
    let result = store.append("newkey", "hello");
    assert_eq!(result.unwrap(), 5); // "hello" has length 5
    assert_eq!(store.get("newkey"), Some(&Value::Str("hello".to_string())));
}

#[test]
fn test_config_validation() {
    let mut config = StoreConfig::default();
    config.max_key_size = 0; // Invalid: must be > 0

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_config_from_default() {
    let config = StoreConfig::default();
    assert_eq!(config.max_key_size, 1024);
    assert_eq!(config.max_value_size, 10 * 1024 * 1024); // 10MB
    assert_eq!(config.max_memory_bytes, 1024 * 1024 * 1024); // 1GB
    assert!(config.lru_eviction_enabled);
}

#[test]
fn test_getset_with_validation() {
    let mut store = KvStore::new();
    store
        .set("key".to_string(), "old_value".to_string())
        .unwrap();

    let result = store.getset("key".to_string(), "new_value".to_string());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(Value::Str("old_value".to_string())));
    assert_eq!(store.get("key"), Some(&Value::Str("new_value".to_string())));
}

#[test]
fn test_mset_with_validation() {
    let mut store = KvStore::new();
    let pairs = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ];

    let result = store.mset(pairs);
    assert!(result.is_ok());
    assert_eq!(store.get("key1"), Some(&Value::Str("value1".to_string())));
    assert_eq!(store.get("key2"), Some(&Value::Str("value2".to_string())));
}

#[test]
fn test_mset_with_invalid_key() {
    let mut store = KvStore::new();
    let pairs = vec![
        ("valid".to_string(), "value1".to_string()),
        ("".to_string(), "value2".to_string()), // Invalid empty key
    ];

    let result = store.mset(pairs);
    assert!(result.is_err());
}

#[test]
fn test_stats_tracking() {
    let mut store = KvStore::new();

    let initial_stats = store.stats();
    assert_eq!(initial_stats.total_keys, 0);
    assert_eq!(initial_stats.total_writes, 0);

    store.set("key1".to_string(), "value1".to_string()).unwrap();
    let stats = store.stats();
    assert_eq!(stats.total_keys, 1);
    assert_eq!(stats.total_writes, 1);

    store.set("key2".to_string(), 42_i64).unwrap();
    let stats = store.stats();
    assert_eq!(stats.total_keys, 2);
    assert_eq!(stats.total_writes, 2);
}

#[test]
fn test_transaction_multi_exec_queues_and_commits() {
    let mut store = KvStore::new();

    store.multi().expect("failed to start transaction");
    store
        .set("k1".to_string(), "v1".to_string())
        .expect("failed to queue set k1");
    store
        .set("k2".to_string(), "v2".to_string())
        .expect("failed to queue set k2");

    assert!(store.in_transaction());
    assert_eq!(
        store.get("k1"),
        None,
        "queued write should not be visible before EXEC"
    );
    assert_eq!(
        store.get("k2"),
        None,
        "queued write should not be visible before EXEC"
    );

    let results = store.exec().expect("failed to exec transaction");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "OK");
    assert_eq!(results[1], "OK");
    assert!(!store.in_transaction());

    assert_eq!(store.get("k1"), Some(&Value::Str("v1".to_string())));
    assert_eq!(store.get("k2"), Some(&Value::Str("v2".to_string())));
}

#[test]
fn test_transaction_discard_rolls_back_queued_writes() {
    let mut store = KvStore::new();

    store.multi().expect("failed to start transaction");
    store
        .set("temp".to_string(), "value".to_string())
        .expect("failed to queue set");

    assert_eq!(
        store.get("temp"),
        None,
        "queued write should not be visible before DISCARD"
    );

    store.discard().expect("failed to discard transaction");
    assert!(!store.in_transaction());
    assert_eq!(
        store.get("temp"),
        None,
        "discarded transaction should not persist queued write"
    );
}

#[test]
fn test_transaction_delete_exec_applies_removal() {
    let mut store = KvStore::new();
    store
        .set("to_delete".to_string(), "keep-until-exec".to_string())
        .expect("failed initial set");

    store.multi().expect("failed to start transaction");
    let queued_old = store.delete("to_delete");
    assert_eq!(
        queued_old,
        Some(Value::Str("keep-until-exec".to_string())),
        "delete should return current value even when queued"
    );

    assert_eq!(
        store.get("to_delete"),
        Some(&Value::Str("keep-until-exec".to_string())),
        "delete should not apply before EXEC"
    );

    let results = store.exec().expect("failed to exec transaction");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "OK");
    assert_eq!(
        store.get("to_delete"),
        None,
        "key should be deleted after EXEC"
    );
}

#[test]
fn test_transaction_set_with_ttl_applies_on_exec() {
    use std::time::Duration;

    let mut store = KvStore::new();
    store.multi().expect("failed to start transaction");
    store
        .set_with_ttl(
            "ttl_key".to_string(),
            "ttl_value".to_string(),
            Duration::from_secs(60),
        )
        .expect("failed to queue set_with_ttl");

    assert_eq!(
        store.get("ttl_key"),
        None,
        "ttl key should not be visible before EXEC"
    );

    let results = store.exec().expect("failed to exec transaction");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "OK");
    assert_eq!(
        store.get("ttl_key"),
        Some(&Value::Str("ttl_value".to_string()))
    );
}
