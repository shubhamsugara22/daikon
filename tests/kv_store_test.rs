use rust_kv_store::config::StoreConfig;
use rust_kv_store::error::KvStoreError;
use rust_kv_store::kv_store::{KvStore, Value};
use rust_kv_store::pubsub::PubSub;
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

// ============ Pub/Sub Tests ============

#[test]
fn test_pubsub_subscribe_and_publish() {
    let pubsub = PubSub::new();
    let subscription_id = PubSub::new_subscriber_id();

    // Subscribe to a channel
    pubsub
        .subscribe("news".to_string(), subscription_id.clone())
        .expect("subscribe failed");

    // Publish a message
    let count = pubsub
        .publish("news".to_string(), "breaking news!".to_string())
        .expect("publish failed");
    assert_eq!(count, 1, "should have 1 subscriber");

    // Poll messages
    let messages = pubsub
        .poll_messages(subscription_id, 10)
        .expect("poll failed");
    assert_eq!(messages.len(), 1, "should have 1 message");
    assert_eq!(messages[0].message, "breaking news!");
    assert_eq!(messages[0].channel, "news");
}

#[test]
fn test_pubsub_multiple_subscribers() {
    let pubsub = PubSub::new();
    let sub1 = PubSub::new_subscriber_id();
    let sub2 = PubSub::new_subscriber_id();
    let sub3 = PubSub::new_subscriber_id();

    // Subscribe all three to the same channel
    pubsub
        .subscribe("alerts".to_string(), sub1.clone())
        .expect("subscribe1 failed");
    pubsub
        .subscribe("alerts".to_string(), sub2.clone())
        .expect("subscribe2 failed");
    pubsub
        .subscribe("alerts".to_string(), sub3.clone())
        .expect("subscribe3 failed");

    // Publish to the channel
    let count = pubsub
        .publish("alerts".to_string(), "alert message".to_string())
        .expect("publish failed");
    assert_eq!(count, 3, "should have 3 subscribers");

    // Each subscriber should get the message
    let msg1 = pubsub
        .poll_messages(sub1, 10)
        .expect("poll1 failed");
    let msg2 = pubsub
        .poll_messages(sub2, 10)
        .expect("poll2 failed");
    let msg3 = pubsub
        .poll_messages(sub3, 10)
        .expect("poll3 failed");

    assert_eq!(msg1.len(), 1);
    assert_eq!(msg2.len(), 1);
    assert_eq!(msg3.len(), 1);
}

#[test]
fn test_pubsub_unsubscribe() {
    let pubsub = PubSub::new();
    let sub1 = PubSub::new_subscriber_id();
    let sub2 = PubSub::new_subscriber_id();

    // Subscribe both
    pubsub
        .subscribe("channel".to_string(), sub1.clone())
        .expect("subscribe1 failed");
    pubsub
        .subscribe("channel".to_string(), sub2.clone())
        .expect("subscribe2 failed");

    // Unsubscribe one
    pubsub
        .unsubscribe("channel".to_string(), sub1.clone())
        .expect("unsubscribe failed");

    // Publish
    let count = pubsub
        .publish("channel".to_string(), "message".to_string())
        .expect("publish failed");
    assert_eq!(count, 1, "should have 1 subscriber after unsubscribe");

    // Only sub2 should have the message
    let messages = pubsub
        .poll_messages(sub2, 10)
        .expect("poll failed");
    assert_eq!(messages.len(), 1);
}

#[test]
fn test_pubsub_multiple_channels() {
    let pubsub = PubSub::new();
    let sub = PubSub::new_subscriber_id();

    // Subscribe to multiple channels
    pubsub
        .subscribe("sports".to_string(), sub.clone())
        .expect("subscribe to sports failed");
    pubsub
        .subscribe("weather".to_string(), sub.clone())
        .expect("subscribe to weather failed");
    pubsub
        .subscribe("tech".to_string(), sub.clone())
        .expect("subscribe to tech failed");

    // Publish to each channel
    pubsub
        .publish("sports".to_string(), "goal!".to_string())
        .expect("publish to sports failed");
    pubsub
        .publish("weather".to_string(), "sunny".to_string())
        .expect("publish to weather failed");
    pubsub
        .publish("tech".to_string(), "AI update".to_string())
        .expect("publish to tech failed");

    // Poll all messages (should be 3)
    let messages = pubsub
        .poll_messages(sub, 10)
        .expect("poll failed");
    assert_eq!(messages.len(), 3);

    // Verify we got messages from all channels
    let channels: std::collections::HashSet<_> =
        messages.iter().map(|m| &m.channel).collect();
    assert_eq!(channels.len(), 3);
}

#[test]
fn test_pubsub_list_channels() {
    let pubsub = PubSub::new();
    let sub = PubSub::new_subscriber_id();

    // Subscribe to multiple channels
    pubsub
        .subscribe("ch1".to_string(), sub.clone())
        .expect("subscribe to ch1 failed");
    pubsub
        .subscribe("ch2".to_string(), sub.clone())
        .expect("subscribe to ch2 failed");
    pubsub
        .subscribe("ch3".to_string(), sub.clone())
        .expect("subscribe to ch3 failed");

    // List channels
    let channels = pubsub
        .list_channels()
        .expect("list channels failed");
    assert_eq!(channels.len(), 3);
    assert!(channels.contains(&"ch1".to_string()));
    assert!(channels.contains(&"ch2".to_string()));
    assert!(channels.contains(&"ch3".to_string()));
}

#[test]
fn test_pubsub_list_subscribers() {
    let pubsub = PubSub::new();
    let sub1 = PubSub::new_subscriber_id();
    let sub2 = PubSub::new_subscriber_id();
    let sub3 = PubSub::new_subscriber_id();

    // Subscribe multiple to same channel
    pubsub
        .subscribe("channel".to_string(), sub1.clone())
        .expect("subscribe1 failed");
    pubsub
        .subscribe("channel".to_string(), sub2.clone())
        .expect("subscribe2 failed");
    pubsub
        .subscribe("channel".to_string(), sub3.clone())
        .expect("subscribe3 failed");

    // List subscribers
    let subscribers = pubsub
        .list_subscribers("channel".to_string())
        .expect("list subscribers failed");
    assert_eq!(subscribers.len(), 3);
}

#[test]
fn test_pubsub_pending_message_count() {
    let pubsub = PubSub::new();
    let sub = PubSub::new_subscriber_id();

    pubsub
        .subscribe("channel".to_string(), sub.clone())
        .expect("subscribe failed");

    // Publish multiple messages
    pubsub
        .publish("channel".to_string(), "msg1".to_string())
        .expect("publish1 failed");
    pubsub
        .publish("channel".to_string(), "msg2".to_string())
        .expect("publish2 failed");
    pubsub
        .publish("channel".to_string(), "msg3".to_string())
        .expect("publish3 failed");

    // Check pending count
    let count = pubsub
        .pending_message_count(sub.clone())
        .expect("pending count failed");
    assert_eq!(count, 3);

    // Poll one message and check count again
    pubsub
        .poll_messages(sub.clone(), 1)
        .expect("poll failed");
    let count = pubsub
        .pending_message_count(sub)
        .expect("pending count failed");
    assert_eq!(count, 2);
}

#[test]
fn test_pubsub_empty_channel_cleanup() {
    let pubsub = PubSub::new();
    let sub = PubSub::new_subscriber_id();

    pubsub
        .subscribe("channel".to_string(), sub.clone())
        .expect("subscribe failed");

    // Unsubscribe
    pubsub
        .unsubscribe("channel".to_string(), sub)
        .expect("unsubscribe failed");

    // Channel should be cleaned up
    let channels = pubsub
        .list_channels()
        .expect("list channels failed");
    assert!(channels.is_empty(), "empty channels should be cleaned up");
}

#[test]
fn test_pubsub_message_fifo_order() {
    let pubsub = PubSub::new();
    let sub = PubSub::new_subscriber_id();

    pubsub
        .subscribe("channel".to_string(), sub.clone())
        .expect("subscribe failed");

    // Publish messages in order
    for i in 0..5 {
        pubsub
            .publish("channel".to_string(), format!("msg{}", i))
            .expect(&format!("publish msg{} failed", i));
    }

    // Poll and verify FIFO order
    let messages = pubsub
        .poll_messages(sub, 10)
        .expect("poll failed");
    assert_eq!(messages.len(), 5);

    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.message, format!("msg{}", i));
    }
}

