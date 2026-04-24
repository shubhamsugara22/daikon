use rust_kv_store::config::StoreConfig;
use rust_kv_store::error::KvStoreError;
use rust_kv_store::kv_store::{KeyspaceEventKind, KvStore, Value};
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
    let config = StoreConfig {
        max_value_size: 100,
        ..StoreConfig::default()
    };
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
    let config = StoreConfig {
        max_memory_bytes: 500,
        max_value_size: 500,
        lru_eviction_enabled: true,
        ..StoreConfig::default()
    };
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
    let config = StoreConfig {
        max_memory_bytes: 1000,
        max_value_size: 500,
        lru_eviction_enabled: true,
        ..StoreConfig::default()
    };
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
    let config = StoreConfig {
        max_key_size: 0,
        ..StoreConfig::default()
    };

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
    let msg1 = pubsub.poll_messages(sub1, 10).expect("poll1 failed");
    let msg2 = pubsub.poll_messages(sub2, 10).expect("poll2 failed");
    let msg3 = pubsub.poll_messages(sub3, 10).expect("poll3 failed");

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
    let messages = pubsub.poll_messages(sub2, 10).expect("poll failed");
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
    let messages = pubsub.poll_messages(sub, 10).expect("poll failed");
    assert_eq!(messages.len(), 3);

    // Verify we got messages from all channels
    let channels: std::collections::HashSet<_> = messages.iter().map(|m| &m.channel).collect();
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
    let channels = pubsub.list_channels().expect("list channels failed");
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
    pubsub.poll_messages(sub.clone(), 1).expect("poll failed");
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
    let channels = pubsub.list_channels().expect("list channels failed");
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
            .unwrap_or_else(|e| panic!("publish msg{} failed: {}", i, e));
    }

    // Poll and verify FIFO order
    let messages = pubsub.poll_messages(sub, 10).expect("poll failed");
    assert_eq!(messages.len(), 5);

    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.message, format!("msg{}", i));
    }
}

// ── HyperLogLog tests ────────────────────────────────────────────────────────

#[test]
fn test_hll_pfadd_returns_positive_count() {
    let mut store = KvStore::new();
    let count = store
        .pfadd(
            "hll1".to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .expect("pfadd failed");
    assert!(count > 0, "estimated cardinality should be at least 1");
}

#[test]
fn test_hll_pfcount_matches_pfadd_result() {
    let mut store = KvStore::new();
    let add_count = store
        .pfadd("hll1".to_string(), vec!["x".to_string(), "y".to_string()])
        .expect("pfadd failed");
    let read_count = store.pfcount("hll1").expect("pfcount failed");
    assert_eq!(add_count, read_count);
}

#[test]
fn test_hll_pfadd_grows_on_new_values() {
    let mut store = KvStore::new();
    store
        .pfadd("hll1".to_string(), vec!["a".to_string()])
        .expect("pfadd failed");
    let count_after_one = store.pfcount("hll1").expect("pfcount failed");

    // Add many distinct values
    let more: Vec<String> = (0..100).map(|i| format!("val{}", i)).collect();
    store.pfadd("hll1".to_string(), more).expect("pfadd failed");
    let count_after_many = store.pfcount("hll1").expect("pfcount failed");

    assert!(
        count_after_many > count_after_one,
        "adding 100 distinct values must increase the estimate"
    );
}

#[test]
fn test_hll_pfcount_missing_key_errors() {
    let store = KvStore::new();
    let result = store.pfcount("no_such_key");
    assert!(result.is_err(), "pfcount on missing key should error");
}

#[test]
fn test_hll_pfmerge_combines_estimates() {
    let mut store = KvStore::new();
    store
        .pfadd("src1".to_string(), vec!["a".to_string(), "b".to_string()])
        .expect("pfadd src1 failed");
    store
        .pfadd("src2".to_string(), vec!["c".to_string(), "d".to_string()])
        .expect("pfadd src2 failed");

    let merged_count = store
        .pfmerge("dst".to_string(), &["src1".to_string(), "src2".to_string()])
        .expect("pfmerge failed");

    // The merged HLL should capture more unique values than either source alone
    let src1_count = store.pfcount("src1").expect("pfcount src1");
    let src2_count = store.pfcount("src2").expect("pfcount src2");
    assert!(
        merged_count >= src1_count.min(src2_count),
        "merged estimate should be at least as large as the smaller source"
    );
}

#[test]
fn test_hll_type_mismatch_on_string_key() {
    let mut store = KvStore::new();
    store
        .set("strkey".to_string(), "hello".to_string())
        .expect("set failed");
    let result = store.pfcount("strkey");
    assert!(
        matches!(result, Err(KvStoreError::TypeMismatch { .. })),
        "pfcount on a String key should return TypeMismatch"
    );
}

// ── List data type tests ──

#[test]
fn test_lpush_creates_list_and_returns_length() {
    let mut store = KvStore::new();
    let len = store
        .lpush("mylist", vec!["a".into(), "b".into(), "c".into()])
        .unwrap();
    assert_eq!(len, 3);
    // LPUSH reverses order like Redis: c, b, a
    let items = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(items, vec!["c", "b", "a"]);
}

#[test]
fn test_rpush_creates_list_and_returns_length() {
    let mut store = KvStore::new();
    let len = store
        .rpush("mylist", vec!["a".into(), "b".into(), "c".into()])
        .unwrap();
    assert_eq!(len, 3);
    let items = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(items, vec!["a", "b", "c"]);
}

#[test]
fn test_lpush_rpush_combined() {
    let mut store = KvStore::new();
    store.rpush("mylist", vec!["a".into()]).unwrap();
    store.lpush("mylist", vec!["b".into()]).unwrap();
    store.rpush("mylist", vec!["c".into()]).unwrap();
    let items = store.lrange("mylist", 0, -1).unwrap();
    assert_eq!(items, vec!["b", "a", "c"]);
}

#[test]
fn test_lpop_rpop() {
    let mut store = KvStore::new();
    store
        .rpush("mylist", vec!["a".into(), "b".into(), "c".into()])
        .unwrap();

    assert_eq!(store.lpop("mylist").unwrap(), Some("a".into()));
    assert_eq!(store.rpop("mylist").unwrap(), Some("c".into()));
    assert_eq!(store.llen("mylist").unwrap(), 1);
    assert_eq!(store.lpop("mylist").unwrap(), Some("b".into()));
    // Empty list
    assert_eq!(store.lpop("mylist").unwrap(), None);
    assert_eq!(store.rpop("mylist").unwrap(), None);
}

#[test]
fn test_lpop_rpop_missing_key() {
    let mut store = KvStore::new();
    assert_eq!(store.lpop("nokey").unwrap(), None);
    assert_eq!(store.rpop("nokey").unwrap(), None);
}

#[test]
fn test_lrange_with_negative_indices() {
    let mut store = KvStore::new();
    store
        .rpush(
            "mylist",
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
        )
        .unwrap();
    // Last two elements
    assert_eq!(store.lrange("mylist", -2, -1).unwrap(), vec!["c", "d"]);
    // All elements via negative
    assert_eq!(
        store.lrange("mylist", -4, -1).unwrap(),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn test_lrange_out_of_bounds() {
    let mut store = KvStore::new();
    store.rpush("mylist", vec!["a".into(), "b".into()]).unwrap();
    // Overshoot end → clamped
    let items = store.lrange("mylist", 0, 100).unwrap();
    assert_eq!(items, vec!["a", "b"]);
    // start > stop → empty
    let items = store.lrange("mylist", 5, 1).unwrap();
    assert!(items.is_empty());
}

#[test]
fn test_lrange_missing_key() {
    let store = KvStore::new();
    let items = store.lrange("nokey", 0, -1).unwrap();
    assert!(items.is_empty());
}

#[test]
fn test_llen() {
    let mut store = KvStore::new();
    assert_eq!(store.llen("mylist").unwrap(), 0);
    store.rpush("mylist", vec!["a".into(), "b".into()]).unwrap();
    assert_eq!(store.llen("mylist").unwrap(), 2);
    store.lpop("mylist").unwrap();
    assert_eq!(store.llen("mylist").unwrap(), 1);
}

#[test]
fn test_list_type_mismatch_on_string_key() {
    let mut store = KvStore::new();
    store
        .set("strkey".to_string(), "hello".to_string())
        .unwrap();
    assert!(matches!(
        store.lpush("strkey", vec!["a".into()]),
        Err(KvStoreError::TypeMismatch { .. })
    ));
    assert!(matches!(
        store.rpush("strkey", vec!["a".into()]),
        Err(KvStoreError::TypeMismatch { .. })
    ));
    assert!(matches!(
        store.lpop("strkey"),
        Err(KvStoreError::TypeMismatch { .. })
    ));
    assert!(matches!(
        store.rpop("strkey"),
        Err(KvStoreError::TypeMismatch { .. })
    ));
    assert!(matches!(
        store.lrange("strkey", 0, -1),
        Err(KvStoreError::TypeMismatch { .. })
    ));
    assert!(matches!(
        store.llen("strkey"),
        Err(KvStoreError::TypeMismatch { .. })
    ));
}

// ── Keyspace notification tests ──

#[test]
fn test_keyspace_events_disabled_by_default() {
    let mut store = KvStore::new();
    assert!(!store.keyspace_notifications_enabled());
    store.set("a".to_string(), "1".to_string()).unwrap();
    let events = store.drain_keyspace_events();
    assert!(events.is_empty(), "no events when notifications disabled");
}

#[test]
fn test_keyspace_events_set_emits_event() {
    let mut store = KvStore::new();
    store.set_keyspace_notifications_enabled(true);

    store.set("mykey".to_string(), "val".to_string()).unwrap();

    let events = store.drain_keyspace_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, KeyspaceEventKind::Set);
    assert_eq!(events[0].key, "mykey");
    assert!(events[0].timestamp > 0);
}

#[test]
fn test_keyspace_events_delete_emits_event() {
    let mut store = KvStore::new();
    store.set_keyspace_notifications_enabled(true);

    store.set("delme".to_string(), "val".to_string()).unwrap();
    store.drain_keyspace_events(); // clear the set event

    store.delete("delme").unwrap();
    let events = store.drain_keyspace_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, KeyspaceEventKind::Delete);
    assert_eq!(events[0].key, "delme");
}

#[test]
fn test_keyspace_events_expired_emits_event() {
    use std::time::Duration;

    let mut store = KvStore::new();
    store.set_keyspace_notifications_enabled(true);

    // Set a key with a 0-second TTL so it expires immediately
    store
        .set_with_ttl(
            "ttlkey".to_string(),
            "val".to_string(),
            Duration::from_secs(0),
        )
        .unwrap();
    store.drain_keyspace_events(); // clear the set event

    // Wait briefly then run cleanup
    std::thread::sleep(Duration::from_millis(50));
    let cleaned = store.cleanup_expired();
    assert!(cleaned > 0, "key should have been cleaned up");

    let events = store.drain_keyspace_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, KeyspaceEventKind::Expired);
    assert_eq!(events[0].key, "ttlkey");
}

#[test]
fn test_keyspace_events_drain_clears_queue() {
    let mut store = KvStore::new();
    store.set_keyspace_notifications_enabled(true);

    store.set("a".to_string(), "1".to_string()).unwrap();
    store.set("b".to_string(), "2".to_string()).unwrap();

    let events = store.drain_keyspace_events();
    assert_eq!(events.len(), 2);

    // Second drain should be empty
    let events2 = store.drain_keyspace_events();
    assert!(events2.is_empty(), "drain should clear the queue");
}

#[test]
fn test_keyspace_events_toggle_enabled() {
    let mut store = KvStore::new();
    assert!(!store.keyspace_notifications_enabled());

    store.set_keyspace_notifications_enabled(true);
    assert!(store.keyspace_notifications_enabled());

    store.set("a".to_string(), "1".to_string()).unwrap();
    assert_eq!(store.drain_keyspace_events().len(), 1);

    // Disable and verify no events
    store.set_keyspace_notifications_enabled(false);
    store.set("b".to_string(), "2".to_string()).unwrap();
    assert!(store.drain_keyspace_events().is_empty());
}

#[test]
fn test_keyspace_events_multiple_ops_ordered() {
    let mut store = KvStore::new();
    store.set_keyspace_notifications_enabled(true);

    store.set("x".to_string(), "1".to_string()).unwrap();
    store.set("y".to_string(), "2".to_string()).unwrap();
    store.delete("x").unwrap();

    let events = store.drain_keyspace_events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, KeyspaceEventKind::Set);
    assert_eq!(events[0].key, "x");
    assert_eq!(events[1].kind, KeyspaceEventKind::Set);
    assert_eq!(events[1].key, "y");
    assert_eq!(events[2].kind, KeyspaceEventKind::Delete);
    assert_eq!(events[2].key, "x");
}

#[test]
fn test_keyspace_event_kind_display() {
    assert_eq!(format!("{}", KeyspaceEventKind::Set), "set");
    assert_eq!(format!("{}", KeyspaceEventKind::Delete), "del");
    assert_eq!(format!("{}", KeyspaceEventKind::Expired), "expired");
    assert_eq!(format!("{}", KeyspaceEventKind::Evicted), "evicted");
}

// ─── Hash Map Tests ───────────────────────────────────────────────────────────

#[test]
fn test_hash_basic_hset_hget() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("name".to_string(), "Alice".to_string());
    let added = store.hset("user:1", fields).unwrap();
    assert_eq!(added, 1);

    let val = store.hget("user:1", "name").unwrap();
    assert_eq!(val, Some("Alice".to_string()));

    let missing = store.hget("user:1", "age").unwrap();
    assert_eq!(missing, None);
}

#[test]
fn test_hash_multiple_fields() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("f1".to_string(), "v1".to_string());
    fields.insert("f2".to_string(), "v2".to_string());
    fields.insert("f3".to_string(), "v3".to_string());
    let added = store.hset("myhash", fields).unwrap();
    assert_eq!(added, 3);

    let all = store.hgetall("myhash").unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("f1").unwrap(), "v1");
    assert_eq!(all.get("f2").unwrap(), "v2");
    assert_eq!(all.get("f3").unwrap(), "v3");
}

#[test]
fn test_hash_hmget() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("a".to_string(), "1".to_string());
    fields.insert("b".to_string(), "2".to_string());
    store.hset("h", fields).unwrap();

    let keys = vec!["a".to_string(), "missing".to_string(), "b".to_string()];
    let results = store.hmget("h", &keys).unwrap();
    assert_eq!(results[0], Some("1".to_string()));
    assert_eq!(results[1], None);
    assert_eq!(results[2], Some("2".to_string()));
}

#[test]
fn test_hash_hdel() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("x".to_string(), "10".to_string());
    fields.insert("y".to_string(), "20".to_string());
    store.hset("h", fields).unwrap();

    let removed = store.hdel("h", &["x".to_string()]).unwrap();
    assert_eq!(removed, 1);

    assert_eq!(store.hlen("h").unwrap(), 1);
    assert_eq!(store.hget("h", "x").unwrap(), None);

    // deleting a non-existent field returns 0
    let removed2 = store.hdel("h", &["gone".to_string()]).unwrap();
    assert_eq!(removed2, 0);
}

#[test]
fn test_hash_hkeys_hvals() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("k1".to_string(), "v1".to_string());
    fields.insert("k2".to_string(), "v2".to_string());
    store.hset("h", fields).unwrap();

    let mut keys = store.hkeys("h").unwrap();
    keys.sort();
    assert_eq!(keys, vec!["k1", "k2"]);

    let mut vals = store.hvals("h").unwrap();
    vals.sort();
    assert_eq!(vals, vec!["v1", "v2"]);
}

#[test]
fn test_hash_hlen() {
    let mut store = KvStore::new();

    assert_eq!(store.hlen("nokey").unwrap(), 0);

    let mut fields = std::collections::HashMap::new();
    fields.insert("a".to_string(), "1".to_string());
    fields.insert("b".to_string(), "2".to_string());
    store.hset("h", fields).unwrap();
    assert_eq!(store.hlen("h").unwrap(), 2);
}

#[test]
fn test_hash_hexists() {
    let mut store = KvStore::new();

    let mut fields = std::collections::HashMap::new();
    fields.insert("present".to_string(), "yes".to_string());
    store.hset("h", fields).unwrap();

    assert!(store.hexists("h", "present").unwrap());
    assert!(!store.hexists("h", "absent").unwrap());
    assert!(!store.hexists("nokey", "f").unwrap());
}

#[test]
fn test_hash_hincrby() {
    let mut store = KvStore::new();

    // starts at 0 if field doesn't exist
    let r1 = store.hincrby("counter", "hits", 5).unwrap();
    assert_eq!(r1, 5);

    let r2 = store.hincrby("counter", "hits", 3).unwrap();
    assert_eq!(r2, 8);

    let r3 = store.hincrby("counter", "hits", -2).unwrap();
    assert_eq!(r3, 6);
}

#[test]
fn test_hash_hincrbyfloat() {
    let mut store = KvStore::new();

    let r1 = store.hincrbyfloat("h", "score", 1.5).unwrap();
    assert!((r1 - 1.5).abs() < f64::EPSILON);

    let r2 = store.hincrbyfloat("h", "score", 0.5).unwrap();
    assert!((r2 - 2.0).abs() < f64::EPSILON);
}

#[test]
fn test_hash_type_error() {
    let mut store = KvStore::new();

    store
        .set("strkey".to_string(), "value".to_string())
        .unwrap();

    let mut fields = std::collections::HashMap::new();
    fields.insert("f".to_string(), "v".to_string());
    assert!(store.hset("strkey", fields).is_err());
    assert!(store.hget("strkey", "f").is_err());
    assert!(store.hlen("strkey").is_err());
}

#[test]
fn test_hash_nonexistent_key() {
    let store = KvStore::new();

    assert_eq!(store.hgetall("nope").unwrap().len(), 0);
    assert_eq!(store.hkeys("nope").unwrap().len(), 0);
    assert_eq!(store.hvals("nope").unwrap().len(), 0);
    assert_eq!(store.hlen("nope").unwrap(), 0);
    assert_eq!(store.hget("nope", "f").unwrap(), None);
    assert!(!store.hexists("nope", "f").unwrap());
}
