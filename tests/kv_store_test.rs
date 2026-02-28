use rust_kv_store::kv_store::{KvStore, Value};
use std::env;
use std::fs;

#[test]
fn test_versioning_and_prune() {
    use std::time::Duration;
    let mut store = KvStore::new();
    store.set("a".to_string(), "1".to_string());

    let mut path = std::env::temp_dir();
    path.push("kv_store_version.json");

    // cleanup any leftover from previous runs
    let _ = fs::remove_file(&path);

    // first save -> no backup yet
    store.save_with_version(&path, 2).expect("save1 failed");

    // small sleep to ensure different timestamps (only necessary on very fast filesystems)
    std::thread::sleep(Duration::from_millis(10));

    // second save -> creates first backup
    store.set("b".to_string(), "2".to_string());
    store.save_with_version(&path, 2).expect("save2 failed");

    std::thread::sleep(Duration::from_millis(10));

    // third save -> creates another backup and should prune keeping max 2
    store.set("c".to_string(), "3".to_string());
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
fn test_save_and_load() {
    let mut store = KvStore::new();
    store.set("k1".to_string(), "v1".to_string());
    store.set("k2".to_string(), 123_i32);

    let mut path = env::temp_dir();
    path.push("kv_store_test.json");

    // save
    store.save_to_file(&path).expect("save failed");

    // load
    let mut loaded = KvStore::load_from_file(&path).expect("load failed");

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
