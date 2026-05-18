use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rust_kv_store::config::StoreConfig;
use rust_kv_store::kv_store::{KvStore, TransactionOp, Value};
use std::collections::HashMap;

fn benchmark_basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_operations");
    group.sample_size(100);

    // SET operation
    group.bench_function("set", |b| {
        let mut store = KvStore::new();
        b.iter(|| {
            store.set(
                black_box("test_key".to_string()),
                black_box("test_value".to_string()),
            )
        });
    });

    // GET operation
    group.bench_function("get", |b| {
        let mut store = KvStore::new();
        store
            .set("test_key".to_string(), "test_value".to_string())
            .unwrap();
        b.iter(|| store.get(black_box("test_key")));
    });

    // DELETE operation
    group.bench_function("delete", |b| {
        let mut store = KvStore::new();
        b.iter(|| {
            store.set("key".to_string(), "value".to_string()).unwrap();
            store.delete(black_box("key"));
        });
    });

    group.finish();
}

fn benchmark_atomic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_operations");
    group.sample_size(100);

    // INCR operation
    group.bench_function("incr", |b| {
        let mut store = KvStore::new();
        store.set("counter".to_string(), 0i64).unwrap();
        b.iter(|| store.incr(black_box("counter")));
    });

    // APPEND operation
    group.bench_function("append", |b| {
        let mut store = KvStore::new();
        store.set("text".to_string(), "hello".to_string()).unwrap();
        b.iter(|| store.append(black_box("text"), black_box(" world")));
    });

    group.finish();
}

fn benchmark_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_operations");
    group.sample_size(50);

    // MSET with different sizes
    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("mset", size), size, |b, &size| {
            let mut store = KvStore::new();
            let pairs: Vec<_> = (0..size)
                .map(|i| (format!("key{}", i), format!("value{}", i)))
                .collect();
            b.iter(|| store.mset(black_box(pairs.clone())));
        });
    }

    group.finish();
}

fn benchmark_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_operations");
    group.sample_size(50);

    // Store with custom memory limit
    group.bench_function("memory_limit_enforcement", |b| {
        let config = StoreConfig {
            max_memory_bytes: 1_000_000, // 1MB
            max_value_size: 100_000,
            lru_eviction_enabled: true,
            ..StoreConfig::default()
        };
        let mut store = KvStore::with_config(config);

        b.iter(|| {
            for i in 0..100 {
                let key = format!("key{}", i);
                let value = "x".repeat(10_000); // 10KB values
                let _ = store.set(key, value);
            }
        });
    });

    group.finish();
}

fn benchmark_pattern_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_matching");
    group.sample_size(50);

    group.bench_function("keys_wildcard", |b| {
        let mut store = KvStore::new();
        for i in 0..1000 {
            let key = format!("user:{}:data", i);
            store.set(key, "value".to_string()).unwrap();
        }
        b.iter(|| store.keys(black_box("user:*:data")));
    });

    group.finish();
}

fn benchmark_hash_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_operations");
    group.sample_size(80);

    group.bench_function("hset_10_fields", |b| {
        let mut store = KvStore::new();
        let fields: HashMap<String, String> = (0..10)
            .map(|i| (format!("field{}", i), format!("value{}", i)))
            .collect();

        b.iter(|| store.hset(black_box("user:42"), black_box(fields.clone())));
    });

    group.bench_function("hget_existing_field", |b| {
        let mut store = KvStore::new();
        let fields: HashMap<String, String> = (0..20)
            .map(|i| (format!("field{}", i), format!("value{}", i)))
            .collect();
        store.hset("user:42", fields).unwrap();

        b.iter(|| store.hget(black_box("user:42"), black_box("field7")));
    });

    group.bench_function("hmget_10_fields", |b| {
        let mut store = KvStore::new();
        let fields: HashMap<String, String> = (0..100)
            .map(|i| (format!("field{}", i), format!("value{}", i)))
            .collect();
        let query_fields: Vec<String> = (0..10).map(|i| format!("field{}", i)).collect();

        store.hset("user:42", fields).unwrap();
        b.iter(|| store.hmget(black_box("user:42"), black_box(&query_fields)));
    });

    group.bench_function("hincrby_hot_field", |b| {
        let mut store = KvStore::new();
        let mut fields = HashMap::new();
        fields.insert("counter".to_string(), "0".to_string());
        store.hset("counters", fields).unwrap();

        b.iter(|| store.hincrby(black_box("counters"), black_box("counter"), black_box(1)));
    });

    group.finish();
}

fn benchmark_list_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_operations");
    group.sample_size(80);

    group.bench_function("rpush_10_values", |b| {
        let mut store = KvStore::new();
        let values: Vec<String> = (0..10).map(|i| format!("v{}", i)).collect();

        b.iter(|| store.rpush(black_box("jobs"), black_box(values.clone())));
    });

    group.bench_function("lpop_hot_list", |b| {
        let mut store = KvStore::new();
        store
            .rpush(
                "queue",
                (0..100).map(|i| format!("item{}", i)).collect::<Vec<_>>(),
            )
            .unwrap();

        b.iter(|| {
            let _ = store.lpop(black_box("queue"));
            let _ = store.rpush(black_box("queue"), vec!["replenish".to_string()]);
        });
    });

    group.bench_function("lrange_100_from_1000", |b| {
        let mut store = KvStore::new();
        store
            .rpush(
                "timeline",
                (0..1000).map(|i| format!("event{}", i)).collect::<Vec<_>>(),
            )
            .unwrap();

        b.iter(|| store.lrange(black_box("timeline"), black_box(0), black_box(99)));
    });

    group.finish();
}

fn benchmark_pipeline_like_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_like_operations");
    group.sample_size(60);

    for size in [10usize, 50usize, 100usize].iter() {
        group.bench_with_input(
            BenchmarkId::new("multi_exec_set_batch", size),
            size,
            |b, &size| {
                let mut store = KvStore::new();

                b.iter(|| {
                    store.multi().unwrap();
                    for i in 0..size {
                        store
                            .queue_operation(TransactionOp::Set(
                                format!("pipe:key:{}", i),
                                Value::from(format!("value{}", i)),
                                None,
                            ))
                            .unwrap();
                    }
                    store.exec().unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_basic_operations,
    benchmark_atomic_operations,
    benchmark_batch_operations,
    benchmark_memory_usage,
    benchmark_pattern_matching,
    benchmark_hash_operations,
    benchmark_list_operations,
    benchmark_pipeline_like_operations
);
criterion_main!(benches);
