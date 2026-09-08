use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn extract_snapshot_timestamp(filename: &str) -> Option<u64> {
    let filename = filename.strip_suffix(".enc").unwrap_or(filename);
    let core = filename
        .strip_prefix("snapshot_")?
        .strip_suffix(".json")
        .or_else(|| filename.strip_prefix("snapshot_")?.strip_suffix(".json.gz"))
        .or_else(|| filename.strip_prefix("snapshot_")?.strip_suffix(".json.zst"))?;
    let ts_part = core.split('_').next()?;
    ts_part.parse::<u64>().ok()
}

fn main() {
    let dir = std::env::temp_dir().join("pitr-repro");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    let ext = ".json";
    let mut snapshot_filename = format!("snapshot_{}{}", ts, ext);
    let mut snapshot_path = dir.join(&snapshot_filename);
    let mut suffix = 1u64;
    while snapshot_path.exists() {
        snapshot_filename = format!("snapshot_{}_{}{}", ts, suffix, ext);
        snapshot_path = dir.join(&snapshot_filename);
        suffix += 1;
    }
    fs::write(&snapshot_path, b"x").unwrap();
    let ts2 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    let ext2 = ".json";
    let mut snapshot_filename2 = format!("snapshot_{}{}", ts2, ext2);
    let mut snapshot_path2 = dir.join(&snapshot_filename2);
    let mut suffix2 = 1u64;
    while snapshot_path2.exists() {
        snapshot_filename2 = format!("snapshot_{}_{}{}", ts2, suffix2, ext2);
        snapshot_path2 = dir.join(&snapshot_filename2);
        suffix2 += 1;
    }
    fs::write(&snapshot_path2, b"y").unwrap();
    println!("first={} second={}", snapshot_filename, snapshot_filename2);
    for e in fs::read_dir(&dir).unwrap() { let e=e.unwrap(); println!("file={}", e.file_name().to_string_lossy()); println!("ts={:?}", extract_snapshot_timestamp(e.file_name().to_str().unwrap())); }
}
