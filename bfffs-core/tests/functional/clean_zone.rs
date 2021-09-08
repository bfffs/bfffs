// vim: tw=80
use bfffs_core::{
    cache::*,
    database::*,
    ddml::*,
    fs::*,
    idml::*,
    pool::*,
};
use rstest::rstest;
use std::{
    ffi::OsString,
    fs,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    thread,
    time
};
use tempfile::Builder;
use tokio::runtime::Runtime;

type Harness = (Arc<Database>, Fs, Runtime);

fn harness(devsize: u64, zone_size: u64) -> Harness {
    // Fs::new() requires the threaded scheduler
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let tempdir = t!(Builder::new().prefix("test_fs").tempdir());
    let filename = tempdir.path().join("vdev");
    let file = t!(fs::File::create(&filename));
    t!(file.set_len(devsize));
    drop(file);
    let zone_size = NonZeroU64::new(zone_size);
    let cluster = Pool::create_cluster(None, 1, zone_size, 0,
                                       &[filename]);
    let pool = Pool::create(String::from("test_fs"), vec![cluster]);
    let cache = Arc::new(
        Mutex::new(
            Cache::with_capacity(32_000_000)
        )
    );
    let ddml = Arc::new(DDML::new(pool, cache.clone()));
    let idml = IDML::create(ddml, cache);
    let db = Arc::new(Database::create(Arc::new(idml), handle));
    let (db, tree_id) = rt.block_on(async move {
        let tree_id = db.new_fs(Vec::new())
            .await.unwrap();
        (db, tree_id)
    });
    let handle = rt.handle().clone();
    let fs = Fs::new(db.clone(), handle, tree_id);
    (db, fs, rt)
}

// This tests a regression in database::Syncer::run.  However, I can't
// reproduce it without creating a FileSystem, so that's why it's in this
// file.
#[ignore = "Test is slow" ]
#[rstest]
#[case(harness(1 << 20, 32))]
fn sleep_sync_sync(#[case] harness: Harness) {
    let (_db, fs, _rt) = harness;
    thread::sleep(time::Duration::from_millis(5500));
    fs.sync();
    fs.sync();
}

// Minimal test for cleaning zones.  Fills up the first zone and part of the
// second.  Then deletes most of the data in the first zone.  Then cleans
// it, and does not reopen it.  We just have to sort-of take it on faith
// that zone 0 is clean, because the public API doesn't expose zones.
#[rstest]
#[case(harness(1 << 20, 32))]
fn clean_zone(#[case] harness: Harness) {
    let (db, fs, rt) = harness;
    let root = fs.root();
    let small_filename = OsString::from("small");
    let small_fd = fs.create(&root, &small_filename, 0o644, 0, 0).unwrap();
    let buf = vec![42u8; 4096];
    fs.write(&small_fd, 0, &buf[..], 0).unwrap();

    let big_filename = OsString::from("big");
    let big_fd = fs.create(&root, &big_filename, 0o644, 0, 0).unwrap();
    for i in 0..18 {
        fs.write(&big_fd, i * 4096, &buf[..], 0).unwrap();
    }
    fs.sync();

    fs.unlink(&root, Some(&big_fd), &big_filename).unwrap();
    fs.sync();

    rt.block_on(db.clean()).unwrap();
    fs.sync();
}

#[ignore = "Test is slow" ]
#[rstest]
#[case(harness(1 << 30, 512))]
fn clean_zone_leak(#[case] harness: Harness) {
    let (db, fs, rt) = harness;
    let root = fs.root();
    for i in 0..16384 {
        let fname = format!("f.{}", i);
        fs.mkdir(&root, &OsString::from(fname), 0o755, 0, 0).unwrap();
    }
    fs.sync();
    for i in 0..8000 {
        let fname = format!("f.{}", i);
        fs.rmdir(&root, &OsString::from(fname)).unwrap();
    }
    fs.sync();
    let mut statvfs = fs.statvfs().unwrap();
    println!("Before cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    assert!(rt.block_on(db.check()).unwrap());
    rt.block_on(db.clean()).unwrap();
    statvfs = fs.statvfs().unwrap();
    println!("After cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    assert!(rt.block_on(db.check()).unwrap());
    drop(fs);
    let owned_db = Arc::try_unwrap(db)
    .ok().expect("Unwrapping Database failed");
    rt.block_on(owned_db.shutdown());
}

/// A regression test for bug d5b4dab35d9be12ff1505e886ed5ca8ad4b6a526
/// (node.0.get_mut() fails in Tree::flush_r).  As of
/// ca9343b86ba5395c56b3c6fa6a4c50cd8f7540fe, it fails about 30% of the
/// time.
#[ignore = "Test is slow and intermittent" ]
#[rstest]
#[case(harness(1 << 30, 512))]
fn get_mut(#[case] harness: Harness) {
    let (db, fs, rt) = harness;
    let root = fs.root();
    for i in 0..16384 {
        let fname = format!("f.{}", i);
        fs.mkdir(&root, &OsString::from(fname), 0o755, 0, 0).unwrap();
    }
    fs.sync();
    for i in 0..8192 {
        let fname = format!("f.{}", 2 * i);
        fs.rmdir(&root, &OsString::from(fname)).unwrap();
    }
    fs.sync();
    let mut statvfs = fs.statvfs().unwrap();
    println!("Before cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    rt.block_on(db.clean()).unwrap();
    statvfs = fs.statvfs().unwrap();
    println!("After cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
}
