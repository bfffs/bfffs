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

type Harness = (Arc<Database>, Fs);

async fn harness(devsize: u64, zone_size: u64) -> Harness {
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
    let db = Arc::new(Database::create(Arc::new(idml)));
    let tree_id = db.new_fs(Vec::new()).await.unwrap();
    let fs = Fs::new(db.clone(), tree_id).await;
    (db, fs)
}

// This tests a regression in database::Syncer::run.  However, I can't
// reproduce it without creating a FileSystem, so that's why it's in this
// file.
#[ignore = "Test is slow" ]
#[rstest]
#[case(1 << 20, 32)]
#[tokio::test]
async fn sleep_sync_sync(#[case] devsize: u64, #[case] zone_size: u64) {
    let (_db, fs) = harness(devsize, zone_size).await;
    thread::sleep(time::Duration::from_millis(5500));
    fs.sync().await;
    fs.sync().await;
}

// Minimal test for cleaning zones.  Fills up the first zone and part of the
// second.  Then deletes most of the data in the first zone.  Then cleans
// it, and does not reopen it.  We just have to sort-of take it on faith
// that zone 0 is clean, because the public API doesn't expose zones.
#[rstest]
#[case(1 << 20, 32)]
#[tokio::test]
async fn clean_zone(#[case] devsize: u64, #[case] zone_size: u64) {
    let (db, fs) = harness(devsize, zone_size).await;
    let root = fs.root();
    let small_filename = OsString::from("small");
    let small_fd = fs.create(&root, &small_filename, 0o644, 0, 0).await
        .unwrap();
    let buf = vec![42u8; 4096];
    fs.write(&small_fd, 0, &buf[..], 0).await
        .unwrap();

    let big_filename = OsString::from("big");
    let big_fd = fs.create(&root, &big_filename, 0o644, 0, 0).await
        .unwrap();
    for i in 0..18 {
        fs.write(&big_fd, i * 4096, &buf[..], 0).await
            .unwrap();
    }
    fs.sync().await;

    fs.unlink(&root, Some(&big_fd), &big_filename).await.unwrap();
    fs.sync().await;

    db.clean().await.unwrap();
    fs.sync().await;
}

#[ignore = "Test is slow" ]
#[rstest]
#[case(1 << 30, 512)]
#[tokio::test]
async fn clean_zone_leak(#[case] devsize: u64, #[case] zone_size: u64) {
    let (db, fs) = harness(devsize, zone_size).await;
    let root = fs.root();
    for i in 0..16384 {
        let fname = format!("f.{}", i);
        fs.mkdir(&root, &OsString::from(fname), 0o755, 0, 0).await.unwrap();
    }
    fs.sync().await;
    for i in 0..8000 {
        let fname = format!("f.{}", i);
        fs.rmdir(&root, &OsString::from(fname)).await.unwrap();
    }
    fs.sync().await;
    let mut statvfs = fs.statvfs().await.unwrap();
    println!("Before cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    assert!(db.check().await.unwrap());
    db.clean().await.unwrap();
    statvfs = fs.statvfs().await.unwrap();
    println!("After cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    assert!(db.check().await.unwrap());
    drop(fs);
    let owned_db = Arc::try_unwrap(db)
    .ok().expect("Unwrapping Database failed");
    owned_db.shutdown().await;
}

/// A regression test for bug d5b4dab35d9be12ff1505e886ed5ca8ad4b6a526
/// (node.0.get_mut() fails in Tree::flush_r).  As of
/// ca9343b86ba5395c56b3c6fa6a4c50cd8f7540fe, it fails about 30% of the
/// time.
#[ignore = "Test is slow and intermittent" ]
#[rstest]
#[case(1 << 30, 512)]
#[tokio::test]
async fn get_mut(#[case] devsize: u64, #[case] zone_size: u64) {
    let (db, fs) = harness(devsize, zone_size).await;
    let root = fs.root();
    for i in 0..16384 {
        let fname = format!("f.{}", i);
        fs.mkdir(&root, &OsString::from(fname), 0o755, 0, 0).await.unwrap();
    }
    fs.sync().await;
    for i in 0..8192 {
        let fname = format!("f.{}", 2 * i);
        fs.rmdir(&root, &OsString::from(fname)).await.unwrap();
    }
    fs.sync().await;
    let mut statvfs = fs.statvfs().await.unwrap();
    println!("Before cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
    db.clean().await.unwrap();
    statvfs = fs.statvfs().await.unwrap();
    println!("After cleaning: {:?} free out of {:?}",
             statvfs.f_bfree, statvfs.f_blocks);
}
