// vim: tw=80

/// Constructs a real filesystem, fills it, and cleans it.
macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name t;

    use bfffs::{
        common::cache::*,
        common::database::*,
        common::ddml::*,
        common::fs::*,
        common::idml::*,
        common::pool::*,
    };
    use futures::{Future, future};
    use std::{
        ffi::OsString,
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio_io_pool::Runtime;

    fixture!( mocks(zone_size: u64) -> (Arc<Database>, Fs, Runtime) {
        setup(&mut self) {
            let mut rt = Runtime::new();
            let handle = rt.handle().clone();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(TempDir::new("test_fs"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let zone_size = NonZeroU64::new(*self.zone_size);
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 1, 1, zone_size, 0, &[filename])
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_fs"), vec![cluster])
                    .map(|pool| {
                        let cache = Arc::new(
                            Mutex::new(
                                Cache::with_capacity(1_000_000)
                            )
                        );
                        let ddml = Arc::new(DDML::new(pool, cache.clone()));
                        let idml = IDML::create(ddml, cache);
                        Arc::new(Database::create(Arc::new(idml), handle))
                    })
                })
            })).unwrap();
            let tree_id = rt.block_on(db.new_fs()).unwrap();
            let fs = Fs::new(db.clone(), rt.handle().clone(), tree_id);
            (db, fs, rt)
        }
    });

    #[ignore = "Test is slow and intermittent" ]
    test clean_zone(mocks(512)) {
        let (db, fs, _rt) = mocks.val;
        for i in 0..16384 {
            let fname = format!("f.{}", i);
            fs.mkdir(1, &OsString::from(fname), 0o755).unwrap();
        }
        fs.sync();
        for i in 0..8000 {
            let fname = format!("f.{}", i);
            fs.rmdir(1, &OsString::from(fname)).unwrap();
        }
        fs.sync();
        let statvfs = fs.statvfs();
        println!("Before cleaning: {:?} free out of {:?}",
                 statvfs.f_bfree, statvfs.f_blocks);
        db.clean().wait().unwrap();
        println!("After cleaning: {:?} free out of {:?}",
                 statvfs.f_bfree, statvfs.f_blocks);
    }

    /// A regression test for bug d5b4dab35d9be12ff1505e886ed5ca8ad4b6a526
    /// (node.0.get_mut() fails in Tree::flush_r).  As of 664:7ce31a1d42db, it
    /// fails about 30% of the time.
    #[ignore = "Test is slow and intermittent" ]
    test get_mut(mocks(512)) {
        let (db, fs, _rt) = mocks.val;
        for i in 0..16384 {
            let fname = format!("f.{}", i);
            fs.mkdir(1, &OsString::from(fname), 0o755).unwrap();
        }
        fs.sync();
        for i in 0..8192 {
            let fname = format!("f.{}", 2 * i);
            fs.rmdir(1, &OsString::from(fname)).unwrap();
        }
        fs.sync();
        let statvfs = fs.statvfs();
        println!("Before cleaning: {:?} free out of {:?}",
                 statvfs.f_bfree, statvfs.f_blocks);
        db.clean().wait().unwrap();
        println!("After cleaning: {:?} free out of {:?}",
                 statvfs.f_bfree, statvfs.f_blocks);
    }

}
