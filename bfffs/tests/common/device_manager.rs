// vim: tw=80
use galvanic_test::test_suite;

// Constructs a real filesystem and tests the common FS routines, without mounting
test_suite! {
    name device_manager;

    use bfffs::{
        common::database::*,
        common::device_manager::*,
        common::cache::*,
        common::ddml::*,
        common::idml::*,
        common::pool::*,
    };
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        sync::{Arc, Mutex}
    };
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Runtime;

    fixture!( mocks(n: i16, k: i16, f: i16)
              -> (Runtime, DevManager, Vec<String>, TempDir)
    {
        params {
            vec![
                (1, 1, 0),      // Single-disk configuration
                (3, 3, 1),      // RAID configuration
            ].into_iter()
        }
        setup(&mut self) {
            let n = *self.n;
            let k = *self.k;
            let f = *self.f;
            let mut rt = tokio::runtime::Builder::new()
                .threaded_scheduler()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            let len = 1 << 30;  // 1GB
            let tempdir =
                t!(Builder::new().prefix("test_device_manager").tempdir());
            let paths = (0..n).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                fname
            }).collect::<Vec<_>>();
            let pathsclone = paths.clone();
            let handle = rt.handle().clone();
            let cluster = Pool::create_cluster(None, k, None, f, &paths);
            let pool = Pool::create(String::from("test_device_manager"),
                vec![cluster]);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = Database::create(idml, handle);
            rt.block_on(
                db.sync_transaction()
            ).unwrap();
            let dev_manager = DevManager::default();
            (rt, dev_manager, pathsclone, tempdir)
        }
    });

    // No disks have been tasted
    test empty(mocks) {
        assert!(mocks.val.1.importable_pools().is_empty());
    }

    // Import a single pool by its name.  Try both single-disk and raid pools
    test import_by_name(mocks) {
        let (rt, dm, paths, _tempdir) = mocks.val;
        for path in paths.iter() {
            dm.taste(path);
        }
        let handle = rt.handle().clone();
        let _db = dm.import_by_name("test_device_manager", handle).unwrap();
    }

    // Import a single pool by its UUID
    test import_by_uuid(mocks) {
        let (rt, dm, paths, _tempdir) = mocks.val;
        for path in paths.iter() {
            dm.taste(path);
        }
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "test_device_manager");
        let handle = rt.handle().clone();
        let _db = dm.import_by_uuid(uuid, handle).unwrap();
    }

    /// DeviceManager::import_clusters on a single pool
    test import_clusters(mocks) {
        let (mut rt, dm, paths, _tempdir) = mocks.val;
        for path in paths.iter() {
            dm.taste(path);
        }
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "test_device_manager");
        let clusters = rt.block_on(async move {
            dm.import_clusters(uuid).await
        }).unwrap();
        assert_eq!(clusters.len(), 1);
    }
}
