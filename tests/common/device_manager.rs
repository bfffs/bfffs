// vim: tw=80
use galvanic_test::*;

/// Constructs a real filesystem and tests the common FS routines, without mounting
macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

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
    use futures::{ Future, future, };
    use std::{
        fs,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::{
        executor::current_thread::TaskExecutor,
        runtime::current_thread::Runtime
    };

    fixture!( mocks() -> (Runtime, DevManager, Vec<String>, TempDir) {
        setup(&mut self) {
            let mut rt = Runtime::new().unwrap();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(TempDir::new("test_device_manager"));
            let paths = (0..3).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                fname
            }).collect::<Vec<_>>();
            let pathsclone = paths.clone();
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 3, 3, None, 1, &paths)
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_device_manager"), vec![cluster])
                }).map(|pool| {
                    let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
                    let ddml = Arc::new(DDML::new(pool, cache.clone()));
                    let idml = Arc::new(IDML::create(ddml, cache));
                    let te = TaskExecutor::current();
                    Database::create(idml, te)
                })
            })).unwrap();
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

    // Import a single pool
    test import(mocks) {
        let (mut rt, dm, paths, _tempdir) = mocks.val;
        dm.taste(&paths[2]);
        dm.taste(&paths[1]);
        dm.taste(&paths[0]);
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "test_device_manager");
        let _idml = rt.block_on(future::lazy(move || {
            let te = TaskExecutor::current();
            dm.import(uuid, te)
        })).unwrap();
    }

    /// DeviceManager::import_clusters on a single pool
    test import_clusters(mocks) {
        let (mut rt, dm, paths, _tempdir) = mocks.val;
        dm.taste(&paths[2]);
        dm.taste(&paths[1]);
        dm.taste(&paths[0]);
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "test_device_manager");
        let clusters = rt.block_on(future::lazy(move || {
            dm.import_clusters(uuid)
        })).unwrap();
        assert_eq!(clusters.len(), 1);
    }
}
