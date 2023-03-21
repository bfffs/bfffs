// vim: tw=80

/// Constructs a real filesystem and tests the common FS routines, without
/// mounting
mod device_manager {
    use bfffs_core::{
        Error,
        Uuid,
        database::*,
        device_manager::*,
        cache::*,
        ddml::*,
        idml::*
    };
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use rstest_reuse::{apply, template};
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempfile::TempDir;

    type Harness = (DevManager, Vec<PathBuf>, TempDir);

    async fn harness(n: usize, m: usize, k: i16, f: i16, cs: Option<usize>,
               wb: Option<usize>)
        -> Harness
    {
        let (tempdir, paths, pool) = crate::PoolBuilder::new()
            .disks(n)
            .mirror_size(m)
            .stripe_size(k)
            .redundancy_level(f)
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        db.sync_transaction().await.unwrap();
        let mut dev_manager = DevManager::default();
        if let Some(cs) = cs {
            dev_manager.cache_size(cs);
        }
        if let Some(wb) = wb {
            dev_manager.writeback_size(wb);
        }
        (dev_manager, paths, tempdir)
    }

    #[template]
    #[rstest(h,
             case(harness(1, 1, 1, 0, None, None)), // Single-disk configuration
             case(harness(2, 2, 1, 0, None, None)), // RAID1
             case(harness(3, 1, 3, 1, None, None)), // RAID5 configuration
             case(harness(6, 2, 3, 1, None, None)), // RAID51 configuration
     )]
    fn all_configs(h: Harness) {}

    /// No disks have been tasted
    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn empty(#[future] h: Harness) {
        assert!(h.0.importable_pools().is_empty());
    }

    #[rstest(h, case(harness(1, 1, 1, 0, Some(100_000_000), None)))]
    #[tokio::test]
    #[awt]
    async fn cache_size(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
        let db = dm.import_by_name("functional_test_pool").await.unwrap();
        assert_eq!(db.cache_size(), 100_000_000);
    }

    /// Import a single pool by its name.  Try both single-disk and raid pools
    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn import_by_name(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        for path in paths.iter() {
            dm.taste(path).await.unwrap();
        }
        dm.import_by_name("functional_test_pool").await.unwrap();
    }

    /// Fail to import a nonexistent pool by name
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    #[tokio::test]
    #[awt]
    async fn import_by_name_enoent(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
        let e = dm.import_by_name("does_not_exist").await
            .err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }


    /// Import a single pool by its UUID
    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn import_by_uuid(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        for path in paths.iter() {
            dm.taste(path).await.unwrap();
        }
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "functional_test_pool");
        dm.import_by_uuid(uuid).await.unwrap();
    }

    /// Fail to import a nonexistent pool by UUID
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    #[tokio::test]
    #[awt]
    async fn import_by_uuid_enoent(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
        let e = dm.import_by_uuid(Uuid::new_v4()).await.err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    /// DeviceManager::import_clusters on a single pool
    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn import_clusters(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        for path in paths.iter() {
            dm.taste(path).await.unwrap();
        }
        let (name, uuid) = dm.importable_pools().pop().unwrap();
        assert_eq!(name, "functional_test_pool");
        let clusters = dm.import_clusters(uuid).await.unwrap();
        assert_eq!(clusters.len(), 1);
    }

    /// DeviceManager::import_clusters for a nonexistent pool
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    #[tokio::test]
    #[awt]
    async fn import_clusters_enoent(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
        let e = dm.import_clusters(Uuid::new_v4()).await.err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    #[rstest(h, case(harness(1, 1, 1, 0, None, Some(100_000_000))))]
    #[tokio::test]
    #[awt]
    async fn writeback_size(#[future] h: Harness) {
        let (dm, paths, _tempdir) = h;
        dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
        let db = dm.import_by_name("functional_test_pool").await.unwrap();
        assert_eq!(db.writeback_size(), 100_000_000);
    }
}
