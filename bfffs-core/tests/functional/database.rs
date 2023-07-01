// vim: tw=80
use bfffs_core::cache::*;
use bfffs_core::database::*;
use bfffs_core::ddml::DDML;
use bfffs_core::idml::IDML;
use futures::{StreamExt, TryStreamExt, future};
use rstest::rstest;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex}
};

const POOLNAME: &str = "TestPool";

async fn open_db(path: &Path) -> Database {
    let mut manager = Manager::default();
    manager.taste(path).await.unwrap();
    manager.import_by_name(POOLNAME).await.unwrap()
}

mod persistence {
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
    };
    use super::*;
    use tempfile::TempDir;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_DB_LABEL: [u8; 40] = [
        // The database's label only has one member: the forest
        // First comes the allocation table
        // Height as 64 bits
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // min_int_fanout as 16 bits
        0x4c, 0x00,
        // max_int_fanout as 16 bits
                    0x2e, 0x01,
        // min_leaf_fanout as 16 bits
                                0x5b, 0x00,
        // max_leaf_fanout as 16 bits
                                            0x6b, 0x01,
        // leaf node max size in bytes, as 64-bits
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Root node's address as a RID
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
        // Root node's TXG range as a pair of 32-bit numbers
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
    ];

    async fn harness() -> (Database, TempDir, Vec<PathBuf>) {
        let ph = crate::PoolBuilder::new()
            .chunksize(1)
            .fsize(1 << 26)     // 64 MB
            .name(POOLNAME)
            .build();

        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        // Due to bincode's variable-length encoding and the
        // unpredictability of the root filesystem's timestamp, writing the
        // label will have unpredictable results if we create a root
        // filesystem.  TODO: make it predictable by using utimensat on the
        // root filesystem
        // let tree_id = db.create_fs(None, "").await.unwrap();
        (db, ph.tempdir, ph.paths)
    }

    // Test open-after-write
    #[tokio::test]
    async fn open() {
        let (old_db, _tempdir, paths) = harness().await;
        old_db.sync_transaction().await.unwrap();
        drop(old_db);
        let _db = open_db(&paths[0]);
    }

    #[tokio::test]
    async fn sync_transaction() {
        let (db, _tempdir, paths) = harness().await;
        db.sync_transaction().await.unwrap();
        let mut f = fs::File::open(&paths[0]).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, pool, and idml labels
        f.seek(SeekFrom::Start(334)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master,
        assert_eq!(&v[0..40], &GOLDEN_DB_LABEL[0..40]);
        // Rest of the buffer should be zero-filled
        assert!(v[40..].iter().all(|&x| x == 0));
    }
}

mod database {
    use bfffs_core::{
        Error,
    };
    use super::*;
    use tempfile::TempDir;

    const POOLNAME: &str = "TestPool";

    fn new_empty_database() -> (Database, TempDir, Vec<PathBuf>) {
        let ph = crate::PoolBuilder::new()
            .fsize(1 << 26)     // 64 MB
            .name(POOLNAME)
            .chunksize(1)
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        (db, ph.tempdir, ph.paths)
    }

    async fn harness() -> (Database, TempDir, TreeID, Vec<PathBuf>) {
        let (db, tempdir, paths) = new_empty_database();
        let tree_id = db.create_fs(None, "").await.unwrap();
        (db, tempdir, tree_id, paths)
    }

    #[tokio::test]
    async fn dump_forest() {
        let (db, _tempdir, _tree_id, _paths) = harness().await;
        db.sync_transaction().await.unwrap();   // Flush forest to disk
        let mut buf = Vec::with_capacity(1024);
        db.dump_forest(&mut buf).await.unwrap();
        let forest = String::from_utf8(buf).unwrap();
        let expected = r#"---
limits:
  min_int_fanout: 76
  max_int_fanout: 302
  min_leaf_fanout: 91
  max_leaf_fanout: 363
  _max_size: 4194304
root:
  height: 1
  elem:
    key:
      tree_id: 0
      offset: 0
    txgs:
      start: 0
      end: 1
    ptr:
      Addr: 2
...
---
2:
  Leaf:
    credit: 0
    items:
      ? tree_id: 0
        offset: 0
      : Tree:
          parent: ~
          tod:
            height: 1
            _reserved: 0
            limits:
              min_int_fanout: 91
              max_int_fanout: 364
              min_leaf_fanout: 576
              max_leaf_fanout: 2302
              _max_size: 4194304
            root: 1
            txgs:
              start: 0
              end: 1
"#;
        pretty_assertions::assert_eq!(expected, forest);
    }

    #[tokio::test]
    async fn open_filesystem() {
        let (db, _tempdir, tree_id, paths) = harness().await;
        // Sync the database, then drop and reopen it.  That's the only way to
        // clear Inner::fs_trees
        db.sync_transaction().await.unwrap();
        drop(db);
        let db = open_db(&paths[0]).await;
        db.fsread(tree_id, |_| future::ok(())).await.unwrap();
    }

    mod check {
        use super::*;

        /// A newly created Database with a single file system should check
        /// successfully.
        #[tokio::test]
        async fn empty() {
            let (db, _tempdir, _first_tree_id, _paths) = harness().await;
            assert!(db.check().await.unwrap());
        }
    }

    mod create_fs {
        use super::*;

        /// Creating a new filesystem, when the database's in-memory cache is
        /// cold, should not reuse a TreeID.
        #[tokio::test]
        async fn cold_cache() {
            let (db, _tempdir, first_tree_id, paths) = harness().await;
            // Sync the database, then drop and reopen it.  That's the only way
            // to clear Inner::fs_trees
            db.sync_transaction().await.unwrap();
            drop(db);
            let db = open_db(&paths[0]).await;

            let tree_id = db.create_fs(None, "").await.unwrap();
            assert_ne!(tree_id, first_tree_id);
        }

        #[tokio::test]
        async fn twice() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let tree_id1 = db.create_fs(None, "").await.unwrap();
            assert_ne!(tree_id1, first_tree_id);

            let tree_id2 = db.create_fs(None, "")
                .await
                .unwrap();
            assert_ne!(tree_id2, first_tree_id);
            assert_ne!(tree_id2, tree_id1);
        }
    }

    mod destroy_fs {
        use super::*;

        async fn assert_no_such_tree(
            db: &Database,
            parent: Option<TreeID>,
            tree_id: TreeID,
            name: &str)
        {
            /* The tree's name should be removed from the Forest */
            assert_eq!(Ok((None, None)), db.lookup_fs(name).await);
            if let Some(p) = parent {
                // Really, readdir may return undestroyed sister datasets, but
                // none of these tests create any.
                assert!(db.readdir(p, 0).next().await.is_none());
            }
            /* And so should the tree itself */
            let r = db.fsread(tree_id, |_| future::ok(())).await;
            assert_eq!(Err(Error::ENOENT), r);
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn child(#[case] sync: bool) {
            let (db, _tempdir, parent, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(parent), "foo")
                .await
                .unwrap();
            if sync {
                db.sync_transaction().await.unwrap();
            }
            db.destroy_fs(Some(parent), tree_id1, "foo").await.unwrap();

            assert_no_such_tree(&db, Some(parent), tree_id1, "foo").await;
        }

        /// Can't destroy a tree with a child.
        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn ebusy(#[case] sync: bool) {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let _tree_id1 = db.create_fs(Some(first_tree_id), "foo")
                .await
                .unwrap();
            if sync {
                db.sync_transaction().await.unwrap();
            }
            assert_eq!(
                Err(Error::EBUSY),
                db.destroy_fs(None, first_tree_id, "").await
            );
        }

        /// Can't destroy a tree that does not exist
        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn enoent(#[case] sync: bool) {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            if sync {
                db.sync_transaction().await.unwrap();
            }
            assert_eq!(
                Err(Error::ENOENT),
                db.destroy_fs(Some(first_tree_id), TreeID(42), "foo").await
            );
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn grandchild(#[case] sync: bool) {
            let (db, _tempdir, tree_id0, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(tree_id0), "foo")
                .await
                .unwrap();
            let tree_id2 = db.create_fs(Some(tree_id1), "bar")
                .await
                .unwrap();
            if sync {
                db.sync_transaction().await.unwrap();
            }
            db.destroy_fs(Some(tree_id1), tree_id2, "bar").await.unwrap();

            assert_no_such_tree(&db, Some(tree_id1), tree_id2, "foo/bar").await;
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn root(#[case] sync: bool) {
            let (db, _tempdir, tree_id, _paths) = harness().await;
            if sync {
                db.sync_transaction().await.unwrap();
            }
            db.destroy_fs(None, tree_id, "").await.unwrap();

            assert_no_such_tree(&db, None, tree_id, "").await;
        }
    }

    mod lookup_fs {
        use pretty_assertions::assert_eq;
        use super::*;

        #[tokio::test]
        async fn child() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo")
                .await
                .unwrap();
            assert_eq!(Ok((Some(TreeID(0)), Some(tree_id1))),
                       db.lookup_fs("foo").await);
        }

        #[tokio::test]
        async fn grandchild() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo")
                .await
                .unwrap();
            let tree_id2 = db.create_fs(Some(tree_id1), "bar")
                .await
                .unwrap();
            assert_eq!(Ok((Some(tree_id1), Some(tree_id2))),
                       db.lookup_fs("foo/bar").await);
        }

        #[tokio::test]
        async fn no_root_filesystem() {
            let (db, _tempdir, _paths) = new_empty_database();
            assert_eq!(Ok((None, None)), db.lookup_fs("").await);
        }

        #[tokio::test]
        async fn root() {
            let (db, _tempdir, _first_tree_id, _paths) = harness().await;
            assert_eq!(Ok((None, Some(TreeID(0)))), db.lookup_fs("").await);
        }
    }

    mod readdir {
        use pretty_assertions::assert_eq;
        use super::*;

        #[tokio::test]
        async fn dataset_does_not_exist() {
            let (db, _tempdir, _first_tree_id, _paths) = harness().await;
            assert_eq!(
                Ok(vec![]),
                db.readdir(TreeID(666), 0).try_collect::<Vec<_>>().await
            );
        }

        #[tokio::test]
        async fn no_children() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            assert_eq!(
                Ok(vec![]),
                db.readdir(first_tree_id, 0).try_collect::<Vec<_>>().await
            );
        }

        #[tokio::test]
        async fn one_child() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo")
                .await
                .unwrap();
            let children = db.readdir(first_tree_id, 0)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(children[0].name, "foo");
            assert_eq!(children[0].id, tree_id1);
        }

        #[tokio::test]
        async fn two_children() {
            let (db, _tempdir, first_tree_id, _paths) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo")
                .await
                .unwrap();
            let tree_id2 = db.create_fs(Some(first_tree_id), "bar")
                .await
                .unwrap();
            let children = db.readdir(first_tree_id, 0)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            // The order of results is determined by a hash function and is
            // reproducible but not meaningful
            assert_eq!(2, children.len());
            assert_eq!(children[0].name, "bar");
            assert_eq!(children[0].id, tree_id2);
            assert_eq!(children[1].name, "foo");
            assert_eq!(children[1].id, tree_id1);

            // Now read results again, but provide an offset to skip the first
            // child.
            let children2 = db.readdir(first_tree_id, children[0].offs)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(1, children2.len());
            assert_eq!(children2[0].name, "foo");
            assert_eq!(children2[0].id, tree_id1);
        }
    }

    #[tokio::test]
    async fn shutdown() {
        let ph = crate::PoolBuilder::new()
            .build();
        let cache = Arc::new(
            Mutex::new(
                Cache::with_capacity(4_194_304)
            )
        );
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = IDML::create(ddml, cache);
        let db = Database::create(Arc::new(idml));
        db.shutdown().await;
    }

    mod status {
        use super::*;

        type Harness = (Database, Vec<PathBuf>, TempDir);
        const POOLNAME: &str = "StatusPool";

        struct Config {
            n: usize,
            m: usize,
            k: i16,
            f: i16,
            nclusters: usize
        }

        fn config(n: usize, m: usize, k: i16, f: i16, nc: usize) -> Config {
            Config{n, m, k, f, nclusters: nc}
        }

        async fn harness(config: &Config) -> Harness {
            let ph = crate::PoolBuilder::new()
                .name(POOLNAME)
                .disks(config.n)
                .mirror_size(config.m)
                .stripe_size(config.k)
                .redundancy_level(config.f)
                .nclusters(config.nclusters)
                .build();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
            let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = Database::create(idml);

            (db, ph.paths, ph.tempdir)
        }

        /// Status of a healthy, well-balanced pool.
        #[rstest(config,
                 case(config(1, 1, 1, 0, 1)),       // Single disk
                 case(config(2, 1, 1, 0, 2)),       // RAID0
                 case(config(2, 2, 1, 0, 1)),       // RAID1
                 case(config(3, 1, 3, 1, 1)),       // RAID5
                 case(config(6, 1, 3, 1, 2)),       // RAID50
                 case(config(6, 2, 3, 1, 1)),       // RAID51
                 case(config(12, 2, 3, 1, 2)))]     // RAID510
        #[tokio::test]
        async fn normal(config: Config) {
            let (db, _paths, _tempdir) = harness(&config).await;
            let stat = db.status();
            assert_eq!(stat.name, POOLNAME);
            assert_eq!(stat.clusters.len(), config.nclusters);
            for cluster in stat.clusters.iter() {
                let rn = config.n / (config.m * config.nclusters);
                if rn == 1 {
                    assert_eq!(cluster.codec, "NonRedundant");
                } else {
                    assert_eq!(cluster.codec,
                           format!("PrimeS-{},{},{}", rn, config.k, config.f));
                }
                assert_eq!(cluster.mirrors.len(), rn);
                for mirror in cluster.mirrors.iter() {
                    assert_eq!(mirror.leaves.len(), config.m);
                }
            }
        }
    }

    mod sync_transaction {
        use super::*;
        use divbuf::DivBufShared;
        use bfffs_core::fs_tree::{FSKey, FSValue, InlineExtent, ObjKey};

        /// If the file system crashes in the middle of a transaction, the pool
        /// can still be imported at the old transaction.
        // TODO: write a torture test based on this, that fills the pool with
        // differently sized files on each iteration.
        #[tokio::test]
        async fn crash_and_restore() {
            let ph = crate::PoolBuilder::new()
                .fsize(1 << 26)     // 64 MB
                .zone_size(17)
                .build();
            let uuid = ph.pool.uuid();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
            let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = Database::create(idml);
            let tree_id = db.create_fs(None, "").await.unwrap();

            let ino = 42;
            let n = 2;
            let z = 65536;

            for i in 0..n {
                db.fswrite(tree_id, n, 0, 0, 1000000000, move |dataset| async move {
                    // Write some big extents.  Without an inode, this will be
                    // orphaned data, but it will still consume space, which is
                    // all we need for this particular test.
                    let k = FSKey::new(ino, ObjKey::Extent(i as u64 * z));
                    let dbs = Arc::new(DivBufShared::from(vec![0; z as usize]));
                    let extent = InlineExtent::new(dbs);
                    let v = FSValue::InlineExtent(extent);
                    dataset.insert(k, v).await.map(drop)?;
                    Ok(())
                }).await.unwrap();
                db.sync_transaction().await.unwrap();
            }
            db.fswrite(tree_id, n, 0, 0, 1000000000, move |dataset| async move {
                // Now rewrite enough data to completely free the first zone
                for i in 0..n {
                    let k = FSKey::new(ino, ObjKey::Extent(i as u64 * z));
                    let dbs = Arc::new(DivBufShared::from(vec![0; z as usize]));
                    let extent = InlineExtent::new(dbs);
                    let v = FSValue::InlineExtent(extent);
                    dataset.insert(k, v).await.map(drop)?;
                }
                Ok(())
            }).await.unwrap();

            // Now drop the database without syncing it
            db.shutdown().await;

            // And reopen
            let mut manager = database::Manager::default();
            manager.taste(&ph.paths[0]).await.unwrap();
            let db = manager.import_by_uuid(uuid).await.unwrap();
            assert!(db.check().await.unwrap());
        }
    }

    // TODO: add a test that Database::flush gets called periodically.  Verify
    // by writing some data, then checking the size of the writeback cache until
    // it goes to zero.
}

/// Tests database::Manager
mod manager {
    use crate::require_root;
    use bfffs_core::{
        Error,
        Uuid,
        database::*,
        cache::*,
        ddml::DDML,
        idml::IDML
    };
    use function_name::named;
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use rstest_reuse::{apply, template};
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempfile::TempDir;

    #[derive(Clone, Copy, Debug)]
    struct Config {
        /// Number of disks
        n: usize,
        /// Disks per mirror
        m: usize,
        /// Total disks per RAID stripe
        k: i16,
        /// Parity disks per RAID stripe
        f: i16,
        /// Number of RAID clusters
        nclusters: usize,
        /// Cache size
        cs: Option<usize>,
        /// Writeback size
        wb: Option<usize>,
    }
    
    impl Config {
        fn new(
            n: usize,
            m: usize,
            k: i16,
            f: i16,
            nclusters: usize,
            cs: Option<usize>,
            wb: Option<usize>)
            -> Self
        {
            Self{n, m, k, f, nclusters, cs, wb}
        }
    }

    struct Harness {
        manager: Manager,
        paths: Vec<PathBuf>,
        _tempdir: TempDir,
        gnops: Vec<crate::Gnop>
    }

    async fn harness(c: &Config, gnop: bool) -> Harness
    {
        let ph = crate::PoolBuilder::new()
            .disks(c.n)
            .mirror_size(c.m)
            .stripe_size(c.k)
            .redundancy_level(c.f)
            .nclusters(c.nclusters)
            .gnop(gnop)
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        db.sync_transaction().await.unwrap();
        db.shutdown().await;

        let mut manager = Manager::default();
        if let Some(cs) = c.cs {
            manager.cache_size(cs);
        }
        if let Some(wb) = c.wb {
            manager.writeback_size(wb);
        }
        Harness{manager, paths: ph.paths, _tempdir: ph.tempdir, gnops: ph.gnops}
    }

    #[template]
    #[rstest(c,
             case(Config::new(1, 1, 1, 0, 1, None, None)), // Single-disk
             case(Config::new(2, 1, 1, 0, 2, None, None)), // RAID0
             case(Config::new(2, 2, 1, 0, 1, None, None)), // RAID1
             case(Config::new(3, 1, 3, 1, 1, None, None)), // RAID5
             case(Config::new(6, 2, 3, 1, 1, None, None)), // RAID51
             case(Config::new(12, 2, 3, 1, 2, None, None)), // RAID510
     )]
    fn all_configs(c: Config) {}

    /// No disks have been tasted
    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, None, None)))]
    #[tokio::test]
    async fn empty(c: Config) {
        let h = harness(&c, false).await;
        assert!(h.manager.importable_pools().is_empty());
    }

    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, Some(100_000_000), None)))]
    #[tokio::test]
    async fn cache_size(c: Config) {
        let mut h = harness(&c, false).await;
        h.manager.taste(h.paths.into_iter().next().unwrap()).await.unwrap();
        let db = h.manager.import_by_name("functional_test_pool").await.unwrap();
        assert_eq!(db.cache_size(), 100_000_000);
    }

    /// Import a single pool by its name.
    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, None, None)))]
    #[tokio::test]
    async fn import_by_name(c: Config) {
        let mut h = harness(&c, false).await;
        for path in h.paths.iter() {
            h.manager.taste(path).await.unwrap();
        }
        h.manager.import_by_name("functional_test_pool").await.unwrap();
    }

    /// Fail to import a nonexistent pool by name
    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, None, None)))]
    #[tokio::test]
    async fn import_by_name_enoent(c: Config) {
        let mut h = harness(&c, false).await;
        h.manager.taste(h.paths.into_iter().next().unwrap()).await.unwrap();
        let e = h.manager.import_by_name("does_not_exist").await
            .err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }


    /// Import a single pool by its UUID.  Try both single-disk and raid pools.
    #[apply(all_configs)]
    #[tokio::test]
    async fn import_by_uuid(c: Config) {
        let mut h = harness(&c, false).await;
        for path in h.paths.iter() {
            h.manager.taste(path).await.unwrap();
        }
        let (name, uuid) = h.manager.importable_pools().pop().unwrap();
        assert_eq!(name, "functional_test_pool");
        h.manager.import_by_uuid(uuid).await.unwrap();
    }

    /// Try to import a single pool, but all of the disks are gone.
    #[named]
    #[apply(all_configs)]
    #[tokio::test]
    async fn import_by_uuid_no_disks(c: Config) {
        require_root!();
        let mut h = harness(&c, true).await;
        for path in h.paths.iter() {
            h.manager.taste(path).await.unwrap();
        }
        h.gnops.drain(..);
        // importable_pools should still work
        let (name, uuid) = h.manager.importable_pools().pop().unwrap();
        assert_eq!(name, "functional_test_pool");
        // But actually importing them should not.  The precise error code
        // depends on the test circumstances; we won't assert on it.
        h.manager.import_by_uuid(uuid).await.err().unwrap();
    }

    /// Fail to import a nonexistent pool by UUID
    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, None, None)))]
    #[tokio::test]
    #[awt]
    async fn import_by_uuid_enoent(c: Config) {
        let mut h = harness(&c, false).await;
        h.manager.taste(h.paths.into_iter().next().unwrap()).await.unwrap();
        let e = h.manager.import_by_uuid(Uuid::new_v4()).await.err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    #[rstest(c, case(Config::new(1, 1, 1, 0, 1, None, Some(100_000_000))))]
    #[tokio::test]
    async fn writeback_size(c: Config) {
        let mut h = harness(&c, false).await;
        h.manager.taste(h.paths.into_iter().next().unwrap()).await.unwrap();
        let db = h.manager.import_by_name("functional_test_pool").await.unwrap();
        assert_eq!(db.writeback_size(), 100_000_000);
    }
}
