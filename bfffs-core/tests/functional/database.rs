// vim: tw=80
use bfffs_core::cache::*;
use bfffs_core::cluster;
use bfffs_core::database::*;
use bfffs_core::ddml::*;
use bfffs_core::idml::*;
use bfffs_core::pool::*;
use bfffs_core::vdev_block::*;
use bfffs_core::vdev_file::*;
use bfffs_core::raid;
use futures::{TryStreamExt, future};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex}
};

async fn open_db(path: PathBuf) -> Database {
    let (leaf, reader) = VdevFile::open(path).await.unwrap();
    let block = VdevBlock::new(leaf);
    let (vr, lr) = raid::open(None, vec![(block, reader)]);
    let cluster = cluster::Cluster::open(vr).await.unwrap();
    let (pool, reader) = Pool::open(None, vec![(cluster, lr)]);
    let cache = Cache::with_capacity(4_194_304);
    let arc_cache = Arc::new(Mutex::new(cache));
    let ddml = Arc::new(DDML::open(pool, arc_cache.clone()));
    let (idml, reader) = IDML::open(ddml, arc_cache, 1<<30, reader);
    Database::open(Arc::new(idml), reader)
}

mod persistence {
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64,
    };
    use super::*;
    use tempfile::{Builder, TempDir};

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

    const POOLNAME: &str = "TestPool";

    async fn harness() -> (Database, TempDir, PathBuf) {
        let len = 1 << 26;  // 64 MB
        let tempdir = Builder::new()
            .prefix("test_database_persistence")
            .tempdir()
            .unwrap();
        let filename = tempdir.path().join("vdev");
        {
            let file = fs::File::create(&filename).unwrap();
            file.set_len(len).unwrap();
        }
        let paths = [filename.clone()];
        let cs = NonZeroU64::new(1);
        let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
        let clusters = vec![cluster];
        let pool = Pool::create(POOLNAME.to_string(), clusters);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        // Due to bincode's variable-length encoding and the
        // unpredictability of the root filesystem's timestamp, writing the
        // label will have unpredictable results if we create a root
        // filesystem.  TODO: make it predictable by using utimensat on the
        // root filesystem
        // let tree_id = db.create_fs(None, "", Vec::new()).await.unwrap();
        (db, tempdir, filename)
    }

    // Test open-after-write
    #[tokio::test]
    async fn open() {
        let (old_db, _tempdir, path) = harness().await;
        old_db.sync_transaction().await.unwrap();
        drop(old_db);
        let _db = open_db(path);
    }

    #[tokio::test]
    async fn sync_transaction() {
        let (db, _tempdir, path) = harness().await;
        db.sync_transaction().await.unwrap();
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, pool, and idml labels
        f.seek(SeekFrom::Start(294)).unwrap();
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

mod t {
    use bfffs_core::{
        cache::*,
        pool::*,
        property::*,
        ddml::*,
        idml::*,
    };
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        num::NonZeroU64,
    };
    use super::*;
    use tempfile::{Builder, TempDir};

    const POOLNAME: &str = "TestPool";

    fn new_empty_database() -> (Database, TempDir) {
        let len = 1 << 26;  // 64 MB
        let tempdir = Builder::new()
            .prefix("test_database_t")
            .tempdir()
            .unwrap();
        let filename = tempdir.path().join("vdev");
        {
            let file = fs::File::create(&filename).unwrap();
            file.set_len(len).unwrap();
        }
        let paths = [filename];
        let cs = NonZeroU64::new(1);
        let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
        let clusters = vec![cluster];
        let pool = Pool::create(POOLNAME.to_string(), clusters);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        (db, tempdir)
    }

    async fn harness() -> (Database, TempDir, TreeID) {
        let (db, tempdir) = new_empty_database();
        let tree_id = db.create_fs(None, "", Vec::new()).await.unwrap();
        (db, tempdir, tree_id)
    }

    #[tokio::test]
    async fn get_prop_default() {
        let (db, _tempdir, tree_id) = harness().await;

        let (val, source) = db.get_prop(tree_id, PropertyName::Atime)
            .await
            .unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    #[tokio::test]
    async fn open_filesystem() {
        let (db, tempdir, tree_id) = harness().await;
        // Sync the database, then drop and reopen it.  That's the only way to
        // clear Inner::fs_trees
        db.sync_transaction().await.unwrap();
        drop(db);
        let filename = tempdir.path().join("vdev");
        let db = open_db(filename).await;
        db.fsread(tree_id, |_| future::ok(())).await.unwrap();
    }

    mod create_fs {
        use pretty_assertions::assert_eq;
        use super::*;

        #[tokio::test]
        async fn with_props() {
            let (db, _tempdir, first_tree_id) = harness().await;
            let props = vec![Property::RecordSize(5)];
            let tree_id = db.create_fs(None, "", props).await.unwrap();
            let (val, source) = db.get_prop(tree_id, PropertyName::RecordSize)
                .await
                .unwrap();
            assert_ne!(tree_id, first_tree_id);
            assert_eq!(val, Property::RecordSize(5));
            assert_eq!(source, PropertySource::Local);
        }

        /// Creating a new filesystem, when the database's in-memory cache is
        /// cold, should not reuse a TreeID.
        #[tokio::test]
        async fn cold_cache() {
            let (db, tempdir, first_tree_id) = harness().await;
            // Sync the database, then drop and reopen it.  That's the only way
            // to clear Inner::fs_trees
            db.sync_transaction().await.unwrap();
            drop(db);
            let filename = tempdir.path().join("vdev");
            let db = open_db(filename).await;

            let tree_id = db.create_fs(None, "", vec![]).await.unwrap();
            assert_ne!(tree_id, first_tree_id);
        }

        #[tokio::test]
        async fn twice() {
            let (db, _tempdir, first_tree_id) = harness().await;
            let tree_id1 = db.create_fs(None, "", vec![]).await.unwrap();
            assert_ne!(tree_id1, first_tree_id);

            let tree_id2 = db.create_fs(None, "", vec![])
                .await
                .unwrap();
            assert_ne!(tree_id2, first_tree_id);
            assert_ne!(tree_id2, tree_id1);
        }
    }

    mod lookup_fs {
        use pretty_assertions::assert_eq;
        use super::*;

        #[tokio::test]
        async fn no_root_filesystem() {
            let (db, _tempdir) = new_empty_database();
            assert_eq!(Ok(None), db.lookup_fs("").await);
        }
    }

    mod readdir {
        use pretty_assertions::assert_eq;
        use super::*;

        #[tokio::test]
        async fn dataset_does_not_exist() {
            let (db, _tempdir, _first_tree_id) = harness().await;
            assert_eq!(
                Ok(vec![]),
                db.readdir(TreeID(666), 0).try_collect::<Vec<_>>().await
            );
        }

        #[tokio::test]
        async fn no_children() {
            let (db, _tempdir, first_tree_id) = harness().await;
            assert_eq!(
                Ok(vec![]),
                db.readdir(first_tree_id, 0).try_collect::<Vec<_>>().await
            );
        }

        #[tokio::test]
        async fn one_child() {
            let (db, _tempdir, first_tree_id) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo", vec![])
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
            let (db, _tempdir, first_tree_id) = harness().await;
            let tree_id1 = db.create_fs(Some(first_tree_id), "foo", vec![])
                .await
                .unwrap();
            let tree_id2 = db.create_fs(Some(first_tree_id), "bar", vec![])
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
    async fn set_prop() {
        let (db, _tempdir, tree_id) = harness().await;

        db.set_prop(tree_id, Property::Atime(false)).await.unwrap();
        let (val, source) = db.get_prop(tree_id, PropertyName::Atime).await
            .unwrap();
        assert_eq!(val, Property::Atime(false));
        assert_eq!(source, PropertySource::Local);
    }

    // TODO: add a test for getting a non-cached property, once it's possible to
    // make multiple datasets

    // TODO: add tests for inherited properties, once it's possible to make
    // multiple datasets.

    #[tokio::test]
    async fn shutdown() {
        let len = 1 << 30;  // 1GB
        let tempdir = Builder::new()
            .prefix("database.tempdir()::shutdown")
            .tempdir()
            .unwrap();
        let filename = tempdir.path().join("vdev");
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        drop(file);
        let cluster = Pool::create_cluster(None, 1, None, 0, &[filename]);
        let pool = Pool::create(String::from("database::shutdown"),
            vec![cluster]);
        let cache = Arc::new(
            Mutex::new(
                Cache::with_capacity(4_194_304)
            )
        );
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = IDML::create(ddml, cache);
        let db = Database::create(Arc::new(idml));
        db.shutdown().await;
    }

    // TODO: add a test that Database::flush gets called periodically.  Verify
    // by writing some data, then checking the size of the writeback cache until
    // it goes to zero.
}
