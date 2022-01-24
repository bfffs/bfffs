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
use futures::{TryFutureExt, future};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex}
};
use super::*;
use tokio::runtime::Runtime;

fn open_db(rt: &Runtime, path: PathBuf) -> Database {
    rt.block_on(async move {
        VdevFile::open(path)
        .and_then(|(leaf, reader)| {
            let block = VdevBlock::new(leaf);
            let (vr, lr) = raid::open(None, vec![(block, reader)]);
            cluster::Cluster::open(vr)
            .map_ok(move |cluster| (cluster, lr))
        }).map_ok(move |(cluster, reader)|{
            let (pool, reader) = Pool::open(None, vec![(cluster, reader)]);
            let cache = Cache::with_capacity(4_194_304);
            let arc_cache = Arc::new(Mutex::new(cache));
            let ddml = Arc::new(DDML::open(pool, arc_cache.clone()));
            let (idml, reader) = IDML::open(ddml, arc_cache, 1<<30, reader);
            Database::open(Arc::new(idml), reader)
        }).await
    }).unwrap()
}

mod persistence {
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
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
        0x62, 0x00,
        // max_int_fanout as 16 bits
                    0x85, 0x01,
        // min_leaf_fanout as 16 bits
                                0x72, 0x00,
        // max_leaf_fanout as 16 bits
                                            0xc6, 0x01,
        // leaf node max size in bytes, as 64-bits
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Root node's address as a RID
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
        // Root node's TXG range as a pair of 32-bit numbers
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
    ];

    const POOLNAME: &str = "TestPool";

    #[fixture]
    fn objects() -> (Runtime, Database, TempDir, PathBuf) {
        let len = 1 << 26;  // 64 MB
        let tempdir = t!(
            Builder::new().prefix("test_database_persistence").tempdir()
        );
        let filename = tempdir.path().join("vdev");
        {
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
        }
        let paths = [filename.clone()];
        let rt = basic_runtime();
        let cs = NonZeroU64::new(1);
        let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
        let clusters = vec![cluster];
        let pool = Pool::create(POOLNAME.to_string(), clusters);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = rt.block_on(async {
            Database::create(idml)
        });
        // Due to bincode's variable-length encoding and the
        // unpredictability of the root filesystem's timestamp, writing the
        // label will have unpredictable results if we create a root
        // filesystem.  TODO: make it predictable by using utimensat on the
        // root filesystem
        // let tree_id = rt.block_on(db.create_fs(Vec::new())).unwrap();
        (rt, db, tempdir, filename)
    }

    // Test open-after-write
    #[rstest]
    fn open(objects: (Runtime, Database, TempDir, PathBuf)) {
        let (rt, old_db, _tempdir, path) = objects;
        rt.block_on(
            old_db.sync_transaction()
        ).unwrap();
        drop(old_db);
        let _db = open_db(&rt, path);
    }

    #[rstest]
    fn sync_transaction(objects: (Runtime, Database, TempDir, PathBuf)) {
        let (rt, db, _tempdir, path) = objects;
        rt.block_on(
            db.sync_transaction()
        ).unwrap();
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
    use rstest::{fixture, rstest};
    use std::{
        fs,
        num::NonZeroU64,
    };
    use super::*;
    use tempfile::{Builder, TempDir};

    const POOLNAME: &str = "TestPool";

    #[fixture]
    fn objects() -> (Runtime, Database, TempDir, TreeID) {
        let len = 1 << 26;  // 64 MB
        let tempdir =
            t!(Builder::new().prefix("test_database_t").tempdir());
        let filename = tempdir.path().join("vdev");
        {
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
        }
        let paths = [filename];
        let rt = basic_runtime();
        let cs = NonZeroU64::new(1);
        let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
        let clusters = vec![cluster];
        let pool = Pool::create(POOLNAME.to_string(), clusters);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let (db, tree_id) = rt.block_on(async move {
            let db = Database::create(idml);
            let tree_id = db.create_fs(Vec::new()).await.unwrap();
            (db, tree_id)
        });
        (rt, db, tempdir, tree_id)
    }

    #[rstest]
    fn get_prop_default(objects: (Runtime, Database, TempDir, TreeID)) {
        let (rt, db, _tempdir, tree_id) = objects;

        let (val, source) = rt.block_on(async {
            db.get_prop(tree_id, PropertyName::Atime)
            .await
        }).unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    #[rstest]
    fn open_filesystem(objects: (Runtime, Database, TempDir, TreeID)) {
        let (rt, db, tempdir, tree_id) = objects;
        // Sync the database, then drop and reopen it.  That's the only way to
        // clear Inner::fs_trees
        rt.block_on(
            db.sync_transaction()
        ).unwrap();
        drop(db);
        let filename = tempdir.path().join("vdev");
        let db = open_db(&rt, filename);
        rt.block_on(async move {
            db.fsread(tree_id, |_| future::ok(()))
            .await
        }).unwrap();
    }

    mod create_fs {
        use pretty_assertions::assert_eq;
        use super::*;

        #[rstest]
        fn with_props(objects: (Runtime, Database, TempDir, TreeID)) {
            let (rt, db, _tempdir, first_tree_id) = objects;
            let props = vec![Property::RecordSize(5)];
            let tree_id = rt.block_on(async {
                db.create_fs(props)
                .await
            }).unwrap();
            let (val, source) = rt.block_on(async {
                db.get_prop(tree_id, PropertyName::RecordSize)
                .await
            }).unwrap();
            assert_ne!(tree_id, first_tree_id);
            assert_eq!(val, Property::RecordSize(5));
            assert_eq!(source, PropertySource::Local);
        }

        /// Creating a new filesystem, when the database's in-memory cache is
        /// cold, should not reuse a TreeID.
        #[rstest]
        fn cold_cache(objects: (Runtime, Database, TempDir, TreeID)) {
            let (rt, db, tempdir, first_tree_id) = objects;
            // Sync the database, then drop and reopen it.  That's the only way
            // to clear Inner::fs_trees
            rt.block_on(
                db.sync_transaction()
            ).unwrap();
            drop(db);
            let filename = tempdir.path().join("vdev");
            let db = open_db(&rt, filename);

            let tree_id = rt.block_on(async {
                db.create_fs(vec![])
                .await
            }).unwrap();
            assert_ne!(tree_id, first_tree_id);
        }

        #[rstest]
        fn twice(objects: (Runtime, Database, TempDir, TreeID)) {
            let (rt, db, _tempdir, first_tree_id) = objects;
            let tree_id1 = rt.block_on(async {
                db.create_fs(vec![])
                .await
            }).unwrap();
            assert_ne!(tree_id1, first_tree_id);

            let tree_id2 = rt.block_on(async {
                db.create_fs(vec![])
                .await
            }).unwrap();
            assert_ne!(tree_id2, first_tree_id);
            assert_ne!(tree_id2, tree_id1);
        }
    }

    #[rstest]
    fn set_prop(objects: (Runtime, Database, TempDir, TreeID)) {
        let (rt, db, _tempdir, tree_id) = objects;

        let (val, source) = rt.block_on(async {
            db.set_prop(tree_id, Property::Atime(false))
            .and_then(move |_| {
                db.get_prop(tree_id, PropertyName::Atime)
            }).await
        }).unwrap();
        assert_eq!(val, Property::Atime(false));
        assert_eq!(source, PropertySource::Local);
    }

    // TODO: add a test for getting a non-cached property, once it's possible to
    // make multiple datasets

    // TODO: add tests for inherited properties, once it's possible to make
    // multiple datasets.

    #[test]
    fn shutdown() {
        let rt = basic_runtime();
        let len = 1 << 30;  // 1GB
        let tempdir =
            t!(Builder::new().prefix("database.tempdir()::shutdown").tempdir());
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
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
        rt.block_on(async {
            let db = Database::create(Arc::new(idml));
            db.shutdown().await
        });
    }

    // TODO: add a test that Database::flush gets called periodically.  Verify
    // by writing some data, then checking the size of the writeback cache until
    // it goes to zero.
}
