// vim: tw=80
use bfffs::common::cache::*;
use bfffs::common::cluster;
use bfffs::common::database::*;
use bfffs::common::ddml::*;
use bfffs::common::idml::*;
use bfffs::common::pool::*;
use bfffs::common::vdev_block::*;
use bfffs::common::vdev_file::*;
use bfffs::common::raid;
use futures::{TryFutureExt, future};
use galvanic_test::test_suite;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex}
};
use super::super::*;
use tokio::runtime::Runtime;

fn open_db(rt: &mut Runtime, path: PathBuf) -> Database {
    let handle = rt.handle().clone();
    rt.block_on(async move {
        VdevFile::open(path)
        .and_then(|(leaf, reader)| {
                let block = VdevBlock::new(leaf);
                let (vr, lr) = raid::open(None, vec![(block, reader)]);
                cluster::Cluster::open(vr)
                .map_ok(move |cluster| (cluster, lr))
        }).and_then(move |(cluster, reader)|{
            let proxy = ClusterProxy::new(cluster);
            Pool::open(None, vec![(proxy, reader)])
        }).map_ok(|(pool, reader)| {
            let cache = Cache::with_capacity(1_000_000);
            let arc_cache = Arc::new(Mutex::new(cache));
            let ddml = Arc::new(DDML::open(pool, arc_cache.clone()));
            let (idml, reader) = IDML::open(ddml, arc_cache, reader);
            Database::open(Arc::new(idml), handle, reader)
        }).await
    }).unwrap()
}

test_suite! {
    name persistence;

    use galvanic_test::*;
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

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Database, TempDir, PathBuf) {
        setup(&mut self) {
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
            let mut rt = basic_runtime();
            let handle = rt.handle().clone();
            let pool = rt.block_on(async {
                let cs = NonZeroU64::new(1);
                let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
                let clusters = vec![cluster];
                future::try_join_all(clusters)
                .map_err(|_| unreachable!())
                .and_then(|clusters|
                    Pool::create(POOLNAME.to_string(), clusters)
                ).await
            }).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = rt.block_on(async move {
                let db = Database::create(idml, handle);
                future::ok::<Database, ()>(db)
                .await
            }).unwrap();
            // Due to bincode's variable-length encoding and the
            // unpredictability of the root filesystem's timestamp, writing the
            // label will have unpredictable results if we create a root
            // filesystem.  TODO: make it predictable by using utimensat on the
            // root filesystem
            // let tree_id = rt.block_on(db.new_fs(Vec::new())).unwrap();
            (rt, db, tempdir, filename)
        }
    });

    // Test open-after-write
    test open(objects()) {
        let (mut rt, old_db, _tempdir, path) = objects.val;
        rt.block_on(
            old_db.sync_transaction()
        ).unwrap();
        drop(old_db);
        let _db = open_db(&mut rt, path);
    }

    test sync_transaction(objects()) {
        let (mut rt, db, _tempdir, path) = objects.val;
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

test_suite! {
    name t;

    use bfffs::common::{
        cache::*,
        pool::*,
        property::*,
        ddml::*,
        idml::*,
    };
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        num::NonZeroU64,
    };
    use super::*;
    use tempfile::{Builder, TempDir};

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Database, TempDir, TreeID) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir =
                t!(Builder::new().prefix("test_database_t").tempdir());
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let mut rt = basic_runtime();
            let handle = rt.handle().clone();
            let pool = rt.block_on(async {
                let cs = NonZeroU64::new(1);
                let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
                let clusters = vec![cluster];
                future::try_join_all(clusters)
                .map_err(|_| unreachable!())
                .and_then(|clusters|
                    Pool::create(POOLNAME.to_string(), clusters)
                ).await
            }).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = rt.block_on(async move {
                let db = Database::create(idml, handle);
                future::ok::<Database, ()>(db)
                .await
            }).unwrap();
            let tree_id = rt.block_on(async {
                db.new_fs(Vec::new())
                .await
            }).unwrap();
            (rt, db, tempdir, tree_id)
        }
    });

    test get_prop_default(objects()) {
        let (mut rt, db, _tempdir, tree_id) = objects.val;

        let (val, source) = rt.block_on(async {
            db.get_prop(tree_id, PropertyName::Atime)
            .await
        }).unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    test open_filesystem(objects()) {
        let (mut rt, db, tempdir, tree_id) = objects.val;
        // Sync the database, then drop and reopen it.  That's the only way to
        // clear Inner::fs_trees
        rt.block_on(
            db.sync_transaction()
        ).unwrap();
        drop(db);
        let filename = tempdir.path().join("vdev");
        let db = open_db(&mut rt, filename);
        rt.block_on(async move {
            db.fsread(tree_id, |_| future::ok(()))
            .await
        }).unwrap();
    }

    test new_fs_with_props(objects()) {
        let (mut rt, db, _tempdir, _first_tree_id) = objects.val;
        let props = vec![Property::RecordSize(5)];
        let tree_id = rt.block_on(async {
            db.new_fs(props)
            .await
        }).unwrap();
        let (val, source) = rt.block_on(async {
            db.get_prop(tree_id, PropertyName::RecordSize)
            .await
        }).unwrap();
        assert_eq!(val, Property::RecordSize(5));
        assert_eq!(source, PropertySource::Local);
    }

    test set_prop(objects()) {
        let (mut rt, db, _tempdir, tree_id) = objects.val;

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

    test shutdown() {
        let mut rt = basic_runtime();
        let handle = rt.handle().clone();
        let len = 1 << 30;  // 1GB
        let tempdir =
            t!(Builder::new().prefix("database.tempdir()::shutdown").tempdir());
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
        drop(file);
        let mut db = rt.block_on(async move {
            Pool::create_cluster(None, 1, None, 0, &[filename])
            .map_err(|_| unreachable!())
            .and_then(|cluster| {
                Pool::create(String::from("database::shutdown"), vec![cluster])
                .map_ok(|pool| {
                    let cache = Arc::new(
                        Mutex::new(
                            Cache::with_capacity(1_000_000)
                        )
                    );
                    let ddml = Arc::new(DDML::new(pool, cache.clone()));
                    let idml = IDML::create(ddml, cache);
                    Database::create(Arc::new(idml), handle)
                })
            }).await
        }).unwrap();
        rt.block_on(db.shutdown()).unwrap();
    }
}
