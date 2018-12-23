// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name persistence;

    use bfffs::common::*;
    use bfffs::common::cache::*;
    use bfffs::common::database::*;
    use bfffs::common::vdev_block::*;
    use bfffs::common::vdev_raid::*;
    use bfffs::common::cluster;
    use bfffs::common::pool::*;
    use bfffs::common::ddml::*;
    use bfffs::common::idml::*;
    use bfffs::sys::vdev_file::*;
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64,
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::{
        executor::current_thread::TaskExecutor,
        runtime::current_thread::Runtime
    };

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_DB_LABEL: [u8; 104] = [
        //// Past the IDML::Label, we have a DB::Label
        0xa1, 0x66, 0x66, 0x6f, 0x72, 0x65, 0x73, 0x74, // .fforest
        0xa4, 0x66, 0x68, 0x65, 0x69, 0x67, 0x68, 0x74, // .fheight
        0x01, 0x66, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x73, // .flimits
        0xa3, 0x6a, 0x6d, 0x69, 0x6e, 0x5f, 0x66, 0x61, // .jmin_fa
        0x6e, 0x6f, 0x75, 0x74, 0x17, 0x6a, 0x6d, 0x61, // nout.jma
        0x78, 0x5f, 0x66, 0x61, 0x6e, 0x6f, 0x75, 0x74, // x_fanout
        0x18, 0x5c, 0x69, 0x5f, 0x6d, 0x61, 0x78, 0x5f, // .Hi_max_
        0x73, 0x69, 0x7a, 0x65, 0x1a, 0x00, 0x40, 0x00, // size..@.
        0x00, 0x64, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x64, // .droot.d
        0x74, 0x78, 0x67, 0x73, 0xa2, 0x65, 0x73, 0x74, // txgs.est
        0x61, 0x72, 0x74, 0x00, 0x63, 0x65, 0x6e, 0x64, // art.cend
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
    ];

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Database, TempDir, PathBuf) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_database_persistence"));
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let cs = NonZeroU64::new(1);
                let cluster = Pool::create_cluster(cs, 1, 1, None, 0, &paths);
                let clusters = vec![cluster];
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create(POOLNAME.to_string(), clusters)
                    )
            })).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = rt.block_on(future::lazy(|| {
                let te = TaskExecutor::current();
                let db = Database::create(idml, te);
                future::ok::<Database, ()>(db)
            })).unwrap();
            // Due to bincode's variable-length encoding and the
            // unpredictability of the root filesystem's timestamp, writing the
            // label will have unpredictable results if we create a root
            // filesystem.  TODO: make it predictable by using utimensat on the
            // root filesystem
            // let tree_id = rt.block_on(db.new_fs(Vec::new())).unwrap();
            (rt, db, tempdir, filename)
        }
    });

    test open(objects()) {
        let (mut rt, old_db, _tempdir, path) = objects.val;
        rt.block_on(
            old_db.sync_transaction()
        ).unwrap();
        drop(old_db);
        let _db = rt.block_on(future::lazy(|| {
            VdevFile::open(path)
            .and_then(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr)
                    .map(move |cluster| (cluster, lr))
            }).and_then(move |(cluster, reader)|{
                let proxy = ClusterProxy::new(cluster);
                Pool::open(None, vec![(proxy, reader)])
            }).map(|(pool, reader)| {
                let cache = cache::Cache::with_capacity(1_000_000);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                let (idml, reader) = idml::IDML::open(ddml, arc_cache, reader);
                let te = TaskExecutor::current();
                Database::open(Arc::new(idml), te, reader)
            })
        })).unwrap();
    }

    test sync_transaction(objects()) {
        let (mut rt, db, _tempdir, path) = objects.val;
        rt.block_on(
            db.sync_transaction()
        ).unwrap();
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, pool, and idml labels
        f.seek(SeekFrom::Start(0x25a)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master,
        assert_eq!(&v[0..104], &GOLDEN_DB_LABEL[0..104]);
        // Rest of the buffer should be zero-filled
        assert!(v[104..].iter().all(|&x| x == 0));
    }
}

test_suite! {
    name t;

    use bfffs::common::{
        cache::*,
        database::*,
        pool::*,
        property::*,
        ddml::*,
        idml::*,
    };
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::{
        executor::current_thread::TaskExecutor,
        runtime::current_thread::Runtime
    };

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Database, TempDir, TreeID) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_database_t"));
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let cs = NonZeroU64::new(1);
                let cluster = Pool::create_cluster(cs, 1, 1, None, 0, &paths);
                let clusters = vec![cluster];
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create(POOLNAME.to_string(), clusters)
                    )
            })).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = rt.block_on(future::lazy(|| {
                let te = TaskExecutor::current();
                let db = Database::create(idml, te);
                future::ok::<Database, ()>(db)
            })).unwrap();
            let tree_id = rt.block_on(future::lazy(|| {
                db.new_fs(Vec::new())
            })).unwrap();
            (rt, db, tempdir, tree_id)
        }
    });

    test get_prop_default(objects()) {
        let (mut rt, db, _tempdir, tree_id) = objects.val;

        let (val, source) = rt.block_on(future::lazy(|| {
            db.get_prop(tree_id, PropertyName::Atime)
        })).unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    test new_fs_with_props(objects()) {
        let (mut rt, db, _tempdir, _first_tree_id) = objects.val;
        let props = vec![Property::RecordSize(5)];
        let tree_id = rt.block_on(future::lazy(|| {
            db.new_fs(props)
        })).unwrap();
        let (val, source) = rt.block_on(future::lazy(|| {
            db.get_prop(tree_id, PropertyName::RecordSize)
        })).unwrap();
        assert_eq!(val, Property::RecordSize(5));
        assert_eq!(source, PropertySource::Local);
    }

    test set_prop(objects()) {
        let (mut rt, db, _tempdir, tree_id) = objects.val;

        let (val, source) = rt.block_on(future::lazy(|| {
            db.set_prop(tree_id, Property::Atime(false))
            .and_then(move |_| {
                db.get_prop(tree_id, PropertyName::Atime)
            })
        })).unwrap();
        assert_eq!(val, Property::Atime(false));
        assert_eq!(source, PropertySource::Local);
    }

    // TODO: add a test for getting a non-cached property, once it's possible to
    // make multiple datasets

    // TODO: add tests for inherited properties, once it's possible to make
    // multiple datasets.
}
