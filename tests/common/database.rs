// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

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
    const GOLDEN_DB_LABEL: [u8; 64] = [
        //// Past the IDML::Label, we have a DB::Label
        0x66, 0x66, 0x6f, 0x72, 0x65, 0x73, 0x74, 0x98, // fforest.
        0x3c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // <.......
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, 0x00, // ....@...
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
    ];

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Database, TempDir, PathBuf) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_idml_persistence"));
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
            // let tree_id = rt.block_on(db.new_fs()).unwrap();
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
                    cluster::Cluster::open(vr,lr)
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
        f.seek(SeekFrom::Start(0x21e)).unwrap();
        //f.seek(SeekFrom::Start(0x142)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master,
        assert_eq!(&v[0..64], &GOLDEN_DB_LABEL[0..64]);
        // Rest of the buffer should be zero-filled
        assert!(v[64..].iter().all(|&x| x == 0));
    }
}
