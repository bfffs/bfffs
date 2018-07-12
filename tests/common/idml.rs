// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name persistence;

    use arkfs::common::*;
    use arkfs::common::cache::*;
    use arkfs::common::pool::*;
    use arkfs::common::ddml::*;
    use arkfs::common::idml::*;
    use futures::future;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::{runtime::current_thread, reactor::Handle};

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_IDML_LABEL: [u8; 7] = [
        // Past the Pool::Label, we have an IDML::Label
        0xa1, 0x63, 0x74, 0x78, 0x67, 0x18, 0x2a,       // .ctxg.*
    ];

    fixture!( objects() -> (IDML, TempDir, PathBuf) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_idml_persistence"));
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let cluster = Pool::create_cluster(1, 1, 1, None, 0, &paths,
                                               Handle::default());
            let clusters = vec![cluster];
            let pool = Pool::create("TestPool".to_string(), clusters);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = IDML::create(ddml, cache);
            (idml, tempdir, filename)
        }
    });

    // Testing VdevRaid::open with golden labels is too hard, because we need to
    // store separate golden labels for each VdevLeaf.  Instead, we'll just
    // check that we can open-after-write
    /* test open_all(objects()) {
        let (old_pool, _tempdir, paths) = objects.val;
        let name = old_pool.name().to_string();
        let uuid = old_pool.uuid();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let label_writer = LabelWriter::new();
            old_pool.write_label(label_writer)
        })).unwrap();
        drop(old_pool);
        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::open(name.clone(), paths, Handle::default())
        })).unwrap();
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    } */

    test write_label(objects()) {
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            objects.val.0.write_label(TxgT::from(42))
        })).unwrap();
        let path = &objects.val.2;
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, and pool labels
        f.seek(SeekFrom::Start(0x13d)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master, skipping the checksum and UUID
        // fields
        assert_eq!(&v[0..7], &GOLDEN_IDML_LABEL[0..7]);
        // Rest of the buffer should be zero-filled
        assert!(v[7..].iter().all(|&x| x == 0));
    }
}
