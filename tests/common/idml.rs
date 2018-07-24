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
    use tokio::runtime::current_thread::Runtime;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_IDML_LABEL: [u8; 228] = [
        // Past the Pool::Label, we have an IDML::Label
        0xa4, 0x66, 0x61, 0x6c, 0x6c, 0x6f, 0x63, 0x74, // .falloct
        0x98, 0x54, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // .T......
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, // .....@..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2a, 0x00, // ......*.
        0x00, 0x00, 0x18, 0x2b, 0x00, 0x00, 0x00, 0x01, // ...+....
        0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x0c, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, // ........
        0x00, 0x18, 0x9f, 0x18, 0xfa, 0x18, 0xe3, 0x18, // ........
        0x4c, 0x18, 0xa2, 0x18, 0x4d, 0x18, 0xea, 0x18, // L...M...
        0x69, 0x68, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x72, // ihnext_r
        0x69, 0x64, 0x00, 0x64, 0x72, 0x69, 0x64, 0x74, // id.dridt
        0x98, 0x52, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // .R......
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, // .....@..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x18, 0x2a, 0x00, 0x00, 0x00, // ....*...
        0x18, 0x2b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, // .+......
        0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, // ........
        0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x18, // ........
        0x9f, 0x18, 0xfa, 0x18, 0xe3, 0x18, 0x4c, 0x18, // ......L.
        0xa2, 0x18, 0x4d, 0x18, 0xea, 0x18, 0x69, 0x63, // ..M...ic
        0x74, 0x78, 0x67, 0x18, 0x2a, 0x00, 0x00, 0x00, // txg.*...
        0x00, 0x00, 0x00, 0x00                          // ....
    ];

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, IDML, TempDir, PathBuf) {
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
                let cluster = Pool::create_cluster(1, 1, 1, None, 0, &paths);
                let clusters = vec![cluster];
                Pool::create(POOLNAME.to_string(), clusters)
            })).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = IDML::create(ddml, cache);
            (rt, idml, tempdir, filename)
        }
    });

    // Testing VdevRaid::open with golden labels is too hard, because we need to
    // store separate golden labels for each VdevLeaf.  Instead, we'll just
    // check that we can open-after-write
    test open_all(objects()) {
        let (mut rt, old_idml, _tempdir, path) = objects.val;
        let txg = TxgT::from(42);
        rt.block_on(future::lazy(|| {
            old_idml.write_label(txg)
        })).unwrap();
        drop(old_idml);
        let _idml = rt.block_on(future::lazy(|| {
            IDML::open(POOLNAME.to_string(), vec![path])
        })).unwrap();
    }

    test write_label(objects()) {
        let (mut rt, old_idml, _tempdir, path) = objects.val;
        rt.block_on(future::lazy(|| {
            old_idml.write_label(TxgT::from(42))
        })).unwrap();
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, and pool labels
        f.seek(SeekFrom::Start(0x142)).unwrap();
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
        assert_eq!(&v[0..228], &GOLDEN_IDML_LABEL[0..228]);
        // Rest of the buffer should be zero-filled
        assert!(v[228..].iter().all(|&x| x == 0));
    }
}
