// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name persistence;

    use arkfs::common::pool::*;
    use futures::future;
    use std::{fs, io::{Read, Seek, SeekFrom}};
    use tempdir::TempDir;
    use tokio::{runtime::current_thread, reactor::Handle};

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_POOL_LABEL: [u8; 86] = [
        // Past the VdevRaid::Label, we have a VdevRaid::Label
        0xa4, 0x64, 0x6e, 0x61, 0x6d, 0x65, 0x68, 0x54, // .dnamehT
        0x65, 0x73, 0x74, 0x50, 0x6f, 0x6f, 0x6c, 0x64, // estPoold
        0x75, 0x75, 0x69, 0x64, 0x50,                   // uuidP
        // This is the pool UUID
                                      0x21, 0xae, 0xd6, //      !..
        0x3d, 0x8c, 0x73, 0x45, 0x66, 0x85, 0x40, 0xd5, // =.sEf.@.
        0x48, 0x26, 0xe0, 0x7e, 0xb8,                   // H&.~.
        // UUID over, here's the rest of the label
                                      0x63, 0x74, 0x78, //      ctx
        0x67, 0x00, 0x68, 0x63, 0x68, 0x69, 0x6c, 0x64, // g.hchild
        0x72, 0x65, 0x6e, 0x82,                         // ren.
        // These are the Cluster UUIDs
                                0x50, 0xd2, 0xf5, 0x4a, //     P..J
        0xae, 0x6d, 0xa4, 0x4a, 0x11, 0x9a, 0x1f, 0x39, // .m.J...9
        0x26, 0x34, 0x4a, 0xe6, 0x7d, 0x50, 0xc1, 0xc5, // &4J.}P..
        0xcd, 0x99, 0xdb, 0x4e, 0x4a, 0x17, 0xa4, 0x52, // ...NJ..R
        0x53, 0xdd, 0xff, 0x86, 0x3b, 0x9d,             // S...;.
    ];

    fixture!( objects() -> (Pool, TempDir, Vec<String>) {
        setup(&mut self) {
            let num_disks = 2;
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_pool_persistence"));
            let paths = (0..num_disks).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                fname
            }).collect::<Vec<_>>();
            let clusters = paths.iter().map(|p| {
                Pool::create_cluster(1, 1, 1, 0, &[p][..], Handle::default())
            }).collect::<Vec<_>>();;
            let pool = Pool::create("TestPool".to_string(), clusters);
            (pool, tempdir, paths)
        }
    });

    // Testing VdevRaid::open with golden labels is too hard, because we need to
    // store separate golden labels for each VdevLeaf.  Instead, we'll just
    // check that we can open-after-write
    test open_all(objects()) {
        let (old_pool, _tempdir, paths) = objects.val;
        let name = old_pool.name().to_string();
        let uuid = old_pool.uuid();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            old_pool.write_label()
        })).unwrap();
        drop(old_pool);
        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::open(name.clone(), paths, Handle::default())
        })).unwrap();
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    }

    test write_label(objects()) {
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            objects.val.0.write_label()
        })).unwrap();
        for path in objects.val.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            // Skip leaf, raid, and cluster labels
            f.seek(SeekFrom::Start(0xfd)).unwrap();
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
            assert_eq!(&v[0..21], &GOLDEN_POOL_LABEL[0..21]);
            assert_eq!(&v[38..53], &GOLDEN_POOL_LABEL[38..53]);
            // Rest of the buffer should be zero-filled
            assert!(v[86..].iter().all(|&x| x == 0));
        }
    }
}
