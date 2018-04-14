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
    use std::fs;
    use std::io::{Read, Seek, SeekFrom};
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    const GOLDEN_POOL_LABEL: [u8; 81] = [
        // Past the VdevRaid::Label, we have a VdevRaid::Label
                                                  0xa3,// ...B....
        0x64, 0x6e, 0x61, 0x6d, 0x65, 0x68, 0x54, 0x65,// dnamehTe
        0x73, 0x74, 0x50, 0x6f, 0x6f, 0x6c, 0x64, 0x75,// stPooldu
        0x75, 0x69, 0x64, 0x50,
        // This is the pool UUID
                                0xff, 0x68, 0xe4, 0x59,// uidP.h.Y
        0xea, 0xf8, 0x43, 0xc4, 0x84, 0xcb, 0xee, 0xe9,// ..C.....
        0xdb, 0x56, 0x1b, 0x49,
        // UUID over, here's the rest of the label
                                0x68, 0x63, 0x68, 0x69,// .V.Ihchi
        0x6c, 0x64, 0x72, 0x65, 0x6e, 0x82,
        // These are the Cluster UUIDs
                                            0x50, 0x6a,// ldren.Pj
        0xc9, 0x59, 0x54, 0xf1, 0xf4, 0x4b, 0xd2, 0x96,// .YT..K..
        0x97, 0x24, 0x38, 0x4a, 0xac, 0x9f, 0x87, 0x50,// .$8J...P
        0xcb, 0xef, 0x21, 0x74, 0x43, 0xff, 0x41, 0xf5,// ..!tC.A.
        0xbc, 0x72, 0x40, 0x33, 0xa9, 0x72, 0x89, 0x69,// .r@3.r.i
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
        current_thread::block_on_all(future::lazy(|| {
            old_pool.write_label()
        })).unwrap();
        drop(old_pool);
        let pool = current_thread::block_on_all(future::lazy(|| {
            Pool::open(name.clone(), paths, Handle::default())
        })).unwrap();
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    }

    test write_label(objects()) {
        current_thread::block_on_all(future::lazy(|| {
            objects.val.0.write_label()
        })).unwrap();
        for path in objects.val.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            // Skip leaf, raid, and cluster labels
            f.seek(SeekFrom::Start(0xea)).unwrap();
            f.read_exact(&mut v).unwrap();
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[0..21], &GOLDEN_POOL_LABEL[0..21]);
            assert_eq!(&v[38..48], &GOLDEN_POOL_LABEL[38..48]);
            // Rest of the buffer should be zero-filled
            assert!(v[81..].iter().all(|&x| x == 0));
        }
    }
}
