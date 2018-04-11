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

    const GOLDEN_POOL_LABEL: [u8; 67] = [
        // Past the VdevRaid::Label, we have a VdevRaid::Label
                                                  0xa2,// ...B....
        0x64, 0x75, 0x75, 0x69, 0x64, 0x50,
        // This is the pool UUID
                                            0x13, 0xac,// duuidP..
        0x27, 0x3a, 0x6c, 0x96, 0x4c, 0xdf, 0xbf, 0x1e,// ':l.L...
        0xd8, 0x07, 0x79, 0x19, 0x69, 0x6c,
        // UUID over, here's the rest of the label
                                            0x68, 0x63,// ..y.ilhc
        0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x82,// hildren.
        // These are the Cluster UUIDs
        0x50, 0xd7, 0xca, 0x7a, 0xb7, 0xea, 0xb5, 0x47,// P..z...G
        0x61, 0xb5, 0x97, 0xc6, 0x6a, 0x79, 0x33, 0xc0,// a...jy3.
        0x7a, 0x50, 0x29, 0x86, 0x19, 0xc7, 0xc2, 0x63,// zP)....c
        0x4e, 0x2a, 0x99, 0x8f, 0x16, 0x56, 0x19, 0x86,// N*...V..
        0xd0, 0xf8
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
            let pool = Pool::create(clusters);
            (pool, tempdir, paths)
        }
    });

    test write_label(objects()) {
        current_thread::block_on_all(future::lazy(|| {
            objects.val.0.write_label()
        })).unwrap();
        for path in objects.val.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(0xbf)).unwrap();   // Skip leaf & raid labels
            f.read_exact(&mut v).unwrap();
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[0..7], &GOLDEN_POOL_LABEL[0..7]);
            assert_eq!(&v[23..33], &GOLDEN_POOL_LABEL[23..33]);
            // Rest of the buffer should be zero-filled
            assert!(v[67..].iter().all(|&x| x == 0));
        }
    }
}
