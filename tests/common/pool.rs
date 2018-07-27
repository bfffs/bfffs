// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name persistence;

    use arkfs::sys::vdev_file::*;
    use arkfs::common::vdev_block::*;
    use arkfs::common::vdev_raid::*;
    use arkfs::common::cluster;
    use arkfs::common::label::*;
    use arkfs::common::pool::*;
    use futures::{Future, future};
    use std::{fs, io::{Read, Seek, SeekFrom}};
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_POOL_LABEL: [u8; 81] = [
        // Past the Cluster::Label, we have a Pool::Label
        0xa3, 0x64, 0x6e, 0x61, 0x6d, 0x65, 0x68, 0x54, // .dnamehT
        0x65, 0x73, 0x74, 0x50, 0x6f, 0x6f, 0x6c, 0x64, // estPoold
        0x75, 0x75, 0x69, 0x64, 0x50,                   // uuidP
        // This is the pool UUID
                                      0x21, 0xae, 0xd6, //      !..
        0x3d, 0x8c, 0x73, 0x45, 0x66, 0x85, 0x40, 0xd5, // =.sEf.@.
        0x48, 0x26, 0xe0, 0x7e, 0xb8,                   // H&.~.
        // UUID over, here's the rest of the label
                                      0x68, 0x63, 0x68, //      hch
        0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x82,       // ildren.
        // These are the Cluster UUIDs
                                                  0x50, //        P
        0xae, 0xb7, 0x6e, 0xfd, 0x77, 0x4b, 0x4b, 0x40, // ..n.wKK@
        0x86, 0x59, 0x21, 0xed, 0xc7, 0x7e, 0x62, 0xae, // .Y!..~b.
        0x50, 0x2d, 0x11, 0xd9, 0x4b, 0xff, 0x4f, 0x4b, // P-..K.OK
        0xb3, 0x8c, 0x22, 0x3c, 0x84, 0xc4, 0x91, 0x70, // .."<...p
        0x1d,
    ];

    fixture!( objects() -> (Runtime, Pool, TempDir, Vec<String>) {
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
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let clusters = paths.iter().map(|p| {
                    Pool::create_cluster(1, 1, 1, None, 0, &[p][..])
                }).collect::<Vec<_>>();
                Pool::create("TestPool".to_string(), clusters)
            })).unwrap();
            (rt, pool, tempdir, paths)
        }
    });

    // Test open-after-write for Pool
    test open(objects()) {
        let (mut rt, old_pool, _tempdir, paths) = objects.val;
        let name = old_pool.name().to_string();
        let uuid = old_pool.uuid();
        rt.block_on(future::lazy(|| {
            let label_writer = LabelWriter::new();
            old_pool.write_label(label_writer)
        })).unwrap();
        drop(old_pool);
        let (pool, _label_reader) = rt.block_on(future::lazy(|| {
            let c0_fut = VdevFile::open(paths[0].clone())
                .map(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr,lr)
            });
            let c1_fut = VdevFile::open(paths[1].clone())
                .map(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr,lr)
            });
            c0_fut.join(c1_fut)
                .and_then(move |((c0, c0r), (c1,c1r))| {
                    let proxy0 = ClusterProxy::new(c0);
                    let proxy1 = ClusterProxy::new(c1);
                    Pool::open(Some(uuid), vec![(proxy0, c0r), (proxy1,c1r)])
                })
        })).unwrap();
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    }

    test write_label(objects()) {
        let (mut rt, old_pool, _tempdir, paths) = objects.val;
        rt.block_on(future::lazy(|| {
            let label_writer = LabelWriter::new();
            old_pool.write_label(label_writer)
        })).unwrap();
        for path in paths {
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
            assert_eq!(&v[37..46], &GOLDEN_POOL_LABEL[37..46]);
            // Rest of the buffer should be zero-filled
            assert!(v[81..].iter().all(|&x| x == 0));
        }
    }
}
