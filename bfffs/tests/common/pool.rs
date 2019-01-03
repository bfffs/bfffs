// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name persistence;

    use bfffs::common::vdev_file::*;
    use bfffs::common::vdev_block::*;
    use bfffs::common::raid::*;
    use bfffs::common::cluster;
    use bfffs::common::label::*;
    use bfffs::common::pool::*;
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_POOL_LABEL: [u8; 72] = [
        // Past the VdevRaid::Label, we have a Pool::Label
        // First is the Pool's name as a String, beginning with a 64-bit length
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x54, 0x65, 0x73, 0x74, 0x50, 0x6f, 0x6f, 0x6c, // TestPool
        // Then the Pool's UUID
        0x62, 0x2d, 0x3e, 0xa4, 0x92, 0x74, 0x4b, 0xfa,
        0x8b, 0x41, 0xda, 0x1b, 0xfb, 0x44, 0xe0, 0xc9,
        // Then a vector of VdevRaid children.  First the count of children as a
        // 64-bit number, then each child's UUID as a 128-bit number.
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xca, 0x17, 0x16, 0xba, 0x78, 0xe5, 0x46, 0xe0,
        0x96, 0x5e, 0x2c, 0x04, 0x3f, 0xab, 0x65, 0x0a,
        0xbe, 0x55, 0x44, 0x83, 0xac, 0x4a, 0x4f, 0x5b,
        0xab, 0x9d, 0xa5, 0x1a, 0x9d, 0x11, 0x5f, 0xfb,
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
                    let cs = NonZeroU64::new(1);
                    Pool::create_cluster(cs, 1, 1, None, 0, &[p][..])
                }).collect::<Vec<_>>();
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create("TestPool".to_string(), clusters)
                    )
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
            let label_writer = LabelWriter::new(0);
            old_pool.flush(0)
            .join(old_pool.write_label(label_writer))
        })).unwrap();
        drop(old_pool);
        let (pool, _label_reader) = rt.block_on(future::lazy(|| {
            let c0_fut = VdevFile::open(paths[0].clone())
                .and_then(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr)
                    .map(move |cluster| (cluster, lr))
            });
            let c1_fut = VdevFile::open(paths[1].clone())
                .and_then(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr)
                    .map(move |cluster| (cluster, lr))
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
            let label_writer = LabelWriter::new(0);
            old_pool.write_label(label_writer)
        })).unwrap();
        for path in paths {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            // Skip leaf, raid, and cluster labels
            f.seek(SeekFrom::Start(128)).unwrap();
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
            assert_eq!(&v[0..16], &GOLDEN_POOL_LABEL[0..16]);
            assert_eq!(&v[32..40], &GOLDEN_POOL_LABEL[32..40]);
            // Rest of the buffer should be zero-filled
            assert!(v[72..].iter().all(|&x| x == 0));
        }
    }
}
