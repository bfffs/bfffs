// vim: tw=80
use bfffs_core::{
    cluster,
    label::*,
    pool::*,
    raid,
    vdev_block::*,
    vdev_file::*,
    Error,
    TxgT
};
use divbuf::DivBufShared;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    num::NonZeroU64
};
use tempfile::{Builder, TempDir};

mod persistence {
    use futures::{TryFutureExt, future};
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use super::*;

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

    type Harness = (Pool, TempDir, Vec<String>);
    #[fixture]
    fn harness() -> Harness {
        let num_disks = 2;
        let len = 1 << 26;  // 64 MB
        let tempdir =
            t!(Builder::new().prefix("test_pool_persistence").tempdir());
        let paths = (0..num_disks).map(|i| {
            let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
            let file = t!(fs::File::create(&fname));
            t!(file.set_len(len));
            fname
        }).collect::<Vec<_>>();
        let clusters = paths.iter().map(|p| {
            let cs = NonZeroU64::new(1);
            Pool::create_cluster(cs, 1, None, 0, &[p][..])
        }).collect::<Vec<_>>();
        let pool = Pool::create("TestPool".to_string(), clusters);
        (pool, tempdir, paths)
    }

    // Test open-after-write for Pool
    #[rstest]
    #[tokio::test]
    async fn open(harness: Harness) {
        let (old_pool, _tempdir, paths) = harness;
        let name = old_pool.name().to_string();
        let uuid = old_pool.uuid();
        let label_writer = LabelWriter::new(0);
        future::try_join(old_pool.flush(0), old_pool.write_label(label_writer))
            .await.unwrap();
        drop(old_pool);
        let c0_fut = VdevFile::open(paths[0].clone())
            .and_then(|(leaf, reader)| {
                let block = VdevBlock::new(leaf);
                let (vr, lr) = raid::open(None, vec![(block, reader)]);
                cluster::Cluster::open(vr)
                .map_ok(move |cluster| (cluster, lr))
        });
        let c1_fut = VdevFile::open(paths[1].clone())
            .and_then(|(leaf, reader)| {
                let block = VdevBlock::new(leaf);
                let (vr, lr) = raid::open(None, vec![(block, reader)]);
                cluster::Cluster::open(vr)
                .map_ok(move |cluster| (cluster, lr))
        });
        let ((c0, c0r), (c1, c1r)) = future::try_join(c0_fut, c1_fut)
            .await.unwrap();
        let (pool, _) = Pool::open(Some(uuid), vec![(c0, c0r), (c1,c1r)]);
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    }

    #[rstest]
    #[tokio::test]
    async fn write_label(harness: Harness) {
        let (old_pool, _tempdir, paths) = harness;
        let label_writer = LabelWriter::new(0);
        old_pool.write_label(label_writer).await.unwrap();
        for path in paths {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            // Skip leaf, raid, and cluster labels
            f.seek(SeekFrom::Start(108)).unwrap();
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

mod t {
    use super::*;

    #[tokio::test]
    async fn enospc() {
        let len = 1 << 16;  // 64 KB
        let tempdir =
            t!(Builder::new().prefix("test_pool_enospc").tempdir());
        let path = format!("{}/vdev", tempdir.path().display());
        let file = t!(fs::File::create(&path));
        t!(file.set_len(len));
        let clusters = vec![
            Pool::create_cluster(None, 1, NonZeroU64::new(16), 0, &[path][..])
        ];
        let pool = Pool::create("TestPool".to_string(), clusters);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let txg = TxgT::from(1);
        // Given current label sizes, this cluster is large enough for 6 data
        // blocks.
        for _ in 0..6 {
            let db0 = dbs.try_const().unwrap();
            pool.write(db0, txg).await.unwrap();
        }
        assert_eq!(pool.used(), 6);
        let db0 = dbs.try_const().unwrap();
        assert_eq!(Err(Error::ENOSPC), pool.write(db0, txg).await);
        assert_eq!(pool.used(), 6);
    }
}
