// vim: tw=80
use bfffs_core::{
    label::*,
    pool::*,
    vdev::{FaultedReason, Health},
    Error,
    TxgT
};
use divbuf::DivBufShared;
use rstest::{fixture, rstest};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};

#[fixture]
fn harness() -> crate::PoolHarness {
    crate::PoolBuilder::new()
        .disks(2)
        .nclusters(2)
        .name("TestPool")
        .chunksize(1)
        .zone_size(16)
        .build()
}

mod fault {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn disk(mut harness: crate::PoolHarness) {
        let stat = harness.pool.status();
        let uuid = stat.clusters[0].mirrors[0].leaves[0].uuid;
        harness.pool.fault(uuid).await.unwrap();

        let stat = harness.pool.status();
        assert_eq!(stat.health,
                   Health::Faulted(FaultedReason::InsufficientRedundancy));
    }

    #[rstest]
    #[tokio::test]
    async fn fault_self(mut harness: crate::PoolHarness) {
        assert_eq!(Err(Error::EINVAL), harness.pool.fault(harness.pool.uuid()).await);
    }
}

mod persistence {
    use futures::future;
    use pretty_assertions::assert_eq;
    use super::*;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_POOL_LABEL: [u8; 64] = [
        // Past the VdevRaid::Label, we have a Pool::Label
        // First is the Pool's name as a String, beginning with a 32-bit length
        0x08, 0x00, 0x00, 0x00, 0x54, 0x65, 0x73, 0x74, //     Test
        0x50, 0x6f, 0x6f, 0x6c,                         // Pool
        // Then the Pool's UUID
                                0x62, 0x2d, 0x3e, 0xa4,
        0x92, 0x74, 0x4b, 0xfa, 0x8b, 0x41, 0xda, 0x1b,
        0xfb, 0x44, 0xe0, 0xc9,
        // Then a vector of VdevRaid children.  First the count of children as a
        // 32-bit number, then each child's UUID as a 128-bit number.
                                0x02, 0x00, 0x00, 0x00,
        0xca, 0x17, 0x16, 0xba, 0x78, 0xe5, 0x46, 0xe0,
        0x96, 0x5e, 0x2c, 0x04, 0x3f, 0xab, 0x65, 0x0a,
        0xbe, 0x55, 0x44, 0x83, 0xac, 0x4a, 0x4f, 0x5b,
        0xab, 0x9d, 0xa5, 0x1a, 0x9d, 0x11, 0x5f, 0xfb,
    ];

    // Test open-after-write for Pool
    #[rstest]
    #[tokio::test]
    async fn open(harness: crate::PoolHarness) {
        let name = harness.pool.name().to_string();
        let uuid = harness.pool.uuid();
        let label_writer = LabelWriter::new(0);
        future::try_join(harness.pool.flush(0), harness.pool.write_label(label_writer))
            .await.unwrap();
        drop(harness.pool);

        let mut manager = Manager::default();
        for path in harness.paths.iter() {
            manager.taste(path).await.unwrap();
        }
        let (pool, _) = manager.import(uuid).await.unwrap();
        assert_eq!(name, pool.name());
        assert_eq!(uuid, pool.uuid());
    }

    #[rstest]
    #[tokio::test]
    async fn write_label(harness: crate::PoolHarness) {
        let ph = harness;
        let label_writer = LabelWriter::new(0);
        ph.pool.write_label(label_writer).await.unwrap();
        for path in ph.paths {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            // Skip leaf, mirror, raid, and cluster labels
            f.seek(SeekFrom::Start(144)).unwrap();
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
            assert_eq!(&v[0..12], &GOLDEN_POOL_LABEL[0..12]);
            assert_eq!(&v[28..32], &GOLDEN_POOL_LABEL[28..32]);
            // Rest of the buffer should be zero-filled
            assert!(v[64..].iter().all(|&x| x == 0));
        }
    }
}

mod t {
    use super::*;

    #[tokio::test]
    async fn enospc() {
        let ph = crate::PoolBuilder::new()
            .fsize(1 << 16)     // 64 kB
            .zone_size(16)
            .build();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let txg = TxgT::from(1);
        // Given current label sizes, this cluster is large enough for 6 data
        // blocks.
        for _ in 0..6 {
            let db0 = dbs.try_const().unwrap();
            ph.pool.write(db0, txg).await.unwrap();
        }
        assert_eq!(ph.pool.used(), 6);
        let db0 = dbs.try_const().unwrap();
        assert_eq!(Err(Error::ENOSPC), ph.pool.write(db0, txg).await);
        assert_eq!(ph.pool.used(), 6);
    }
}
