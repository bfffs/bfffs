// vim: tw=80
/// These tests use a real VdevBlock object
mod basic {
    use bfffs_core::{vdev::*, vdev_block::*};
    use divbuf::DivBufShared;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::fs;
    use tempfile::{Builder, TempDir};

    #[fixture]
    fn vdev() -> (VdevBlock, TempDir) {
        let len = 1 << 26;  // 64MB
        let tempdir = t!(
            Builder::new().prefix("test_vdev_block").tempdir()
        );
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
        let vdev = VdevBlock::create(filename, None).unwrap();
        (vdev, tempdir)
    }

    #[rstest]
    fn lba2zone(vdev: (VdevBlock, TempDir)) {
        assert_eq!(vdev.0.lba2zone(0), None);
        assert_eq!(vdev.0.lba2zone(9), None);
        assert_eq!(vdev.0.lba2zone(10), Some(0));
        assert_eq!(vdev.0.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(vdev.0.lba2zone(1 << 16), Some(1));
    }

    #[rstest]
    fn path(vdev: (VdevBlock, TempDir)) {
        assert_eq!(vdev.0.path(), vdev.1.path().join("vdev"));
    }

    #[rstest]
    fn size(vdev: (VdevBlock, TempDir)) {
        assert_eq!(vdev.0.size(), 16_384);
    }

    #[rstest]
    fn zone_limits(vdev: (VdevBlock, TempDir)) {
        assert_eq!(vdev.0.zone_limits(0), (10, 1 << 16));
        assert_eq!(vdev.0.zone_limits(1), (1 << 16, 2 << 16));
    }

    #[rstest]
    #[tokio::test]
    async fn check_iovec_bounds_within(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let lba = vdev.0.size() - 1;
        vdev.0.write_at(wbuf, lba).await.unwrap();
    }

    #[should_panic]
    #[rstest]
    #[tokio::test]
    async fn check_iovec_bounds_over(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let lba = vdev.0.size();
        vdev.0.write_at(wbuf, lba).await.unwrap();
    }

    #[should_panic]
    #[rstest]
    #[tokio::test]
    async fn check_iovec_bounds_spans(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let wbuf = dbs.try_const().unwrap();
        let lba = vdev.0.size() - 1;
        vdev.0.write_at(wbuf, lba).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn check_sglist_bounds_within(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let wbuf0 = wbuf.slice_to(1024);
        let wbuf1 = wbuf.slice_from(1024);
        let wbufs = vec![wbuf0, wbuf1];
        let lba = vdev.0.size() - 1;
        vdev.0.writev_at(wbufs, lba).await.unwrap();
    }

    #[should_panic]
    #[rstest]
    #[tokio::test]
    async fn check_sglist_bounds_over(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let wbuf0 = wbuf.slice_to(1024);
        let wbuf1 = wbuf.slice_from(1024);
        let wbufs = vec![wbuf0, wbuf1];
        let lba = vdev.0.size();
        vdev.0.writev_at(wbufs, lba).await.unwrap();
    }

    #[should_panic]
    #[rstest]
    #[tokio::test]
    async fn check_sglist_bounds_spans(vdev: (VdevBlock, TempDir)) {
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let wbuf = dbs.try_const().unwrap();
        let wbuf0 = wbuf.slice_to(5120);
        let wbuf1 = wbuf.slice_from(5120);
        let wbufs = vec![wbuf0, wbuf1];
        let lba = vdev.0.size() - 1;
        vdev.0.writev_at(wbufs, lba).await.unwrap();
    }
}

mod persistence {
    use bfffs_core::{
        *,
        label::*,
        vdev::*,
        vdev_block::*,
    };
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::Read,
        num::NonZeroU64,
        os::unix::fs::FileExt,
        path::PathBuf
    };
    use tempfile::{Builder, TempDir};

    const GOLDEN: [u8; 76] = [
        // First 16 bytes are file magic
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        // Next 8 bytes are a checksum
        0xc8, 0xdc, 0x2a, 0xd9, 0xac, 0x54, 0xfd, 0xd1,
        // Next 8 bytes are the contents length, in BE
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2c,
        // The rest is a serialized VdevBlock::Label object.
        // First comes the VdevBlock's UUID.
        0xdc, 0x3e, 0xa5, 0x16, 0xd4, 0xf0, 0x4b, 0x44,
        0x96, 0x34, 0x6a, 0x9e, 0x60, 0x13, 0x1f, 0xb1,
        // Then the number of LBAS per zone
        0xbe, 0xba, 0x7e, 0x1a, 0xef, 0xbe, 0xad, 0xde,
        // Then the number of LBAs as a 64-bit number
        0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // The number of LBAs reserved for the spacemap
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // And finally the label's transaction number
        0x04, 0x03, 0x02, 0x01
    ];

    type Harness = (PathBuf, TempDir);

    #[fixture]
    fn harness() -> Harness {
        let len = 1 << 26;  // 64MB
        let tempdir = t!(
            Builder::new().prefix("test_vdev_block_persistence").tempdir()
        );
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
        (filename, tempdir)
    }

    /// Open the golden master label
    #[rstest]
    #[tokio::test]
    async fn open(harness: Harness) {
        let golden_uuid = Uuid::parse_str(
            "dc3ea516-d4f0-4b44-9634-6a9e60131fb1").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(harness.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
        }
        let mut manager = Manager::default();
        manager.taste(&harness.0).await.unwrap();
        let (vdev, _) = manager.import(golden_uuid).await.unwrap();
        assert_eq!(vdev.size(), 16_384);
        assert_eq!(vdev.txg(), TxgT::from(0x01020304));
        assert_eq!(vdev.uuid(), golden_uuid);
        let _ = harness.1;
    }

    // Open a device with only corrupted labels
    #[rstest]
    #[tokio::test]
    async fn open_ecksum(harness: Harness) {
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(harness.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
            let zeros = [0u8; 1];
            // Corrupt the labels' checksum
            f.write_all_at(&zeros, offset0 + 16).unwrap();
            f.write_all_at(&zeros, offset1 + 16).unwrap();
        }
        let mut manager = Manager::default();
        let e = manager.taste(&harness.0).await
            .err()
            .expect("Opening the file should've failed");
        assert_eq!(e, Error::EINTEGRITY);
    }

    /// Open a device that only has one valid label, the first one
    #[rstest]
    #[tokio::test]
    async fn open_first_label_only(harness: Harness) {
        let golden_uuid = Uuid::parse_str(
            "dc3ea516-d4f0-4b44-9634-6a9e60131fb1").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(harness.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
        }
        let mut manager = Manager::default();
        manager.taste(&harness.0).await.unwrap();
        let (vdev, _) = manager.import(golden_uuid).await.unwrap();
        assert_eq!(vdev.size(), 16_384);
        assert_eq!(vdev.txg(), TxgT::from(0x01020304));
        assert_eq!(vdev.uuid(), golden_uuid);
        let _ = harness.1;
    }

    // Open a device without a valid label
    #[rstest]
    #[tokio::test]
    async fn open_invalid(harness: Harness) {
        let mut manager = Manager::default();
        let e = manager.taste(&harness.0).await
            .err()
            .expect("Opening the file should've failed");
        assert_eq!(e, Error::EINVAL);
    }

    /// Open a device that only has one valid label, the second one
    #[rstest]
    #[tokio::test]
    async fn open_second_label_only(harness: Harness) {
        let golden_uuid = Uuid::parse_str(
            "dc3ea516-d4f0-4b44-9634-6a9e60131fb1").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(harness.0.clone()).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
        }
        let mut manager = Manager::default();
        manager.taste(&harness.0).await.unwrap();
        let (vdev, _) = manager.import(golden_uuid).await.unwrap();
        assert_eq!(vdev.size(), 16_384);
        assert_eq!(vdev.txg(), TxgT::from(0x01020304));
        assert_eq!(vdev.uuid(), golden_uuid);
        let _ = harness.1;
    }

    // Write the label, and compare to a golden master
    #[rstest]
    #[tokio::test]
    async fn write_label(harness: Harness) {
        let lbas_per_zone = NonZeroU64::new(0xdead_beef_1a7e_babe);
        let vdev = VdevBlock::create(harness.0.clone(), lbas_per_zone)
            .unwrap();
        let label_writer = LabelWriter::new(0);
        let txg = TxgT::from(0x01020304);
        vdev.write_label(label_writer, txg).await.unwrap();

        let mut f = std::fs::File::open(harness.0).unwrap();
        let mut v = vec![0; 4096];
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
            println!("UUID is {}", vdev.uuid());
        } */
        // Compare against the golden master, skipping the checksum and UUID
        // fields
        assert_eq!(&v[0..16], &GOLDEN[0..16]);
        assert_eq!(&v[24..32], &GOLDEN[24..32]);
        assert_eq!(&v[48..GOLDEN.len()], &GOLDEN[48..GOLDEN.len()]);
    }
}
