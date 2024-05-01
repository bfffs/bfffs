// vim: tw=80
#![allow(clippy::redundant_clone)]   // False positive

mod basic {
    use bfffs_core::{
        BYTES_PER_LBA,
        Error,
        vdev::*,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        ops::Deref,
        path::PathBuf,
    };
    use tempfile::{Builder, TempDir};

    struct Harness {
        vdev: VdevFile,
        path: PathBuf,
        _tempdir: TempDir
    }

    #[fixture]
    fn harness() -> Harness {
        let len = 1 << 26;  // 64MB
        let tempdir = Builder::new()
            .prefix("test_vdev_file_basic")
            .tempdir()
            .unwrap();
        let filename = tempdir.path().join("vdev");
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        let pb = filename.to_path_buf();
        let vdev = VdevFile::create(filename, None).unwrap();
        Harness{vdev, path: pb, _tempdir: tempdir}
    }

    // pet kcov
    #[rstest]
    fn debug(harness: Harness) {
        format!("{:?}", harness.vdev);
    }

    #[test]
    fn create_enoent() {
        let dir = Builder::new()
            .prefix("test_create_enoent")
            .tempdir()
            .unwrap();
        let path = dir.path().join("vdev");
        let e = VdevFile::create(path, None)
            .err()
            .unwrap();
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
    }

    /// erase_zone on a plain file should succeed.  If fspacectl is supported,
    /// that region of the file should be zeroed.  Otherwise, nothing should
    /// happen.
    #[rstest]
    #[tokio::test]
    async fn erase_zone(harness: Harness) {
        let mut f = fs::File::open(harness.path).unwrap();
        let mut rbuf = vec![0u8; 4096];

        // First, write a record
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            harness.vdev.write_at(wbuf.clone(), 10).await
            .unwrap();
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, wbuf.deref());
        }

        let zl = harness.vdev.zone_limits(0);
        harness.vdev.erase_zone(0, zl.1 - 1).await.unwrap();

        // verify that it got erased, if fspacectl is supported here
        #[cfg(have_fspacectl)]
        {
            let expected = vec![0u8; 4096];
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }
    }

    /// Erasing a zone twice in the life of a vdev_file takes a different code
    /// path.  Exercise it, too.
    #[cfg(have_fspacectl)]
    #[rstest]
    #[tokio::test]
    async fn erase_zone_twice(harness: Harness) {
        let mut f = fs::File::open(harness.path).unwrap();
        let mut rbuf = vec![0u8; 4096];

        // First, write some data to two zones.
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            for zone in 0..2 {
                let zl = harness.vdev.zone_limits(zone);
                let wbuf = dbs.try_const().unwrap();
                harness.vdev.write_at(wbuf.clone(), zl.0).await
                .unwrap();
            }
        }

        // Now erase both zones.
        harness.vdev.erase_zone(0, vd.zone_limits(0).1 - 1).await.unwrap();
        harness.vdev.erase_zone(1, vd.zone_limits(1).1 - 1).await.unwrap();

        // verify that they got erased.
        let expected = vec![0u8; 4096];
        for zone in 0..2 {
            let zl = harness.vdev.zone_limits(zone);
            f.seek(SeekFrom::Start(zl.0 * BYTES_PER_LBA as u64)).unwrap();
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }
    }



    #[rstest]
    fn lba2zone(harness: Harness) {
        assert_eq!(harness.vdev.lba2zone(0), None);
        assert_eq!(harness.vdev.lba2zone(9), None);
        assert_eq!(harness.vdev.lba2zone(10), Some(0));
        assert_eq!(harness.vdev.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(harness.vdev.lba2zone(1 << 16), Some(1));
    }

    #[rstest]
    fn size(harness: Harness) {
        assert_eq!(harness.vdev.size(), 16_384);
    }

    #[rstest]
    fn zone_limits(harness: Harness) {
        assert_eq!(harness.vdev.zone_limits(0), (10, 1 << 16));
        assert_eq!(harness.vdev.zone_limits(1), (1 << 16, 2 << 16));
    }

    #[rstest]
    fn zones(harness: Harness) {
        assert_eq!(harness.vdev.zones(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn open_enoent() {
        let dir = Builder::new()
            .prefix("test_open_enoent")
            .tempdir()
            .unwrap();
        let path = dir.path().join("vdev");
        let e = VdevFile::open(path).await
            .err()
            .unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    #[rstest]
    #[tokio::test]
    async fn read_at() {
        // Create the initial file
        let dir = Builder::new()
            .prefix("test_read_at")
            .tempdir()
            .unwrap();
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = fs::File::create(&path).unwrap();
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the labels
            f.write_all(wbuf.as_slice()).unwrap();
            f.set_len(1 << 26).unwrap();
        }

        // Run the test
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        let vdev = VdevFile::create(path, None).unwrap();
        vdev.read_at(rbuf, 10).await.unwrap();
        assert_eq!(&dbs.try_const().unwrap()[..], &wbuf[..]);
    }

    #[rstest]
    #[tokio::test]
    async fn readv_at() {
        // Create the initial file
        let dir = Builder::new()
            .prefix("test_readv_at")
            .tempdir()
            .unwrap();
        let path = dir.path().join("vdev");
        let wbuf = (0..8192)
            .map(|i| (i / 16) as u8)
            .collect::<Vec<_>>();
        {
            let mut f = fs::File::create(&path).unwrap();
            f.seek(SeekFrom::Start(10 * BYTES_PER_LBA as u64)).unwrap();
            f.write_all(wbuf.as_slice()).unwrap();
            f.set_len(1 << 26).unwrap();
        }

        // Run the test
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let rbufs = vec![rbuf0, rbuf1];
        let vdev = VdevFile::create(path, None).unwrap();
        
        vdev.readv_at(rbufs, 10).await.unwrap();
        assert_eq!(&dbs0.try_const().unwrap()[..], &wbuf[..4096]);
        assert_eq!(&dbs1.try_const().unwrap()[..], &wbuf[4096..]);
    }

    #[rstest]
    #[tokio::test]
    async fn write_at(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        harness.vdev.write_at(wbuf.clone(), 10).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(rbuf, wbuf.deref());
    }

    #[should_panic(expected = "Don't overwrite the labels!")]
    #[rstest]
    #[tokio::test]
    async fn write_at_overwrite_label(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        harness.vdev.write_at(wbuf, 0).await.unwrap();
    }

    #[should_panic(expected = "Don't overwrite the labels!")]
    #[rstest]
    #[tokio::test]
    async fn writev_at_overwrite_label(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        harness.vdev.writev_at(vec![wbuf], 0).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn write_at_lba(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        harness.vdev.write_at(wbuf.clone(), 11).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(11 * 4096)).unwrap();
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(rbuf, wbuf.deref());
    }

    #[rstest]
    #[tokio::test]
    async fn writev_at(harness: Harness) {
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![1u8; 4096]);
        let wbuf0 = dbs0.try_const().unwrap();
        let wbuf1 = dbs1.try_const().unwrap();
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 8192];
        harness.vdev.writev_at(wbufs, 10).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(10 * BYTES_PER_LBA as u64)).unwrap();
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(&rbuf[0..4096], wbuf0.deref());
        assert_eq!(&rbuf[4096..8192], wbuf1.deref());
    }

    #[rstest]
    #[tokio::test]
    async fn read_after_write(harness: Harness) {
        let vd = harness.vdev;
        let dbsw = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbsw.try_const().unwrap();
        let dbsr = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbsr.try_mut().unwrap();
        vd.write_at(wbuf.clone(), 10).await.unwrap();
        vd.read_at(rbuf, 10).await.unwrap();
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }
}

/// Tests that use a device file
mod dev {
    use crate::{require_root, Md};
    use bfffs_core::{
        vdev::Vdev,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use function_name::named;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{self, Read, Seek, SeekFrom},
        num::NonZeroU64,
        ops::Deref,
    };

    fn harness() -> io::Result<(VdevFile, Md)> {
        let md = Md::new()?;
        let zones_per_lba = NonZeroU64::new(8192);  // 32 MB zones
        let vd = VdevFile::create(md.as_path(), zones_per_lba).unwrap();
        Ok((vd, md))
    }

    /// For devices that support TRIM, erase_zone should do it.
    #[named]
    #[tokio::test]
    async fn erase_zone() {
        require_root!();

        let (vd, md) = harness().unwrap();
        let mut rbuf = vec![0u8; 4096];
        let mut f = fs::File::open(&md.0).unwrap();

        // First, write a record
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            vd.write_at(wbuf.clone(), 10).await.unwrap();
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, wbuf.deref());
        }

        // Actually erase the zone
        vd.erase_zone(0, vd.zone_limits(0).1 - 1).await.unwrap();

        // verify that it got erased
        {
            let expected = vec![0u8; 4096];
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }

        // Must drop vdev before md
        drop(vd);
    }
}
