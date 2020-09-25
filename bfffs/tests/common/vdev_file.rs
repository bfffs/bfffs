// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name basic;

    use bfffs::common::{
        Error,
        vdev::*,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use futures::TryFutureExt;
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        ops::Deref,
        path::PathBuf,
    };
    use tempfile::{Builder, TempDir};
    use tokio::runtime;

    fixture!( vdev() -> (VdevFile, PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir =
                t!(Builder::new().prefix("test_vdev_file_basic").tempdir());
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pb = filename.to_path_buf();
            let vdev = VdevFile::create(filename, None).unwrap();
            (vdev, pb, tempdir)
        }
    });

    // pet kcov
    test debug(vdev) {
        format!("{:?}", vdev.val.0);
    }

    test lba2zone(vdev) {
        assert_eq!(vdev.val.0.lba2zone(0), None);
        assert_eq!(vdev.val.0.lba2zone(9), None);
        assert_eq!(vdev.val.0.lba2zone(10), Some(0));
        assert_eq!(vdev.val.0.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(vdev.val.0.lba2zone(1 << 16), Some(1));
    }

    test size(vdev) {
        assert_eq!(vdev.val.0.size(), 16_384);
    }

    test zone_limits(vdev) {
        assert_eq!(vdev.val.0.zone_limits(0), (10, 1 << 16));
        assert_eq!(vdev.val.0.zone_limits(1), (1 << 16, 2 << 16));
    }

    test zones(vdev) {
        assert_eq!(vdev.val.0.zones(), 1);
    }

    test open_enoent() {
        let dir = t!(
            Builder::new().prefix("test_read_at").tempdir()
        );
        let path = dir.path().join("vdev");
        let mut rt = runtime::Runtime::new().unwrap();
        let e = rt.block_on(VdevFile::open(path))
            .err()
            .unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    test read_at() {
        // Create the initial file
        let dir = t!(Builder::new().prefix("test_read_at").tempdir());
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = t!(fs::File::create(&path));
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the labels
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        let vdev = VdevFile::create(path, None).unwrap();
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.read_at(rbuf, 10).await
        }).unwrap();
        assert_eq!(&dbs.try_const().unwrap()[..], &wbuf[..]);
    }

    test readv_at() {
        // Create the initial file
        let dir = t!(Builder::new().prefix("test_readv_at").tempdir());
        let path = dir.path().join("vdev");
        let wbuf = (0..4096)
            .map(|i| (i / 16) as u8)
            .collect::<Vec<_>>();
        {
            let mut f = t!(fs::File::create(&path));
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the labels
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let dbs0 = DivBufShared::from(vec![0u8; 1024]);
        let dbs1 = DivBufShared::from(vec![0u8; 3072]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let rbufs = vec![rbuf0, rbuf1];
        let vdev = VdevFile::create(path, None).unwrap();
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.readv_at(rbufs, 10).await
        }).unwrap();
        assert_eq!(&dbs0.try_const().unwrap()[..], &wbuf[..1024]);
        assert_eq!(&dbs1.try_const().unwrap()[..], &wbuf[1024..]);
    }

    test write_at(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.val.0.write_at(wbuf.clone(), 10).await
        }).unwrap();
        let mut f = t!(fs::File::open(vdev.val.1));
        f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    #[should_panic]
    test write_at_overwrite_label(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.val.0.write_at(wbuf, 0).await
        }).unwrap();
    }

    test write_at_lba(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.val.0.write_at(wbuf.clone(), 11).await
        }).unwrap();
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(11 * 4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test writev_at(vdev) {
        let dbs0 = DivBufShared::from(vec![0u8; 1024]);
        let dbs1 = DivBufShared::from(vec![1u8; 3072]);
        let wbuf0 = dbs0.try_const().unwrap();
        let wbuf1 = dbs1.try_const().unwrap();
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 4096];
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vdev.val.0.writev_at(wbufs, 10).await
        }).unwrap();
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(10 * 4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(&rbuf[0..1024], wbuf0.deref().deref());
        assert_eq!(&rbuf[1024..4096], wbuf1.deref().deref());
    }

    test read_after_write(vdev) {
        let vd = vdev.val.0;
        let dbsw = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbsw.try_const().unwrap();
        let dbsr = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbsr.try_mut().unwrap();
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async {
            vd.write_at(wbuf.clone(), 10)
                .and_then(|_| {
                    vd.read_at(rbuf, 10)
                }).await
        }).unwrap();
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }
}

test_suite! {
    name persistence;

    use bfffs::common::{
        *,
        label::*,
        vdev::*,
        vdev_file::*
    };
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::Read,
        num::NonZeroU64,
        os::unix::fs::FileExt,
        path::PathBuf
    };
    use std;
    use tempfile::{Builder, TempDir};
    use tokio::runtime;

    const GOLDEN: [u8; 72] = [
        // First 16 bytes are file magic
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        // Next 8 bytes are a checksum
        0x2e, 0x43, 0xc2, 0x5d, 0x1f, 0x55, 0x20, 0x3b,
        // Next 8 bytes are the contents length, in BE
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28,
        // The rest is a serialized VdevFile::Label object.
        // First comes the VdevFile's UUID.
        0x3f, 0xa1, 0xf6, 0xb9, 0x54, 0xb1, 0x4a, 0x10,
        0xbc, 0x6b, 0x5b, 0x2a, 0x15, 0xe8, 0xa0, 0x3d,
        // Then the number of LBAS per zone
        0xbe, 0xba, 0x7e, 0x1a, 0xef, 0xbe, 0xad, 0xde,
        // Then the number of LBAs as a 64-bit number
        0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Finally the number of LBAs reserved for the spacemap
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fixture!( fixture() -> (PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(
                Builder::new().prefix("test_vdev_file_persistence").tempdir()
            );
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pb = filename.to_path_buf();
            (pb, tempdir)
        }
    });

    /// Open the golden master label
    test open(fixture) {
        let golden_uuid = Uuid::parse_str(
            "3fa1f6b9-54b1-4a10-bc6b-5b2a15e8a03d").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
        }
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async { VdevFile::open(fixture.val.0).await})
        .and_then(|(vdev, _label_reader)| {
            assert_eq!(vdev.size(), 16_384);
            assert_eq!(vdev.uuid(), golden_uuid);
            Ok(())
        }).unwrap();
        let _ = fixture.val.1;
    }

    // Open a device with only corrupted labels
    test open_ecksum(fixture) {
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
            let zeros = [0u8; 1];
            // Corrupt the labels' checksum
            f.write_all_at(&zeros, offset0 + 16).unwrap();
            f.write_all_at(&zeros, offset1 + 16).unwrap();
        }
        let mut rt = runtime::Runtime::new().unwrap();
        let e = rt.block_on(async { VdevFile::open(fixture.val.0).await})
            .err()
            .expect("Opening the file should've failed");
        assert_eq!(e, Error::ECKSUM);
    }

    /// Open a device that only has one valid label, the first one
    test open_first_label_only(fixture) {
        let golden_uuid = Uuid::parse_str(
            "3fa1f6b9-54b1-4a10-bc6b-5b2a15e8a03d").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            let offset0 = 0;
            f.write_all_at(&GOLDEN, offset0).unwrap();
        }
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async{ VdevFile::open(fixture.val.0).await})
        .and_then(|(vdev, _label_reader)| {
            assert_eq!(vdev.size(), 16_384);
            assert_eq!(vdev.uuid(), golden_uuid);
            Ok(())
        }).unwrap();
        let _ = fixture.val.1;
    }

    // Open a device without a valid label
    test open_invalid(fixture) {
        let mut rt = runtime::Runtime::new().unwrap();
        let e = rt.block_on(async { VdevFile::open(fixture.val.0).await })
            .err()
            .expect("Opening the file should've failed");
        assert_eq!(e, Error::EINVAL);
    }

    /// Open a device that only has one valid label, the second one
    test open_second_label_only(fixture) {
        let golden_uuid = Uuid::parse_str(
            "3fa1f6b9-54b1-4a10-bc6b-5b2a15e8a03d").unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            let offset1 = 4 * BYTES_PER_LBA as u64;
            f.write_all_at(&GOLDEN, offset1).unwrap();
        }
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async { VdevFile::open(fixture.val.0).await })
        .and_then(|(vdev, _label_reader)| {
            assert_eq!(vdev.size(), 16_384);
            assert_eq!(vdev.uuid(), golden_uuid);
            Ok(())
        }).unwrap();
        let _ = fixture.val.1;
    }

    // Write the label, and compare to a golden master
    test write_label(fixture) {
        let lbas_per_zone = NonZeroU64::new(0xdead_beef_1a7e_babe);
        let vdev = VdevFile::create(fixture.val.0.clone(), lbas_per_zone)
            .unwrap();
        let mut rt = runtime::Runtime::new().unwrap();
        let label_writer = LabelWriter::new(0);
        rt.block_on(async { vdev.write_label(label_writer).await })
            .unwrap();

        let mut f = std::fs::File::open(fixture.val.0).unwrap();
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
