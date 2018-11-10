// vim: tw=80
use galvanic_test::*;

test_suite! {
    name basic;

    use bfffs::{
        common::{
            vdev::*,
            vdev_leaf::*
        },
        sys::vdev_file::*
    };
    use divbuf::DivBufShared;
    use futures::{future, Future};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        ops::Deref,
        path::PathBuf,
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread;

    fixture!( vdev() -> (VdevFile, PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_file_basic"));
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
        assert_eq!(vdev.val.0.lba2zone(7), None);
        assert_eq!(vdev.val.0.lba2zone(8), Some(0));
        assert_eq!(vdev.val.0.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(vdev.val.0.lba2zone(1 << 16), Some(1));
    }

    test size(vdev) {
        assert_eq!(vdev.val.0.size(), 16_384);
    }

    test zone_limits(vdev) {
        assert_eq!(vdev.val.0.zone_limits(0), (8, 1 << 16));
        assert_eq!(vdev.val.0.zone_limits(1), (1 << 16, 2 << 16));
    }

    test zones(vdev) {
        assert_eq!(vdev.val.0.zones(), 1);
    }

    test read_at() {
        // Create the initial file
        let dir = t!(TempDir::new("test_read_at"));
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = t!(fs::File::create(&path));
            f.seek(SeekFrom::Start(4 * 4096)).unwrap();   // Skip the label
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        let vdev = VdevFile::create(path, None).unwrap();
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.read_at(rbuf, 4)
        })));
        assert_eq!(&dbs.try().unwrap()[..], &wbuf[..]);
    }

    test readv_at() {
        // Create the initial file
        let dir = t!(TempDir::new("test_readv_at"));
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = t!(fs::File::create(&path));
            f.seek(SeekFrom::Start(4 * 4096)).unwrap();   // Skip the label
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let mut rbuf0 = dbs.try_mut().unwrap();
        let rbuf1 = rbuf0.split_off(1024);
        let rbufs = vec![rbuf0, rbuf1];
        let vdev = VdevFile::create(path, None).unwrap();
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.readv_at(rbufs, 4)
        })));
        assert_eq!(&dbs.try().unwrap()[..], &wbuf[..]);
    }

    test write_at(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let mut rbuf = vec![0u8; 4096];
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.write_at(wbuf.clone(), 8)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        f.seek(SeekFrom::Start(8 * 4096)).unwrap();   // Skip the label
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    #[should_panic]
    test write_at_overwrite_label(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        vdev.val.0.write_at(wbuf.clone(), 0);
    }

    test write_at_lba(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let mut rbuf = vec![0u8; 4096];
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.write_at(wbuf.clone(), 9)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(9 * 4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test writev_at(vdev) {
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let mut wbuf0 = dbs.try().unwrap();
        let wbuf1 = wbuf0.split_off(1024);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 4096];
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, 8)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(8 * 4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(&rbuf[0..1024], wbuf0.deref().deref());
        assert_eq!(&rbuf[1024..4096], wbuf1.deref().deref());
    }

    test read_after_write(vdev) {
        let vd = vdev.val.0;
        let dbsw = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbsw.try().unwrap();
        let dbsr = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbsr.try_mut().unwrap();
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vd.write_at(wbuf.clone(), 8)
                .and_then(|_| {
                    vd.read_at(rbuf, 8)
                })
        })));
        assert_eq!(wbuf, dbsr.try().unwrap());
    }
}

test_suite! {
    name persistence;

    use bfffs::{
        common::{
            *,
            label::*,
            vdev::*,
            vdev_leaf::*
        },
        sys::vdev_file::*
    };
    use futures::{future, Future};
    use std::{
        fs,
        io::{Read, Write},
        num::NonZeroU64,
        path::PathBuf
    };
    use std;
    use tempdir::TempDir;
    use tokio::runtime::current_thread;
    use uuid::Uuid;

    const GOLDEN: [u8; 85] = [
        // First 16 bytes are file magic
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        // Next 8 bytes are a checksum
        0x41, 0xd1, 0x12, 0xee, 0x9f, 0x1d, 0xdc, 0xa1,
        // Next 8 bytes are the contents length, in BE
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, // .......2
        // The rest is a serialized VdevFile::Label object
        0xa3, 0x64, 0x75, 0x75, 0x69, 0x64, 0x50,       // .duuidP
        // These 16 bytes are a UUID
                                                  0x41,
        0x4f, 0x23, 0x38, 0x39, 0xcf, 0x45, 0x7f, 0xb1,
        0xcb, 0xd4, 0x61, 0xf5, 0xa7, 0xcc, 0x4b,
        // This is the rest of the LabelData
                                                  0x6d, //        m
        0x6c, 0x62, 0x61, 0x73, 0x5f, 0x70, 0x65, 0x72, // lbas_per
        0x5f, 0x7a, 0x6f, 0x6e, 0x65, 0x1b, 0xde, 0xad, // _zone...
        0xbe, 0xef, 0x1a, 0x7e, 0xba, 0xbe, 0x64, 0x6c, // ...~..dl
        0x62, 0x61, 0x73, 0x19, 0x40,                   // bas.@
    ];

    fixture!( fixture() -> (PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_file_persistence"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pb = filename.to_path_buf();
            (pb, tempdir)
        }
    });

    // Open the golden master label
    test open(fixture) {
        let golden_uuid = Uuid::parse_str(
            "414f2338-39cf-457f-b1cb-d461f5a7cc4b").unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            f.write_all(&GOLDEN).unwrap();
        }
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            VdevFile::open(fixture.val.0)
                .and_then(|(vdev, _label_reader)| {
                    assert_eq!(vdev.size(), 16_384);
                    assert_eq!(vdev.uuid(), golden_uuid);
                    Ok(())
                })
        })));
        let _ = fixture.val.1;
    }

    // Open a device without a valid label
    test open_invalid(fixture) {
        let e = current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            VdevFile::open(fixture.val.0)
        })).err().expect("Opening the file should've failed");
        assert_eq!(e, Error::EINVAL);
    }

    // Write the label, and compare to a golden master
    test write_label(fixture) {
        let lbas_per_zone = NonZeroU64::new(0xdead_beef_1a7e_babe);
        let vdev = VdevFile::create(fixture.val.0.clone(), lbas_per_zone)
            .unwrap();
        t!(current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let label_writer = LabelWriter::new(0);
            vdev.write_label(label_writer)
        })));

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
        assert_eq!(&v[24..39], &GOLDEN[24..39]);
        assert_eq!(&v[55..GOLDEN.len()], &GOLDEN[55..GOLDEN.len()]);
    }
}
