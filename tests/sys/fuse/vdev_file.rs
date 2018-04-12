macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name basic;

    use arkfs::common::vdev::*;
    use arkfs::common::vdev_leaf::*;
    use arkfs::sys::vdev_file::*;
    use futures::{future, Future};
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::ops::Deref;
    use std::path::PathBuf;
    use divbuf::DivBufShared;
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    fixture!( vdev() -> (VdevFile, PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_file_basic"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pb = filename.to_path_buf();
            let vdev = VdevFile::create(filename, Handle::current()).unwrap();
            (vdev, pb, tempdir)
        }
    });

    test lba2zone(vdev) {
        assert_eq!(vdev.val.0.lba2zone(0), None);
        assert_eq!(vdev.val.0.lba2zone(3), None);
        assert_eq!(vdev.val.0.lba2zone(4), Some(0));
        assert_eq!(vdev.val.0.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(vdev.val.0.lba2zone(1 << 16), Some(1));
    }

    test size(vdev) {
        assert_eq!(vdev.val.0.size(), 16_384);
    }

    test zone_limits(vdev) {
        assert_eq!(vdev.val.0.zone_limits(0), (4, 1 << 16));
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
        let vdev = VdevFile::create(path, Handle::current()).unwrap();
        t!(current_thread::block_on_all(future::lazy(|| {
            vdev.read_at(rbuf, 4)
        })));
        assert_eq!(dbs.try().unwrap(), wbuf[..]);
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
        let vdev = VdevFile::create(path, Handle::current()).unwrap();
        t!(current_thread::block_on_all(future::lazy(|| {
            vdev.readv_at(rbufs, 4)
        })));
        assert_eq!(dbs.try().unwrap(), wbuf[..]);
    }

    test write_at(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let mut rbuf = vec![0u8; 4096];
        t!(current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf.clone(), 4)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        f.seek(SeekFrom::Start(4 * 4096)).unwrap();   // Skip the label
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
        t!(current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf.clone(), 5)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(5 * 4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test writev_at(vdev) {
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let mut wbuf0 = dbs.try().unwrap();
        let wbuf1 = wbuf0.split_off(1024);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 4096];
        t!(current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, 4)
        })));
        let mut f = t!(fs::File::open(vdev.val.1));
        t!(f.seek(SeekFrom::Start(4 * 4096)));
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
        t!(current_thread::block_on_all(future::lazy(|| {
            vd.write_at(wbuf.clone(), 4)
                .and_then(|_| {
                    vd.read_at(rbuf, 4)
                })
        })));
        assert_eq!(wbuf, dbsr.try().unwrap());
    }
}

test_suite! {
    name persistence;

    use arkfs::common::vdev::*;
    use arkfs::common::label::*;
    use arkfs::sys::vdev_file::*;
    use arkfs::common::vdev_leaf::*;
    use futures::{future, Future};
    use std::fs;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std;
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;
    use uuid::Uuid;

    const GOLDEN: [u8; 82] = [
        // First 16 bytes are file magic
        0x41, 0x72, 0x6b, 0x46, 0x53, 0x20, 0x56, 0x64, // ArkFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        // Next 8 bytes are a checksum
        0xb3, 0x64, 0xb4, 0x42, 0x53, 0xe1, 0xb5, 0x64,
        // Next 8 bytes are the contents length, in BE
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // .......2
        // The rest is a serialized VdevFile::Label object
        0xa3, 0x64, 0x75, 0x75, 0x69, 0x64, 0x50,
        // These 16 bytes are a UUID
                                                  0x19, // .duuidP.
        0xfa, 0x3e, 0xe9, 0x9c, 0x35, 0x4e, 0x11, 0xb1, // .>..5N..
        0x5b, 0xf9, 0x27, 0xcb, 0x7b, 0xc0, 0x61,
        // This is the rest of the LabelData
                                                  0x6d, // ....]..m
        0x6c, 0x62, 0x61, 0x73, 0x5f, 0x70, 0x65, 0x72, // lbas_per
        0x5f, 0x7a, 0x6f, 0x6e, 0x65, 0x1a, 0x00, 0x01, // _zone...
        0x00, 0x00, 0x64, 0x6c, 0x62, 0x61, 0x73, 0x19, // ..dlbas.
        0x40, 0x00
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
            "19fa3ee9-9c35-4e11-b15b-f927cb7bc061").unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(fixture.val.0.clone()).unwrap();
            f.write_all(&GOLDEN).unwrap();
        }
        t!(current_thread::block_on_all(future::lazy(|| {
            VdevFile::open(fixture.val.0, Handle::current())
                .and_then(|(vdev, _label_reader)| {
                    assert_eq!(vdev.size(), 16_384);
                    assert_eq!(vdev.uuid(), golden_uuid);
                    Ok(())
                })
        })));
        let _ = fixture.val.1;
    }

    // Write the label, and compare to a golden master
    test write_label(fixture) {
        let vdev = VdevFile::create(fixture.val.0.clone(),
                                        Handle::current()).unwrap();
        t!(current_thread::block_on_all(future::lazy(|| {
            let label_writer = LabelWriter::new();
            vdev.write_label(label_writer)
        })));

        let mut f = std::fs::File::open(fixture.val.0).unwrap();
        let mut v = vec![0; 4096];
        f.read_exact(&mut v).unwrap();
        // Compare against the golden master, skipping the checksum and UUID
        // fields
        assert_eq!(&v[0..16], &GOLDEN[0..16]);
        assert_eq!(&v[24..39], &GOLDEN[24..39]);
        assert_eq!(&v[55..GOLDEN.len()], &GOLDEN[55..GOLDEN.len()]);
    }
}
