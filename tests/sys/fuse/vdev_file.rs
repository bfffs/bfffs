macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name vdev_file;

    use arkfs::common::vdev::{SGVdev, Vdev};
    use arkfs::sys::vdev_file::*;
    use futures::Future;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::ops::Deref;
    use std::path::PathBuf;
    use bytes::{Bytes, BytesMut};
    use tempdir::TempDir;
    use tokio_core::reactor::Core;

    fixture!( vdev() -> (Core, VdevFile, PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let core = Core::new().unwrap();
            let tempdir = t!(TempDir::new("test_vdev_file"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pb = filename.to_path_buf();
            let vdev = VdevFile::open(filename, core.handle());
            (core, vdev, pb, tempdir)
        }
    });

    test lba2zone(vdev) {
        assert_eq!(vdev.val.1.lba2zone(0), 0);
        assert_eq!(vdev.val.1.lba2zone(1), 0);
        assert_eq!(vdev.val.1.lba2zone((1 << 19) - 1), 0);
        assert_eq!(vdev.val.1.lba2zone(1 << 19), 1);
    }

    test size(vdev) {
        assert_eq!(vdev.val.1.size(), 16_384);
    }

    test start_of_zone(vdev) {
        assert_eq!(vdev.val.1.start_of_zone(0), 0);
        assert_eq!(vdev.val.1.start_of_zone(1), 1 << 19);
    }

    test read_at() {
        // Create the initial file
        let dir = t!(TempDir::new("test_read_at"));
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = t!(fs::File::create(&path));
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let rbuf = BytesMut::from(vec![0u8; 4096]);
        let mut core = Core::new().unwrap();
        let vdev = VdevFile::open(path, core.handle());
        let fut = vdev.read_at(rbuf, 0);
        let result = core.run(fut).unwrap();
        assert_eq!(4096, result.value);
        assert_eq!(result.buf.deref().deref(), wbuf.as_slice());
    }

    test readv_at() {
        // Create the initial file
        let dir = t!(TempDir::new("test_readv_at"));
        let path = dir.path().join("vdev");
        let wbuf = vec![42u8; 4096];
        {
            let mut f = t!(fs::File::create(&path));
            t!(f.write_all(wbuf.as_slice()));
            t!(f.set_len(1 << 26));
        }

        // Run the test
        let rbuf0 = BytesMut::from(vec![0u8; 1024]);
        let rbuf1 = BytesMut::from(vec![0u8; 3072]);
        let rbufs = vec![rbuf0, rbuf1];
        let mut core = Core::new().unwrap();
        let vdev = VdevFile::open(path, core.handle());
        let fut = vdev.readv_at(rbufs, 0);
        let result = core.run(fut).unwrap();
        assert_eq!(4096, result.value);
        assert_eq!(result.buf[0].deref().deref(), &wbuf[0..1024]);
        assert_eq!(result.buf[1].deref().deref(), &wbuf[1024..4096]);
    }

    test write_at(vdev) {
        let wbuf = Bytes::from(vec![42u8; 4096]);
        let mut rbuf = vec![0u8; 4096];
        let fut = vdev.val.1.write_at(wbuf.clone(), 0);
        assert_eq!(4096, vdev.val.0.run(fut).unwrap().value);
        let mut f = t!(fs::File::open(vdev.val.2));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test write_at_lba(vdev) {
        let wbuf = Bytes::from(vec![42u8; 4096]);
        let mut rbuf = vec![0u8; 4096];
        let fut = vdev.val.1.write_at(wbuf.clone(), 1);
        assert_eq!(4096, vdev.val.0.run(fut).unwrap().value);
        let mut f = t!(fs::File::open(vdev.val.2));
        t!(f.seek(SeekFrom::Start(4096)));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test writev_at(vdev) {
        let wbuf0 = Bytes::from(vec![21u8; 1024]);
        let wbuf1 = Bytes::from(vec![42u8; 3072]);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 4096];
        let fut = vdev.val.1.writev_at(wbufs, 0);
        assert_eq!(4096, vdev.val.0.run(fut).unwrap().value);
        let mut f = t!(fs::File::open(vdev.val.2));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(&rbuf[0..1024], wbuf0.deref().deref());
        assert_eq!(&rbuf[1024..4096], wbuf1.deref().deref());
    }

    test read_after_write(vdev) {
        let vd = vdev.val.1;
        let wbuf = Bytes::from(vec![42u8; 4096]);
        let rbuf = BytesMut::from(vec![0u8; 4096]);
        let fut = vd.write_at(wbuf.clone(), 0)
            .and_then(|_| {
                vd.read_at(rbuf, 0)
            });
        let result = vdev.val.0.run(fut).unwrap();
        assert_eq!(4096, result.value);
        assert_eq!(result.buf, wbuf);
    }
}
