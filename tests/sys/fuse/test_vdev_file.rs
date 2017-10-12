macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name vdev_file_tests;

    use arkfs::common::vdev::Vdev;
    use arkfs::sys::vdev_file::*;
    use futures::Future;
    use std::fs;
    use std::io::Read;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::rc::Rc;
    use tempdir::TempDir;
    use tokio_core::reactor::Core;

    fixture!( vdev() -> (Core, VdevFile, PathBuf, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let core = Core::new().unwrap();
            let tempdir = t!(TempDir::new("test_open"));
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
        assert_eq!(vdev.val.1.size(), 16384);
    }

    test start_of_zone(vdev) {
        assert_eq!(vdev.val.1.start_of_zone(0), 0);
        assert_eq!(vdev.val.1.start_of_zone(1), 1 << 19);
    }

    test write_at(vdev) {
        let wbuf = Rc::new(vec![42u8; 4096].into_boxed_slice());
        let mut rbuf = vec![0u8; 4096];
        let fut = vdev.val.1.write_at(wbuf.clone(), 0);
        assert_eq!(4096, vdev.val.0.run(fut).unwrap());
        let mut f = t!(fs::File::open(vdev.val.2));
        t!(f.read_exact(&mut rbuf));
        assert_eq!(rbuf, wbuf.deref().deref());
    }

    test read_after_write(vdev) {
        let vd = vdev.val.1;
        let wbuf = Rc::new(vec![42u8; 4096].into_boxed_slice());
        let rbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let fut = vd.write_at(wbuf.clone(), 0)
            .and_then(|_| {
                vd.read_at(rbuf.clone(), 0)
            });
        assert_eq!(4096, vdev.val.0.run(fut).unwrap());
        assert_eq!(rbuf, wbuf);
    }
}
