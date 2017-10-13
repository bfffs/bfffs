macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name vdev_block_tests;

    use arkfs::common::vdev::{SGVdev, Vdev};
    use arkfs::common::vdev_block::*;
    use arkfs::sys::vdev_file::*;
    use std::rc::Rc;
    use std::fs;
    use tempdir::TempDir;
    use tokio_core::reactor::Core;

    fixture!( vdev() -> (Core, VdevBlock, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let core = Core::new().unwrap();
            let tempdir = t!(TempDir::new("test_vdev_block"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let leaf = Box::new(VdevFile::open(filename, core.handle()));
            let vdev = VdevBlock::open(leaf, core.handle());
            (core, vdev, tempdir)
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

    #[should_panic]
    test check_iovec_bounds_over(vdev) {
        let wbuf = Rc::new(vec![42u8; 4096].into_boxed_slice());
        vdev.val.1.write_at(wbuf, vdev.val.1.size());
    }

    #[should_panic]
    test check_iovec_bounds_spans(vdev) {
        let wbuf = Rc::new(vec![42u8; 8192].into_boxed_slice());
        vdev.val.1.write_at(wbuf, vdev.val.1.size() - 1);
    }

    #[should_panic]
    test check_sglist_bounds_over(vdev) {
        let wbuf0 = Rc::new(vec![21u8; 1024].into_boxed_slice());
        let wbuf1 = Rc::new(vec![42u8; 3072].into_boxed_slice());
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()].into_boxed_slice();
        vdev.val.1.writev_at(wbufs, vdev.val.1.size());
    }

    #[should_panic]
    test check_sglist_bounds_spans(vdev) {
        let wbuf0 = Rc::new(vec![21u8; 5120].into_boxed_slice());
        let wbuf1 = Rc::new(vec![42u8; 7168].into_boxed_slice());
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()].into_boxed_slice();
        vdev.val.1.writev_at(wbufs, vdev.val.1.size() - 2);
    }
}
