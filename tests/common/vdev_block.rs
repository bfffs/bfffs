macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    // These tests use a real VdevLeaf object
    name vdev_block;

    use arkfs::common::vdev::{SGVdev, Vdev};
    use arkfs::common::vdev_block::*;
    use arkfs::sys::vdev_file::*;
    use bytes::Bytes;
    use futures::future;
    use std::fs;
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    fixture!( vdev() -> (VdevBlock, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_block"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let leaf = Box::new(VdevFile::open(filename, Handle::current()));
            let vdev = VdevBlock::open(leaf, Handle::current());
            (vdev, tempdir)
        }
    });

    test lba2zone(vdev) {
        assert_eq!(vdev.val.0.lba2zone(0), 0);
        assert_eq!(vdev.val.0.lba2zone(1), 0);
        assert_eq!(vdev.val.0.lba2zone((1 << 19) - 1), 0);
        assert_eq!(vdev.val.0.lba2zone(1 << 19), 1);
    }

    test size(vdev) {
        assert_eq!(vdev.val.0.size(), 16_384);
    }

    test start_of_zone(vdev) {
        assert_eq!(vdev.val.0.start_of_zone(0), 0);
        assert_eq!(vdev.val.0.start_of_zone(1), 1 << 19);
    }

    #[should_panic]
    test check_block_granularity_under(vdev) {
        let wbuf = Bytes::from(vec![42u8; 4095]);
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 0)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_over(vdev) {
        let wbuf = Bytes::from(vec![42u8; 4097]);
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 0)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_over_multiple_sectors(vdev) {
        let wbuf = Bytes::from(vec![42u8; 16_385]);
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 0)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_writev(vdev) {
        let wbuf0 = Bytes::from(vec![21u8; 1024]);
        let wbuf1 = Bytes::from(vec![42u8; 3073]);
        let wbufs = vec![wbuf0, wbuf1];
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, 0)
        })).unwrap();
    }

    #[should_panic]
    test check_iovec_bounds_over(vdev) {
        let wbuf = Bytes::from(vec![42u8; 4096]);
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf, vdev.val.0.size())
        })).unwrap();
    }

    #[should_panic]
    test check_iovec_bounds_spans(vdev) {
        let wbuf = Bytes::from(vec![42u8; 8192]);
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.write_at(wbuf, vdev.val.0.size() - 1)
        })).unwrap();
    }

    #[should_panic]
    test check_sglist_bounds_over(vdev) {
        let wbuf0 = Bytes::from(vec![21u8; 1024]);
        let wbuf1 = Bytes::from(vec![42u8; 3072]);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, vdev.val.0.size())
        })).unwrap();
    }

    #[should_panic]
    test check_sglist_bounds_spans(vdev) {
        let wbuf0 = Bytes::from(vec![21u8; 5120]);
        let wbuf1 = Bytes::from(vec![42u8; 7168]);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        current_thread::block_on_all(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, vdev.val.0.size() - 2)
        })).unwrap();
    }
}
