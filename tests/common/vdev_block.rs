// vim: tw=80
use galvanic_test::*;

test_suite! {
    // These tests use a real VdevLeaf object
    name vdev_block;

    use bfffs::common::{vdev::*, vdev_block::*};
    use divbuf::DivBufShared;
    use futures::future;
    use std::fs;
    use tempdir::TempDir;
    use tokio::runtime::current_thread;

    fixture!( vdev() -> (VdevBlock, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_block"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let vdev = VdevBlock::create(filename, None).unwrap();
            (vdev, tempdir)
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

    #[should_panic]
    test check_block_granularity_under(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4095]);
        let wbuf = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 1)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_over(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4097]);
        let wbuf = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 1)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_over_multiple_sectors(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 16_385]);
        let wbuf = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.write_at(wbuf, 1)
        })).unwrap();
    }

    #[should_panic]
    test check_block_granularity_writev(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4097]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(1024);
        let wbuf1 = wbuf.slice_from(1024);
        let wbufs = vec![wbuf0, wbuf1];
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vdev.val.0.writev_at(wbufs, 1)
        })).unwrap();
    }

    #[should_panic]
    test check_iovec_bounds_over(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let size = vdev.val.0.size();
            vdev.val.0.write_at(wbuf, size)
        })).unwrap();
    }

    #[should_panic]
    test check_iovec_bounds_spans(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let wbuf = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let size = vdev.val.0.size() - 1;
            vdev.val.0.write_at(wbuf, size)
        })).unwrap();
    }

    #[should_panic]
    test check_sglist_bounds_over(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(1024);
        let wbuf1 = wbuf.slice_from(1024);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let size = vdev.val.0.size();
            vdev.val.0.writev_at(wbufs, size)
        })).unwrap();
    }

    #[should_panic]
    test check_sglist_bounds_spans(vdev) {
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(5120);
        let wbuf1 = wbuf.slice_from(5120);
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let size = vdev.val.0.size() - 1;
            vdev.val.0.writev_at(wbufs, size)
        })).unwrap();
    }
}
