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
    test check_block_granularity_under(vdev) {
        let wbuf = Rc::new(vec![42u8; 4095].into_boxed_slice());
        vdev.val.1.write_at(wbuf, 0);
    }

    #[should_panic]
    test check_block_granularity_over(vdev) {
        let wbuf = Rc::new(vec![42u8; 4097].into_boxed_slice());
        vdev.val.1.write_at(wbuf, 0);
    }

    #[should_panic]
    test check_block_granularity_over_multiple_sectors(vdev) {
        let wbuf = Rc::new(vec![42u8; 16385].into_boxed_slice());
        vdev.val.1.write_at(wbuf, 0);
    }

    #[should_panic]
    test check_block_granularity_writev(vdev) {
        let wbuf0 = Rc::new(vec![21u8; 1024].into_boxed_slice());
        let wbuf1 = Rc::new(vec![42u8; 3073].into_boxed_slice());
        let wbufs = vec![wbuf0, wbuf1].into_boxed_slice();
        vdev.val.1.writev_at(wbufs, 0);
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

#[cfg(feature = "mocks")]
test_suite! {
    // These tests use a mock VdevLeaf object
    name mock_vdev_block;

    use arkfs::common::*;
    use arkfs::common::vdev;
    use arkfs::common::vdev::{SGVdev, Vdev, VdevFut};
    use arkfs::common::vdev_block::*;
    use futures::future;
    use mockers::{Scenario, Sequence};
    use mockers::matchers::ANY;
    use std::io::Error;
    use std::rc::Rc;
    use tokio_core::reactor::{Core, Handle};

    mock!{
        MockVdevLeaf2,
        vdev,
        trait Vdev {
            fn handle(&self) -> Handle;
            fn lba2zone(&self, lba: LbaT) -> ZoneT;
            fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
            fn size(&self) -> LbaT;
            fn start_of_zone(&self, zone: ZoneT) -> LbaT;
            fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
        },
        vdev,
        trait SGVdev  {
            fn readv_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
            fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
        },
        vdev_leaf,
        trait VdevLeaf  {
        }
    }

    fixture!( mocks() -> (Scenario, Box<MockVdevLeaf2>) {
            setup(&mut self) {
            let scenario = Scenario::new();
            let leaf = Box::new(scenario.create_mock::<MockVdevLeaf2>());
            scenario.expect(leaf.size_call()
                                .and_return(16384));
            scenario.expect(leaf.lba2zone_call(ANY)
                                .and_return_clone(0)
                                .times(..));
            scenario.expect(leaf.start_of_zone_call(0)
                                .and_return_clone(0)
                                .times(..));
            (scenario, leaf)
        }
    });

    // Reads should be passed straight through, even if they're out-of-order
    test read_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        seq.expect(leaf.read_at_call(ANY, 1)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        seq.expect(leaf.read_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        scenario.expect(seq);

        let rbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.read_at(rbuf.clone(), 1);
        let second = vdev.read_at(rbuf.clone(), 0);
        let futs = future::Future::join(first, second);
        core.run(futs).unwrap();
    }

    // Basic writing at the WP works
    test write_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.write_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let fut = vdev.write_at(wbuf.clone(), 0);
        core.run(fut).unwrap();
    }

    // Basic vectored writing at the WP works
    test writev_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf0 = Rc::new(vec![0u8; 1024].into_boxed_slice());
        let wbuf1 = Rc::new(vec![0u8; 3072].into_boxed_slice());
        let wbufs = vec![wbuf0, wbuf1].into_boxed_slice();
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let fut = vdev.writev_at(wbufs, 0);
        core.run(fut).unwrap();
    }

    // Writes should be reordered and combined if out-of-order
    test write_at_combining(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        // Issue writes out-of-order
        let first = vdev.write_at(wbuf.clone(), 1);
        let second = vdev.write_at(wbuf.clone(), 0);
        let futs = future::Future::join(first, second);
        core.run(futs).unwrap();
    }

    // Writes should be issued ASAP, even if they could be combined later
    test write_at_issue_asap(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        scenario.expect(leaf.lba2zone_call(2).and_return(0));
        seq.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        seq.expect(leaf.write_at_call(ANY, 2)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        scenario.expect(seq);

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.write_at(wbuf.clone(), 1);
        let second = vdev.write_at(wbuf.clone(), 0);
        let third = vdev.write_at(wbuf.clone(), 2);
        let futs = future::Future::join3(first, second, third);
        core.run(futs).unwrap();
    }
}
