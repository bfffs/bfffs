macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    // These tests use a real VdevLeaf object
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

test_suite! {
    // These tests use a mock VdevLeaf object
    name mock_vdev_block_tests;

    use arkfs::common::*;
    use arkfs::common::vdev::{SGVdev, Vdev, VdevFut};
    use arkfs::common::vdev_block::*;
    use arkfs::common::vdev_leaf::*;
    use futures::future;
    use std::cell::RefCell;
    use std::io::Error;
    use std::mem;
    use std::rc::Rc;
    use std::collections::VecDeque;
    use tokio_core::reactor::{Core, Handle};

    // Roll our own Mock object.  We can't use:
    // mockers: https://github.com/kriomant/mockers/issues/20
    // mock_derive: https://github.com/DavidDeSimone/mock_derive/issues/3
    // mockme because it can only be used by a single test
    // galvanic_mock because it doesn't support specifying a fixed order of
    // expectations
    pub struct MockVdevLeaf {
        handle: Handle,
        record: RefCell<VecDeque<(String, LbaT)>>
    }

    impl MockVdevLeaf {
        fn new(handle: Handle) -> Self {
            MockVdevLeaf {
                handle: handle.clone(),
                record: RefCell::new(VecDeque::<(String, LbaT)>::new())
            }
        }

        pub fn expect(&self, s: &str, lba: LbaT) -> &Self {
            let block_op = self.record.borrow_mut().pop_front().unwrap();
            assert_eq!(s, block_op.0);
            assert_eq!(lba, block_op.1);
            &self
        }

        fn push(&self, s: &str, lba: LbaT) {
            self.record.borrow_mut().push_back((String::from(s), lba));
        }
    }

    impl Vdev for MockVdevLeaf {
        fn handle(&self) -> Handle {
            self.handle.clone()
        }
        fn lba2zone(&self, _: LbaT) -> ZoneT {
            0
        }
        fn size(&self) -> LbaT {
            16384
        }
        fn start_of_zone(&self, _: ZoneT) -> LbaT {
            0
        }
        fn read_at(&self, _: IoVec, lba: LbaT) -> Box<VdevFut> {
            self.push("read_at", lba);
            Box::new(future::ok::<isize, Error>((0)))
        }
        fn write_at(&self, _: IoVec, lba: LbaT) -> Box<VdevFut> {
            self.push("write_at", lba);
            Box::new(future::ok::<isize, Error>((0)))
        }
    }

    impl SGVdev for MockVdevLeaf {
        fn readv_at(&self, _: SGList, lba: LbaT) -> Box<VdevFut> {
            self.push("readv_at", lba);
            Box::new(future::ok::<isize, Error>((0)))
        }
        fn writev_at(&self, _: SGList, lba: LbaT) -> Box<VdevFut> {
            self.push("writev_at", lba);
            Box::new(future::ok::<isize, Error>((0)))
        }
    }

    impl VdevLeaf for MockVdevLeaf {
    }

    // Reads should be passed straight through, even if they're out-of-order
    test read_at() {
        let rbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let leaf = Box::new(MockVdevLeaf::new(core.handle()));
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.read_at(rbuf.clone(), 1);
        let second = vdev.read_at(rbuf.clone(), 0);
        let futs = future::Future::join(first, second);
        core.run(futs).unwrap();
        let final_leaf : &Box<MockVdevLeaf> = unsafe {
            mem::transmute(&vdev.leaf)
        };
        final_leaf.expect("read_at", 1)
            .expect("read_at", 0);
    }

    // Writes should be reordered if out-of-order
    test write_at() {
        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let leaf = Box::new(MockVdevLeaf::new(core.handle()));
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.write_at(wbuf.clone(), 1);
        let second = vdev.write_at(wbuf.clone(), 0);
        let futs = future::Future::join(second, first);
        core.run(futs).unwrap();
        let final_leaf : &Box<MockVdevLeaf> = unsafe {
            mem::transmute(&vdev.leaf)
        };
        final_leaf.expect("write_at", 0)
            .expect("write_at", 1);
    }
}
