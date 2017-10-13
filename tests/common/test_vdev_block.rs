macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name vdev_block_tests;

    use arkfs::common::vdev::Vdev;
    use arkfs::common::vdev_block::*;
    use arkfs::sys::vdev_file::*;
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

    test size(vdev) {
        assert_eq!(vdev.val.1.size(), 16384);
    }
}
