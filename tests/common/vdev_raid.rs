macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    // These tests use real VdevBlock and VdevLeaf objects
    name vdev_raid;

    use arkfs::common::LbaT;
    use arkfs::common::prime_s::PrimeS;
    use arkfs::common::raid::Codec;
    use arkfs::common::vdev::Vdev;
    use arkfs::common::vdev_block::*;
    use arkfs::common::vdev_raid::*;
    use arkfs::sys::vdev_file::*;
    use divbuf::DivBufShared;
    use futures::{Future, future};
    use rand::{Rng, thread_rng};
    use std::fs;
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    fixture!( raid() -> (VdevRaid, TempDir) {
        setup(&mut self) {
            let n = 5;
            let k = 4;
            let f = 1;
            const CHUNKSIZE : LbaT = 2;

            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_raid"));
            let blockdevs : Vec<VdevBlock> = (0..5).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                let leaf = Box::new(VdevFile::open(fname, Handle::current()));
                VdevBlock::open(leaf, Handle::current())
            }).collect();
            let codec = Codec::new(k, f);
            let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
            let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
            (vdev_raid, tempdir)
        }
    });

    test write_read_one_stripe(raid) {
        let mut wvec = vec![0u8; 24576];
        let mut rng = thread_rng();
        for x in wvec.iter_mut() {
            *x = rng.gen();
        }
        let dbsw = DivBufShared::from(wvec);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        let dbsr = DivBufShared::from(vec![0u8; 24576]);
        let r = current_thread::block_on_all(future::lazy(|| {
            raid.val.0.write_at(wbuf1, 0)
                .then(|write_result| {
                    write_result.expect("write_at");
                    raid.val.0.read_at(dbsr.try_mut().unwrap(), 0)
                })
        })).expect("read_at");
        assert_eq!(wbuf0, r.buf.unwrap());
    }
}
