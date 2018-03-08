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
    use arkfs::common::dva::*;
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

    const CHUNKSIZE : LbaT = 2;

    fixture!( raid(n: i16, k: i16, f: i16) -> (VdevRaid, TempDir) {
        params {
            vec![(3, 3, 1),     // Smallest possible configuration
                 (5, 4, 1),     // Smallest PRIME-S declustered configuration
                 (5, 5, 2),     // Smallest double-parity configuration
                 (7, 4, 1),     // Smallest non-ideal PRIME-S configuration
                 (7, 7, 3),     // Smallest triple-parity configuration
                 (11, 9, 4),    // Smallest quad-parity configuration
                 (41, 20, 4),   // Jumbo configuration
            ].into_iter()
        }
        setup(&mut self) {

            let len = 1 << 26;  // 64MB
            let tempdir = t!(TempDir::new("test_vdev_raid"));
            let blockdevs : Vec<VdevBlock> = (0..*self.n).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                let leaf = Box::new(VdevFile::open(fname, Handle::current()));
                VdevBlock::open(leaf, Handle::current())
            }).collect();
            let codec = Codec::new(*self.k as u32, *self.f as u32);
            let locator = Box::new(PrimeS::new(*self.n, *self.k, *self.f));
            let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
            (vdev_raid, tempdir)
        }
    });

    fn make_bufs(k: i16, f: i16, s: usize) -> (DivBufShared, DivBufShared) {
        let chunks = s * (k - f) as usize;
        let lbas = CHUNKSIZE * chunks as LbaT;
        let bytes = ((BYTES_PER_LBA as u64) * lbas) as usize;
        let mut wvec = vec![0u8; bytes];
        let mut rng = thread_rng();
        for x in wvec.iter_mut() {
            *x = rng.gen();
        }
        let dbsw = DivBufShared::from(wvec);
        let dbsr = DivBufShared::from(vec![0u8; bytes]);
        (dbsw, dbsr)
    }

    fn write_read_n_stripes(vdev_raid: VdevRaid, k: i16, f: i16, s: usize) {
        let (dbsw, dbsr) = make_bufs(k, f, s);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        let r = current_thread::block_on_all(future::lazy(|| {
            vdev_raid.write_at(wbuf1, 0)
                .then(|write_result| {
                    write_result.expect("write_at");
                    vdev_raid.read_at(dbsr.try_mut().unwrap(), 0)
                })
        })).expect("read_at");
        assert_eq!(dbsr.len() as isize, r.value);
        assert_eq!(wbuf0, dbsr.try().unwrap());
    }

    test write_read_one_stripe(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.k, *raid.params.f, 1);
    }

    test write_read_two_stripes(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.k, *raid.params.f, 2);
    }

    test write_read_ten_stripes(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.k, *raid.params.f, 10);
    }
}
