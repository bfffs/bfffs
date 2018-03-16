// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    // These tests use real VdevBlock and VdevLeaf objects
    name vdev_raid;

    use arkfs::common::*;
    use arkfs::common::dva::*;
    use arkfs::common::prime_s::PrimeS;
    use arkfs::common::raid::Codec;
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

    fixture!( raid(n: i16, k: i16, f: i16, chunksize: LbaT) ->
              (VdevRaid, TempDir) {

        params {
            vec![(3, 3, 1, 2),      // Smallest possible configuration
                 (5, 4, 1, 2),      // Smallest PRIMES declustered configuration
                 (5, 5, 2, 2),      // Smallest double-parity configuration
                 (7, 4, 1, 2),      // Smallest non-ideal PRIME-S configuration
                 (7, 7, 3, 2),      // Smallest triple-parity configuration
                 (11, 9, 4, 2),     // Smallest quad-parity configuration
                 (41, 20, 4, 2),    // Jumbo configuration
                 (7, 7, 1, 16),     // Large chunk configuration
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
            let vdev_raid = VdevRaid::new(*self.chunksize, codec, locator,
                                      blockdevs.into_boxed_slice());
            (vdev_raid, tempdir)
        }
    });

    fn make_bufs(chunksize: LbaT, k: i16, f: i16, s: usize) ->
        (DivBufShared, DivBufShared) {

        let chunks = s * (k - f) as usize;
        let lbas = chunksize * chunks as LbaT;
        let bytes = BYTES_PER_LBA * lbas as usize;
        let mut wvec = vec![0u8; bytes];
        let mut rng = thread_rng();
        for x in &mut wvec {
            *x = rng.gen();
        }
        let dbsw = DivBufShared::from(wvec);
        let dbsr = DivBufShared::from(vec![0u8; bytes]);
        (dbsw, dbsr)
    }

    fn write_read(mut vr: VdevRaid, wbufs: Vec<IoVec>, rbufs: Vec<IoVecMut>) {
        current_thread::block_on_all(future::lazy(|| {
            future::join_all( {
                let mut lba = 0;
                // The ugly collect() call is necessary to appease the borrow
                // checker, because write_at must mutably borrow the VdevRaid
                let wfuts: Vec<_> = wbufs.into_iter()
                .map(|wb| {
                    let lbas = (wb.len() / BYTES_PER_LBA) as LbaT;
                    let fut = vr.write_at(wb, lba);
                    lba += lbas;
                    fut
                }).collect();
                wfuts }
            ).and_then(|_| {
                future::join_all({
                    // The ugly collect() call is necessary to appease the
                    // borrow checker, because lba is borrowed
                    let mut lba = 0;
                    rbufs.into_iter()
                    .map(|rb| {
                        let lbas = (rb.len() / BYTES_PER_LBA) as LbaT;
                        let fut = vr.read_at(rb, lba);
                        lba += lbas;
                        fut
                    }).collect::<Vec<_>>()
                })
            })
        })).expect("current_thread::block_on_all");
    }

    fn write_read_n_stripes(vr: VdevRaid, chunksize: LbaT, k: i16, f: i16,
                            s: usize) {
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        write_read(vr, vec![wbuf1], vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf0, dbsr.try().unwrap());
    }

    fn writev_read_n_stripes(mut vr: VdevRaid, chunksize: LbaT, k: i16, f: i16,
                             s: usize) {
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(wbuf.len() / 2);
        let sglist = vec![wbuf_l, wbuf_r];
        current_thread::block_on_all(future::lazy(|| {
            vr.writev_at_one(&sglist, 0)
                .then(|write_result| {
                    write_result.expect("writev_at_one");
                    vr.read_at(dbsr.try_mut().unwrap(), 0)
                })
        })).expect("read_at");
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    // Read a stripe in several pieces, from disk
    test read_parts_of_stripe(raid((7, 7, 1, 16))) {
        let m = raid.params.k - raid.params.f;
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let cs = *raid.params.chunksize as usize;
        let wbuf = dbsw.try().unwrap();
        {
            let mut rbuf0 = dbsr.try_mut().unwrap();
            // rbuf0 will get the first part of the first chunk
            let mut rbuf1 = rbuf0.split_off(cs / 4 * BYTES_PER_LBA);
            // rbuf1 will get the middle of the first chunk
            let mut rbuf2 = rbuf1.split_off(cs / 2 * BYTES_PER_LBA);
            // rbuf2 will get the end of the first chunk
            let mut rbuf3 = rbuf2.split_off(cs / 4 * BYTES_PER_LBA);
            // rbuf3 will get an entire chunk
            let mut rbuf4 = rbuf3.split_off(cs * BYTES_PER_LBA);
            // rbuf4 will get 2 chunks
            let mut rbuf5 = rbuf4.split_off(2 * cs * BYTES_PER_LBA);
            // rbuf5 will get one and a half chunks
            // rbuf6 will get the last half chunk
            let rbuf6 = rbuf5.split_off(3 * cs / 2 * BYTES_PER_LBA);
            write_read(raid.val.0, vec![wbuf.clone()],
                       vec![rbuf0, rbuf1, rbuf2, rbuf3, rbuf4, rbuf5, rbuf6]);
        }
        assert_eq!(&wbuf[..], &dbsr.try().unwrap()[..]);
    }


    test write_read_one_stripe(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 1);
    }

    test write_read_two_stripes(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 2);
    }

    // Write at least three rows to the layout.  Writing three rows guarantees
    // that some disks will have two data chunks separated by one parity chunk,
    // which tests the ability of VdevRaid::read_at to split a single disk's
    // data up into multiple VdevBlock::readv_at calls.
    test write_read_three_rows(raid) {
        let rows = 3;
        let stripes = div_roundup((rows * *raid.params.n) as usize,
                                   *raid.params.k as usize);
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, stripes);
    }

    test write_completes_a_partial_stripe(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read(raid.val.0, vec![wbuf_l, wbuf_r],
                   vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_a_bit_more(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        {
            // Truncate buffers to be < 2 stripes' length
            let mut dbwm = dbsw.try_mut().unwrap();
            let dbwm_len = dbwm.len();
            dbwm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
            let mut dbrm = dbsr.try_mut().unwrap();
            dbrm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
        }
        {
            let mut wbuf_l = dbsw.try().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read(raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test write_completes_a_partial_stripe_and_writes_another(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read(raid.val.0, vec![wbuf_l, wbuf_r],
                   vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_two_more(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 3);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read(raid.val.0, vec![wbuf_l, wbuf_r],
                   vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_two_more_with_leftovers(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 4);
        {
            // Truncate buffers to be < 4 stripes' length
            let mut dbwm = dbsw.try_mut().unwrap();
            let dbwm_len = dbwm.len();
            dbwm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
            let mut dbrm = dbsr.try_mut().unwrap();
            dbrm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
        }
        {
            let mut wbuf_l = dbsw.try().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read(raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test write_partial_at_start_of_stripe(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read(raid.val.0, vec![wbuf_short], vec![rbuf_short]);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try().unwrap()[0..BYTES_PER_LBA]);
    }

    // Test that write_at works when directed at the middle of the StripeBuffer.
    // This test requires a chunksize > 2
    test write_partial_at_middle_of_stripe(raid((7, 7, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap().slice_to(2 * BYTES_PER_LBA);
        let wbuf_begin = wbuf.slice_to(BYTES_PER_LBA);
        let wbuf_middle = wbuf.slice_from(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let _ = rbuf.split_off(2 * BYTES_PER_LBA);
            write_read(raid.val.0, vec![wbuf_begin, wbuf_middle], vec![rbuf]);
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    test write_two_stripes_with_leftovers(raid) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 3);
        {
            // Truncate buffers to be < 3 stripes' length
            let mut dbwm = dbsw.try_mut().unwrap();
            let dbwm_len = dbwm.len();
            dbwm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
            let mut dbrm = dbsr.try_mut().unwrap();
            dbrm.try_truncate(dbwm_len - BYTES_PER_LBA).expect("truncate");
        }
        {
            let wbuf = dbsw.try().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            write_read(raid.val.0, vec![wbuf], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test writev_read_one_stripe(raid) {
        writev_read_n_stripes(raid.val.0, *raid.params.chunksize,
                              *raid.params.k, *raid.params.f, 1);
    }
}
