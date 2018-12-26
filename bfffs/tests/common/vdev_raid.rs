// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    // These tests use real VdevBlock and VdevLeaf objects
    name vdev_raid;

    use bfffs::{common::*, common::vdev_raid::*, common::vdev::Vdev};
    use divbuf::DivBufShared;
    use futures::{Future, future};
    use galvanic_test::*;
    use rand::{Rng, thread_rng};
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        num::NonZeroU64
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread;

    fixture!( raid(n: i16, k: i16, f: i16, chunksize: LbaT) ->
              (VdevRaid, TempDir, Vec<String>) {

        params {
            vec![(1, 1, 0, 1),      // NullRaid configuration
                 (3, 3, 1, 2),      // Smallest possible PRIMES configuration
                 (5, 4, 1, 2),      // Smallest PRIMES declustered configuration
                 (5, 5, 2, 2),      // Smallest double-parity configuration
                 (7, 4, 1, 2),      // Smallest non-ideal PRIME-S configuration
                 (7, 7, 3, 2),      // Smallest triple-parity configuration
                 (11, 9, 4, 2),     // Smallest quad-parity configuration
            ].into_iter()
        }
        setup(&mut self) {

            let len = 1 << 30;  // 1 GB
            let tempdir = t!(TempDir::new("test_vdev_raid"));
            let paths = (0..*self.n).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                fname
            }).collect::<Vec<_>>();
            let cs = NonZeroU64::new(*self.chunksize);
            let vdev_raid = VdevRaid::create(cs,
                *self.n, *self.k, None, *self.f, &paths);
            current_thread::Runtime::new().unwrap().block_on(
                vdev_raid.open_zone(0)
            ).expect("open_zone");
            (vdev_raid, tempdir, paths)
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

    fn write_read(vr: &VdevRaid, wbufs: Vec<IoVec>, rbufs: Vec<IoVecMut>,
                  zone: ZoneT, start_lba: LbaT) {
        let mut write_lba = start_lba;
        let mut read_lba = start_lba;
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            future::join_all( {
                wbufs.into_iter()
                .map(|wb| {
                    let lbas = (wb.len() / BYTES_PER_LBA) as LbaT;
                    let fut = vr.write_at(wb, zone, write_lba);
                    write_lba += lbas;
                    fut
                })
            }).and_then(|_| {
                future::join_all({
                    rbufs.into_iter()
                    .map(|rb| {
                        let lbas = (rb.len() / BYTES_PER_LBA) as LbaT;
                        let fut = vr.read_at(rb, read_lba);
                        read_lba += lbas;
                        fut
                    })
                })
            })
        })).expect("current_thread::Runtime::block_on");
    }

    fn write_read0(vr: &VdevRaid, wbufs: Vec<IoVec>, rbufs: Vec<IoVecMut>) {
        let zl = vr.zone_limits(0);
        write_read(vr, wbufs, rbufs, 0, zl.0)
    }

    fn write_read_n_stripes(vr: &VdevRaid, chunksize: LbaT, k: i16, f: i16,
                            s: usize) {
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        write_read0(vr, vec![wbuf1], vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf0, dbsr.try_const().unwrap());
    }

    fn writev_read_n_stripes(vr: &VdevRaid, chunksize: LbaT, k: i16, f: i16,
                             s: usize) {
        let zl = vr.zone_limits(0);
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(wbuf.len() / 2);
        let sglist = vec![wbuf_l, wbuf_r];
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            vr.writev_at_one(&sglist, zl.0)
                .then(|write_result| {
                    write_result.expect("writev_at_one");
                    vr.read_at(dbsr.try_mut().unwrap(), zl.0)
                })
        })).expect("read_at");
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    // read_at should work when directed at the middle of the stripe buffer
    test read_partial_at_middle_of_stripe(raid((3, 3, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let mut wbuf = dbsw.try_const().unwrap().slice_to(2 * BYTES_PER_LBA);
        let _ = wbuf.split_off(2 * BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_begin = rbuf.split_to(BYTES_PER_LBA);
            let rbuf_middle = rbuf.split_to(BYTES_PER_LBA);
            write_read0(&raid.val.0, vec![wbuf.clone()],
                        vec![rbuf_begin, rbuf_middle]);
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    // Read a stripe in several pieces, from disk
    test read_parts_of_stripe(raid((7, 7, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let cs = *raid.params.chunksize as usize;
        let wbuf = dbsw.try_const().unwrap();
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
            write_read0(&raid.val.0, vec![wbuf.clone()],
                        vec![rbuf0, rbuf1, rbuf2, rbuf3, rbuf4, rbuf5, rbuf6]);
        }
        assert_eq!(&wbuf[..], &dbsr.try_const().unwrap()[..]);
    }

    // Read the end of one stripe and the beginning of another
    test read_partial_stripes(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        {
            let mut rbuf_m = dbsr.try_mut().unwrap();
            let rbuf_b = rbuf_m.split_to(BYTES_PER_LBA);
            let l = rbuf_m.len();
            let rbuf_e = rbuf_m.split_off(l - BYTES_PER_LBA);
            write_read0(&raid.val.0, vec![wbuf.clone()],
                        vec![rbuf_b, rbuf_m, rbuf_e]);
        }
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    #[should_panic]
    test read_past_end_of_stripe_buffer(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let rbuf = dbsr.try_mut().unwrap();
        write_read0(&raid.val.0, vec![wbuf_short], vec![rbuf]);
    }

    #[should_panic]
    test read_starts_past_end_of_stripe_buffer(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let mut rbuf = dbsr.try_mut().unwrap();
        let rbuf_r = rbuf.split_off(BYTES_PER_LBA);
        write_read0(&raid.val.0, vec![wbuf_short], vec![rbuf_r]);
    }

    test write_read_one_stripe(raid) {
        write_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 1);
    }

    // read_at_one/write_at_one with a large configuration
    test write_read_one_stripe_jumbo(raid((41, 19, 3, 2))) {
        write_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 1);
    }

    test write_read_two_stripes(raid) {
        write_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 2);
    }

    // read_at_multi/write_at_multi with a large configuration
    test write_read_two_stripes_jumbo(raid((41, 19, 3, 2))) {
        write_read_n_stripes(&raid.val.0, *raid.params.chunksize,
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
        write_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, stripes);
    }

    test write_completes_a_partial_stripe(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(&raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_a_bit_more(raid((3, 3, 1, 2))) {
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
            let mut wbuf_l = dbsw.try_const().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(&raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    test write_completes_a_partial_stripe_and_writes_another(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(&raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_two_more(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 3);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(&raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_two_more_with_leftovers(raid((3, 3, 1, 2))) {
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
            let mut wbuf_l = dbsw.try_const().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(&raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    test write_partial_at_start_of_stripe(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read0(&raid.val.0, vec![wbuf_short], vec![rbuf_short]);
            // After write returns, the DivBufShared should no longer be needed.
            drop(dbsw);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA]);
    }

    // Write less than an LBA at the start of a stripe
    #[allow(clippy::identity_op)]
    test write_tiny_at_start_of_stripe(raid((1, 1, 0, 1))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA * 3 / 4);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read0(&raid.val.0, vec![wbuf_short], vec![rbuf_short]);
            // After write returns, the DivBufShared should no longer be needed.
            drop(dbsw);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA * 3 / 4],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA * 3 / 4]);
        // The remainder of the LBA should've been zero-filled
        let zbuf = vec![0u8; BYTES_PER_LBA * 1 / 4];
        assert_eq!(&zbuf[..],
                   &dbsr.try_const().unwrap()[BYTES_PER_LBA * 3 / 4..]);
    }

    // Write a whole stripe plus a fraction of an LBA more
    test write_stripe_and_a_bit_more(raid((1, 1, 0, 1))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        let wcut = wbuf.len() / 2 + 1024;
        let rcut = wbuf.len() / 2 + BYTES_PER_LBA;
        let wbuf_short = wbuf.slice_to(wcut);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(rcut);
            write_read0(&raid.val.0, vec![wbuf_short], vec![rbuf_short]);
            // After write returns, the DivBufShared should no longer be needed.
            drop(dbsw);
        }
        assert_eq!(&wbuf[0..wcut],
                   &dbsr.try_const().unwrap()[0..wcut]);
        // The remainder of the LBA should've been zero-filled
        let zbuf = vec![0u8; rcut - wcut];
        assert_eq!(&zbuf[..],
                   &dbsr.try_const().unwrap()[wcut..]);
    }

    // Test that write_at works when directed at the middle of the StripeBuffer.
    // This test requires a chunksize > 2
    test write_partial_at_middle_of_stripe(raid((3, 3, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap().slice_to(2 * BYTES_PER_LBA);
        let wbuf_begin = wbuf.slice_to(BYTES_PER_LBA);
        let wbuf_middle = wbuf.slice_from(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let _ = rbuf.split_off(2 * BYTES_PER_LBA);
            write_read0(&raid.val.0, vec![wbuf_begin, wbuf_middle], vec![rbuf]);
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    test write_two_stripes_with_leftovers(raid((3, 3, 1, 2))) {
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
            let wbuf = dbsw.try_const().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(&raid.val.0, vec![wbuf], vec![rbuf]);
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    test writev_read_one_stripe(raid) {
        writev_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                              *raid.params.k, *raid.params.f, 1);
    }

    // Erasing an open zone should fail
    #[should_panic]
    test zone_erase_open(raid((3, 3, 1, 2))) {
        let zone = 1;
        current_thread::Runtime::new().unwrap().block_on( future::lazy(|| {
            raid.val.0.open_zone(zone)
            .and_then(|_| raid.val.0.erase_zone(0))
        })).expect("zone_erase_open");
    }

    test zone_read_closed(raid((3, 3, 1, 2))) {
        let zone = 0;
        let zl = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            raid.val.0.write_at(wbuf0, zone, zl.0)
                .and_then(|_| {
                    raid.val.0.finish_zone(zone)
                }).and_then(|_| {
                    raid.val.0.read_at(rbuf, zl.0)
                })
        })).expect("current_thread::Runtime::block_on");
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    // Close a zone with an incomplete StripeBuffer, then read back from it
    test zone_read_closed_partial(raid((3, 3, 1, 2))) {
        let zone = 0;
        let zl = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
                raid.val.0.write_at(wbuf_short, zone, zl.0)
                    .and_then(|_| {
                        raid.val.0.finish_zone(zone)
                    }).and_then(|_| {
                        raid.val.0.read_at(rbuf_short, zl.0)
                    })
            })).expect("current_thread::Runtime::block_on");
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA]);
    }

    #[should_panic]
    // Writing to an explicitly closed a zone fails
    test zone_write_explicitly_closed(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::Runtime::new().unwrap().block_on(
            raid.val.0.open_zone(zone)
            .and_then(|_| raid.val.0.finish_zone(zone))
        ).expect("open and finish");
        write_read(&raid.val.0, vec![wbuf0], vec![rbuf], zone, start);
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    #[should_panic]
    // Writing to a closed zone should fail
    test zone_write_implicitly_closed(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let dbsw = DivBufShared::from(vec![0;4096]);
        let wbuf = dbsw.try_const().unwrap();
        raid.val.0.write_at(wbuf, zone, start);
    }

    // Opening a closed zone should allow writing
    test zone_write_open(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::Runtime::new().unwrap().block_on(
            raid.val.0.open_zone(zone)
        ).expect("open_zone");
        write_read(&raid.val.0, vec![wbuf0], vec![rbuf], zone, start);
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    // Two zones can be open simultaneously
    test zone_write_two_zones(raid((3, 3, 1, 2))) {
        let vdev_raid = raid.val.0;
        for zone in 1..3 {
            let (start, _) = vdev_raid.zone_limits(zone);
            let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                         *raid.params.f, 1);
            let wbuf0 = dbsw.try_const().unwrap();
            let wbuf1 = dbsw.try_const().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            current_thread::Runtime::new().unwrap().block_on(
                vdev_raid.open_zone(zone)
            ).expect("open_zone");
            write_read(&vdev_raid, vec![wbuf0], vec![rbuf], zone, start);
            assert_eq!(wbuf1, dbsr.try_const().unwrap());
        }
    }
}

test_suite! {
    name persistence;

    use bfffs::common::{label::*, vdev_block::*, vdev_raid::*, vdev::Vdev};
    use bfffs::sys::vdev_file::*;
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread;

    const GOLDEN_VDEV_RAID_LABEL: [u8; 120] = [
        // Past the VdevFile::Label, we have a VdevRaid::Label
        // First comes the VdevFile's UUID.
        0x93, 0x11, 0x4c, 0xef, 0xdb, 0x19, 0x45, 0x6f,
        0x96, 0xef, 0xb8, 0x82, 0xc2, 0x04, 0xe6, 0x92,
        // Then the chunksize in 64 bits
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // disks_per_stripe in 16 bits
        0x03, 0x00,
        // redundancy level in 16 bits
                    0x01, 0x00,
        // LayoutAlgorithm discriminant in 32 bits
                                0x01, 0x00, 0x00, 0x00,
        // Vector of children's UUIDs.  A 64-bit count of children, then each
        // UUID is 64-bits long
        0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xb7, 0x43, 0x24, 0xaa, 0xa5, 0x8c, 0x4c, 0xef,
        0xa8, 0xa8, 0x2c, 0xdd, 0x60, 0x1f, 0x92, 0x79,
        0x9e, 0x34, 0x76, 0x01, 0xef, 0x0e, 0x4e, 0xd0,
        0x97, 0x1e, 0xa1, 0xd3, 0x7d, 0xb5, 0x04, 0x33,
        0x8a, 0xdb, 0x69, 0x16, 0x49, 0x89, 0x4d, 0x0c,
        0xb3, 0xcf, 0xbc, 0x3d, 0x82, 0xbf, 0x54, 0x4a,
        0xd0, 0x02, 0x55, 0xc5, 0x7c, 0x25, 0x4f, 0x8e,
        0x9b, 0x03, 0xef, 0x9d, 0xd3, 0x02, 0xfd, 0xe6,
        0x16, 0xc0, 0xfc, 0x41, 0xeb, 0xd6, 0x4e, 0x71,
        0x97, 0x3c, 0x0c, 0x82, 0x30, 0x26, 0x23, 0x06
    ];

    fixture!( raid() -> (VdevRaid, TempDir, Vec<String>) {
        setup(&mut self) {
            let num_disks = 5;
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_vdev_raid_persistence"));
            let paths = (0..num_disks).map(|i| {
                let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                let file = t!(fs::File::create(&fname));
                t!(file.set_len(len));
                fname
            }).collect::<Vec<_>>();
            let cs = NonZeroU64::new(2);
            let vdev_raid = VdevRaid::create(cs, num_disks, 3, None, 1, &paths);
            (vdev_raid, tempdir, paths)
        }
    });

    // Testing VdevRaid::open with golden labels is too hard, because we
    // need to store separate golden labels for each VdevLeaf.  Instead, we'll
    // just check that we can open-after-write
    test open(raid()) {
        let (old_raid, _tempdir, paths) = raid.val;
        let uuid = old_raid.uuid();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(move || {
            let label_writer = LabelWriter::new(0);
            old_raid.write_label(label_writer).and_then(move |_| {
                future::join_all(paths.into_iter().map(|path| {
                    VdevFile::open(path).map(|(leaf, reader)| {
                        (VdevBlock::new(leaf), reader)
                    })
                }))
            }).map(move |combined| {
                let (vdev_raid, _) = VdevRaid::open(Some(uuid), combined);
                assert_eq!(uuid, vdev_raid.uuid());
            })
        })).unwrap();
    }

    test write_label(raid()) {
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let label_writer = LabelWriter::new(0);
            raid.val.0.write_label(label_writer)
        })).unwrap();
        for path in raid.val.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(72)).unwrap();   // Skip the VdevLeaf label
            f.read_exact(&mut v).unwrap();
            // Uncomment this block to save the binary label for inspection
            /* {
                use std::fs::File;
                use std::io::Write;
                let mut df = File::create("/tmp/label.bin").unwrap();
                df.write_all(&v[..]).unwrap();
                println!("UUID is {}", raid.val.0.uuid());
            } */
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[16..40], &GOLDEN_VDEV_RAID_LABEL[16..40]);
            // Rest of the buffer should be zero-filled
            assert!(v[120..].iter().all(|&x| x == 0));
        }
    }
}
