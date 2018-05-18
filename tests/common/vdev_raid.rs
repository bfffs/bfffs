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

    use arkfs::{common::*, common::vdev_raid::*, common::vdev::Vdev};
    use divbuf::DivBufShared;
    use futures::{Future, future};
    use rand::{Rng, thread_rng};
    use std::fs;
    use tempdir::TempDir;
    use tokio::{executor::current_thread, reactor::Handle};

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
            let mut vdev_raid = VdevRaid::create(*self.chunksize,
                *self.n, *self.k, *self.f, &paths, Handle::default());
            current_thread::block_on_all(
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
        current_thread::block_on_all(future::lazy(|| {
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
        })).expect("current_thread::block_on_all");
    }

    fn write_read0(vr: VdevRaid, wbufs: Vec<IoVec>, rbufs: Vec<IoVecMut>) {
        let zl = vr.zone_limits(0);
        write_read(&vr, wbufs, rbufs, 0, zl.0)
    }

    fn write_read_n_stripes(vr: VdevRaid, chunksize: LbaT, k: i16, f: i16,
                            s: usize) {
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        write_read0(vr, vec![wbuf1], vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf0, dbsr.try().unwrap());
    }

    fn writev_read_n_stripes(vr: &VdevRaid, chunksize: LbaT, k: i16, f: i16,
                             s: usize) {
        let zl = vr.zone_limits(0);
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(wbuf.len() / 2);
        let sglist = vec![wbuf_l, wbuf_r];
        current_thread::block_on_all(future::lazy(|| {
            vr.writev_at_one(&sglist, zl.0)
                .then(|write_result| {
                    write_result.expect("writev_at_one");
                    vr.read_at(dbsr.try_mut().unwrap(), zl.0)
                })
        })).expect("read_at");
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    // read_at should work when directed at the middle of the stripe buffer
    test read_partial_at_middle_of_stripe(raid((3, 3, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let mut wbuf = dbsw.try().unwrap().slice_to(2 * BYTES_PER_LBA);
        let _ = wbuf.split_off(2 * BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_begin = rbuf.split_to(BYTES_PER_LBA);
            let rbuf_middle = rbuf.split_to(BYTES_PER_LBA);
            write_read0(raid.val.0, vec![wbuf.clone()],
                        vec![rbuf_begin, rbuf_middle]);
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    // Read a stripe in several pieces, from disk
    test read_parts_of_stripe(raid((7, 7, 1, 16))) {
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
            write_read0(raid.val.0, vec![wbuf.clone()],
                        vec![rbuf0, rbuf1, rbuf2, rbuf3, rbuf4, rbuf5, rbuf6]);
        }
        assert_eq!(&wbuf[..], &dbsr.try().unwrap()[..]);
    }

    // Read the end of one stripe and the beginning of another
    test read_partial_stripes(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try().unwrap();
        {
            let mut rbuf_m = dbsr.try_mut().unwrap();
            let rbuf_b = rbuf_m.split_to(BYTES_PER_LBA);
            let l = rbuf_m.len();
            let rbuf_e = rbuf_m.split_off(l - BYTES_PER_LBA);
            write_read0(raid.val.0, vec![wbuf.clone()],
                        vec![rbuf_b, rbuf_m, rbuf_e]);
        }
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    #[should_panic]
    test read_past_end_of_stripe_buffer(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let rbuf = dbsr.try_mut().unwrap();
        write_read0(raid.val.0, vec![wbuf_short], vec![rbuf]);
    }

    #[should_panic]
    test read_starts_past_end_of_stripe_buffer(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let mut rbuf = dbsr.try_mut().unwrap();
        let rbuf_r = rbuf.split_off(BYTES_PER_LBA);
        write_read0(raid.val.0, vec![wbuf_short], vec![rbuf_r]);
    }

    test write_read_one_stripe(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 1);
    }

    // read_at_one/write_at_one with a large configuration
    test write_read_one_stripe_jumbo(raid((41, 19, 3, 2))) {
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 1);
    }

    test write_read_two_stripes(raid) {
        write_read_n_stripes(raid.val.0, *raid.params.chunksize,
                             *raid.params.k, *raid.params.f, 2);
    }

    // read_at_multi/write_at_multi with a large configuration
    test write_read_two_stripes_jumbo(raid((41, 19, 3, 2))) {
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

    test write_completes_a_partial_stripe(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
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
            let mut wbuf_l = dbsw.try().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test write_completes_a_partial_stripe_and_writes_another(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 2);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
    }

    test write_completes_a_partial_stripe_and_writes_two_more(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 3);
        let wbuf = dbsw.try().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(raid.val.0, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]);
        assert_eq!(wbuf, dbsr.try().unwrap());
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
            let mut wbuf_l = dbsw.try().unwrap();
            let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(raid.val.0, vec![wbuf_l, wbuf_r], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test write_partial_at_start_of_stripe(raid((3, 3, 1, 2))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read0(raid.val.0, vec![wbuf_short], vec![rbuf_short]);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try().unwrap()[0..BYTES_PER_LBA]);
    }

    // Test that write_at works when directed at the middle of the StripeBuffer.
    // This test requires a chunksize > 2
    test write_partial_at_middle_of_stripe(raid((3, 3, 1, 16))) {
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap().slice_to(2 * BYTES_PER_LBA);
        let wbuf_begin = wbuf.slice_to(BYTES_PER_LBA);
        let wbuf_middle = wbuf.slice_from(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let _ = rbuf.split_off(2 * BYTES_PER_LBA);
            write_read0(raid.val.0, vec![wbuf_begin, wbuf_middle], vec![rbuf]);
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try().unwrap()[0..2 * BYTES_PER_LBA]);
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
            let wbuf = dbsw.try().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            write_read0(raid.val.0, vec![wbuf], vec![rbuf]);
        }
        assert_eq!(&dbsw.try().unwrap()[..], &dbsr.try().unwrap()[..]);
    }

    test writev_read_one_stripe(raid) {
        writev_read_n_stripes(&raid.val.0, *raid.params.chunksize,
                              *raid.params.k, *raid.params.f, 1);
    }

    // Erasing an open zone should fail
    #[should_panic]
    test zone_erase_open(raid((3, 3, 1, 2))) {
        let zone = 1;
        current_thread::block_on_all( future::lazy(|| {
            raid.val.0.open_zone(zone)
            .and_then(|_| raid.val.0.erase_zone(0))
        })).expect("zone_erase_open");
    }

    test zone_read_closed(raid((3, 3, 1, 2))) {
        let zone = 0;
        let zl = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::block_on_all(future::lazy(|| {
            raid.val.0.write_at(wbuf0, zone, zl.0)
                .and_then(|_| {
                    raid.val.0.finish_zone(zone)
                }).and_then(|_| {
                    raid.val.0.read_at(rbuf, zl.0)
                })
        })).expect("current_thread::block_on_all");
        assert_eq!(wbuf1, dbsr.try().unwrap());
    }

    // Close a zone with an incomplete StripeBuffer, then read back from it
    test zone_read_closed_partial(raid((3, 3, 1, 2))) {
        let zone = 0;
        let zl = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf = dbsw.try().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            current_thread::block_on_all(future::lazy(|| {
                raid.val.0.write_at(wbuf_short, zone, zl.0)
                    .and_then(|_| {
                        raid.val.0.finish_zone(zone)
                    }).and_then(|_| {
                        raid.val.0.read_at(rbuf_short, zl.0)
                    })
            })).expect("current_thread::block_on_all");
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try().unwrap()[0..BYTES_PER_LBA]);
    }

    #[should_panic]
    // Writing to an explicitly closed a zone fails
    test zone_write_explicitly_closed(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::block_on_all(
            raid.val.0.open_zone(zone)
            .and_then(|_| raid.val.0.finish_zone(zone))
        ).expect("open and finish");
        write_read(&raid.val.0, vec![wbuf0], vec![rbuf], zone, start);
        assert_eq!(wbuf1, dbsr.try().unwrap());
    }

    #[should_panic]
    // Writing to a closed zone should fail
    test zone_write_implicitly_closed(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let dbsw = DivBufShared::from(vec![0;4096]);
        let wbuf = dbsw.try().unwrap();
        raid.val.0.write_at(wbuf, zone, start);
    }

    // Opening a closed zone should allow writing
    test zone_write_open(raid((3, 3, 1, 2))) {
        let zone = 1;
        let (start, _) = raid.val.0.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                     *raid.params.f, 1);
        let wbuf0 = dbsw.try().unwrap();
        let wbuf1 = dbsw.try().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        current_thread::block_on_all(
            raid.val.0.open_zone(zone)
        ).expect("open_zone");
        write_read(&raid.val.0, vec![wbuf0], vec![rbuf], zone, start);
        assert_eq!(wbuf1, dbsr.try().unwrap());
    }

    // Two zones can be open simultaneously
    test zone_write_two_zones(raid((3, 3, 1, 2))) {
        let vdev_raid = raid.val.0;
        for zone in 1..3 {
            let (start, _) = vdev_raid.zone_limits(zone);
            let (dbsw, dbsr) = make_bufs(*raid.params.chunksize, *raid.params.k,
                                         *raid.params.f, 1);
            let wbuf0 = dbsw.try().unwrap();
            let wbuf1 = dbsw.try().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            current_thread::block_on_all(
                vdev_raid.open_zone(zone)
            ).expect("open_zone");
            write_read(&vdev_raid, vec![wbuf0], vec![rbuf], zone, start);
            assert_eq!(wbuf1, dbsr.try().unwrap());
        }
    }
}

test_suite! {
    name persistence;

    use arkfs::common::{label::*, vdev_raid::*, vdev::Vdev};
    use futures::future;
    use std::{fs, io::{Read, Seek, SeekFrom}};
    use tempdir::TempDir;
    use tokio::{executor::current_thread, reactor::Handle};

    const GOLDEN_VDEV_RAID_LABEL: [u8; 183] = [
        // Past the VdevFile::Label, we have a VdevRaid::Label
                    0xa6, 0x64, 0x75, 0x75, 0x69, 0x64,// @..duuid
        0x50,
        // This is the VdevRaid's UUID
              0xb5, 0xb3, 0x52, 0x6b, 0x3a, 0xd5, 0x47,// P..Rk:.G
        0x73, 0xb7, 0x2e, 0x94, 0xa3, 0x27, 0x4f, 0x9b,// s....'O.
        0xb4,
        // This is the rest of the structure
              0x69, 0x63, 0x68, 0x75, 0x6e, 0x6b, 0x73,// .ichunks
        0x69, 0x7a, 0x65, 0x02, 0x70, 0x64, 0x69, 0x73,// ize.pdis
        0x6b, 0x73, 0x5f, 0x70, 0x65, 0x72, 0x5f, 0x73,// ks_per_s
        0x74, 0x72, 0x69, 0x70, 0x65, 0x03, 0x6a, 0x72,// tripe.jr
        0x65, 0x64, 0x75, 0x6e, 0x64, 0x61, 0x6e, 0x63,// edundanc
        0x79, 0x01, 0x70, 0x6c, 0x61, 0x79, 0x6f, 0x75,// y.playou
        0x74, 0x5f, 0x61, 0x6c, 0x67, 0x6f, 0x72, 0x69,// t_algori
        0x74, 0x68, 0x6d, 0x66, 0x50, 0x72, 0x69, 0x6d,// thmfPrim
        0x65, 0x53, 0x68, 0x63, 0x68, 0x69, 0x6c, 0x64,// eShchild
        0x72, 0x65, 0x6e, 0x85,
        // Here is the array of child UUIDs
                                0x50, 0x80, 0xc5, 0xdf,// ren.P...
        0x40, 0x59, 0xeb, 0x4d, 0x3a, 0x9e, 0xf3, 0xbf,// @Y.M:...
        0x19, 0x48, 0x95, 0x2e, 0xf9, 0x50, 0x95, 0x8e,// .H...P..
        0xb2, 0x9a, 0x24, 0x17, 0x43, 0x5d, 0xb1, 0xd0,// ..$.C]..
        0x77, 0xf0, 0xd2, 0x9d, 0x5e, 0x7d, 0x50, 0xd3,// w...^}P.
        0xf0, 0x24, 0x49, 0xb8, 0x88, 0x45, 0xdd, 0xb6,// .$I..E..
        0xb9, 0x69, 0x52, 0xd5, 0x67, 0xa3, 0xef, 0x50,// .iR.g..P
        0x91, 0x16, 0xab, 0x2e, 0xb7, 0x08, 0x48, 0x9c,// ......H.
        0xb8, 0x76, 0x78, 0x1c, 0x24, 0xf8, 0xd2, 0x87,// .vx.$...
        0x50, 0xa1, 0xce, 0xe2, 0xe7, 0x10, 0xfd, 0x4d,// P......M
        0x80, 0x90, 0xd2, 0x4a, 0xa1, 0x1c, 0x20, 0x14,// ...J.. .
        0x5a
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
            let mut vdev_raid = VdevRaid::create(2, num_disks, 3, 1, &paths,
                                                 Handle::default());
            (vdev_raid, tempdir, paths)
        }
    });

    // Testing VdevRaid::open_all with golden labels is too hard, because we
    // need to store separate golden labels for each VdevLeaf.  Instead, we'll
    // just check that we can open-after-write
    test open_all(raid()) {
        let (old_raid, _tempdir, paths) = raid.val;
        let uuid = old_raid.uuid();
        current_thread::block_on_all(future::lazy(|| {
            let label_writer = LabelWriter::new();
            old_raid.write_label(label_writer)
        })).unwrap();
        drop(old_raid);
        let vdev_raid = current_thread::block_on_all(future::lazy(|| {
            VdevRaid::open_all(paths, Handle::default())
        })).unwrap().pop().unwrap().0;
        assert_eq!(uuid, vdev_raid.uuid());
    }
    
    test write_label(raid()) {
        current_thread::block_on_all(future::lazy(|| {
            let label_writer = LabelWriter::new();
            raid.val.0.write_label(label_writer)
        })).unwrap();
        for path in raid.val.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(82)).unwrap();   // Skip the VdevLeaf label
            f.read_exact(&mut v).unwrap();
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[0..7], &GOLDEN_VDEV_RAID_LABEL[0..7]);
            assert_eq!(&v[23..97], &GOLDEN_VDEV_RAID_LABEL[23..97]);
            // Rest of the buffer should be zero-filled
            assert!(v[183..].iter().all(|&x| x == 0));
        }
    }
}
