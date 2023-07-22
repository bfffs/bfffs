// vim: tw=80
// LCOV_EXCL_START
use super::*;
use futures::{FutureExt, future};
use mockall::predicate::*;
use rstest::rstest;

mod min_max{
    use super::*;

    #[test]
    fn t() {
        let empty: Vec<u8> = Vec::with_capacity(0);
        assert_eq!(min_max(empty.iter()), None);
        assert_eq!(min_max([42u8].iter()), Some((&42, &42)));
        assert_eq!(min_max([1u32, 2u32, 3u32].iter()), Some((&1, &3)));
        assert_eq!(min_max([0i8, -9i8, 18i8, 1i8].iter()), Some((&-9, &18)));
    }
}

mod stripe_buffer {
    use super::*;

    #[test]
    fn empty() {
        let mut sb = StripeBuffer::new(96, 6);
        assert!(!sb.is_full());
        assert!(sb.is_empty());
        assert_eq!(sb.lba(), 96);
        assert_eq!(sb.next_lba(), 96);
        assert_eq!(sb.len(), 0);
        assert!(sb.peek().is_empty());
        let sglist = sb.pop();
        assert!(sglist.is_empty());
        // Adding an empty iovec should change nothing, but add a useless sender
        let dbs = DivBufShared::from(vec![0; 4096]);
        let db = dbs.try_const().unwrap();
        let db0 = db.slice(0, 0);
        assert!(sb.fill(db0).is_empty());
        assert!(!sb.is_full());
        assert!(sb.is_empty());
        assert_eq!(sb.lba(), 96);
        assert_eq!(sb.next_lba(), 96);
        assert_eq!(sb.len(), 0);
        assert!(sb.peek().is_empty());
        let sglist = sb.pop();
        assert!(sglist.is_empty());
    }

    #[test]
    fn fill_when_full() {
        let dbs0 = DivBufShared::from(vec![0; 24576]);
        let db0 = dbs0.try_const().unwrap();
        let dbs1 = DivBufShared::from(vec![1; 4096]);
        let db1 = dbs1.try_const().unwrap();
        {
            let mut sb = StripeBuffer::new(96, 6);
            assert!(sb.fill(db0).is_empty());
            assert_eq!(sb.fill(db1).len(), 4096);
            assert!(sb.is_full());
            assert_eq!(sb.lba(), 96);
            assert_eq!(sb.next_lba(), 102);
            assert_eq!(sb.len(), 24576);
        }
    }

    #[test]
    fn one_iovec() {
        let mut sb = StripeBuffer::new(96, 6);
        let dbs = DivBufShared::from(vec![0; 4096]);
        let db = dbs.try_const().unwrap();
        assert!(sb.fill(db).is_empty());
        assert!(!sb.is_full());
        assert!(!sb.is_empty());
        assert_eq!(sb.lba(), 96);
        assert_eq!(sb.next_lba(), 97);
        assert_eq!(sb.len(), 4096);
        {
            let sglist = sb.peek();
            assert_eq!(sglist.len(), 1);
            assert_eq!(&sglist[0][..], &[0; 4096][..]);
        }
        let sglist = sb.pop();
        assert_eq!(sglist.len(), 1);
        assert_eq!(&sglist[0][..], &[0; 4096][..]);
    }

    // Pad a StripeBuffer that is larger than the ZERO_REGION
    #[test]
    fn pad() {
        let zero_region_lbas = (ZERO_REGION.len() / BYTES_PER_LBA) as LbaT;
        let stripesize = 2 * zero_region_lbas + 1;
        let mut sb = StripeBuffer::new(102, stripesize);
        let dbs = DivBufShared::from(vec![0; BYTES_PER_LBA]);
        let db = dbs.try_const().unwrap();
        assert!(sb.fill(db).is_empty());
        assert!(sb.pad() == stripesize - 1);
        let sglist = sb.pop();
        assert_eq!(sglist.len(), 3);
        assert_eq!(sglist.iter().map(|v| v.len()).sum::<usize>(),
                   stripesize as usize * BYTES_PER_LBA);
    }

    #[test]
    fn reset() {
        let mut sb = StripeBuffer::new(96, 6);
        assert_eq!(sb.lba(), 96);
        sb.reset(108);
        assert_eq!(sb.lba(), 108);
    }

    #[test]
    #[should_panic(expected = "A StripeBuffer with data cannot be moved")]
    fn reset_nonempty() {
        let mut sb = StripeBuffer::new(96, 6);
        let dbs = DivBufShared::from(vec![0; 4096]);
        let db = dbs.try_const().unwrap();
        let _ = sb.fill(db);
        sb.reset(108);
    }

    #[test]
    fn two_iovecs() {
        let mut sb = StripeBuffer::new(96, 6);
        let dbs0 = DivBufShared::from(vec![0; 8192]);
        let db0 = dbs0.try_const().unwrap();
        assert!(sb.fill(db0).is_empty());
        let dbs1 = DivBufShared::from(vec![1; 4096]);
        let db1 = dbs1.try_const().unwrap();
        assert!(sb.fill(db1).is_empty());
        assert!(!sb.is_full());
        assert!(!sb.is_empty());
        assert_eq!(sb.lba(), 96);
        assert_eq!(sb.next_lba(), 99);
        assert_eq!(sb.len(), 12288);
        {
            let sglist = sb.peek();
            assert_eq!(sglist.len(), 2);
            assert_eq!(&sglist[0][..], &[0; 8192][..]);
            assert_eq!(&sglist[1][..], &[1; 4096][..]);
        }
        let sglist = sb.pop();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0; 8192][..]);
        assert_eq!(&sglist[1][..], &[1; 4096][..]);
    }

    #[test]
    fn two_iovecs_overflow() {
        let mut sb = StripeBuffer::new(96, 6);
        let dbs0 = DivBufShared::from(vec![0; 16384]);
        let db0 = dbs0.try_const().unwrap();
        assert!(sb.fill(db0).is_empty());
        let dbs1 = DivBufShared::from(vec![1; 16384]);
        let db1 = dbs1.try_const().unwrap();
        assert_eq!(sb.fill(db1).len(), 8192);
        assert!(sb.is_full());
        assert!(!sb.is_empty());
        assert_eq!(sb.lba(), 96);
        assert_eq!(sb.next_lba(), 102);
        assert_eq!(sb.len(), 24576);
        {
            let sglist = sb.peek();
            assert_eq!(sglist.len(), 2);
            assert_eq!(&sglist[0][..], &[0; 16384][..]);
            assert_eq!(&sglist[1][..], &[1; 8192][..]);
        }
        let sglist = sb.pop();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0; 16384][..]);
        assert_eq!(&sglist[1][..], &[1; 8192][..]);
    }
}

// pet kcov
#[test]
fn debug() {
    let label = Label {
        uuid: Uuid::new_v4(),
        chunksize: 1,
        disks_per_stripe: 2,
        redundancy: 1,
        layout_algorithm: LayoutAlgorithm::PrimeS,
        children: vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()]
    };
    format!("{label:?}");
}

/// A mock Mirror device with some basic expectations
fn mock_mirror() -> Mirror{
    let zl0 = (1, 60_000);
    let zl1 = (60_000, 120_000);
    let mut m = Mirror::default();
    m.expect_size()
        .return_const(262_144u64);
    m.expect_lba2zone()
        .with(eq(1))
        .return_const(Some(0));
    m.expect_lba2zone()
        .with(eq(60_000))
        .return_const(Some(1));
    m.expect_zone_limits()
        .with(eq(0))
        .return_const(zl0);
    m.expect_zone_limits()
        .with(eq(1))
        .return_const(zl1);
    m.expect_optimum_queue_depth()
        .return_const(10u32);
    m
}

/// Test error handling using fake Mirror devices, backed by RAM.
mod errors {
    use super::*;

    use std::sync::Mutex;

    use itertools::Itertools;
    use rand::{Rng, thread_rng};
    use rstest_reuse::{apply, template};

    /// Used to control a FakeMirror
    #[derive(Clone, Copy, Debug)]
    struct MirrorCtl {
        /// The result type that should be returned by read operations
        read_at: Result<()>
    }

    impl Default for MirrorCtl {
        fn default() -> Self {
            Self{read_at: Ok(())}
        }
    }

    /// Create a Mirror device that can actually store data, backed by RAM, and
    /// can be programmed to fail, too.
    fn fake_mirror() -> (Arc<Mutex<MirrorCtl>>, Mirror) {
        const LBAS: LbaT = 256;    // 1 MB

        let ctl = Arc::new(Mutex::new(MirrorCtl::default()));
        let ctl2 = ctl.clone();
        let ctl3 = ctl.clone();
        let ctl4 = ctl.clone();

        let mut mock = Mirror::default();
        let dbs = DivBufShared::from(vec![0; LBAS as usize * BYTES_PER_LBA]);
        let store = Arc::new(dbs);
        let store2 = store.clone();
        let store3 = store.clone();
        let store4 = store.clone();

        let dbs = DivBufShared::from(vec![0; LBAS as usize * BYTES_PER_LBA]);
        let smstore0a = Arc::new(dbs);
        let smstore0b = smstore0a.clone();
        let dbs = DivBufShared::from(vec![0; LBAS as usize * BYTES_PER_LBA]);
        let smstore1a = Arc::new(dbs);
        let smstore1b = smstore1a.clone();

        mock.expect_size()
            .return_const(LBAS);
        mock.expect_optimum_queue_depth()
            .return_const(10u32);
        mock.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 128));
        mock.expect_zone_limits()
            .with(eq(1))
            .return_const((128, 256));
        mock.expect_open_zone()
            .returning(|_| Box::pin(future::ok::<(), Error>(())));
        mock.expect_lba2zone()
            .returning(|lba| {
                if (1..128).contains(&lba) {
                    Some(0)
                } else if (128..256).contains(&lba) {
                    Some(1)
                } else {
                    None
                }
            });
        mock.expect_erase_zone()
            .returning(|_, _| Box::pin(future::ok(())));
        mock.expect_finish_zone()
            .returning(|_, _| Box::pin(future::ok(())));
        mock.expect_write_at()
            .returning(move |buf, lba| {
                let mut dbm = store.try_mut().unwrap();
                let ofs = lba as usize * BYTES_PER_LBA;
                dbm[ofs..(ofs + buf.len())].copy_from_slice(&buf[..]);
                Box::pin(future::ok(()))
            });
        mock.expect_writev_at()
            .returning(move |sglist, lba| {
                let mut offs = lba as usize * BYTES_PER_LBA;
                let mut dbm = store3.try_mut().unwrap();
                for buf in sglist.iter() {
                    dbm[offs..(offs + buf.len())].copy_from_slice(&buf[..]);
                    offs += buf.len();
                }
                Box::pin(future::ok(()))
            });
        mock.expect_write_spacemap()
            .returning(move |sglist, idx, block| {
                let mut offs = block as usize * BYTES_PER_LBA;
                let mut dbm = match idx {
                    0 => smstore0a.try_mut().unwrap(),
                    1 => smstore1a.try_mut().unwrap(),
                    _ => unimplemented!()
                };
                for buf in sglist.iter() {
                    dbm[offs..(offs + buf.len())].copy_from_slice(&buf[..]);
                    offs += buf.len();
                }
                Box::pin(future::ok(()))
            });
        mock.expect_read_at()
            .returning(move |mut buf, lba| {
                let g = ctl3.lock().unwrap();
                if g.read_at.is_ok() {
                    let len = buf.len();
                    let dbr = store2.try_const().unwrap();
                    let ofs = lba as usize * BYTES_PER_LBA;
                    buf[..].copy_from_slice(&dbr[ofs..(ofs + len)]);
                }
                Box::pin(future::ready(g.read_at))
            });
        mock.expect_readv_at()
            .returning(move |mut sglist, lba| {
                let g = ctl2.lock().unwrap();
                if g.read_at.is_ok() {
                    let dbr = store4.try_const().unwrap();
                    let mut ofs = lba as usize * BYTES_PER_LBA;
                    for buf in sglist.iter_mut() {
                        let len = buf.len();
                        buf[..].copy_from_slice(&dbr[ofs..(ofs + len)]);
                        ofs += len;
                    }
                }
                Box::pin(future::ready(g.read_at))
            });
        mock.expect_read_spacemap()
            .returning(move |mut buf, smidx| {
                let g = ctl4.lock().unwrap();
                if g.read_at.is_ok() {
                    let len = buf.len();
                    let dbr = match smidx {
                        0 => smstore0b.try_const().unwrap(),
                        1 => smstore1b.try_const().unwrap(),
                        _ => unimplemented!()
                    };
                    let ofs = 0;
                    buf[..].copy_from_slice(&dbr[ofs..(ofs + len)]);
                }
                Box::pin(future::ready(g.read_at))
            });
        (ctl, mock)
    }

    fn make_bufs(chunksize: LbaT, k: i16, f: i16, s: usize) ->
        (DivBufShared, DivBufShared)
    {
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

    #[derive(Clone, Copy, Debug)]
    struct Config {
        /// Number of disks in the RAID layout
        n: i16,
        /// Number of disks in each RAID stripe
        k: i16,
        /// Number of parity disks in each RAID stripe
        f: i16,
        /// Number of disk failures in each test iteration
        nfailures: usize
    }
    fn config(n: i16, k: i16, f: i16, nfailures: usize) -> Config {
        Config{n, k, f, nfailures}
    }

    struct Harness {
        vdev: Arc<VdevRaid>,
        mctls: Vec<Arc<Mutex<MirrorCtl>>>
    }

    impl Harness {
        async fn reset(&mut self) {
            for ammc in self.mctls.iter() {
                let mut g = ammc.lock().unwrap();
                g.read_at = Ok(());
            }
            self.vdev.finish_zone(0).await.unwrap();
            self.vdev.erase_zone(0).await.unwrap();
            self.vdev.open_zone(0).await.unwrap();
        }
    }

    async fn harness(config: Config, chunksize: LbaT) -> Harness {
        let (mctls, mirrors): (Vec<_>, Vec<_>) = (0..config.n)
            .map(|_| fake_mirror())
            .unzip();
        let cs = NonZeroU64::new(chunksize);
        let vdev = Arc::new(VdevRaid::create(cs, config.k, config.f, mirrors));
        vdev.open_zone(0).await.unwrap();
        Harness { vdev, mctls}
    }

    #[template]
    #[rstest(c,
        // Stupid mirror
        case(config(2, 2, 1, 1)),

        // Smallest possible PRIMES configuration
        case(config(3, 3, 1, 1)),

        // Smallest layout that's more than 50% declustered
        case(config(5, 2, 1, 1)),

        // Smallest PRIMES declustered configuration
        case(config(5, 4, 1, 1)),

        // Smallest non-ideal PRIME-S configuration
        case(config(7, 4, 1, 1)),

        // Smallest double-parity configuration
        case(config(5, 5, 2, 1)),
        case(config(5, 5, 2, 2)),

        // Smallest triple-parity configuration
        case(config(7, 7, 3, 1)),
        case(config(7, 7, 3, 2)),
        case(config(7, 7, 3, 3)),

        // Smallest quad-parity configuration
        case(config(11, 9, 4, 1)),
        case(config(11, 9, 4, 2)),
        case(config(11, 9, 4, 3)),
        // Skip the quad-failure test, because it takes too long.
     )]
    fn recoverable_failures(c: Config) {}

    mod read_at {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Verify that VdevRaid can recover from a recoverable read error, when
        /// reading a single chunk instead of a whole stripe.
        #[apply(recoverable_failures)]
        #[rstest]
        #[tokio::test]
        async fn recoverable_eio_single_chunk(c: Config) {
            let h = harness(c, 1).await;
            let (dbsw, dbsr) = make_bufs(1, c.k, c.f, 1);
            let wbuf0 = dbsw.try_const().unwrap();
            let wbuf1 = dbsw.try_const().unwrap();
            let zl = h.vdev.zone_limits(0);

            // We must repeat the read for every chunk in the stripe to ensure
            // that we'll access the failing disks.
            h.vdev.write_at(wbuf0, 0, zl.0).await.unwrap();
            for id in 0..c.nfailures {
                h.mctls[id].lock().unwrap().read_at = Err(Error::EIO);
            }
            for t in 0..(dbsr.len() / BYTES_PER_LBA) {
                let mut rbuf = dbsr.try_mut().unwrap();
                rbuf.split_off(BYTES_PER_LBA * (t + 1));
                rbuf.split_to(BYTES_PER_LBA * t);
                h.vdev.clone().read_at(rbuf, zl.0 + t as u64).await.unwrap();
            }
            assert!(wbuf1[..] == dbsr.try_const().unwrap()[..],
                "miscompare!");
        }

        /// Reads can recover from disk failures when part of the data is in the
        /// stripe buffer and part on disk..
        #[apply(recoverable_failures)]
        #[rstest]
        #[tokio::test]
        async fn recoverable_eio_stripe_buffer(c: Config)
        {
            let chunksize = 2;
            let mut h = harness(c, chunksize).await;
            let mut started = false;
            for defective_diskids in (0..c.n as usize).combinations(c.nfailures)
            {
                if started {
                    h.reset().await;
                }
                started = true;
                let (dbsw, dbsr) = make_bufs(chunksize, c.k, c.f, 2);
                let mut wbuf = dbsw.try_const().unwrap();
                let three_quarters = wbuf.len() * 3 / 4;
                wbuf.split_off(three_quarters);
                let mut rbuf = dbsr.try_mut().unwrap();
                rbuf.split_off(three_quarters);

                let zone = 0;
                let zl = h.vdev.zone_limits(zone);
                h.vdev.write_at(wbuf.clone(), 0, zl.0).await.unwrap();
                // Now the VdevRaids should contain the data from wbuf.  The
                // first stripesize bytes should be on disk, and the rest in the
                // StripeBuffer.
                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Err(Error::EIO);
                }
                h.vdev.clone().read_at(rbuf, zl.0).await.unwrap();
                let dbr = dbsr.try_const().unwrap();
                assert!(wbuf[..] == dbr[..three_quarters], "miscompare!");

                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Ok(());
                }
            }
        }

        /// Verify that VdevRaid can cope with read EIO errors when reading one
        /// or more stripes at a time.
        #[apply(recoverable_failures)]
        #[rstest]
        #[tokio::test]
        async fn recoverable_eio_whole_stripe(
            c: Config,
            // One stripe provides coverage of read_at_one.  Three stripes
            // covers read_at_multi.  Three is necessary to ensure that parity
            // chunks are sometimes sandwiched between data ones.  Two is
            // necessary to cover cases with highly declustered layouts where
            // some disks won't be read at all by read_at_multi.
            #[values(1, 2, 3)]
            stripes: usize)
        {
            let mut h = harness(c, 1).await;
            let mut started = false;
            for defective_diskids in (0..c.n as usize).combinations(c.nfailures)
            {
                if started {
                    h.reset().await;
                }
                started = true;
                let (dbsw, dbsr) = make_bufs(1, c.k, c.f, stripes);
                let wbuf0 = dbsw.try_const().unwrap();
                let wbuf1 = dbsw.try_const().unwrap();
                let rbuf = dbsr.try_mut().unwrap();

                let zl = h.vdev.zone_limits(0);
                h.vdev.write_at(wbuf0, 0, zl.0).await.unwrap();
                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Err(Error::EIO);
                }
                h.vdev.clone().read_at(rbuf, zl.0).await.unwrap();
                assert!(wbuf1[..] == dbsr.try_const().unwrap()[..],
                    "miscompare!");

                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Ok(());
                }
            }
        }
        /// If there are too many failures to permit reconstruction, the I/O
        /// will fail.
        // Only run this test with fully clustered configs.  With declustered
        // configs, the I/O may nevertheless succeed if the stripe we read from
        // isn't critically degraded.
        #[rstest]
        #[tokio::test]
        async fn unrecoverable_eio(
            #[values(
                // Stupid mirror
                config(2, 2, 1, 2),
                // Smallest possible PRIMES configuration
                config(3, 3, 1, 2),
                // Smallest double-parity configuration
                config(5, 5, 2, 3),
                // Smallest triple-parity configuration
                config(7, 7, 3, 4),
            )]
            c: Config,
            #[values(1, 2)]
            stripes: usize)
        {
            let mut h = harness(c, 1).await;
            let mut started = false;
            for defective_diskids in (0..c.n as usize).combinations(c.nfailures)
            {
                if started {
                    h.reset().await;
                }
                started = true;
                let (dbsw, dbsr) = make_bufs(1, c.k, c.f, stripes);
                let wbuf0 = dbsw.try_const().unwrap();
                let rbuf = dbsr.try_mut().unwrap();

                let zl = h.vdev.zone_limits(0);
                h.vdev.write_at(wbuf0, 0, zl.0).await.unwrap();
                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Err(Error::EIO);
                }
                let r = h.vdev.clone().read_at(rbuf, zl.0).await;
                assert_eq!(r, Err(Error::EIO));

                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Ok(());
                }
            }
        }
    }

    mod read_spacemap {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Verify that VdevRaid::read_spacemap can cope with read EIO errors
        /// when reading one or more stripes at a time.
        #[rstest]
        #[tokio::test]
        async fn recoverable_eio(
            #[values(
                // Stupid mirror
                config(2, 2, 1, 1),
                // Smallest possible PRIMES configuration
                config(3, 3, 1, 2),
                // Smallest double-parity configuration
                config(5, 5, 2, 4),
                // Smallest triple-parity configuration
                config(7, 7, 3, 6),
            )]
            c: Config,
            #[values(0, 1)]
            idx: u32)
        {
            let mut h = harness(c, 1).await;
            let mut started = false;
            let block = 0;
            for defective_diskids in (0..c.n as usize).combinations(c.nfailures)
            {
                if started {
                    h.reset().await;
                }
                started = true;
                let (dbsw, dbsr) = make_bufs(1, 1, 0, 1);
                let wbuf0 = dbsw.try_const().unwrap();
                let wbuf1 = dbsw.try_const().unwrap();
                let rbuf = dbsr.try_mut().unwrap();

                h.vdev.write_spacemap(vec![wbuf0], idx, block).await.unwrap();
                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Err(Error::EIO);
                }
                h.vdev.clone().read_spacemap(rbuf, idx).await.unwrap();
                assert!(wbuf1[..] == dbsr.try_const().unwrap()[..],
                    "miscompare!");

                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Ok(());
                }
            }
        }

        /// If there are too many failures to permit reconstruction, the I/O
        /// will fail.
        // Only run this test with fully clustered configs.  With declustered
        // configs, the I/O may nevertheless succeed if the stripe we read from
        // isn't critically degraded.
        #[rstest]
        #[tokio::test]
        async fn unrecoverable_eio(
            #[values(
                // Stupid mirror
                config(2, 2, 1, 2),
                // Smallest possible PRIMES configuration
                config(3, 3, 1, 3),
                // Smallest double-parity configuration
                config(5, 5, 2, 5),
                // Smallest triple-parity configuration
                config(7, 7, 3, 7),
            )]
            c: Config,
            #[values(0, 1)]
            idx: u32)
        {
            let mut h = harness(c, 1).await;
            let mut started = false;
            let block = 0;
            for defective_diskids in (0..c.n as usize).combinations(c.nfailures)
            {
                if started {
                    h.reset().await;
                }
                started = true;
                let (dbsw, dbsr) = make_bufs(1, 1, 0, 1);
                let wbuf0 = dbsw.try_const().unwrap();
                let rbuf = dbsr.try_mut().unwrap();

                h.vdev.write_spacemap(vec![wbuf0], idx, block).await.unwrap();
                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Err(Error::EIO);
                }
                let r = h.vdev.clone().read_spacemap(rbuf, idx).await;
                assert_eq!(r, Err(Error::EIO));

                for id in defective_diskids.iter() {
                    h.mctls[*id].lock().unwrap().read_at = Ok(());
                }
            }
        }

    }
}

mod erase_zone {
    use super::*;

    // Erase a zone.  VdevRaid doesn't care whether it still has allocated data;
    // that's Cluster's job.  And VdevRaid doesn't care whether the zone is
    // closed or empty; that's the VdevLeaf's job.
    #[test]
    fn ok() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_erase_zone()
                .with(eq(1), eq(59_999))
                .once()
                .return_once(|_, _| Box::pin(future::ok(())));
            Child::present(bd)
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();
        mirrors.push(bd0);
        mirrors.push(bd1);
        mirrors.push(bd2);

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.erase_zone(0).now_or_never().unwrap().unwrap();
    }
}

mod finish_zone {
    use super::*;

    /// A zone with an empty stripe buffer requires no flushing
    #[test]
    fn empty_sb() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .once()
                .with(eq(60_000))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_finish_zone()
                .with(eq(60_000), eq(119_999))
                .once()
                .return_once(|_, _| Box::pin(future::ok(())));
            Child::present(bd)
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();
        mirrors.push(bd0);
        mirrors.push(bd1);
        mirrors.push(bd2);

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.finish_zone(1).now_or_never().unwrap().unwrap();
    }

    /// With a partially full stripe buffer, vdev_raid should pad it with zeros
    /// and write it out during finish_zone
    #[test]
    fn partial_sb() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .with(eq(60_000))
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_finish_zone()
                .with(eq(60_000), eq(119_999))
                .once()
                .return_once(|_, _| Box::pin(future::ok(())));
            bd
        };

        let mut bd0 = bd();
        bd0.expect_writev_at()
            .once()
            .withf(|buf, lba|
                // The first segment is user data
                buf[0][..] == vec![1u8; BYTES_PER_LBA][..] &&
                // Later segments are zero-fill from finish_zone
                buf[1][..] == vec![0u8; BYTES_PER_LBA][..] &&
                *lba == 60_000
            ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        let mut bd1 = bd();
        // This write is from the zero-fill
        bd1.expect_writev_at()
            .once()
            .withf(|buf, lba|
                buf.len() == 1 &&
                buf[0][..] == vec![0u8; 2 * BYTES_PER_LBA][..] &&
                *lba == 60_000
        ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        // This write is generated parity
        let mut bd2 = bd();
        bd2.expect_write_at()
            .once()
            .withf(|buf, lba|
                // single disk parity is a simple XOR
                buf[0..4096] == vec![1u8; BYTES_PER_LBA][..] &&
                buf[4096..8192] == vec![0u8; BYTES_PER_LBA][..] &&
                *lba == 60_000
        ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        let dbs = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 120_000).now_or_never().unwrap().unwrap();
        vdev_raid.finish_zone(1).now_or_never().unwrap().unwrap();
    }
}

mod flush_zone {
    use super::*;

    // Flushing a closed zone is a no-op
    #[test]
    fn closed() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd0 = mock_mirror();
        let bd1 = mock_mirror();
        let bd2 = mock_mirror();
        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.flush_zone(0).1.now_or_never().unwrap().unwrap();
    }

    // Flushing an open zone is a no-op if the stripe buffer is empty
    #[test]
    fn empty_stripe_buffer() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .once()
                .with(eq(60_000))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();
        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.flush_zone(1).1.now_or_never().unwrap().unwrap();
    }

    // Partially written stripes should be flushed by flush_zone
    #[test]
    fn partial_stripe_buffer() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .with(eq(60_000))
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd
        };

        let mut bd0 = bd();
        bd0.expect_writev_at()
            .once()
            .withf(|buf, lba|
                // The first segment is user data
                buf[0][..] == vec![1u8; BYTES_PER_LBA][..] &&
                // Later segments are zero-fill from flush_zone
                buf[1][..] == vec![0u8; BYTES_PER_LBA][..] &&
                *lba == 60_000
            ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        let mut bd1 = bd();
        // This write is from the zero-fill
        bd1.expect_writev_at()
            .once()
            .withf(|buf, lba|
                buf.len() == 1 &&
                buf[0][..] == vec![0u8; 2 * BYTES_PER_LBA][..] &&
                *lba == 60_000
        ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        // This write is generated parity
        let mut bd2 = bd();
        bd2.expect_write_at()
            .once()
            .withf(|buf, lba|
                // single disk parity is a simple XOR
                buf[0..4096] == vec![1u8; BYTES_PER_LBA][..] &&
                buf[4096..8192] == vec![0u8; BYTES_PER_LBA][..] &&
                *lba == 60_000
        ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        let dbs = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 120_000).now_or_never().unwrap().unwrap();
        vdev_raid.flush_zone(1).1.now_or_never().unwrap().unwrap();
    }
}

/// Test basic layout properties
mod layout {
    use super::*;
    use mockall::PredicateBooleanExt;
    use pretty_assertions::assert_eq;

    fn vr(n: i16, k: i16, f:i16, chunksize: LbaT) -> VdevRaid {
        let mut mirrors = Vec::<Child>::new();
        for _ in 0..n {
            let mut mock = Mirror::default();
            mock.expect_size()
                .return_const(262_144u64);
            mock.expect_lba2zone()
                .with(eq(0))
                .return_const(None);
            mock.expect_lba2zone()
                .with(ge(1).and(lt(65536)))
                .return_const(Some(0));
            mock.expect_lba2zone()
                .with(ge(65536).and(lt(131072)))
                .return_const(Some(1));
            mock.expect_optimum_queue_depth()
                .return_const(10u32);
            mock.expect_zone_limits()
                .with(eq(0))
                .return_const((1, 65536));
            mock.expect_zone_limits()
                .with(eq(1))
                // 64k LBAs/zone
                .return_const((65536, 131_072));

            mirrors.push(Child::present(mock));
        }

        VdevRaid::new(chunksize, k, f, Uuid::new_v4(),
                      LayoutAlgorithm::PrimeS, mirrors.into_boxed_slice())
    }

    #[rstest]
    #[case(vr(5, 4, 1, 16))]
    fn small(#[case] vr: VdevRaid) {
        assert_eq!(vr.lba2zone(0), None);
        assert_eq!(vr.lba2zone(95), None);
        assert_eq!(vr.lba2zone(96), Some(0));
        // Last LBA in zone 0
        assert_eq!(vr.lba2zone(245_759), Some(0));
        // First LBA in zone 1
        assert_eq!(vr.lba2zone(245_760), Some(1));

        assert_eq!(vr.optimum_queue_depth(), 12);

        assert_eq!(vr.size(), 983_040);

        assert_eq!(vr.zone_limits(0), (96, 245_760));
        assert_eq!(vr.zone_limits(1), (245_760, 491_520));
    }

    #[rstest]
    #[case(vr(7, 4, 1, 16))]
    fn medium(#[case] vr: VdevRaid) {
        assert_eq!(vr.lba2zone(0), None);
        assert_eq!(vr.lba2zone(95), None);
        assert_eq!(vr.lba2zone(96), Some(0));
        // Last LBA in zone 0
        assert_eq!(vr.lba2zone(344_063), Some(0));
        // First LBA in zone 1
        assert_eq!(vr.lba2zone(344_064), Some(1));

        assert_eq!(vr.optimum_queue_depth(), 17);

        assert_eq!(vr.size(), 1_376_256);

        assert_eq!(vr.zone_limits(0), (96, 344_064));
        assert_eq!(vr.zone_limits(1), (344_064, 688_128));
    }

    // A layout whose depth does not evenly divide the zone size.  The zone size
    // is not even a multiple of this layout's iterations.  So, it has a gap of
    // unused LBAs between zones
    #[rstest]
    #[case(vr(7, 5, 1, 16))]
    fn has_gap(#[case] vr: VdevRaid) {
        assert_eq!(vr.lba2zone(0), None);
        assert_eq!(vr.lba2zone(127), None);
        assert_eq!(vr.lba2zone(128), Some(0));
        // Last LBA in zone 0
        assert_eq!(vr.lba2zone(366_975), Some(0));
        // An LBA in between zones 0 and 1
        assert_eq!(vr.lba2zone(366_976), None);
        // First LBA in zone 1
        assert_eq!(vr.lba2zone(367_040), Some(1));

        assert_eq!(vr.optimum_queue_depth(), 14);

        assert_eq!(vr.size(), 1_468_006);

        assert_eq!(vr.zone_limits(0), (128, 366_976));
        assert_eq!(vr.zone_limits(1), (367_040, 733_952));
    }

    // A layout whose depth does not evenly divide the zone size and has
    // multiple whole stripes per row.  So, it has a gap of multiple stripes
    // between zones.
    #[rstest]
    #[case(vr(11, 3, 1, 16))]
    fn has_multistripe_gap(#[case] vr: VdevRaid) {
        assert_eq!(vr.lba2zone(0), None);
        assert_eq!(vr.lba2zone(159), None);
        assert_eq!(vr.lba2zone(160), Some(0));
        // Last LBA in zone 0
        assert_eq!(vr.lba2zone(480_511), Some(0));
        // LBAs in between zones 0 and 1
        assert_eq!(vr.lba2zone(480_512), None);
        assert_eq!(vr.lba2zone(480_639), None);
        // First LBA in zone 1
        assert_eq!(vr.lba2zone(480_640), Some(1));

        assert_eq!(vr.size(), 1_922_389);

        assert_eq!(vr.zone_limits(0), (160, 480_512));
        assert_eq!(vr.zone_limits(1), (480_640, 961_152));
    }

    // A layout whose chunksize does not evenly divide the zone size.  One or
    // more entire rows must be skipped
    #[rstest]
    #[case(vr(5, 4, 1, 5))]
    fn misaligned_chunksize(#[case] vr: VdevRaid) {
        assert_eq!(vr.lba2zone(0), None);
        assert_eq!(vr.lba2zone(29), None);
        assert_eq!(vr.lba2zone(30), Some(0));
        // Last LBA in zone 0
        assert_eq!(vr.lba2zone(245_744), Some(0));
        // LBAs in the zone 0-1 gap
        assert_eq!(vr.lba2zone(245_745), None);
        assert_eq!(vr.lba2zone(245_774), None);
        // First LBA in zone 1
        assert_eq!(vr.lba2zone(245_775), Some(1));

        assert_eq!(vr.size(), 983_025);

        assert_eq!(vr.zone_limits(0), (30, 245_745));
        assert_eq!(vr.zone_limits(1), (245_775, 491_505));
    }
}

mod open {
    use super::*;
    use itertools::Itertools;
    use crate::{
        mirror,
        raid::{self, vdev_raid}
    };

    /// Regardless of the order in which the devices are given to
    /// raid::open, it will construct itself in the correct order.
    #[test]
    fn ordering() {
        let child_uuid0 = Uuid::new_v4();
        let child_uuid1 = Uuid::new_v4();
        let child_uuid2 = Uuid::new_v4();
        fn mock(child_uuid: &Uuid) -> Mirror {
            let mut m = mock_mirror();
            m.expect_uuid()
                .return_const(*child_uuid);
            m.expect_status()
                .return_const(mirror::Status {
                    health: Health::Online,
                    leaves: Vec::new(),
                    uuid: *child_uuid
                });
            m
        }
        let label = raid::Label::Raid(vdev_raid::Label {
            uuid: Uuid::new_v4(),
            chunksize: 1,
            disks_per_stripe: 3,
            redundancy: 1,
            layout_algorithm: LayoutAlgorithm::PrimeS,
            children: vec![child_uuid0, child_uuid1, child_uuid2]
        });
        let mut serialized = Vec::new();
        let mut lw = LabelWriter::new(0);
        lw.serialize(&label).unwrap();
        for buf in lw.into_sglist().into_iter() {
            serialized.extend(&buf[..]);
        }
        let child_uuids = [child_uuid0, child_uuid1, child_uuid2];

        for perm in child_uuids.iter().permutations(child_uuids.len()) {
            let m0 = mock(perm[0]);
            let m1 = mock(perm[1]);
            let m2 = mock(perm[2]);
            let mut combined = Vec::new();
            for m in [m0, m1, m2] {
                let lr = LabelReader::new(serialized.clone()).unwrap();
                combined.push((m, lr));
            }
            let (vdev_raid, _) = raid::open(Some(label.uuid()), combined);
            let status = vdev_raid.status();
            assert_eq!(status.mirrors[0].uuid(), child_uuid0);
            assert_eq!(status.mirrors[1].uuid(), child_uuid1);
            assert_eq!(status.mirrors[2].uuid(), child_uuid2);
        }
    }
}

mod open_zone {
    use super::*;

    // Reopen a zone that was previously used and unmounted without being
    // closed.  There will be some already-allocated space.  After opening, the
    // raid device should accept a write at the true write pointer, not the
    // beginning of the zone.
    #[test]
    fn reopen() {
        let k = 2;
        let f = 1;
        const CHUNKSIZE: LbaT = 1;

        let mut mirrors = Vec::<Child>::new();
        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_lba2zone()
                .with(eq(60_100))
                .return_const(Some(1));
            bd.expect_open_zone()
                .once()
                .with(eq(60_000))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_write_at()
                .with(always(), eq(60_100))
                .once()
                .return_once(|_, _| Box::pin( future::ok::<(), Error>(())));
            Child::present(bd)
        };
        mirrors.push(bd());    //disk 0
        mirrors.push(bd());    //disk 1

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.reopen_zone(1, 100).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 60_100).now_or_never().unwrap().unwrap();
    }

    // Reopening a zone with wasted chunks should _not_ rewrite the zero-fill
    // area.
    #[test]
    fn reopen_wasted_chunks() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 7;

        let mut mirrors = Vec::<Child>::new();

        let m = || {
            let mut m = mock_mirror();
            m.expect_open_zone()
                .once()
                .with(eq(60_000))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            m.expect_writev_at()
                .never();
            Child::present(m)
        };

        for _ in 0..k {
            mirrors.push(m());
        }

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.reopen_zone(1, 28).now_or_never().unwrap().unwrap();

    }

    // Open a zone that has wasted leading space due to a chunksize misaligned
    // with the zone size.
    #[test]
    fn zero_fill_wasted_chunks() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 7;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .once()
                .with(eq(60_000))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_writev_at()
                .once()
                .withf(|sglist, lba| {
                    let len = sglist.iter().map(|b| b.len()).sum::<usize>();
                    len == 4 * BYTES_PER_LBA && *lba == 60_000
                }).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));
            Child::present(bd)
        };

        mirrors.push(bd());    //disk 0
        mirrors.push(bd());    //disk 1
        mirrors.push(bd());    //disk 2
        mirrors.push(bd());    //disk 3
        mirrors.push(bd());    //disk 4

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
    }

    // Open a zone that has some leading wasted space.  Use mock Mirror objects
    // to verify that the leading wasted space gets zero-filled.
    // Use highly unrealistic disks with 32 LBAs per zone
    #[test]
    fn zero_fill_wasted_stripes() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 1;
        let zl0 = (1, 32);
        let zl1 = (32, 64);

        let mut mirrors = Vec::<Child>::new();

        let bd = |gap_chunks: LbaT| {
            let mut bd = Mirror::default();
            bd.expect_size()
                .return_const(262_144u64);
            bd.expect_lba2zone()
                .with(eq(1))
                .return_const(Some(0));
            bd.expect_zone_limits()
                .with(eq(0))
                .return_const(zl0);
            bd.expect_zone_limits()
                .with(eq(1))
                .return_const(zl1);
            bd.expect_open_zone()
                .once()
                .with(eq(32))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_optimum_queue_depth()
                .return_const(10u32);
            if gap_chunks > 0 {
                bd.expect_writev_at()
                    .once()
                    .withf(move |sglist, lba| {
                        let gap_lbas = gap_chunks * CHUNKSIZE; 
                        let len = sglist.iter().map(|b| b.len()).sum::<usize>();
                        len == gap_lbas as usize * BYTES_PER_LBA && *lba == 32
                    }).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));
            }
            Child::present(bd)
        };

        // On this layout, zone 1 begins at the third row in the repetition.
        // Stripes 2 and 3 are wasted, so disks 0, 1, 2, 4, and 5 have a wasted
        // LBA that needs zero-filling.  Disks 3 and 6 have no wasted LBA.
        mirrors.push(bd(1));  //disk 0
        mirrors.push(bd(2));  //disk 1
        mirrors.push(bd(1));  //disk 2
        mirrors.push(bd(0));  //disk 3
        mirrors.push(bd(1));  //disk 4
        mirrors.push(bd(1));  //disk 5
        mirrors.push(bd(0));  //disk 6

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
    }
}

mod read_at {
    use super::*;

    // Use mock Mirror objects to test that RAID reads hit the right LBAs from
    // the individual disks.  Ignore the actual data values, since we don't have
    // real Mirrors.  Functional testing will verify the data.
    #[test]
    fn one_stripe() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let mut m0 = mock_mirror();
        m0.expect_read_at()
            .once()
            .withf(|buf, lba| {
                buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
                    && *lba == 60_000
            }).return_once(|_, _|  Box::pin(future::ok::<(), Error>(())));
        mirrors.push(Child::present(m0));

        let mut m1 = mock_mirror();
        m1.expect_read_at()
            .once()
            .withf(|buf, lba| {
                buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
                    && *lba == 60_000
            }).return_once(|_, _|  Box::pin( future::ok::<(), Error>(())));
        mirrors.push(Child::present(m1));

        let m2 = mock_mirror();
        mirrors.push(Child::present(m2));

        let vdev_raid = Arc::new(
            VdevRaid::new(CHUNKSIZE, k, f, Uuid::new_v4(),
                          LayoutAlgorithm::PrimeS, mirrors.into_boxed_slice())
        );
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let rbuf = dbs.try_mut().unwrap();
        vdev_raid.read_at(rbuf, 120_000).now_or_never().unwrap().unwrap();
    }

    /// If too many data disks return EIO, then no error recovery is possible
    /// and should not be attempted.
    #[test]
    fn supercritically_degraded() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 1;

        let mut mirrors = Vec::<Child>::new();

        let mut m0 = mock_mirror();
        m0.expect_read_at()
            .once()
            .with(always(), eq(32768))
            .return_once(|_, _|  Box::pin(future::err(Error::EIO)));
        mirrors.push(Child::present(m0));

        let mut m1 = mock_mirror();
        m1.expect_read_at()
            .once()
            .with(always(), eq(32768))
            .return_once(|_, _|  Box::pin(future::err(Error::EIO)));
        mirrors.push(Child::present(m1));

        let mut m2 = mock_mirror();
        // No read here, because this is the parity disk for this stripe, and we
        // won't attempt recovery.
        m2.expect_read_at()
            .never();
        mirrors.push(Child::present(m2));

        let mut m3 = mock_mirror();
        m3.expect_read_at()
            .once()
            .with(always(), eq(32768))
            .return_once(|_, _|  Box::pin(future::ok(())));
        mirrors.push(Child::present(m3));

        let mut m4 = mock_mirror();
        m4.expect_read_at()
            .once()
            .with(always(), eq(32768))
            .return_once(|_, _|  Box::pin(future::ok(())));
        mirrors.push(Child::present(m4));

        let vdev_raid = Arc::new(
            VdevRaid::new(CHUNKSIZE, k, f, Uuid::new_v4(),
                          LayoutAlgorithm::PrimeS, mirrors.into_boxed_slice())
        );
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let rbuf = dbs.try_mut().unwrap();
        let r = vdev_raid.read_at(rbuf, 131_072).now_or_never().unwrap();
        assert_eq!(r, Err(Error::EIO));
    }

}

mod status {
    use super::*;

    use std::path::PathBuf;

    use crate::vdev::Health::*;

    /// When degraded, the VdevRaid's health should be the sum of all missing or
    /// otherwise degraded disks.
    #[rstest]
    #[case(Online, vec![Online, Online, Online])]
    #[case(Degraded(NonZeroU8::new(3).unwrap()),
        vec![Online, Online, Faulted])]
    #[case(Degraded(NonZeroU8::new(3).unwrap()),
        vec![Online, Rebuilding, Online])]
    #[case(Degraded(NonZeroU8::new(1).unwrap()),
        vec![Degraded(NonZeroU8::new(1).unwrap()), Online, Online])]
    #[case(Degraded(NonZeroU8::new(2).unwrap()),
        vec![Degraded(NonZeroU8::new(2).unwrap()), Online, Online])]
    #[case(Degraded(NonZeroU8::new(2).unwrap()),
        vec![Degraded(NonZeroU8::new(1).unwrap()),
             Degraded(NonZeroU8::new(1).unwrap()),
             Online])]
    #[case(Degraded(NonZeroU8::new(4).unwrap()),
        vec![Degraded(NonZeroU8::new(1).unwrap()), Online, Faulted])]
    fn degraded(#[case] health: Health, #[case] children: Vec<Health>) {
        let k = children.len() as i16; // doesn't matter for Health calculation
        let f = 1;
        const CHUNKSIZE: LbaT = 1;

        let mut mirrors = Vec::<Child>::new();
        let m = |mirror_health| {
            let muuid = Uuid::new_v4();
            let mut m = mock_mirror();
            m.expect_uuid()
                .return_const(muuid);
            let leaves = (0..3).map(|_| {
                crate::vdev_block::Status {
                    health: Online,
                    uuid: Default::default(),
                    path: PathBuf::default()
                }
            }).collect::<Vec<_>>();
            m.expect_status()
                .return_const(crate::mirror::Status {
                    health: mirror_health,
                    leaves,
                    uuid: muuid
                });
            Child::present(m)
        };
        for c in children.into_iter() {
            mirrors.push(m(c));
        }

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        assert_eq!(vdev_raid.status().health, health);
    }
}

mod sync_all {
    use super::*;

    #[test]
    fn ok() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::default();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_sync_all()
                .return_once(|| Box::pin(future::ok::<(), Error>(())));
            bd
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();

        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        vdev_raid.sync_all().now_or_never().unwrap().unwrap();
    }

    // It's illegal to sync a VdevRaid without flushing its zones first
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Must call flush_zone before sync_all")]
    fn sync_all_unflushed() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .with(eq(60_000))
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_sync_all()
                .once()
                .return_once(|| Box::pin(future::ok::<(), Error>(())));
            bd
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();

        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());

        let dbs = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 120_000).now_or_never().unwrap().unwrap();
        // Don't flush zone 1 before syncing.  Syncing should panic
        vdev_raid.sync_all().now_or_never().unwrap().unwrap();
    }
}

mod write_at {
    use super::*;

    // Use mock Mirror objects to test that RAID writes hit the right LBAs from
    // the individual disks.  Ignore the actual data values, since we don't have
    // real Mirrors.  Functional testing will verify the data.
    #[test]
    fn one_stripe() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let mut mirrors = Vec::<Child>::new();
        let mut m0 = mock_mirror();
        m0.expect_open_zone()
            .with(eq(60_000))
            .once()
            .return_once(|_| Box::pin(future::ok::<(), Error>(())));
        m0.expect_write_at()
            .once()
            .withf(|buf, lba|
                   buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
                   && *lba == 60_000
            ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        mirrors.push(Child::present(m0));
        let mut m1 = mock_mirror();
        m1.expect_open_zone()
            .with(eq(60_000))
            .once()
            .return_once(|_| Box::pin(future::ok::<(), Error>(())));
        m1.expect_write_at()
            .once()
            .withf(|buf, lba|
                buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
                && *lba == 60_000
            ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));

        mirrors.push(Child::present(m1));
        let mut m2 = mock_mirror();
        m2.expect_open_zone()
            .with(eq(60_000))
            .once()
            .return_once(|_| Box::pin(future::ok::<(), Error>(())));
        m2.expect_write_at()
            .once()
            .withf(|buf, lba|
                buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
                && *lba == 60_000
            ).return_once(|_, _| Box::pin( future::ok::<(), Error>(())));
        mirrors.push(Child::present(m2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 120_000).now_or_never().unwrap().unwrap();
    }

    /// vdev_raid should buffer writes of less than a stripe.  They won't be
    /// sent to the mirrors until flush_zone or finish_zone
    #[test]
    fn partial_stripe() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE: LbaT = 2;

        let mut mirrors = Vec::<Child>::new();

        let bd = || {
            let mut bd = mock_mirror();
            bd.expect_open_zone()
                .with(eq(60_000))
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_writev_at()
                .never();
            bd
        };

        let bd0 = bd();
        let bd1 = bd();
        let bd2 = bd();

        mirrors.push(Child::present(bd0));
        mirrors.push(Child::present(bd1));
        mirrors.push(Child::present(bd2));

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      mirrors.into_boxed_slice());
        let dbs = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        vdev_raid.open_zone(1).now_or_never().unwrap().unwrap();
        vdev_raid.write_at(wbuf, 1, 120_000).now_or_never().unwrap().unwrap();
    }
}
// LCOV_EXCL_STOP
