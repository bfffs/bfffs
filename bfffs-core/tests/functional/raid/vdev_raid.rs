// vim: tw=80
use bfffs_core::{mirror::Mirror, raid::VdevRaid};
use std::{fs, path::PathBuf};
use tempfile::Builder;

#[test]
#[should_panic]
fn create_redundancy_too_big() {
    // VdevRaid::create should panic if the stripesize is greater than the
    // number of disks
    let len = 1 << 30;  // 1 GB
    let num_disks = 5;
    let stripesize = 3;
    let redundancy = 3;
    let tempdir =
        t!(Builder::new().prefix("create_redundancy_too_big").tempdir());
    let mirrors = (0..num_disks).map(|i| {
        let mut fname = PathBuf::from(tempdir.path());
        fname.push(format!("vdev.{i}"));
        let file = t!(fs::File::create(&fname));
        t!(file.set_len(len));
        Mirror::create(&[fname], None).unwrap()
    }).collect::<Vec<_>>();
    VdevRaid::create(None, stripesize, redundancy, mirrors);
}

#[test]
#[should_panic]
fn create_stripesize_too_big() {
    // VdevRaid::create should panic if the stripesize is greater than the
    // number of disks
    let len = 1 << 30;  // 1 GB
    let num_disks = 3;
    let stripesize = 4;
    let redundancy = 1;
    let tempdir =
        t!(Builder::new().prefix("create_stripesize_too_big").tempdir());
    let mirrors = (0..num_disks).map(|i| {
        let mut fname = PathBuf::from(tempdir.path());
        fname.push(format!("vdev.{i}"));
        let file = t!(fs::File::create(&fname));
        t!(file.set_len(len));
        Mirror::create(&[fname], None).unwrap()
    }).collect::<Vec<_>>();
    VdevRaid::create(None, stripesize, redundancy, mirrors);
}

/// These tests use real VdevBlock and VdevLeaf objects
mod vdev_raid {

    use bfffs_core::{
        *,
        mirror::Mirror,
        raid::*,
        vdev::Vdev,
    };
    use divbuf::DivBufShared;
    use futures::{
        FutureExt,
        TryFutureExt,
        TryStreamExt,
        stream::FuturesUnordered
    };
    use rand::{Rng, thread_rng};
    use rstest::rstest;
    use rstest_reuse::{apply, template};
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        num::NonZeroU64,
        path::PathBuf,
        sync::Arc
    };
    use super::super::super::*;
    use tempfile::{Builder, TempDir};

    struct Harness {
        vdev: Arc<VdevRaid>,
        _tempdir: TempDir,
        n: i16,
        k: i16,
        f: i16,
        chunksize: LbaT
    }

    async fn harness(n: i16, k: i16, f: i16, chunksize: LbaT) -> Harness {
        let len = 1 << 30;  // 1 GB
        let tempdir = t!(Builder::new().prefix("test_vdev_raid").tempdir());
        let mirrors = (0..n).map(|i| {
            let mut fname = PathBuf::from(tempdir.path());
            fname.push(format!("vdev.{i}"));
            let file = t!(fs::File::create(&fname));
            t!(file.set_len(len));
            Mirror::create(&[fname], None).unwrap()
        }).collect::<Vec<_>>();
        let cs = NonZeroU64::new(chunksize);
        let vdev = Arc::new(VdevRaid::create(cs, k, f, mirrors));
        vdev.open_zone(0).await
            .expect("open_zone");
        Harness { vdev, _tempdir: tempdir, n, k, f, chunksize }
    }

    #[template]
    #[rstest(h,
             // Stupid mirror
             case(harness(2, 2, 1, 1)),
             // Smallest possible PRIMES configuration
             case(harness(3, 3, 1, 2)),
             // Smallest PRIMES declustered configuration
             case(harness(5, 4, 1, 2)),
             // Smallest double-parity configuration
             case(harness(5, 5, 2, 2)),
             // Smallest non-ideal PRIME-S configuration
             case(harness(7, 4, 1, 2)),
             // Smallest triple-parity configuration
             case(harness(7, 7, 3, 2)),
             // Smallest quad-parity configuration
             case(harness(11, 9, 4, 2)),
     )]
    // XXX Should be called "all_configs", but can't due to
    // https://github.com/la10736/rstest/issues/124
    fn raid_configs(h: Harness) {}

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

    async fn write_read(
        vr: Arc<VdevRaid>,
        wbufs: Vec<IoVec>,
        rbufs: Vec<IoVecMut>,
        zone: ZoneT,
        start_lba: LbaT)
    {
        let mut write_lba = start_lba;
        let mut read_lba = start_lba;
        wbufs.into_iter()
        .map(|wb| {
            let lbas = (wb.len() / BYTES_PER_LBA) as LbaT;
            let fut = vr.write_at(wb, zone, write_lba);
            write_lba += lbas;
            fut
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .and_then(|_| {
            rbufs.into_iter()
            .map(|rb| {
                let lbas = (rb.len() / BYTES_PER_LBA) as LbaT;
                let fut = vr.clone().read_at(rb, read_lba);
                read_lba += lbas;
                fut
            }).collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
        }).await
        .unwrap();
    }

    async fn write_read0(
        vr: Arc<VdevRaid>,
        wbufs: Vec<IoVec>,
        rbufs: Vec<IoVecMut>)
    {
        let zl = vr.zone_limits(0);
        write_read(vr, wbufs, rbufs, 0, zl.0).await
    }

    async fn write_read_n_stripes(
        vr: Arc<VdevRaid>,
        chunksize: LbaT,
        k: i16,
        f: i16,
        s: usize)
    {
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        write_read0(vr, vec![wbuf1], vec![dbsr.try_mut().unwrap()]).await;
        assert_eq!(wbuf0, dbsr.try_const().unwrap());
    }

    async fn writev_read_n_stripes(
        vr: Arc<VdevRaid>,
        chunksize: LbaT,
        k: i16,
        f: i16,
        s: usize)
    {
        let zl = vr.zone_limits(0);
        let (dbsw, dbsr) = make_bufs(chunksize, k, f, s);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(wbuf.len() / 2);
        let sglist = vec![wbuf_l, wbuf_r];
        vr.writev_at_one(&sglist, zl.0)
        .then(|write_result| {
            write_result.expect("writev_at_one");
            vr.read_at(dbsr.try_mut().unwrap(), zl.0)
        }).await
        .expect("read_at");
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    // read_at should work when directed at the middle of the stripe buffer
    #[rstest(h, case(harness(3, 3, 1, 16)))]
    #[tokio::test]
    #[awt]
    async fn read_partial_at_middle_of_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let mut wbuf = dbsw.try_const().unwrap().slice_to(2 * BYTES_PER_LBA);
        let _ = wbuf.split_off(2 * BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_begin = rbuf.split_to(BYTES_PER_LBA);
            let rbuf_middle = rbuf.split_to(BYTES_PER_LBA);
            write_read0(h.vdev, vec![wbuf.clone()],
                        vec![rbuf_begin, rbuf_middle]).await;
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    // Read a stripe in several pieces, from disk
    #[rstest(h, case(harness(7, 7, 1, 16)))]
    #[tokio::test]
    #[awt]
    async fn read_parts_of_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let cs = h.chunksize as usize;
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
            write_read0(h.vdev, vec![wbuf.clone()],
                        vec![rbuf0, rbuf1, rbuf2, rbuf3, rbuf4, rbuf5, rbuf6])
            .await;
        }
        assert_eq!(&wbuf[..], &dbsr.try_const().unwrap()[..]);
    }

    // Read the end of one stripe and the beginning of another
    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn read_partial_stripes(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        {
            let mut rbuf_m = dbsr.try_mut().unwrap();
            let rbuf_b = rbuf_m.split_to(BYTES_PER_LBA);
            let l = rbuf_m.len();
            let rbuf_e = rbuf_m.split_off(l - BYTES_PER_LBA);
            write_read0(h.vdev, vec![wbuf.clone()],
                        vec![rbuf_b, rbuf_m, rbuf_e]).await;
        }
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[should_panic(expected = "Read beyond the stripe buffer into unallocated space")]
    #[tokio::test]
    #[awt]
    async fn read_past_end_of_stripe_buffer(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let rbuf = dbsr.try_mut().unwrap();
        write_read0(h.vdev, vec![wbuf_short], vec![rbuf]).await;
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[should_panic(expected = "Read beyond the stripe buffer into unallocated space")]
    #[tokio::test]
    #[awt]
    async fn read_starts_past_end_of_stripe_buffer(#[future] h: Harness)
    {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        let mut rbuf = dbsr.try_mut().unwrap();
        let rbuf_r = rbuf.split_off(BYTES_PER_LBA);
        write_read0(h.vdev, vec![wbuf_short], vec![rbuf_r]).await;
    }

    #[apply(raid_configs)]
    #[tokio::test]
    #[awt]
    async fn write_read_one_stripe(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 1).await;
    }

    // read_at_one/write_at_one with a large configuration
    #[rstest(h, case(harness(41, 19, 3, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_read_one_stripe_jumbo(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 1).await;
    }

    #[apply(raid_configs)]
    #[tokio::test]
    #[awt]
    async fn write_read_two_stripes(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 2).await;
    }

    // read_at_multi/write_at_multi with a large configuration
    #[rstest(h, case(harness(41, 19, 3, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_read_two_stripes_jumbo(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 2).await;
    }

    // Write at least three rows to the layout.  Writing three rows guarantees
    // that some disks will have two data chunks separated by one parity chunk,
    // which tests the ability of VdevRaid::read_at to split a single disk's
    // data up into multiple VdevBlock::readv_at calls.
    #[apply(raid_configs)]
    #[tokio::test]
    #[awt]
    async fn write_read_three_rows(#[future] h: Harness) {
        let rows = 3;
        let stripes = div_roundup((rows * h.n) as usize, h.k as usize);
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, stripes).await;
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_completes_a_partial_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(h.vdev, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]).await;
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_completes_a_partial_stripe_and_writes_a_bit_more(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 2);
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
            write_read0(h.vdev, vec![wbuf_l, wbuf_r], vec![rbuf]).await;
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_completes_a_partial_stripe_and_writes_another(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(h.vdev, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]).await;
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_completes_a_partial_stripe_and_writes_two_more(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 3);
        let wbuf = dbsw.try_const().unwrap();
        let mut wbuf_l = wbuf.clone();
        let wbuf_r = wbuf_l.split_off(BYTES_PER_LBA);
        write_read0(h.vdev, vec![wbuf_l, wbuf_r],
                    vec![dbsr.try_mut().unwrap()]).await;
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_completes_a_partial_stripe_and_writes_two_more_with_leftovers(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 4);
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
            write_read0(h.vdev, vec![wbuf_l, wbuf_r], vec![rbuf]).await;
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_partial_at_start_of_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read0(h.vdev, vec![wbuf_short], vec![rbuf_short]).await;
            // After write returns, the DivBufShared should no longer be needed.
            drop(dbsw);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA]);
    }

    // Write less than an LBA at the start of a stripe
    #[rstest(h, case(harness(2, 2, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn write_tiny_at_start_of_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA * 3 / 4);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            write_read0(h.vdev, vec![wbuf_short], vec![rbuf_short]).await;
            // After write returns, the DivBufShared should no longer be needed.
            drop(dbsw);
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA * 3 / 4],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA * 3 / 4]);
        // The remainder of the LBA should've been zero-filled
        let zbuf = vec![0u8; BYTES_PER_LBA / 4];
        assert_eq!(&zbuf[..],
                   &dbsr.try_const().unwrap()[BYTES_PER_LBA * 3 / 4..]);
    }

    // Write a whole stripe plus a fraction of an LBA more
    #[rstest(h, case(harness(2, 2, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn write_stripe_and_a_bit_more(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 2);
        let wbuf = dbsw.try_const().unwrap();
        let wcut = wbuf.len() / 2 + 1024;
        let rcut = wbuf.len() / 2 + BYTES_PER_LBA;
        let wbuf_short = wbuf.slice_to(wcut);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(rcut);
            write_read0(h.vdev, vec![wbuf_short], vec![rbuf_short]).await;
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
    #[rstest(h, case(harness(3, 3, 1, 16)))]
    #[tokio::test]
    #[awt]
    async fn write_partial_at_middle_of_stripe(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap().slice_to(2 * BYTES_PER_LBA);
        let wbuf_begin = wbuf.slice_to(BYTES_PER_LBA);
        let wbuf_middle = wbuf.slice_from(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let _ = rbuf.split_off(2 * BYTES_PER_LBA);
            write_read0(h.vdev, vec![wbuf_begin, wbuf_middle], vec![rbuf]).await;
        }
        assert_eq!(&wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA],
                   "{:#?}\n{:#?}", &wbuf[..],
                   &dbsr.try_const().unwrap()[0..2 * BYTES_PER_LBA]);
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_two_stripes_with_leftovers(#[future] h: Harness) {
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 3);
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
            write_read0(h.vdev, vec![wbuf], vec![rbuf]).await;
        }
        assert_eq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    #[apply(raid_configs)]
    #[tokio::test]
    #[awt]
    async fn writev_read_one_stripe(#[future] h: Harness) {
        writev_read_n_stripes(h.vdev, h.chunksize,
                              h.k, h.f, 1).await;
    }

    // Erasing an open zone should fail
    #[should_panic(expected = "Tried to erase an open zone")]
    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn zone_erase_open(#[future] h: Harness) {
        let zone = 1;
        h.vdev.open_zone(zone)
        .and_then(|_| h.vdev.erase_zone(0)).await
        .expect("zone_erase_open");
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn zone_read_closed(#[future] h: Harness) {
        let zone = 0;
        let zl = h.vdev.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        h.vdev.write_at(wbuf0, zone, zl.0).await.unwrap();
        h.vdev.finish_zone(zone).await.unwrap();
        h.vdev.read_at(rbuf, zl.0).await.unwrap();
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    // Close a zone with an incomplete StripeBuffer, then read back from it
    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn zone_read_closed_partial(#[future] h: Harness) {
        let zone = 0;
        let zl = h.vdev.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf = dbsw.try_const().unwrap();
        let wbuf_short = wbuf.slice_to(BYTES_PER_LBA);
        {
            let mut rbuf = dbsr.try_mut().unwrap();
            let rbuf_short = rbuf.split_to(BYTES_PER_LBA);
            h.vdev.write_at(wbuf_short, zone, zl.0).await.unwrap();
            h.vdev.finish_zone(zone).await.unwrap();
            h.vdev.read_at(rbuf_short, zl.0).await.unwrap();
        }
        assert_eq!(&wbuf[0..BYTES_PER_LBA],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA]);
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[should_panic(expected = "Can't write to a closed zone")]
    #[tokio::test]
    #[awt]
    // Writing to an explicitly closed a zone fails
    async fn zone_write_explicitly_closed(#[future] h: Harness) {
        let zone = 1;
        let (start, _) = h.vdev.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        h.vdev.open_zone(zone)
            .and_then(|_| h.vdev.finish_zone(zone)).await
            .expect("open and finish");
        write_read(h.vdev, vec![wbuf0], vec![rbuf], zone, start).await;
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    #[should_panic(expected = "Can't write to a closed zone")]
    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    // Writing to a closed zone should fail
    async fn zone_write_implicitly_closed(#[future] h: Harness) {
        let zone = 1;
        let (start, _) = h.vdev.zone_limits(zone);
        let dbsw = DivBufShared::from(vec![0;4096]);
        let wbuf = dbsw.try_const().unwrap();
        h.vdev.write_at(wbuf, zone, start).await.unwrap();
    }

    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    // Opening a closed zone should allow writing
    async fn zone_write_open(#[future] h: Harness) {
        let zone = 1;
        let (start, _) = h.vdev.zone_limits(zone);
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
        let wbuf0 = dbsw.try_const().unwrap();
        let wbuf1 = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        h.vdev.open_zone(zone).await.expect("open_zone");
        write_read(h.vdev, vec![wbuf0], vec![rbuf], zone, start).await;
        assert_eq!(wbuf1, dbsr.try_const().unwrap());
    }

    // Two zones can be open simultaneously
    #[rstest(h, case(harness(3, 3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn zone_write_two_zones(#[future] h: Harness) {
        let vdev_raid = h.vdev;
        for zone in 1..3 {
            let (start, _) = vdev_raid.zone_limits(zone);
            let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 1);
            let wbuf0 = dbsw.try_const().unwrap();
            let wbuf1 = dbsw.try_const().unwrap();
            let rbuf = dbsr.try_mut().unwrap();
            vdev_raid.open_zone(zone).await.expect("open_zone");
            write_read(vdev_raid.clone(), vec![wbuf0], vec![rbuf], zone, start).await;
            assert_eq!(wbuf1, dbsr.try_const().unwrap());
        }
    }
}

mod persistence {
    use bfffs_core::{
        label::*,
        mirror::Mirror,
        vdev_block::*,
        vdev::Vdev,
        vdev_file::*,
        raid::{self, VdevRaid, VdevRaidApi},
    };
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64,
        path::PathBuf,
        sync::Arc
    };
    use tempfile::{Builder, TempDir};

    const GOLDEN_VDEV_RAID_LABEL: [u8; 124] = [
        // Past the VdevFile::Label, we have a raid::Label
        // First comes the VdevRaid discriminant
        0x01, 0x00, 0x00, 0x00,
        // Then the VdevRaid label, beginning with a UUID
                                0x2f, 0x27, 0x51, 0xe5,
        0xe8, 0x58, 0x45, 0x1b, 0x92, 0xb5, 0x24, 0x0f,
        0x23, 0x7b, 0xc9, 0xbe,
        // Then the chunksize in 64 bits
                                0x02, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        // disks_per_stripe in 16 bits
                                0x03, 0x00,
        // redundancy level in 16 bits
                                            0x01, 0x00,
        // LayoutAlgorithm discriminant in 32 bits
        0x00, 0x00, 0x00, 0x00,
        // Vector of children's UUIDs.  A 64-bit count of children, then each
        // UUID is 64-bits long
                                0x05, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x2c, 0x9a, 0x3b, 0xfd,
        0x9c, 0xaf, 0x4c, 0xf8, 0xba, 0x40, 0xc6, 0x64,
        0x2f, 0x88, 0x5a, 0x01, 0x62, 0xbf, 0xbd, 0x54,
        0x9f, 0xa3, 0x41, 0x65, 0x8e, 0x75, 0xfa, 0x7e,
        0xcb, 0x52, 0x45, 0x2e, 0xd3, 0x14, 0x96, 0x91,
        0x17, 0x18, 0x4a, 0xc5, 0xbd, 0x06, 0x24, 0xd1,
        0xd2, 0xa9, 0x6d, 0x67, 0x24, 0x31, 0xb8, 0x32,
        0x01, 0x63, 0x45, 0xd5, 0xa7, 0x9c, 0xec, 0x10,
        0x6b, 0xfe, 0x9b, 0x7c, 0xb8, 0x79, 0x31, 0x82,
        0x2f, 0xc9, 0x4c, 0x40, 0x84, 0xd3, 0xff, 0xd5,
        0xb8, 0x3b, 0x18, 0x8e,
    ];

    type Harness = (Arc<VdevRaid>, TempDir, Vec<PathBuf>);
    #[fixture]
    fn harness() -> Harness {
        let num_disks = 5;
        let len = 1 << 26;  // 64 MB
        let tempdir = t!(
            Builder::new().prefix("test_vdev_raid_persistence").tempdir()
        );
        let paths = (0..num_disks).map(|i| {
            let mut fname = PathBuf::from(tempdir.path());
            fname.push(format!("vdev.{i}"));
            let file = t!(fs::File::create(&fname));
            t!(file.set_len(len));
            fname
        }).collect::<Vec<_>>();
        let mirrors = paths.iter().map(|fname|
            Mirror::create(&[fname], None).unwrap()
        ).collect::<Vec<_>>();
        let cs = NonZeroU64::new(2);
        let vdev_raid = Arc::new(VdevRaid::create(cs, 3, 1, mirrors));
        (vdev_raid, tempdir, paths)
    }

    // Testing VdevRaid::open with golden labels is too hard, because we
    // need to store separate golden labels for each VdevLeaf.  Instead, we'll
    // just check that we can open-after-write
    #[rstest]
    #[tokio::test]
    async fn open_after_write(harness: Harness) {
        let (old_raid, _tempdir, paths) = harness;
        let uuid = old_raid.uuid();
        let label_writer = LabelWriter::new(0);
        old_raid.write_label(label_writer).await.unwrap();
        let mut combined = Vec::new();
        for path in paths {
            let (leaf, reader) = VdevFile::open(path).await.unwrap();
            let mirror_children = vec![(VdevBlock::new(leaf), reader)];
            let (mirror, reader) = Mirror::open(None, mirror_children);
            combined.push((mirror, reader));
        }
        let (vdev_raid, _) = raid::open(Some(uuid), combined);
        assert_eq!(uuid, vdev_raid.uuid());
    }

    #[rstest]
    #[tokio::test]
    async fn write_label(harness: Harness) {
        let label_writer = LabelWriter::new(0);
        harness.0.write_label(label_writer).await.unwrap();
        for path in harness.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(112)).unwrap();   // Skip leaf, mirror labels
            f.read_exact(&mut v).unwrap();
            // Uncomment this block to save the binary label for inspection
            /* {
                use std::fs::File;
                use std::io::Write;
                let mut df = File::create("/tmp/label.bin").unwrap();
                df.write_all(&v[..]).unwrap();
                println!("UUID is {}", harness.0.uuid());
            } */
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[0..4], &GOLDEN_VDEV_RAID_LABEL[0..4]);
            assert_eq!(&v[20..44], &GOLDEN_VDEV_RAID_LABEL[20..44]);
            // Rest of the buffer should be zero-filled
            assert!(v[124..].iter().all(|&x| x == 0));
        }
    }
}
