// vim: tw=80
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc
};

use divbuf::DivBufShared;
use futures::{
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    stream::FuturesUnordered
};
use nonzero_ext::nonzero;
use rstest::{fixture, rstest};
use rstest_reuse::{apply, template};
use tempfile::{Builder, TempDir};

use bfffs_core::{
    BYTES_PER_LBA,
    Error,
    LbaT,
    label::*,
    mirror::Mirror,
    vdev::{FaultedReason, Health, Vdev},
    raid::{Manager, VdevRaid, VdevRaidApi},
    Uuid
};

use crate::assert_bufeq;

/// Make a pair of buffers for reading and writing.  The contents of the write
/// buffer will consist of u64's holding the offset from the start of the buffer
/// in bytes.
fn make_bufs(chunksize: LbaT, k: i16, f: i16, s: usize) ->
    (DivBufShared, DivBufShared)
{
    const Z: usize = std::mem::size_of::<LbaT>();
    let chunks = s * (k - f) as usize;
    let lbas = chunksize * chunks as LbaT;
    let bytes = BYTES_PER_LBA * lbas as usize;
    let wvec = (0..bytes).map(|i| {
        let bofs = i - i % Z;
        let bshift = 8 * (Z - 1 - i % Z);
        ((bofs >> bshift) & 0xFF) as u8
    }).collect::<Vec<_>>();
    let dbsw = DivBufShared::from(wvec);
    let dbsr = DivBufShared::from(vec![0u8; bytes]);
    (dbsw, dbsr)
}

struct Harness {
    vdev: Arc<VdevRaid>,
    _tempdir: TempDir,
    paths: Vec<PathBuf>,
    n: i16,
    k: i16,
    f: i16,
    chunksize: LbaT,
}

async fn harness(n: i16, k: i16, f: i16, chunksize: LbaT) -> Harness {
    let len = 1 << 30;  // 1 GB
    let tempdir = t!(
        Builder::new().prefix("test_vdev_raid_persistence").tempdir()
    );
    let paths = (0..n).map(|i| {
        let mut fname = PathBuf::from(tempdir.path());
        fname.push(format!("vdev.{i}"));
        let file = t!(fs::File::create(&fname));
        t!(file.set_len(len));
        fname
    }).collect::<Vec<_>>();
    let mirrors = paths.iter().map(|fname|
        Mirror::create(&[fname], None).unwrap()
    ).collect::<Vec<_>>();
    let cs = NonZeroU64::new(chunksize);
    let vdev = Arc::new(VdevRaid::create(cs, k, f, mirrors));
    vdev.open_zone(0).await
        .expect("open_zone");
    Harness{vdev, _tempdir: tempdir, paths, n, k, f, chunksize}
}

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

mod erase_zone {
    use super::*;

    #[fixture]
    async fn harness() -> Harness {
        super::harness(3, 3, 1, 2).await
    }

    // Erasing an open zone should fail
    #[should_panic(expected = "Tried to erase an open zone")]
    #[rstest]
    #[tokio::test]
    #[awt]
    async fn erase_open_zone(#[future] harness: Harness) {
        let zone = 1;
        harness.vdev.open_zone(zone)
        .and_then(|_| harness.vdev.erase_zone(0)).await
        .expect("zone_erase_open");
    }
}

/// Tests related to fault-tolerance
mod errors {
    use bfffs_core::{
        *,
        mirror::Mirror,
        raid::*,
        vdev::Vdev,
    };
    use function_name::named;
    use rstest::rstest;
    use std::{
        num::NonZeroU64,
        sync::Arc
    };
    use super::make_bufs;
    use super::super::super::*;
    use tempfile::{Builder, TempDir};

    #[derive(Clone, Copy, Debug)]
    struct Config {
        /// Number of disks in the RAID layout
        n: i16,
        /// Number of disks in each RAID stripe
        k: i16,
        /// Number of parity disks in each RAID stripe
        f: i16,
    }
    fn config(n: i16, k: i16, f: i16) -> Config {
        Config{n, k, f}
    }

    struct Harness {
        vdev: Arc<VdevRaid>,
        _tempdir: TempDir,
        gnops: Vec<Gnop>,
    }

    async fn harness(config: Config, chunksize: LbaT) -> Harness {
        let tempdir = Builder::new()
            .prefix("vdev_raid::errors")
            .tempdir()
            .unwrap();
        let cs = NonZeroU64::new(chunksize);
        let gnops = (0..config.n).map(|_| Gnop::new().unwrap())
            .collect::<Vec<_>>();
        let mirrors = gnops.iter()
            .map(|gnop| {
                Mirror::create(&[gnop.as_path()], None).unwrap()
            }).collect::<Vec<_>>();
        let vdev = Arc::new(VdevRaid::create(cs, config.k, config.f, mirrors));
        vdev.open_zone(0).await.unwrap();
        Harness { vdev, _tempdir: tempdir, gnops}
    }

    mod read_at {
        use super::*;

        /// Use gnop to inject read errors in leaf vdevs, and verify that
        /// VdevRaid can recover.
        // Full coverage is provided in the unit tests.
        #[named]
        #[rstest]
        #[tokio::test]
        #[ignore = "https://github.com/bfffs/bfffs/issues/297" ]
        async fn recoverable_eio() {
            require_root!();
            // Stupid mirror; trivial configuration
            let c = config(2, 2, 1);
            // Read from enough stripes that the defective disk must be present
            // in at least one.
            let stripes = c.n * 2;
            let h = harness(c, 1).await;
            let (dbsw, dbsr) = make_bufs(1, c.k, c.f, stripes as usize);
            let wbuf0 = dbsw.try_const().unwrap();
            let wbuf1 = dbsw.try_const().unwrap();
            let rbuf = dbsr.try_mut().unwrap();

            let zl = h.vdev.zone_limits(0);
            h.vdev.write_at(wbuf0, 0, zl.0).await.unwrap();
            h.gnops[0].error_prob(100);
            h.vdev.clone().read_at(rbuf, zl.0).await.unwrap();
            assert!(wbuf1[..] == dbsr.try_const().unwrap()[..],
                "miscompare!");
        }
    }

    mod read_spacemap {
        use super::*;

        /// Use gnop to inject read errors in leaf vdevs, and verify that
        /// VdevRaid can cope when reading the spacemap.
        // Full coverage is provided in the unit tests.
        #[named]
        #[rstest]
        #[tokio::test]
        async fn recoverable_eio() {
            require_root!();
            // Stupid mirror; trivial configuration
            let c = config(2, 2, 1);
            let h = harness(c, 1).await;
            let block = 0;
            let idx = 0;
            let (dbsw, dbsr) = make_bufs(1, 1, 0, 2);
            let wbuf0 = dbsw.try_const().unwrap();
            let wbuf1 = dbsw.try_const().unwrap();
            let rbuf = dbsr.try_mut().unwrap();

            h.vdev.write_spacemap(vec![wbuf0], idx, block).await.unwrap();
            h.gnops[0].error_prob(100);
            h.vdev.clone().read_spacemap(rbuf, idx).await.unwrap();
            assert!(wbuf1[..] == dbsr.try_const().unwrap()[..],
                "miscompare!");
        }
    }
}

mod fault {
    use super::*;

    /// Create a fully-clustered VdevRaid with `mirrors` members, each having
    /// `d` disks per mirror, and `f` redundancy at the RAID level
    async fn harness(nmirrors: usize, f: i16, d: usize) -> Harness
    {
        let chunksize = 1;
        let n = nmirrors * d;
        let k = nmirrors as i16;
        let len = 1 << 30;  // 1 GB
        let tempdir = Builder::new()
            .prefix("test_vdev_raid_fault")
            .tempdir()
            .unwrap();
        let paths = (0..n).map(|i| {
            let mut fname = PathBuf::from(tempdir.path());
            fname.push(format!("vdev.{i}"));
            let file = t!(fs::File::create(&fname));
            t!(file.set_len(len));
            fname
        }).collect::<Vec<_>>();
        let mut mirrors = Vec::new();
        for m in 0..nmirrors {
            let mirror = Mirror::create(&paths[(m * d)..(m * d + d)], None);
            mirrors.push(mirror.unwrap());
        }
        let cs = NonZeroU64::new(chunksize);
        let vdev = Arc::new(VdevRaid::create(cs, k, f, mirrors));
        vdev.open_zone(0).await
            .expect("open_zone");
        Harness{vdev, _tempdir: tempdir, paths, n: n as i16, k, f, chunksize}
    }

    #[rstest(h, case(harness(3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn disk_enoent(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let r = Arc::get_mut(&mut h.vdev).unwrap().fault(Uuid::new_v4());
        assert_eq!(Err(Error::ENOENT), r);
    }

    #[rstest(h, case(harness(3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn disk_missing(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let duuid = h.vdev.status().mirrors[0].leaves[0].uuid;

        Arc::get_mut(&mut h.vdev).unwrap().fault(duuid).unwrap();
        Arc::get_mut(&mut h.vdev).unwrap().fault(duuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].leaves[0].health,
                   Health::Faulted(FaultedReason::User));
    }

    /// Fault a disk which is the only child of a mirror
    #[rstest(h, case(harness(3, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn disk_only_child(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let duuid = h.vdev.status().mirrors[0].leaves[0].uuid;
        Arc::get_mut(&mut h.vdev).unwrap().fault(duuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health,
                   Health::Faulted(FaultedReason::User));

        // Ensure that basic vdev methods work on an array that's degraded like
        // this:
        assert_eq!(Some(0), h.vdev.lba2zone(32));
        assert_eq!(4, h.vdev.zones());
        assert_eq!(10, h.vdev.optimum_queue_depth());
        assert_eq!((20, 131072), h.vdev.zone_limits(0));
    }

    #[rstest(h, case(harness(3, 1, 2)))]
    #[tokio::test]
    #[awt]
    async fn disk_present(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let duuid = h.vdev.status().mirrors[0].leaves[0].uuid;
        Arc::get_mut(&mut h.vdev).unwrap().fault(duuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].leaves[0].health,
                   Health::Faulted(FaultedReason::User));
    }

    #[rstest(h, case(harness(3, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn mirror_present(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let muuid = h.vdev.status().mirrors[0].uuid;
        Arc::get_mut(&mut h.vdev).unwrap().fault(muuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health,
                   Health::Faulted(FaultedReason::User));
    }

    #[rstest(h, case(harness(3, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn mirror_missing(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let muuid = h.vdev.status().mirrors[0].uuid;
        Arc::get_mut(&mut h.vdev).unwrap().fault(muuid).unwrap();
        Arc::get_mut(&mut h.vdev).unwrap().fault(muuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health,
                   Health::Faulted(FaultedReason::User));
    }

    // Attempt to fault the entire RAID device
    #[rstest(h, case(harness(3, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn raid(#[future] h: Harness) {
        let mut h = h;  //rstest doesn't allow declaring args as mutable
        let ruuid = h.vdev.uuid();
        let r = Arc::get_mut(&mut h.vdev).unwrap().fault(ruuid);
        assert_eq!(Err(Error::EINVAL), r);
    }
}

/// Tests for the I/O path of VdevRaid.  Most do both reads and writes.
mod io {
    use super::*;

    use bfffs_core::{IoVec, IoVecMut, ZoneT};

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
    fn all_configs(h: Harness) {}

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

    async fn write_read_spacemap(
        vr: Arc<VdevRaid>,
        wbufs: Vec<IoVec>,
        rbuf: IoVecMut,
        idx: u32,
        block: LbaT)
    {
        vr.write_spacemap(wbufs, idx, block).await.unwrap();
        vr.read_spacemap(rbuf, idx).await.unwrap();
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
        assert_bufeq!(&wbuf0, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&wbuf, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&wbuf[..],
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
        assert_bufeq!(&wbuf[..], &dbsr.try_const().unwrap()[..]);
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
        assert_bufeq!(&wbuf[..], &dbsr.try_const().unwrap()[..]);
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

    #[rstest(h, case(harness(3, 3, 1, 1)))]
    #[tokio::test]
    #[awt]
    async fn read_spacemap(
        #[future] h: Harness,
        #[values(0, 1)]
        idx: u32
        ) {
        let (dbsw, dbsr) = make_bufs(1, 1, 0, 1);
        let wbuf = dbsw.try_const().unwrap();
        let rbuf = dbsr.try_mut().unwrap();
        let block = 0;
        write_read_spacemap(h.vdev, vec![wbuf.clone()], rbuf, idx, block).await;
        assert_bufeq!(&wbuf[..], &dbsr.try_const().unwrap()[..]);
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

    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn write_read_one_stripe(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 1).await;
    }

    // Read exactly one stripe's worth of data that isn't stripe-aligned, with
    // the end of the read falling somewhere on the last chunk of a stripe.
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[tokio::test]
    async fn read_one_stripe_unaligned(#[case] x: LbaT) {
        let h = harness(3,3,1,4).await;
        let zl = h.vdev.zone_limits(0);
        let stripe_lbas = h.chunksize * (h.k - h.f) as LbaT;
        let (dbsw, dbsr) = make_bufs(h.chunksize, h.k, h.f, 2);
        let wbufs = vec![dbsw.try_const().unwrap()];
        let mut rbuf = dbsr.try_mut().unwrap();
        let rbuf0 = rbuf.split_to((stripe_lbas - x) as usize * BYTES_PER_LBA);
        let rbuf1 = rbuf.split_to(stripe_lbas as usize * BYTES_PER_LBA);
        let rbufs = vec![rbuf0, rbuf1];
        write_read(h.vdev, wbufs, rbufs, 0, zl.0).await;
    }

    // read_at_one/write_at_one with a large configuration
    #[rstest(h, case(harness(41, 19, 3, 2)))]
    #[tokio::test]
    #[awt]
    async fn write_read_one_stripe_jumbo(#[future] h: Harness) {
        write_read_n_stripes(h.vdev, h.chunksize, h.k, h.f, 1).await;
    }

    #[apply(all_configs)]
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
    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn write_read_three_rows(#[future] h: Harness) {
        let rows = 3;
        let stripes = ((rows * h.n) as usize).div_ceil(h.k as usize);
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
        assert_bufeq!(&wbuf, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&dbsw.try_const().unwrap()[..],
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
        assert_bufeq!(&wbuf, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&wbuf, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&dbsw.try_const().unwrap()[..],
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
        assert_bufeq!(&wbuf[0..BYTES_PER_LBA],
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
        assert_bufeq!(&wbuf[0..BYTES_PER_LBA * 3 / 4],
                   &dbsr.try_const().unwrap()[0..BYTES_PER_LBA * 3 / 4]);
        // The remainder of the LBA should've been zero-filled
        let zbuf = vec![0u8; BYTES_PER_LBA / 4];
        assert_bufeq!(&zbuf[..],
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
        assert_bufeq!(&wbuf[0..wcut],
                   &dbsr.try_const().unwrap()[0..wcut]);
        // The remainder of the LBA should've been zero-filled
        let zbuf = vec![0u8; rcut - wcut];
        assert_bufeq!(&zbuf[..],
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
        assert_bufeq!(&wbuf[..],
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
        assert_bufeq!(&dbsw.try_const().unwrap()[..],
                   &dbsr.try_const().unwrap()[..]);
    }

    #[apply(all_configs)]
    #[tokio::test]
    #[awt]
    async fn writev_read_one_stripe(#[future] h: Harness) {
        writev_read_n_stripes(h.vdev, h.chunksize,
                              h.k, h.f, 1).await;
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
        assert_bufeq!(&wbuf1, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&wbuf[0..BYTES_PER_LBA],
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
        assert_bufeq!(&wbuf1, &dbsr.try_const().unwrap());
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
        assert_bufeq!(&wbuf1, &dbsr.try_const().unwrap());
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
            assert_bufeq!(&wbuf1, &dbsr.try_const().unwrap());
        }
    }
}

mod open {
    use super::*;

    #[fixture]
    async fn harness() -> Harness {
        super::harness(3, 3, 1, 2).await
    }

    /// It should be possible to import a raid when some children are missing
    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[tokio::test]
    #[awt]
    async fn missing_children(
        #[future] harness: Harness,
        #[case] missing: usize)
    {
        let uuid = harness.vdev.uuid();
        let label_writer = LabelWriter::new(0);
        harness.vdev.write_label(label_writer).await.unwrap();
        let old_status = harness.vdev.status();
        drop(harness.vdev);

        fs::remove_file(harness.paths[missing].clone()).unwrap();
        let mut manager = Manager::default();
        for path in harness.paths.iter() {
            let _ = manager.taste(path).await;
        }
        let (vr, _) = manager.import(uuid).await.unwrap();
        assert_eq!(uuid, vr.uuid());
        let status = vr.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        for i in 0..harness.paths.len() {
            assert_eq!(old_status.mirrors[i].uuid, status.mirrors[i].uuid);
            if i != missing {
                // BFFFS doesn't yet remember the paths of disks, whether
                // they're imported or not, missing or present.
                assert_eq!(old_status.mirrors[i].leaves[0].path,
                           status.mirrors[i].leaves[0].path);
            }
        }
    }
}

mod persistence {
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

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

    #[fixture]
    async fn harness() -> Harness {
        super::harness(5, 3, 1, 2).await
    }

    // Testing VdevRaid::open with golden labels is too hard, because we
    // need to store separate golden labels for each VdevLeaf.  Instead, we'll
    // just check that we can open-after-write
    #[rstest]
    //#[case(harness(5, 3, 1, 2))]
    #[tokio::test]
    #[awt]
    async fn open_after_write(#[future] harness: Harness) {
        let uuid = harness.vdev.uuid();
        let label_writer = LabelWriter::new(0);
        harness.vdev.write_label(label_writer).await.unwrap();
        drop(harness.vdev);

        let mut manager = Manager::default();
        for path in harness.paths.iter() {
            manager.taste(path).await.unwrap();
        }
        let (vdev_raid, _) = manager.import(uuid).await.unwrap();
        assert_eq!(uuid, vdev_raid.uuid());
    }

    #[rstest]
    #[tokio::test]
    #[awt]
    async fn write_label(#[future] harness: Harness) {
        let label_writer = LabelWriter::new(0);
        harness.vdev.write_label(label_writer).await.unwrap();
        for path in harness.paths {
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
                println!("UUID is {}", harness.vdev.uuid());
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
