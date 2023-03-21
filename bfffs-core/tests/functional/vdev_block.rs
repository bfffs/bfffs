// vim: tw=80
/// These tests use a real VdevBlock object
    use bfffs_core::{vdev::*, vdev_block::*};
use divbuf::DivBufShared;
use pretty_assertions::assert_eq;
use rstest::{fixture, rstest};
use std::fs;
use tempfile::{Builder, TempDir};

#[fixture]
fn vdev() -> (VdevBlock, TempDir) {
    let len = 1 << 26;  // 64MB
    let tempdir = t!(
        Builder::new().prefix("test_vdev_block").tempdir()
    );
    let filename = tempdir.path().join("vdev");
    let file = t!(fs::File::create(&filename));
    t!(file.set_len(len));
    let vdev = VdevBlock::create(filename, None).unwrap();
    (vdev, tempdir)
}

#[rstest]
fn lba2zone(vdev: (VdevBlock, TempDir)) {
    assert_eq!(vdev.0.lba2zone(0), None);
    assert_eq!(vdev.0.lba2zone(9), None);
    assert_eq!(vdev.0.lba2zone(10), Some(0));
    assert_eq!(vdev.0.lba2zone((1 << 16) - 1), Some(0));
    assert_eq!(vdev.0.lba2zone(1 << 16), Some(1));
}

#[rstest]
fn size(vdev: (VdevBlock, TempDir)) {
    assert_eq!(vdev.0.size(), 16_384);
}

#[rstest]
fn zone_limits(vdev: (VdevBlock, TempDir)) {
    assert_eq!(vdev.0.zone_limits(0), (10, 1 << 16));
    assert_eq!(vdev.0.zone_limits(1), (1 << 16, 2 << 16));
}

#[rstest]
#[tokio::test]
async fn check_iovec_bounds_within(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 4096]);
    let wbuf = dbs.try_const().unwrap();
    let lba = vdev.0.size() - 1;
    vdev.0.write_at(wbuf, lba).await.unwrap();
}

#[should_panic]
#[rstest]
#[tokio::test]
async fn check_iovec_bounds_over(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 4096]);
    let wbuf = dbs.try_const().unwrap();
    let lba = vdev.0.size();
    vdev.0.write_at(wbuf, lba).await.unwrap();
}

#[should_panic]
#[rstest]
#[tokio::test]
async fn check_iovec_bounds_spans(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 8192]);
    let wbuf = dbs.try_const().unwrap();
    let lba = vdev.0.size() - 1;
    vdev.0.write_at(wbuf, lba).await.unwrap();
}

#[rstest]
#[tokio::test]
async fn check_sglist_bounds_within(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 4096]);
    let wbuf = dbs.try_const().unwrap();
    let wbuf0 = wbuf.slice_to(1024);
    let wbuf1 = wbuf.slice_from(1024);
    let wbufs = vec![wbuf0, wbuf1];
    let lba = vdev.0.size() - 1;
    vdev.0.writev_at(wbufs, lba).await.unwrap();
}

#[should_panic]
#[rstest]
#[tokio::test]
async fn check_sglist_bounds_over(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 4096]);
    let wbuf = dbs.try_const().unwrap();
    let wbuf0 = wbuf.slice_to(1024);
    let wbuf1 = wbuf.slice_from(1024);
    let wbufs = vec![wbuf0, wbuf1];
    let lba = vdev.0.size();
    vdev.0.writev_at(wbufs, lba).await.unwrap();
}

#[should_panic]
#[rstest]
#[tokio::test]
async fn check_sglist_bounds_spans(vdev: (VdevBlock, TempDir)) {
    let dbs = DivBufShared::from(vec![42u8; 8192]);
    let wbuf = dbs.try_const().unwrap();
    let wbuf0 = wbuf.slice_to(5120);
    let wbuf1 = wbuf.slice_from(5120);
    let wbufs = vec![wbuf0, wbuf1];
    let lba = vdev.0.size() - 1;
    vdev.0.writev_at(wbufs, lba).await.unwrap();
}
