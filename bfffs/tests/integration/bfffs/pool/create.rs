use std::{fs, path::PathBuf};

use assert_cmd::prelude::*;
use bfffs_core::{
    controller::Controller,
    device_manager::DevManager,
    property::{Property, PropertyName, PropertySource},
    vdev::Vdev,
    vdev_file::VdevFile,
};
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::bfffs;

type Harness = (Vec<PathBuf>, TempDir);

/// Create a single temporary file for backing store
#[fixture]
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix(concat!(module_path!(), "."))
        .tempdir()
        .unwrap();
    let mut paths = Vec::new();
    for i in 0..6 {
        let filename = tempdir.path().join(format!("vdev.{}", i));
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        let pb = filename.to_path_buf();
        paths.push(pb);
    }
    (paths, tempdir)
}

async fn open(pool_name: &str, filenames: &[PathBuf]) -> Controller {
    let dev_manager = DevManager::default();
    for pb in filenames {
        dev_manager.taste(pb.clone()).await.unwrap();
    }
    let uuid = dev_manager
        .importable_pools()
        .iter()
        .find(|(name, _uuid)| *name == pool_name)
        .unwrap()
        .1;
    let db = dev_manager.import_by_uuid(uuid).await.unwrap();
    Controller::new(db)
}

#[rstest]
#[tokio::test]
async fn atime(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("--properties")
        .arg("atime=off")
        .arg(pool_name)
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    // Hard-code tree id until BFFFS supports multiple datasets
    let controller = open(pool_name, &filenames[0..1]).await;
    let fs = controller.new_fs(pool_name).await.unwrap();
    let (val, src) = fs.get_prop(PropertyName::Atime).await.unwrap();
    assert_eq!(val, Property::Atime(false));
    assert_eq!(src, PropertySource::LOCAL);
}

/// Try to create a pool backed by a nonexistent file
#[test]
fn enoent() {
    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg("/tmp/does_not_exist_a8hra3hraw938hfwf")
        .assert()
        .failure();
    // TODO: check exact error message, once BFFFS has robust error handling
}

#[test]
fn help() {
    bfffs()
        .arg("pool")
        .arg("create")
        .arg("-h")
        .assert()
        .success();
}

/// Multiple properties may be comma-delimited.
#[rstest]
#[tokio::test]
async fn multiprops(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("--properties")
        .arg("atime=off,record_size=65536")
        .arg(pool_name)
        .arg(&filenames[0])
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    open("mypool", &filenames[0..1]).await;
}

// Create a RAID pool
#[rstest]
#[tokio::test]
async fn raid(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg("raid")
        .arg("3")
        .arg("1")
        .arg(&filenames[0])
        .arg(&filenames[1])
        .arg(&filenames[2])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..3]).await;
}

#[rstest]
#[tokio::test]
async fn recsize(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("--properties")
        .arg("record_size=65536")
        .arg(pool_name)
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    let controller = open(pool_name, &filenames[0..1]).await;
    let fs = controller.new_fs(pool_name).await.unwrap();
    let (val, src) = fs.get_prop(PropertyName::RecordSize).await.unwrap();
    assert_eq!(val, Property::RecordSize(16));
    assert_eq!(src, PropertySource::LOCAL);
}

// Stripe a pool across two disks
#[rstest]
#[tokio::test]
async fn stripe(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg(&filenames[0])
        .arg(&filenames[1])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..2]).await;
}

// Create a pool striped across two raid arrays
#[rstest]
#[tokio::test]
async fn striped_raid(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg("raid")
        .arg("3")
        .arg("1")
        .arg(&filenames[0])
        .arg(&filenames[1])
        .arg(&filenames[2])
        .arg("raid")
        .arg("3")
        .arg("1")
        .arg(&filenames[3])
        .arg(&filenames[4])
        .arg(&filenames[5])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..6]).await;
}

#[rstest]
#[tokio::test]
async fn zone_size(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("--zone-size")
        .arg("512")
        .arg("mypool")
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    let (vdev, _) = VdevFile::open(filenames[0].clone()).await.unwrap();
    assert_eq!(2, vdev.zones());
}
