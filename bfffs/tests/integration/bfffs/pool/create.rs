use std::{fs, path::PathBuf};

use assert_cmd::prelude::*;
use bfffs_core::{
    controller::Controller,
    database,
    mirror,
    property::{Property, PropertyName, PropertySource},
    vdev::Vdev,
    vdev_block,
    Uuid,
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
    for i in 0..12 {
        let filename = tempdir.path().join(format!("vdev.{i}"));
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        let pb = filename.to_path_buf();
        paths.push(pb);
    }
    (paths, tempdir)
}

async fn open(pool_name: &str, filenames: &[PathBuf]) -> Controller {
    let mut manager = database::Manager::default();
    for pb in filenames {
        manager.taste(pb.clone()).await.unwrap();
    }
    let uuid = manager
        .importable_pools()
        .iter()
        .find(|(name, _uuid)| *name == pool_name)
        .unwrap()
        .1;
    let db = manager.import_by_uuid(uuid).await.unwrap();
    Controller::new(db)
}

#[rstest]
#[tokio::test]
async fn atime(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .args(["pool", "create", "--properties", "atime=off"])
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
        .args(["pool", "create", "mypool"])
        .arg("/tmp/does_not_exist_a8hra3hraw938hfwf")
        .assert()
        .failure();
    // TODO: check exact error message, once BFFFS has robust error handling
}

#[test]
fn help() {
    bfffs().args(["pool", "create", "-h"]).assert().success();
}

/// Multiple properties may be comma-delimited.
#[rstest]
#[tokio::test]
async fn multiprops(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .args([
            "pool",
            "create",
            "--properties",
            "atime=off,recordsize=65536",
        ])
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
        .args(["pool", "create", "mypool"])
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    open("mypool", &filenames[0..1]).await;
}

// Create a mirrored pool
#[rstest]
#[tokio::test]
async fn mirror(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "mirror"])
        .args([&filenames[0], &filenames[1]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..2]).await;
}

// Create a RAID10 pool: a stripe of mirrors
#[rstest]
#[tokio::test]
async fn raid10(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "mirror"])
        .args([&filenames[0], &filenames[1]])
        .arg("mirror")
        .args([&filenames[2], &filenames[3]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..4]).await;
}

#[rstest]
#[tokio::test]
async fn recsize(harness: Harness) {
    let (filenames, _tempdir) = harness;
    let pool_name = "mypool";

    bfffs()
        .args(["pool", "create", "--properties", "recordsize=65536"])
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
        .args(["pool", "create", "mypool"])
        .args([&filenames[0], &filenames[1]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..2]).await;
}

// Create a raid50 pool: striped across two raid arrays
#[rstest]
#[tokio::test]
async fn raid50(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "raid", "3", "1"])
        .args([&filenames[0], &filenames[1], &filenames[2]])
        .args(["raid", "3", "1"])
        .args([&filenames[3], &filenames[4], &filenames[5]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..6]).await;
}

// Create a raid51 pool: a raid of mirrors
#[rstest]
#[tokio::test]
async fn raid51(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "raid", "3", "1", "mirror"])
        .args([&filenames[0], &filenames[1]])
        .arg("mirror")
        .args([&filenames[2], &filenames[3]])
        .arg("mirror")
        .args([&filenames[4], &filenames[5]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..6]).await;
}

// Create a raid510 pool: a stripe of raids of mirrors
#[rstest]
#[tokio::test]
async fn raid510(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "raid", "3", "1", "mirror"])
        .args([&filenames[0], &filenames[1]])
        .arg("mirror")
        .args([&filenames[2], &filenames[3]])
        .arg("mirror")
        .args([&filenames[4], &filenames[5]])
        .args(["raid", "3", "1", "mirror"])
        .args([&filenames[6], &filenames[7]])
        .arg("mirror")
        .args([&filenames[8], &filenames[9]])
        .arg("mirror")
        .args([&filenames[10], &filenames[11]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames).await;
}

// Create a triply mirrored pool
#[rstest]
#[tokio::test]
async fn triple_mirror(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "mypool", "mirror"])
        .args([&filenames[0], &filenames[1], &filenames[2]])
        .assert()
        .success();

    // Check that we can actually open them.
    open("mypool", &filenames[0..3]).await;
}

#[rstest]
#[tokio::test]
async fn zone_size(harness: Harness) {
    let (filenames, _tempdir) = harness;

    bfffs()
        .args(["pool", "create", "--zone-size", "512", "mypool"])
        .arg(&filenames[0])
        .assert()
        .success();

    // Check that we can actually open it.
    let mut manager = vdev_block::Manager::default();
    let mut lr = manager.taste(&filenames[0]).await.unwrap();
    let ml: mirror::Label = lr.deserialize().unwrap();
    let uuid: Uuid = ml.children[0];
    let (vb, _) = manager.import(uuid).await.unwrap();
    assert_eq!(2, vb.zones());
}
