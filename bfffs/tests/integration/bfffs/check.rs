use std::{fs, path::PathBuf};

use assert_cmd::prelude::*;
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::bfffs;

type Harness = (PathBuf, TempDir);

/// Create a pool for backing store
#[fixture]
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix("test_integration_bfffs_check")
        .tempdir()
        .unwrap();
    let filename = tempdir.path().join("vdev");
    let file = fs::File::create(&filename).unwrap();
    file.set_len(len).unwrap();

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("mypool")
        .arg(&filename)
        .assert()
        .success();

    (filename, tempdir)
}

#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    let (filename, _tempdir) = harness;

    bfffs()
        .arg("check")
        .arg("mypool")
        .arg(filename)
        .assert()
        .success();
}
