use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:      Bfffsd,
    pub _tempdir: TempDir,
    pub sockpath: PathBuf,
}

/// Create a pool for backing store
#[fixture]
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix("test_integration_bfffs_fs_create")
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

    let sockpath = tempdir.path().join("bfffsd.sock");
    let bfffsd: Bfffsd = Command::new(cargo_bin("bfffsd"))
        .arg("--sock")
        .arg(sockpath.to_str().unwrap())
        .arg("mypool")
        .arg(filename.to_str().unwrap())
        .spawn()
        .unwrap()
        .into();

    // We must wait for bfffsd to be ready to receive commands
    waitfor(Duration::from_secs(5), || {
        fs::metadata(&sockpath)
            .map(|md| md.file_type().is_socket())
            .unwrap_or(false)
    })
    .expect("Timeout waiting for bfffsd to listen");

    Harness {
        _bfffsd: bfffsd,
        sockpath,
        _tempdir: tempdir,
    }
}

#[rstest]
#[tokio::test]
async fn eexist(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create")
        .arg("mypool/foo")
        .assert()
        .success();

    // Creating the same file system again should fail
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create")
        .arg("mypool/foo")
        .assert()
        .failure()
        .stderr("Error: EEXIST\n");
}

#[test]
fn help() {
    bfffs().arg("fs").arg("create").arg("-h").assert().success();
}

#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create")
        .arg("mypool/foo")
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn noatime(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create")
        .arg("-o")
        .arg("atime=off")
        .arg("mypool/foo")
        .assert()
        .success();
}
