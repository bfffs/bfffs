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

/// Create a single temporary file for backing store
#[fixture]
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix("test_integration_bfffs_pool_clean")
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

/// Successfully initiate cleaning.
// In the future it might be nice to check that this worked by reporting the
// pool's cleanliness.  But right now there's no way to do that.
#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("pool")
        .arg("clean")
        .arg("mypool")
        .assert()
        .success();
}

/// No such pool
#[rstest]
#[tokio::test]
async fn enoent(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("pool")
        .arg("clean")
        .arg("does_not_exist_pool")
        .assert()
        .failure()
        .stderr("Error: ENOENT\n");
}
