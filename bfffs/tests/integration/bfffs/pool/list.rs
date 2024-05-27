use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use rstest::rstest;
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:      Bfffsd,
    pub _tempdir: TempDir,
    pub sockpath: PathBuf,
}

/// Create a test pool
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix(concat!(module_path!(), "."))
        .tempdir()
        .unwrap();
    let filename = tempdir.path().join("vdev");
    let file = fs::File::create(&filename).unwrap();
    file.set_len(len).unwrap();

    bfffs()
        .args(["pool", "create", "mypool"])
        .arg(&filename)
        .assert()
        .success();

    let sockpath = tempdir.path().join("bfffsd.sock");
    let bfffsd: Bfffsd = Command::new(cargo_bin("bfffsd"))
        .arg("--sock")
        .arg(sockpath.as_os_str())
        .arg("mypool")
        .arg(filename.as_os_str())
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
async fn all() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["pool", "list"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n",
        );
}

#[rstest]
#[tokio::test]
async fn enoent() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["pool", "list", "mypoolx"])
        .assert()
        .failure()
        .stderr("No such file or directory\n");
}

#[test]
fn help() {
    bfffs().args(["pool", "list", "-h"]).assert().success();
}

#[rstest]
#[tokio::test]
async fn one() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["pool", "list", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n",
        );
}

// TODO: list multiple pools, when Controller supports that
// TODO: list lots of pools, too many for one RPC, when Controller supports that.
// TODO: list a single pool when multiple are present
