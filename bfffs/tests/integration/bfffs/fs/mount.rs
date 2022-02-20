use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use nix::mount::{unmount, MntFlags};
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:      Bfffsd,
    pub tempdir:  TempDir,
    pub sockpath: PathBuf,
}

/// Create a pool for backing store
#[fixture]
fn harness() -> Harness {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix("test_integration_bfffs_fs_mount")
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
        tempdir,
    }
}

#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    require_fusefs!("bfffs::fs::mount::ok");

    let mountpoint = harness.tempdir.path().join("mnt");
    fs::create_dir(&mountpoint).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("mypool")
        .arg(&mountpoint)
        .assert()
        .success();

    unmount(&mountpoint, MntFlags::empty()).unwrap();
}

#[rstest]
#[tokio::test]
async fn options(harness: Harness) {
    require_fusefs!("bfffs::fs::mount::options");

    let mountpoint = harness.tempdir.path().join("mnt");
    fs::create_dir(&mountpoint).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("-o")
        .arg("atime=off")
        .arg("mypool")
        .arg(&mountpoint)
        .assert()
        .success();

    // TODO: figure out how to check if atime is active.

    unmount(&mountpoint, MntFlags::empty()).unwrap();
}

/// Mount a dataset other than the pool root
#[rstest]
#[tokio::test]
async fn subfs(harness: Harness) {
    require_fusefs!("bfffs::fs::mount::subfs");

    let mountpoint = harness.tempdir.path().join("mnt");
    fs::create_dir(&mountpoint).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create")
        .arg("mypool/foo")
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("mypool/foo")
        .arg(&mountpoint)
        .assert()
        .success();

    unmount(&mountpoint, MntFlags::empty()).unwrap();
}
