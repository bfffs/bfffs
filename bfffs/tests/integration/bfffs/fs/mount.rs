use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use function_name::named;
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
        .prefix(concat!(module_path!(), "."))
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

// Unmount the file system and remount it in the same location
#[named]
#[rstest]
#[tokio::test]
async fn mount_again(harness: Harness) {
    require_fusefs!();

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

#[named]
#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    require_fusefs!();

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

#[named]
#[rstest]
#[tokio::test]
async fn options(harness: Harness) {
    require_fusefs!();

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
#[named]
#[rstest]
#[tokio::test]
async fn subfs(harness: Harness) {
    require_fusefs!();

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

/// It should not be possible to mount the same file system twice
#[named]
#[rstest]
#[tokio::test]
async fn ebusy(harness: Harness) {
    require_fusefs!();

    let mountpoint1 = harness.tempdir.path().join("mnt1");
    fs::create_dir(&mountpoint1).unwrap();
    let mountpoint2 = harness.tempdir.path().join("mnt2");
    fs::create_dir(&mountpoint2).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("mypool")
        .arg(&mountpoint1)
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("mypool")
        .arg(&mountpoint2)
        .assert()
        .failure()
        .stderr("Error: EBUSY\n");

    unmount(&mountpoint1, MntFlags::empty()).unwrap();
}
