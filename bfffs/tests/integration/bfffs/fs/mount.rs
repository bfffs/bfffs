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
    _bfffsd:        Bfffsd,
    pub tempdir:    TempDir,
    pub mountpoint: PathBuf,
    pub sockpath:   PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ignore_errors = unmount(&self.mountpoint, MntFlags::empty());
    }
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

    let mountpoint = tempdir.path().join("mnt");
    fs::create_dir(&mountpoint).unwrap();
    bfffs()
        .args(["pool", "create", "-p"])
        .arg(format!("mountpoint={}", mountpoint.display()))
        .arg("mypool")
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
        mountpoint,
        tempdir,
    }
}

// Unmount the file system and remount it in the same location
#[named]
#[rstest]
#[tokio::test]
async fn mount_again(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();

    unmount(&harness.mountpoint, MntFlags::empty()).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();
}

#[named]
#[rstest]
#[tokio::test]
async fn ok(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();
}

#[named]
#[rstest]
#[tokio::test]
async fn options(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "-o", "atime=off", "mypool"])
        .assert()
        .success();

    // TODO: figure out how to check if atime is active.
}

/// Mount a dataset other than the pool root
#[named]
#[rstest]
#[tokio::test]
async fn subfs(harness: Harness) {
    require_fusefs!();

    let submp = harness.tempdir.path().join("mnt").join("foo");
    fs::create_dir(&submp).unwrap();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool/foo"])
        .assert()
        .success();

    unmount(&submp, MntFlags::empty()).unwrap();
}

// TODO: test with overridden mountpoint, once that is possible.

// TODO: once the mountpoint can be overridden, check that it is not be possible
// to mount the same file system twice, similar to the old ebusy test, from
// before the mountpoint property was introduced.

// TODO: test with alternate mountpoint
