use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use function_name::named;
use nix::{
    mount::{unmount, MntFlags},
    sys::statfs::statfs,
};
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:        Bfffsd,
    pub mountpoint: PathBuf,
    _tempdir:       TempDir,
    pub sockpath:   PathBuf,
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
        _tempdir: tempdir,
    }
}

/// Without -f, a busy file system should not be unmounted
#[named]
#[rstest]
#[tokio::test]
async fn ebusy(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();

    let fname = harness.mountpoint.join("foo");
    let f = std::fs::File::create(fname);

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "unmount", "mypool"])
        .assert()
        .failure()
        .stderr("Error: EBUSY\n");

    drop(f);
    // the VOP_RECLAIM may happen asynchronously, so we may need to retry the
    // unmount.
    loop {
        match unmount(&harness.mountpoint, MntFlags::empty()) {
            Ok(()) => break,
            Err(nix::Error::EBUSY) => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => panic!("unmount: {}", e),
        }
    }
}

/// The file system isn't mounted
#[named]
#[rstest]
#[tokio::test]
async fn einval(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "unmount", "mypool"])
        .assert()
        .failure()
        .stderr("Error: EINVAL\n");
}

/// With -f, a busy file system should be unmounted
#[named]
#[rstest]
#[tokio::test]
async fn force(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();

    let fname = harness.mountpoint.join("foo");
    let _f = std::fs::File::create(fname);

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "unmount", "-f", "mypool"])
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

    assert_eq!(
        "fusefs.bfffs",
        statfs(&harness.mountpoint).unwrap().filesystem_type_name()
    );

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "unmount", "mypool"])
        .assert()
        .success();

    assert_ne!(
        "fusefs.bfffs",
        statfs(&harness.mountpoint).unwrap().filesystem_type_name()
    );
}
