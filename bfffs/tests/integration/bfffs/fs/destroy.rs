use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use function_name::named;
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:      Bfffsd,
    _tempdir:     TempDir,
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
    let bfffsd: Bfffsd = Command::new(cargo_bin!("bfffsd"))
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
async fn child(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool/foo"])
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn grandchild(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo/bar"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool/foo/bar"])
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn enoent(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool/foo/bar/baz"])
        .assert()
        .failure()
        .stderr("No such file or directory\n");
}

/// It is an error to attempt to destroy a mounted file system
#[named]
#[rstest]
#[tokio::test]
async fn mounted(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "mount", "mypool"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool"])
        .assert()
        .failure()
        .stderr("Device busy\n");

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "unmount", "mypool"])
        .assert()
        .success();
}

// Can't delete a dataset with children
#[rstest]
#[tokio::test]
async fn parent(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool"])
        .assert()
        .failure()
        .stderr("Device busy\n");
}

#[rstest]
#[tokio::test]
async fn root_fs(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.as_os_str())
        .args(["fs", "destroy", "mypool"])
        .assert()
        .success();
}
