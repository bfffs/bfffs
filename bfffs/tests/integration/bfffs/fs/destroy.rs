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
        .arg("pool")
        .arg("create")
        .arg("-p")
        .arg(format!("mountpoint={}", mountpoint.display()))
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
async fn child(harness: Harness) {
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
        .arg("destroy")
        .arg("mypool/foo")
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn grandchild(harness: Harness) {
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
        .arg("create")
        .arg("mypool/foo/bar")
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("destroy")
        .arg("mypool/foo/bar")
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn enoent(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("destroy")
        .arg("mypool/foo/bar/baz")
        .assert()
        .failure()
        .stderr("Error: ENOENT\n");
}

/// It is an error to attempt to destroy a mounted file system
#[named]
#[rstest]
#[tokio::test]
async fn mounted(harness: Harness) {
    require_fusefs!();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("mount")
        .arg("mypool")
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("destroy")
        .arg("mypool")
        .assert()
        .failure()
        .stderr("Error: EBUSY\n");

    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("unmount")
        .arg("mypool")
        .assert()
        .success();
}

// Can't delete a dataset with children
#[rstest]
#[tokio::test]
async fn parent(harness: Harness) {
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
        .arg("destroy")
        .arg("mypool")
        .assert()
        .failure()
        .stderr("Error: EBUSY\n");
}

#[rstest]
#[tokio::test]
async fn root_fs(harness: Harness) {
    bfffs()
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("destroy")
        .arg("mypool")
        .assert()
        .success();
}

// TODO: destroy a child and a grand child
