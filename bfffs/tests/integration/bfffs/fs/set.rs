use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::prelude::*;
use rstest::rstest;
use tempfile::{Builder, TempDir};

use super::super::super::*;

struct Harness {
    _bfffsd:      Bfffsd,
    pub _tempdir: TempDir,
    pub sockpath: PathBuf,
}

/// Create a pool for backing store and optionally some datasets
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
async fn atime() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "atime=off", "mypool"])
        .assert()
        .success()
        .stdout("");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "value,source", "atime", "mypool"])
        .assert()
        .success()
        .stdout("off\tlocal\n");
}

#[rstest]
#[tokio::test]
async fn mountpoint() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "mountpoint=/mnt", "mypool"])
        .assert()
        .success()
        .stdout("");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-po", "value,source", "mountpoint", "mypool"])
        .assert()
        .success()
        .stdout("/mnt\tlocal\n");
}

#[rstest]
#[tokio::test]
async fn multiple_datasets() {
    let h = harness();

    // create some extra datasets
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "mypool/a"])
        .assert()
        .success();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "mypool/b"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "atime=off", "mypool/a", "mypool/b"])
        .assert()
        .success()
        .stdout("");

    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "name,value,source", "atime"])
        .args(["mypool/a", "mypool/b"])
        .assert()
        .success()
        .stdout("mypool/a\toff\tlocal\nmypool/b\toff\tlocal\n");
}

#[rstest]
#[tokio::test]
async fn multiple_properties() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "atime=on,recsize=16384", "mypool"])
        .assert()
        .success()
        .stdout("");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "value,source", "atime", "mypool"])
        .assert()
        .success()
        .stdout("on\tlocal\n");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "value,source", "recsize", "mypool"])
        .assert()
        .success()
        .stdout("16384\tlocal\n");
}

#[rstest]
#[tokio::test]
async fn name() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "name=otherpool", "mypool"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("This property is read-only"));
}

#[rstest]
#[tokio::test]
async fn recsize() {
    let h = harness();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "set", "recsize=16384", "mypool"])
        .assert()
        .success()
        .stdout("");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "value,source", "recsize", "mypool"])
        .assert()
        .success()
        .stdout("16384\tlocal\n");
}
