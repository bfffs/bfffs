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
fn harness<S: AsRef<str>>(dsnames: &[S]) -> Harness {
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

    for dsname in dsnames {
        bfffs()
            .arg("--sock")
            .arg(sockpath.as_os_str())
            .args(["fs", "create"])
            .arg(dsname.as_ref())
            .assert()
            .success();
    }

    Harness {
        _bfffsd: bfffsd,
        sockpath,
        _tempdir: tempdir,
    }
}

#[rstest]
#[tokio::test]
async fn depth() {
    let h = harness(&[
        "mypool/brother",
        "mypool/brother/nephew",
        "mypool/sister",
        "mypool/sister/niece",
    ]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-d", "0", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   PROPERTY   VALUE   SOURCE\n\
             mypool recordsize 128 kiB default\n",
        );
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-d", "1", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME           PROPERTY   VALUE   SOURCE\n\
             mypool         recordsize 128 kiB default\n\
             mypool/brother recordsize 128 kiB default\n\
             mypool/sister  recordsize 128 kiB default\n",
        );
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-d", "2", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME                  PROPERTY   VALUE   SOURCE\n\
             mypool                recordsize 128 kiB default\n\
             mypool/brother        recordsize 128 kiB default\n\
             mypool/brother/nephew recordsize 128 kiB default\n\
             mypool/sister         recordsize 128 kiB default\n\
             mypool/sister/niece   recordsize 128 kiB default\n",
        );
}

#[rstest]
#[tokio::test]
async fn fields() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-o", "name,value", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   VALUE\n\
             mypool 128 kiB\n",
        );
}

#[rstest]
#[tokio::test]
async fn fields_parseable() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "-o", "name,value", "recsize", "mypool"])
        .assert()
        .success()
        .stdout("mypool\t131072\n");
}

#[rstest]
#[tokio::test]
async fn multiple_datasets() {
    let h = harness(&["mypool/foo", "mypool/bar"]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "recsize", "mypool/foo", "mypool/bar"])
        .assert()
        .success()
        .stdout(
            "NAME       PROPERTY   VALUE   SOURCE\n\
             mypool/bar recordsize 128 kiB default\n\
             mypool/foo recordsize 128 kiB default\n",
        );
}

#[rstest]
#[tokio::test]
async fn multiple_properties() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "atime,recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   PROPERTY   VALUE   SOURCE\n\
             mypool atime      on      default\n\
             mypool recordsize 128 kiB default\n",
        );
}

#[rstest]
#[tokio::test]
async fn parseable() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-p", "recsize", "mypool"])
        .assert()
        .success()
        .stdout("mypool\trecordsize\t131072\tdefault\n");
}

#[rstest]
#[tokio::test]
async fn recursive() {
    let h = harness(&[
        "mypool/brother",
        "mypool/brother/nephew",
        "mypool/sister",
        "mypool/sister/niece",
    ]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-r", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME                  PROPERTY   VALUE   SOURCE\n\
             mypool                recordsize 128 kiB default\n\
             mypool/brother        recordsize 128 kiB default\n\
             mypool/brother/nephew recordsize 128 kiB default\n\
             mypool/sister         recordsize 128 kiB default\n\
             mypool/sister/niece   recordsize 128 kiB default\n",
        );
}

#[rstest]
#[tokio::test]
async fn simple() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   PROPERTY   VALUE   SOURCE\n\
             mypool recordsize 128 kiB default\n",
        );
}

#[rstest]
#[tokio::test]
async fn sources() {
    let h = harness::<&'static str>(&[]);

    // Create a dataset with a non-default mountpoint
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "-o", "mountpoint=/foo", "mypool/foo"])
        .assert()
        .success();

    // And another two datasets that will inherit it
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo/bar"])
        .assert()
        .success();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "mypool/foo/bar/baz"])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-r", "-s", "local", "mountpoint", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME       PROPERTY   VALUE SOURCE\n\
             mypool/foo mountpoint /foo  local\n",
        );
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "get", "-r", "-s", "inherited", "mountpoint", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME               PROPERTY   VALUE        SOURCE\n\
             mypool/foo/bar     mountpoint /foo/bar     inherited\n\
             mypool/foo/bar/baz mountpoint /foo/bar/baz inherited\n",
        );
}
