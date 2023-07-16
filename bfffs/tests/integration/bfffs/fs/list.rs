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
        .args(["fs", "list", "-d", "0", "mypool"])
        .assert()
        .success()
        .stdout("NAME\nmypool\n");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-d", "1", "mypool"])
        .assert()
        .success()
        .stdout("NAME\nmypool\nmypool/brother\nmypool/sister\n");
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-d", "2", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
            mypool\n\
            mypool/brother\n\
            mypool/brother/nephew\n\
            mypool/sister\n\
            mypool/sister/niece\n",
        );
}

#[rstest]
#[tokio::test]
async fn enoent() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-o", "name", "mypoolx"])
        .assert()
        .failure()
        .stderr("Error: ENOENT\n");
}

#[test]
fn help() {
    bfffs().args(["fs", "list", "-h"]).assert().success();
}

/// By default, numeric properties should be printed in human-friendly units
#[rstest]
#[tokio::test]
async fn humanize() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-o", "name,recsize", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   RECSIZE\n\
            mypool 128 kiB\n",
        );
}

#[rstest]
#[tokio::test]
async fn multi_arg() {
    let h = harness(&[
        "mypool/brother",
        "mypool/sister",
        "mypool/brother/grandson",
        "mypool/sister/granddaughter",
        "mypool/dog",
    ]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "mypool/brother", "mypool/sister"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool/brother\n\
             mypool/sister\n",
        );
}

#[rstest]
#[tokio::test]
async fn lots() {
    // CHUNKQTY is a private constant declared in bfffsd.  Test that directories
    // with more entries than that can be correctly listed.
    const CHUNKQTY: usize = 64;

    let mut dsnames = vec![];
    for i in 0..=CHUNKQTY {
        dsnames.push(format!("mypool/{i}"))
    }
    let h = harness(&dsnames);
    dsnames.sort();
    let mut expected = vec![String::from("NAME"), String::from("mypool")];
    expected.extend(dsnames);
    let mut expected = expected.join("\n");
    expected.push('\n');
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-r", "mypool"])
        .assert()
        .success()
        .stdout(expected);
}

/// With -H the header row should not be printed, and the columns should be
/// separated by tabs.
#[rstest]
#[tokio::test]
async fn parseable() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-p", "-o", "name,atime,mountpoint,recsize"])
        .arg("mypool")
        .assert()
        .success()
        .stdout("mypool\ton\t/mypool\t131072\n");
}

#[rstest]
#[tokio::test]
async fn props() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-o", "name,atime,mountpoint", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME   ATIME MOUNTPOINT\n\
             mypool on    /mypool\n",
        );
}

#[rstest]
#[tokio::test]
async fn pool_only() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n",
        );
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
        .args(["fs", "list", "-r", "mypool"])
        .assert()
        .success()
        .stdout(
            "NAME\n\
            mypool\n\
            mypool/brother\n\
            mypool/brother/nephew\n\
            mypool/sister\n\
            mypool/sister/niece\n",
        );
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "list", "-r", "mypool/brother"])
        .assert()
        .success()
        .stdout("NAME\nmypool/brother\nmypool/brother/nephew\n");
}

#[rstest]
#[tokio::test]
async fn sort() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "-o", "atime=off,recsize=65536"])
        .arg("mypool/a")
        .assert()
        .success();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "-o", "atime=on,recsize=65536"])
        .arg("mypool/b")
        .assert()
        .success();
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args(["fs", "create", "-o", "atime=off,recsize=131072"])
        .arg("mypool/c")
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args([
            "fs",
            "list",
            "-o",
            "name,atime,recsize",
            "-r",
            "-s",
            "atime",
            "-s",
            "recsize",
            "-s",
            "name",
            "mypool",
        ])
        .assert()
        .success()
        .stdout(
            "NAME     ATIME RECSIZE\n\
             mypool/a off   64 kiB\n\
             mypool/c off   128 kiB\n\
             mypool/b on    64 kiB\n\
             mypool   on    128 kiB\n",
        );

    bfffs()
        .arg("--sock")
        .arg(h.sockpath.as_os_str())
        .args([
            "fs",
            "list",
            "-o",
            "name,atime,recsize",
            "-r",
            "-s",
            "recsize",
            "-s",
            "atime",
            "-s",
            "name",
            "mypool",
        ])
        .assert()
        .success()
        .stdout(
            "NAME     ATIME RECSIZE\n\
             mypool/a off   64 kiB\n\
             mypool/b on    64 kiB\n\
             mypool/c off   128 kiB\n\
             mypool   on    128 kiB\n",
        );
}

#[rstest]
#[test]
fn sort_by_unlisted_property() {
    bfffs()
        .args(["fs", "list", "-o", "name,atime", "-s", "recsize", "mypool"])
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Cannot sort by a property that isn't listed",
        ));
}
