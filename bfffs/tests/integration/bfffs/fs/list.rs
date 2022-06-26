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

    for dsname in dsnames {
        bfffs()
            .arg("--sock")
            .arg(sockpath.to_str().unwrap())
            .arg("fs")
            .arg("create")
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
async fn child() {
    let h = harness(&["mypool/child"]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool")
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n\
             mypool/child\n",
        );
}

#[rstest]
#[tokio::test]
async fn grandchild() {
    let h = harness(&["mypool/child", "mypool/child/grandchild"]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool/child")
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool/child\n\
             mypool/child/grandchild\n",
        );
}

#[test]
fn help() {
    bfffs().arg("fs").arg("list").arg("-h").assert().success();
}

/// By default, numeric properties should be printed in human-friendly units
#[rstest]
#[tokio::test]
async fn humanize() {
    let h = harness::<&'static str>(&[]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("-o")
        .arg("name,recsize")
        .arg("mypool")
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
    ]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool/brother")
        .arg("mypool/sister")
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool/brother\n\
             mypool/brother/grandson\n\
             mypool/sister\n\
             mypool/sister/granddaughter\n",
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
        dsnames.push(format!("mypool/{}", i))
    }
    let h = harness(&dsnames);
    dsnames.sort();
    let mut expected = vec![String::from("NAME"), String::from("mypool")];
    expected.extend(dsnames.into_iter());
    let mut expected = expected.join("\n");
    expected.push('\n');
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool")
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
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("-p")
        .arg("-o")
        .arg("name,atime,mountpoint,recsize")
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
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("-o")
        .arg("name,atime,mountpoint")
        .arg("mypool")
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
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool")
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n",
        );
}

#[rstest]
#[tokio::test]
async fn siblings() {
    let h = harness(&["mypool/brother", "mypool/sister"]);
    bfffs()
        .arg("--sock")
        .arg(h.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("list")
        .arg("mypool")
        .assert()
        .success()
        .stdout(
            "NAME\n\
             mypool\n\
             mypool/brother\n\
             mypool/sister\n",
        );
}
