use std::{fs, path::PathBuf};

use assert_cmd::prelude::*;
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

use super::super::super::bfffs;

type Harness = (PathBuf, TempDir);

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

    (filename, tempdir)
}

#[test]
fn help() {
    bfffs()
        .arg("debug")
        .arg("dump")
        .arg("-h")
        .assert()
        .success();
}

#[rstest]
#[tokio::test]
async fn forest(harness: Harness) {
    let (filename, _tempdir) = harness;

    // The output format is tested by the Fs functional tests.  In the
    // integration tests, just test that it looks basically correct.
    bfffs()
        .arg("debug")
        .arg("dump")
        .arg("--forest")
        .arg("mypool")
        .arg(filename)
        .assert()
        .success()
        .stdout(predicates::str::contains(
            r"root:
  height: 1
  elem:
    key:
      tree_id: 0
      offset: 0
    txgs:
      start: 0
      end: 1
    ptr:
      Addr:",
        ));
}
#[rstest]
#[tokio::test]
async fn fsm(harness: Harness) {
    let (filename, _tempdir) = harness;

    bfffs()
        .arg("debug")
        .arg("dump")
        .arg("-f")
        .arg("mypool")
        .arg(filename)
        .assert()
        .success()
        .stdout(
            r"FreeSpaceMap: 4 Zones: 0 Closed, 3 Empty, 1 Open
 Zone | TXG |                              Space                               |
------|-----|------------------------------------------------------------------|
    0 | 0-  |                                                                  |
    1 |  -  |                                                                  |

",
        );
}

#[rstest]
#[tokio::test]
async fn tree(harness: Harness) {
    let (filename, _tempdir) = harness;

    // The output format is tested by the Fs functional tests.  In the
    // integration tests, just test that it looks basically correct.
    bfffs()
        .arg("debug")
        .arg("dump")
        .arg("-t")
        .arg("mypool")
        .arg(filename)
        .assert()
        .success()
        .stdout(predicates::str::contains(
            r"root:
  height: 1
  elem:
    key: 0-0-00000000000000
    txgs:
      start: 0
      end: 1
    ptr:
      Addr: 1
",
        ));
}
