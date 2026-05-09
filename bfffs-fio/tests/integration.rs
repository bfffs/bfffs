use std::{fs, io::Write, path::PathBuf, process::Command};

use assert_cmd::prelude::*;
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

type Harness = (PathBuf, TempDir);

const POOLNAME: &str = "fio-testpool";

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

    Command::cargo_bin("bfffs")
        .unwrap()
        .args(["pool", "create", POOLNAME])
        .arg(&filename)
        .assert()
        .success();

    (filename, tempdir)
}

#[rstest]
fn smoke(harness: Harness) {
    let dylib = test_cdylib::build_current_project();
    let cfg_path = harness.1.path().join("test.fio");
    let mut f = fs::File::create(&cfg_path).unwrap();
    write!(
        &mut f,
        r#"
[global]
ioengine=external:{}

bs=128k
size=128m
iodepth=4
thread
end_fsync=1
create_on_open=1
pool={}
vdevs={}
rw=readwrite
[fio_test_file]"#,
        dylib.display(),
        POOLNAME,
        harness.0.display()
    )
    .unwrap();

    let output = Command::new("fio").arg(&cfg_path).ok().unwrap();
    print!("{}", String::from_utf8_lossy(&output.stdout));
}
