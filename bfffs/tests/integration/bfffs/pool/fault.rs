//! Integration tests for "bfffs pool fault".
//!
//! This file exists for the purpose of testing the CLI.  Comprehensively
//! testing all of the things that can be faulted falls to bfffs-core's
//! functional tests.
use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use ::bfffs::Bfffs;
use assert_cmd::{cargo::cargo_bin, prelude::*};
use tempfile::{Builder, TempDir};

use super::super::super::*;

const POOLNAME: &str = "FaultPool";

struct Files {
    paths:   Vec<PathBuf>,
    tempdir: TempDir,
}

/// Create some temporary files for backing stores.
fn mk_files() -> Files {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix(concat!(module_path!(), "."))
        .tempdir()
        .unwrap();
    let mut paths = Vec::new();
    for i in 0..12 {
        let filename = tempdir.path().join(format!("vdev.{i}"));
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        let pb = filename.to_path_buf();
        paths.push(pb);
    }
    Files { paths, tempdir }
}

struct Daemon {
    _bfffsd:      Bfffsd,
    pub sockpath: PathBuf,
}

/// Start bfffsd and import the pool
fn start_bfffsd(files: &Files) -> Daemon {
    let sockpath = files.tempdir.path().join("bfffsd.sock");
    let bfffsd: Bfffsd = Command::new(cargo_bin("bfffsd"))
        .arg("--sock")
        .arg(sockpath.as_os_str())
        .arg(POOLNAME)
        .args(&files.paths[..])
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

    Daemon {
        _bfffsd: bfffsd,
        sockpath,
    }
}

#[tokio::test]
async fn enoent() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create"])
        .arg(POOLNAME)
        .arg(&files.paths[0])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args([
            "pool",
            "fault",
            POOLNAME,
            "00000000-1111-2222-3333-0123456789ab",
        ])
        .assert()
        .failure()
        .stderr("No such file or directory\n");
}

#[tokio::test]
async fn by_path() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create"])
        .arg(POOLNAME)
        .arg("mirror")
        .args(&files.paths[0..2])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "fault", POOLNAME, files.paths[0].to_str().unwrap()])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                    HEALTH 
 FaultPool                                               Degraded(1) 
   mirror                                                Degraded(1) 
     {:51} Faulted 
     {:51} Online 
",
            "",
            files.paths[1].display()
        ));
}

#[tokio::test]
async fn by_uuid() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create"])
        .arg(POOLNAME)
        .arg("mirror")
        .args(&files.paths[0..2])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    let libbfffs = Bfffs::new(&daemon.sockpath).await.unwrap();
    let stat = libbfffs.pool_status(POOLNAME.to_string()).await.unwrap();
    let uuid = format!("{}", &stat.clusters[0].mirrors[0].leaves[0].uuid);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "fault", POOLNAME, &uuid])
        .assert()
        .success();

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                    HEALTH 
 FaultPool                                               Degraded(1) 
   mirror                                                Degraded(1) 
     {:51} Faulted 
     {:51} Online 
",
            "",
            files.paths[1].display()
        ));
}
