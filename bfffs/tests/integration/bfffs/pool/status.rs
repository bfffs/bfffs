use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use assert_cmd::{cargo::cargo_bin, prelude::*};
use tempfile::{Builder, TempDir};

use super::super::super::*;

const POOLNAME: &str = "StatusPool";

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
async fn all() {
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
        .args(["pool", "status"])
        .assert()
        .success()
        .stdout(predicates::str::diff(format!(
            " NAME                                                   HEALTH 
 StatusPool                                             Online 
   {:51}  Online \n",
            files.paths[0].display()
        )));
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
        .args(["pool", "status", "mypoolx"])
        .assert()
        .failure()
        .stderr("Error: ENOENT\n");
}

#[test]
fn help() {
    bfffs().args(["pool", "status", "-h"]).assert().success();
}

#[tokio::test]
async fn mirror() {
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
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                     HEALTH 
 StatusPool                                               Online 
   mirror                                                 Online 
     {:51}  Online 
     {:51}  Online 
",
            files.paths[0].display(),
            files.paths[1].display()
        ));
}

#[tokio::test]
async fn mirror_with_missing_child() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create"])
        .arg(POOLNAME)
        .arg("mirror")
        .args(&files.paths[0..2])
        .assert()
        .success();
    fs::remove_file(&files.paths[1]).unwrap();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                     HEALTH 
 StatusPool                                               Degraded(1) 
   mirror                                                 Degraded(1) 
     {:51}  Online 
     {:51}  Faulted 
",
            files.paths[0].display(),
            ""
        ));
}

#[tokio::test]
async fn one_disk() {
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
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                   HEALTH 
 StatusPool                                             Online 
   {:51}  Online \n",
            files.paths[0].display()
        ));
}

#[tokio::test]
async fn raid_with_missing_child() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create", POOLNAME, "raid", "3", "1"])
        .args(&files.paths[0..3])
        .assert()
        .success();
    fs::remove_file(&files.paths[1]).unwrap();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                     HEALTH 
 StatusPool                                               Degraded(1) 
   PrimeS-3,3,1                                           Degraded(1) 
     {:51}  Online 
     {:51}  Faulted 
     {:51}  Online 
",
            files.paths[0].display(),
            "mirror",
            files.paths[2].display(),
        ));
}

#[tokio::test]
async fn raid5() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create", POOLNAME, "raid", "3", "1"])
        .args(&files.paths[0..3])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                     HEALTH 
 StatusPool                                               Online 
   PrimeS-3,3,1                                           Online 
     {:51}  Online 
     {:51}  Online 
     {:51}  Online 
",
            files.paths[0].display(),
            files.paths[1].display(),
            files.paths[2].display()
        ));
}

#[tokio::test]
async fn raid50() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create", POOLNAME, "raid", "3", "1"])
        .args(&files.paths[0..3])
        .args(["raid", "3", "1"])
        .args(&files.paths[3..6])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
            " NAME                                                     HEALTH 
 StatusPool                                               Online 
   PrimeS-3,3,1                                           Online 
     {:51}  Online 
     {:51}  Online 
     {:51}  Online 
   PrimeS-3,3,1                                           Online 
     {:51}  Online 
     {:51}  Online 
     {:51}  Online 
",
            files.paths[0].display(),
            files.paths[1].display(),
            files.paths[2].display(),
            files.paths[3].display(),
            files.paths[4].display(),
            files.paths[5].display()
        ));
}

#[tokio::test]
async fn raid51() {
    let files = mk_files();
    bfffs()
        .args(["pool", "create", POOLNAME, "raid", "3", "1"])
        .arg("mirror")
        .args(&files.paths[0..3])
        .arg("mirror")
        .args(&files.paths[3..6])
        .arg("mirror")
        .args(&files.paths[6..9])
        .assert()
        .success();
    let daemon = start_bfffsd(&files);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args(["pool", "status", POOLNAME])
        .assert()
        .success()
        .stdout(format!(
" NAME                                                       HEALTH 
 StatusPool                                                 Online 
   PrimeS-3,3,1                                             Online 
     mirror                                                 Online 
       {:51}  Online 
       {:51}  Online 
       {:51}  Online 
     mirror                                                 Online 
       {:51}  Online 
       {:51}  Online 
       {:51}  Online 
     mirror                                                 Online 
       {:51}  Online 
       {:51}  Online 
       {:51}  Online 
",
            files.paths[0].display(),
            files.paths[1].display(),
            files.paths[2].display(),
            files.paths[3].display(),
            files.paths[4].display(),
            files.paths[5].display(),
            files.paths[6].display(),
            files.paths[7].display(),
            files.paths[8].display()
        ));
}
