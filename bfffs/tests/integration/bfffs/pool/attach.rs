//! Integration tests for "bfffs pool attach".
use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use ::bfffs::Bfffs;
use assert_cmd::{cargo::cargo_bin, prelude::*};
use bfffs_core::vdev::Health;
use tempfile::{Builder, TempDir};
use tokio::time::sleep;

use super::super::super::*;

const POOLNAME: &str = "AttachPool";

struct Files {
    paths:   Vec<PathBuf>,
    tempdir: TempDir,
}

fn mk_files() -> Files {
    let len = 1 << 30; // 1 GB
    let tempdir = Builder::new()
        .prefix(concat!(module_path!(), "."))
        .tempdir()
        .unwrap();
    let mut paths = Vec::new();
    for i in 0..3 {
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

fn start_bfffsd(files: &Files) -> Daemon {
    let sockpath = files.tempdir.path().join("bfffsd.sock");
    let bfffsd: Bfffsd = Command::new(cargo_bin!("bfffsd"))
        .arg("--sock")
        .arg(sockpath.as_os_str())
        .arg(POOLNAME)
        .args(&files.paths[..])
        .spawn()
        .unwrap()
        .into();

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

async fn wait_for_healthy(libbfffs: &Bfffs) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let stat =
                libbfffs.pool_status(POOLNAME.to_string()).await.unwrap();
            if stat.health == Health::Online {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Timeout waiting for pool to become healthy");
}

#[tokio::test]
async fn enoent() {
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
        .args([
            "pool",
            "attach",
            POOLNAME,
            "00000000-1111-2222-3333-0123456789ab",
            files.paths[2].to_str().unwrap(),
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
        .args([
            "pool",
            "attach",
            POOLNAME,
            files.paths[0].to_str().unwrap(),
            files.paths[2].to_str().unwrap(),
        ])
        .assert()
        .success();

    let libbfffs = Bfffs::new(&daemon.sockpath).await.unwrap();
    wait_for_healthy(&libbfffs).await;
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
    let mirror_uuid = format!("{}", stat.clusters[0].mirrors[0].uuid);

    bfffs()
        .arg("--sock")
        .arg(daemon.sockpath.as_os_str())
        .args([
            "pool",
            "attach",
            POOLNAME,
            &mirror_uuid,
            files.paths[2].to_str().unwrap(),
        ])
        .assert()
        .success();

    wait_for_healthy(&libbfffs).await;
}
