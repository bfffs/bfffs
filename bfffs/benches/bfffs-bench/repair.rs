// vim: tw=80
use std::{
    fs,
    io::Write,
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;
use bfffs::Bfffs;
use bfffs_core::vdev::Health;
use clap::{crate_version, Parser};
use si_scale::helpers::bibytes;
use tokio::time::sleep;

use super::{util::bfffs, Benchmark, Harness};

/// Measure pool repair speed
#[derive(Parser, Clone, Debug, Default)]
#[clap(version = crate_version!())]
pub struct Cli {
    // For compatibility with Cargo
    #[clap(long)]
    bench:     bool,
    #[clap(
        long,
        help = "Time to spend filling file system in seconds",
        default_value = "1"
    )]
    fill_time: f64,
}

impl Cli {
    pub fn build(self) -> Repair {
        Repair {
            bfffs:     None,
            fill_time: self.fill_time,
            fsize:     0,
        }
    }
}

pub struct Repair {
    bfffs:     Option<Bfffs>,
    fill_time: f64,
    fsize:     usize,
}

impl Benchmark for Repair {
    async fn setup(&mut self, harness: &Harness) {
        const BSIZE: usize = 1 << 17;

        let mp = harness.mountpoint.join("testfs");
        fs::create_dir_all(&mp).unwrap();

        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("create")
            .arg("testpool/testfs")
            .assert()
            .success();

        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("mount")
            .arg("testpool/testfs")
            .assert()
            .success();

        {
            let zbuf = vec![0u8; BSIZE];
            let mut zfile = fs::File::create(mp.join("zfile")).unwrap();
            let duration = Duration::from_secs_f64(self.fill_time);
            let start = Instant::now();
            loop {
                zfile.write_all(&zbuf).unwrap();
                self.fsize += zbuf.len();
                if start.elapsed() >= duration {
                    break;
                }
            }
            zfile.sync_all().unwrap();
        }

        self.bfffs = Some(Bfffs::new(&harness.sockpath).await.unwrap());
    }

    fn print(&self, elapsed: &Duration) {
        println!(
            "repair: {}/s in {} s for {}",
            bibytes(self.fsize as f64 / elapsed.as_secs_f64()),
            elapsed.as_secs_f64(),
            bibytes(self.fsize as f64)
        );
    }

    async fn run(&mut self, harness: &Harness) {
        let last_device = harness.devices.last().expect("pool has no devices");

        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("pool")
            .arg("attach")
            .arg("testpool")
            .arg(last_device)
            .arg(&harness.repair_device)
            .assert()
            .success();

        let bfffs = self.bfffs.as_ref().unwrap();
        loop {
            let stat = bfffs.pool_status("testpool".to_string()).await.unwrap();
            if stat.health == Health::Online {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}
