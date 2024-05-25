// vim: tw=80
use std::{
    fs,
    io::Write,
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;
use clap::{crate_version, Parser};
use si_scale::helpers::bibytes;

use super::{util::bfffs, Benchmark, Harness};

/// Measure file system destruction speed
#[derive(Parser, Clone, Debug, Default)]
#[clap(version = crate_version!())]
pub struct Cli {
    // For compatibility with Cargo
    #[clap(long)]
    bench:      bool,
    #[clap(long, help = "Drop the cache after fill and before destroy")]
    drop_cache: bool,
    #[clap(
        long,
        help = "Time to spend filling file system in seconds",
        default_value = "1"
    )]
    fill_time:  f64,
    /// File system properties, comma delimited.
    #[clap(short = 'p', long, value_delimiter(','))]
    properties: Option<String>,
}

impl Cli {
    pub fn build(self) -> FsDestroy {
        FsDestroy {
            drop_cache: self.drop_cache,
            fill_time:  self.fill_time,
            fsize:      0,
            properties: self.properties,
        }
    }
}

pub struct FsDestroy {
    drop_cache: bool,
    fill_time:  f64,
    fsize:      usize,
    properties: Option<String>,
}

impl Benchmark for FsDestroy {
    async fn setup(&mut self, harness: &Harness) {
        const BSIZE: usize = 1 << 17;

        let mp = harness.mountpoint.join("testfs");
        fs::create_dir_all(&mp).unwrap();

        let mut bfffs_cmd = bfffs();
        bfffs_cmd
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("create");
        if let Some(props) = &self.properties {
            bfffs_cmd.arg("-o").arg(props);
        }
        bfffs_cmd.arg("testpool/testfs").assert().success();

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
                let elapsed = start.elapsed();
                if elapsed >= duration {
                    break;
                }
            }
            zfile.sync_all().unwrap();
        }

        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("unmount")
            .arg("-f")
            .arg("testpool/testfs")
            .assert()
            .success();

        if self.drop_cache {
            bfffs()
                .arg("--sock")
                .arg(harness.sockpath.to_str().unwrap())
                .arg("debug")
                .arg("drop-cache")
                .assert()
                .success();
        }
    }

    fn print(&self, elapsed: &Duration) {
        println!(
            "fs_destroy: {}/s in {} s for {}",
            bibytes(self.fsize as f64 / elapsed.as_secs_f64()),
            elapsed.as_secs_f64(),
            bibytes(self.fsize as f64)
        );
    }

    async fn run(&mut self, harness: &Harness) {
        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("destroy")
            .arg("testpool/testfs")
            .assert()
            .success();
    }
}
