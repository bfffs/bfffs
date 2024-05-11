#! vim: tw=80
use std::time::Duration;

use bfffs::Bfffs;
use clap::{crate_version, Parser};

use super::{Benchmark, Harness};

/// Measure file system creation speed
#[derive(Parser, Clone, Debug, Default)]
#[clap(version = crate_version!())]
pub struct Cli {
    #[clap(long)]
    bench: bool,
    #[clap(
        default_value = "1000",
        short = 'n',
        long,
        help = "Number of file systems to create"
    )]
    count: usize,
}

impl Cli {
    pub fn build(self) -> FsCreate {
        FsCreate {
            bfffs: None,
            count: self.count,
        }
    }
}

pub struct FsCreate {
    /// Bfffsd socket
    bfffs: Option<Bfffs>,
    /// Number of file systems to create
    count: usize,
}

impl Benchmark for FsCreate {
    async fn setup(&mut self, harness: &Harness) {
        self.bfffs = Some(Bfffs::new(&harness.sockpath).await.unwrap());
    }

    fn print(&self, elapsed: &Duration) {
        println!(
            "fs_create: {} creations per second",
            self.count as f64 / elapsed.as_secs_f64()
        );
    }

    async fn run(&mut self, _harness: &Harness) {
        let bfffs = self.bfffs.as_ref().unwrap();
        for i in 0..self.count {
            bfffs
                .fs_create(format!("testpool/{i}"), Vec::new())
                .await
                .unwrap();
        }
    }
}
