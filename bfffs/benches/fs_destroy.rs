use std::{
    fs,
    io::Write,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;
use bfffs::Result;
use clap::{crate_version, Parser};
use freebsd_libgeom as geom;
use nix::{
    mount::{unmount, MntFlags},
    sys::signal,
    unistd::Pid,
};
use regex::Regex;
use si_scale::helpers::bibytes;
use tempfile::{Builder, TempDir};
use tracing_subscriber::EnvFilter;

pub mod util {
    include!("../tests/integration/util.rs");
}
use util::{bfffs, bfffsd, waitfor, Bfffsd};

/// Measure file system destruction speed
#[derive(Parser, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    #[clap(long)]
    bench:      bool,
    #[clap(
        short = 'd',
        long,
        require_value_delimiter(true),
        value_delimiter(',')
    )]
    devices:    Vec<String>,
    #[clap(long, help = "Drop the cache after fill and before destroy")]
    drop_cache: bool,
    #[clap(
        long,
        help = "Time to spend filling file system in seconds",
        default_value = "1"
    )]
    fill_time:  f64,
    #[clap(long, help = "Record a flamegraph to this file")]
    flamegraph: Option<PathBuf>,
    /// Daemon options, comma delimited.
    #[clap(
        short = 'o',
        long,
        require_value_delimiter(true),
        value_delimiter(',')
    )]
    options:    Option<String>,
    /// File system propertties, comma delimited.
    #[clap(
        short = 'p',
        long,
        require_value_delimiter(true),
        value_delimiter(',')
    )]
    properties: Option<String>,
    #[clap(required(false))]
    filter:     Option<String>,
}

/// Run a function while monitoring the given PID with dtrace.  Record its
/// flamegraph to the specified file.
fn dtraceit<F: Fn() -> T, T>(f: F, pid: Pid, fname: &Path) -> T {
    let mut dtrace = Command::new("dtrace")
        .stdout(Stdio::piped())
        .arg("-p")
        .arg(format!("{}", pid))
        .arg("-n")
        .arg("profile-497 /pid == $target && arg1/ { @[ustack()] = count(); }")
        .spawn()
        .unwrap();
    let mut stackcollapse = Command::new("stackcollapse.pl")
        .stdin(dtrace.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let flamegraph = Command::new("flamegraph.pl")
        .stdin(stackcollapse.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    // Give dtrace time to start up
    std::thread::sleep(std::time::Duration::from_secs(1));

    let output = f();

    signal::kill(pid, signal::Signal::SIGINT).unwrap();
    dtrace.wait().unwrap();
    let svg = flamegraph.wait_with_output().unwrap().stdout;
    fs::File::create(fname).unwrap().write_all(&svg).unwrap();

    output
}

struct Harness {
    bfffsd:         Bfffsd,
    pub tempdir:    TempDir,
    pub mountpoint: PathBuf,
    pub sockpath:   PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ignore_errors = unmount(&self.mountpoint, MntFlags::empty());
    }
}

fn setup(mut devices: Vec<String>, options: Option<String>) -> Harness {
    let tempdir = Builder::new()
        .prefix(concat!("bench.fs_destroy", "."))
        .tempdir()
        .unwrap();
    let sockpath = tempdir.path().join("bfffsd.sock");

    if devices.is_empty() {
        let len = 1 << 40; // 1 TiB
        let filename = tempdir.path().join("vdev");
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
        devices.push(filename.to_str().unwrap().to_owned());
    }

    let mountpoint = tempdir.path().join("mnt");

    bfffs()
        .arg("pool")
        .arg("create")
        .arg("-p")
        .arg(format!("mountpoint={}", mountpoint.display()))
        .arg("testpool")
        .args(devices.clone())
        .assert()
        .success();

    let mut bfffsd_cmd = bfffsd();
    bfffsd_cmd.arg("--sock").arg(sockpath.to_str().unwrap());
    if let Some(opts) = options {
        bfffsd_cmd.arg("-o").arg(opts);
    }
    let bfffsd = bfffsd_cmd
        .arg("testpool")
        .args(devices)
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

    Harness {
        bfffsd,
        sockpath,
        mountpoint,
        tempdir,
    }
}

/// Create a file system, fill it with files, and time how long it takes
/// to destroy it.
fn fs_destroy(
    devices: Vec<String>,
    fill_time: f64,
    options: Option<String>,
    properties: Option<String>,
    flamegraph: Option<PathBuf>,
    drop_cache: bool,
) -> Result<()> {
    const BSIZE: usize = 1 << 17;
    let sdevs = devices
        .iter()
        .filter_map(|fulldev| fulldev.strip_prefix("/dev/"))
        .collect::<Vec<_>>();
    let use_libgeom = !sdevs.is_empty();
    let geom_regex =
        Regex::new(&format!("^({})$", &sdevs[..].join("|"))).unwrap();

    let harness = setup(devices, options);

    let mp = harness.tempdir.path().join("mnt").join("testfs");
    fs::create_dir_all(&mp).unwrap();

    let mut bfffs_cmd = bfffs();
    bfffs_cmd
        .arg("--sock")
        .arg(harness.sockpath.to_str().unwrap())
        .arg("fs")
        .arg("create");
    if let Some(props) = properties {
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

    let mut fsize = 0;
    {
        let zbuf = vec![0u8; BSIZE];
        let mut zfile = fs::File::create(mp.join("zfile")).unwrap();
        let duration = Duration::from_secs_f64(fill_time);
        let start = Instant::now();
        loop {
            zfile.write_all(&zbuf).unwrap();
            fsize += zbuf.len();
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

    if drop_cache {
        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("debug")
            .arg("drop-cache")
            .assert()
            .success();
    }

    let doit = || {
        let start = Instant::now();
        bfffs()
            .arg("--sock")
            .arg(harness.sockpath.to_str().unwrap())
            .arg("fs")
            .arg("destroy")
            .arg("testpool/testfs")
            .assert()
            .success();
        start.elapsed()
    };

    let mut before = geom::Snapshot::new().unwrap();

    let elapsed = if let Some(fname) = flamegraph {
        let pid = Pid::from_raw(harness.bfffsd.id() as libc::pid_t);
        dtraceit(doit, pid, &fname)
    } else {
        doit()
    };

    let mut after = geom::Snapshot::new().unwrap();

    println!(
        "fs_destroy: {}/s in {} s for {}",
        bibytes(fsize as f64 / elapsed.as_secs_f64()),
        elapsed.as_secs_f64(),
        bibytes(fsize as f64)
    );

    let etime = f64::from(after.timestamp() - before.timestamp());
    let mut tree = geom::Tree::new().unwrap();
    if use_libgeom {
        println!("name         ops    kb/t    ms/t   reads  writes  %b");
        for (afterstat, beforestat) in after.iter_pair(Some(&mut before)) {
            let stats = geom::Statistics::compute(afterstat, beforestat, etime);
            if let Some(gident) = tree.lookup(afterstat.id()) {
                if !gident.is_provider() {
                    continue;
                }
                if let Ok(ident) = gident.name() {
                    let ident = ident.to_string_lossy();
                    if !geom_regex.is_match(&ident) {
                        continue;
                    }
                    println!(
                        "{:8} {:>7.0} {:>7.0} {:>7.0} {:>7.0} {:>7.0} {:>3.0}",
                        ident,
                        stats.total_transfers(),
                        stats.kb_per_transfer_read(),
                        stats.ms_per_transaction_read(),
                        stats.total_transfers_read(),
                        stats.total_transfers_write(),
                        stats.busy_pct()
                    );
                }
            }
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli: Cli = Cli::parse();

    // Run individual benchmarks in serial
    fs_destroy(
        cli.devices,
        cli.fill_time,
        cli.options,
        cli.properties,
        cli.flamegraph,
        cli.drop_cache,
    )?;

    Ok(())
}
