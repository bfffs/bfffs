// vim: tw=80
use std::{
    ffi::OsString,
    fs,
    future::Future,
    io::Write,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{self, Command, Stdio},
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;
use clap::{crate_version, Parser};
use freebsd_libgeom as geom;
use nix::{
    mount::{unmount, MntFlags},
    sys::signal,
    unistd::Pid,
};
use regex::Regex;
use tempfile::{Builder, TempDir};
use tracing_subscriber::EnvFilter;
pub mod util {
    include!("../../tests/integration/util.rs");
}
pub use util::bfffs;
use util::{bfffsd, waitfor, Bfffsd};

mod fs_create;
mod fs_destroy;

#[enum_dispatch::enum_dispatch(BenchmarkImpl)]
trait Benchmark {
    async fn setup(&mut self, harness: &Harness);
    async fn run(&mut self, harness: &Harness);
    //async fn teardown(&mut self);
    fn print(&self, elapsed: &Duration);
}

#[enum_dispatch::enum_dispatch]
enum BenchmarkImpl {
    FsCreate(fs_create::FsCreate),
    FsDestroy(fs_destroy::FsDestroy),
}

/// The standard test harness used by all benchmarks that need bfffsd
pub struct Harness {
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

impl Harness {
    pub fn new(mut devices: Vec<String>, options: Option<String>) -> Self {
        let tempdir = Builder::new().prefix("bfffs-bench").tempdir().unwrap();
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

    /// Return the daemon's pid
    pub fn pid(&self) -> Pid {
        Pid::from_raw(self.bfffsd.id() as libc::pid_t)
    }
}

#[derive(Parser, Clone, Debug)]
enum SubCommand {
    FsCreate(fs_create::Cli),
    FsDestroy(fs_destroy::Cli),
}

#[derive(Parser, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    /// Create pool on these devices, or create a file-backed pool if blank.
    #[clap(short = 'd', long, value_delimiter(','))]
    devices:    Vec<String>,
    #[clap(long, help = "Record a flamegraph to this file")]
    flamegraph: Option<PathBuf>,
    /// Daemon options, comma delimited.
    #[clap(short = 'o', long, value_delimiter(','))]
    options:    Option<String>,
    /// Name of the specific benchmark to run, or None to smoke test all
    /// benchmarks.
    #[clap(subcommand)]
    subcommand: Option<SubCommand>,
    /// Cargo always passes either --bench when invoked like
    /// "cargo bench --bench bfffs-bench", even when the standard benchmark
    /// harness is not in use.  So we need to accept such arguments, and ignore
    /// them.
    #[clap(long, hide = true)]
    bench:      bool,
}

/// Run a function while monitoring the given PID with dtrace.  Record its
/// flamegraph to the specified file.
async fn dtraceit<B, F>(f: F, pid: Pid, fname: &Path) -> Duration
where
    F: FnOnce() -> B,
    B: Future<Output = ()>,
{
    let mut dtrace = Command::new("dtrace")
        .stdout(Stdio::piped())
        .arg("-p")
        .arg(format!("{pid}"))
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

    let start = Instant::now();
    f().await;
    let elapsed = start.elapsed();

    signal::kill(pid, signal::Signal::SIGINT).unwrap();
    dtrace.wait().unwrap();
    let svg = flamegraph.wait_with_output().unwrap().stdout;
    fs::File::create(fname).unwrap().write_all(&svg).unwrap();

    elapsed
}

async fn bench_one<'a>(
    flamegraph: Option<&'a PathBuf>,
    geom_regex: &'a Option<Regex>,
    mut benchmark: BenchmarkImpl,
    harness: &'a Harness,
) {
    benchmark.setup(harness).await;

    let mut before = geom::Snapshot::new().unwrap();
    let elapsed = if let Some(fname) = flamegraph {
        let pid = harness.pid();
        dtraceit(|| benchmark.run(harness), pid, fname).await
    } else {
        let start = Instant::now();
        benchmark.run(harness).await;
        start.elapsed()
    };
    let mut after = geom::Snapshot::new().unwrap();

    benchmark.print(&elapsed);
    let etime = f64::from(after.timestamp() - before.timestamp());
    let mut tree = geom::Tree::new().unwrap();
    if let Some(geom_regex) = geom_regex {
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
}

async fn bench_all<'a>(geom_regex: &'a Option<Regex>, harness: &'a Harness) {
    for benchmark in [
        BenchmarkImpl::FsCreate(
            fs_create::Cli::parse_from::<_, OsString>([]).build(),
        ),
        BenchmarkImpl::FsDestroy(
            fs_destroy::Cli::parse_from::<_, OsString>([]).build(),
        ),
    ] {
        bench_one(None, geom_regex, benchmark, harness).await
    }
}

#[tokio::main(flavor = "current_thread")]
#[function_name::named]
async fn main() {
    require_fusefs!();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli: Cli = Cli::parse();

    let sdevs = cli
        .devices
        .iter()
        .filter_map(|fulldev| fulldev.strip_prefix("/dev/"))
        .collect::<Vec<_>>();
    let use_libgeom = !sdevs.is_empty();
    let geom_regex = if use_libgeom {
        Some(Regex::new(&format!("^({})$", &sdevs[..].join("|"))).unwrap())
    } else {
        None
    };

    let harness = Harness::new(cli.devices, cli.options);
    let benchmark = match cli.subcommand {
        Some(SubCommand::FsCreate(subcmd)) => {
            BenchmarkImpl::FsCreate(subcmd.build())
        }
        Some(SubCommand::FsDestroy(subcmd)) => {
            BenchmarkImpl::FsDestroy(subcmd.build())
        }
        None => {
            if cli.flamegraph.is_some() {
                eprintln!(
                    "Error: cannot use --flamegraph without specifying a \
                     specific benchmark"
                );
                process::exit(2);
            }
            bench_all(&geom_regex, &harness).await;
            return;
        }
    };

    bench_one(cli.flamegraph.as_ref(), &geom_regex, benchmark, &harness).await;
}
