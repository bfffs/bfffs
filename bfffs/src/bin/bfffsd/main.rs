// vim: tw=80

use std::{
    fs::Permissions,
    io,
    os::unix::{fs::PermissionsExt, io::RawFd},
    path::{Path, PathBuf},
    process::exit,
    sync::Arc,
};

use bfffs_core::{database::*, device_manager::DevManager, rpc, Error};
use clap::{crate_version, Clap};
use fuse3::{raw::Session, MountOptions};
use futures::{Future, FutureExt};
use nix::{
    fcntl::{open, OFlag},
    sys::stat::Mode,
    unistd,
};
use tokio::signal::unix::{signal, SignalKind};
use tokio_seqpacket::{UCred, UnixSeqpacketListener};
use tracing::warn;
use tracing_subscriber::EnvFilter;

mod fs;

use crate::fs::FuseFs;

#[derive(Clap, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    /// Mount options, comma delimited.  Apply to all BFFFS mounts
    #[clap(short = 'o', long, require_delimiter(true), value_delimiter(','))]
    options:   Vec<String>,
    #[clap(long, default_value = "/var/run/bfffsd.sock")]
    sock:      PathBuf,
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    devices:   Vec<String>,
}

/// bfffsd's communications socket
struct Socket {
    sockpath: PathBuf,
    listener: UnixSeqpacketListener,
    _lockfd:  RawFd,
}

impl Drop for Socket {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ignore = std::fs::remove_file(&self.sockpath);
            self.sockpath.set_extension("lock");
            let _ignore = std::fs::remove_file(&self.sockpath);
        }
    }
}

impl Socket {
    fn new(path: &Path) -> Self {
        let sockpath = path.to_owned();
        let mut lockaddr = path.to_owned();
        lockaddr.set_extension("lock");
        let _lockfd = open(
            &lockaddr,
            OFlag::O_EXLOCK | OFlag::O_RDWR | OFlag::O_CREAT,
            Mode::from_bits(0o600).unwrap(),
        )
        .unwrap_or_else(|_| {
            eprintln!("Could not obtain lockfile");
            std::process::exit(1);
        });
        let _ignore_result = std::fs::remove_file(path);
        let listener = UnixSeqpacketListener::bind(path).unwrap();
        std::fs::set_permissions(path, Permissions::from_mode(0o666)).unwrap();
        Socket {
            sockpath,
            listener,
            _lockfd,
        }
    }
}

struct Bfffsd {
    db:           Arc<Database>,
    _dev_manager: DevManager,
    mount_opts:   MountOptions,
    sock:         Socket,
}

impl Bfffsd {
    async fn new(cli: Cli) -> Self {
        let mut cache_size: Option<usize> = None;
        let mut writeback_size: Option<usize> = None;

        let sock = Socket::new(&cli.sock);

        let mut mount_opts = MountOptions::default();
        mount_opts.fs_name("bfffs");
        if nix::unistd::getuid().is_root() {
            mount_opts.allow_other(true);
            mount_opts.default_permissions(true);
        }
        mount_opts.no_open_support(true);
        mount_opts.no_open_dir_support(true);
        // Unconditionally disable the kernel's buffer cache; BFFFS has its own
        mount_opts.custom_options("direct_io");
        for o in cli.options.iter() {
            if let Some((name, value)) = o.split_once("=") {
                if name == "cache_size" {
                    let v = value.parse().unwrap_or_else(|_| {
                        eprintln!("cache_size must be numeric");
                        exit(2);
                    });
                    cache_size = Some(v);
                    continue;
                } else if name == "writeback_size" {
                    let v = value.parse().unwrap_or_else(|_| {
                        eprintln!("writeback_size must be numeric");
                        exit(2);
                    });
                    writeback_size = Some(v);
                    continue;
                }
                // else, must be a mount_fusefs option
            }
            // Must be a mount_fusefs option
            mount_opts.custom_options(o);
        }

        let mut dev_manager = DevManager::default();
        if let Some(cs) = cache_size {
            dev_manager.cache_size(cs);
        }
        if let Some(wbs) = writeback_size {
            dev_manager.writeback_size(wbs);
        }

        for dev in cli.devices.iter() {
            // TODO: taste devices in parallel
            dev_manager.taste(dev).await.unwrap();
        }

        let uuid = dev_manager
            .importable_pools()
            .iter()
            .find(|(name, _uuid)| **name == cli.pool_name)
            .unwrap_or_else(|| {
                eprintln!("error: pool {} not found", cli.pool_name);
                std::process::exit(1);
            })
            .1;
        let db = Arc::new(dev_manager.import_by_uuid(uuid).await.unwrap());

        Bfffsd {
            db,
            _dev_manager: dev_manager,
            mount_opts,
            sock,
        }
    }

    fn mount(
        &self,
        mountpoint: PathBuf,
        tree_id: TreeID,
    ) -> impl Future<Output = Result<(), io::Error>> + Send {
        let mo2 = self.mount_opts.clone();
        FuseFs::new(self.db.clone(), tree_id)
            .then(move |fs| Session::new(mo2).mount(fs, mountpoint))
    }

    async fn process(&self, req: rpc::Request, creds: UCred) -> rpc::Response {
        match req {
            rpc::Request::Mount(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::Mount(Err(Error::EPERM))
                } else {
                    // TODO: check that the mount succeeded.  This will require
                    // an extension to fuse3.
                    tokio::spawn(self.mount(req.mountpoint, req.tree_id));
                    rpc::Response::Mount(Ok(()))
                }
            }
        }
    }

    async fn run(mut self) {
        let db2 = self.db.clone();

        // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term,
        // this is very handy for debugging the cleaner.
        tokio::spawn(async move {
            let mut stream = signal(SignalKind::user_defined1()).unwrap();
            loop {
                stream.recv().await;
                db2.clean().await.unwrap()
            }
        });

        loop {
            const BUFSIZ: usize = 4096;
            let mut buf = vec![0u8; BUFSIZ];

            let peer = self.sock.listener.accept().await.unwrap();
            let nread = peer.recv(&mut buf).await.unwrap();
            if nread == 0 {
                warn!("Client did not send request");
            } else if nread >= BUFSIZ {
                warn!("Client sent unexpectedly large request");
            } else {
                buf.truncate(nread);
                let req: rpc::Request = bincode::deserialize(&buf[..]).unwrap();
                let creds = peer.peer_cred().unwrap();
                let resp = self.process(req, creds).await;
                let encoded: Vec<u8> = bincode::serialize(&resp).unwrap();
                let nwrite = peer.send(&encoded).await;
                if nwrite.is_err() || nwrite.unwrap() != encoded.len() {
                    warn!("Client disconnected before reading response");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli: Cli = Cli::parse();

    let bfffsd = Bfffsd::new(cli).await;
    bfffsd.run().await;
}

#[cfg(test)]
mod t {
    use clap::ErrorKind::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["bfffsd"])]
    #[case(vec!["bfffsd", "testpool"])]
    fn missing_arg(#[case] args: Vec<&str>) {
        let e = Cli::try_parse_from(args).unwrap_err();
        assert!(
            e.kind == MissingRequiredArgument ||
                e.kind == DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn options() {
        let args = vec![
            "bfffsd",
            "-o",
            "allow_other,default_permissions",
            "testpool",
            "--sock",
            "/tmp/bfffs.sock",
            "/dev/da0",
        ];
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.pool_name, "testpool");
        assert_eq!(cli.sock, Path::new("/tmp/bfffs.sock"));
        assert_eq!(cli.options, vec!["allow_other", "default_permissions"]);
        assert_eq!(cli.devices[0], "/dev/da0");
    }

    #[test]
    fn plain() {
        let args = vec!["bfffsd", "testpool", "/dev/da0"];
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.pool_name, "testpool");
        assert_eq!(cli.sock, Path::new("/var/run/bfffsd.sock"));
        assert!(cli.options.is_empty());
        assert_eq!(cli.devices[0], "/dev/da0");
    }
}
