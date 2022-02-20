use std::{path::PathBuf, process::exit, sync::Arc};

use bfffs::{Bfffs, Result};
use bfffs_core::{
    database::TreeID,
    device_manager::DevManager,
    property::Property,
};
use clap::{crate_version, Parser};
use futures::{future, TryFutureExt, TryStreamExt};

#[derive(Parser, Clone, Debug)]
/// Consistency check
struct Check {
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks:     Vec<PathBuf>,
}

impl Check {
    // Offline consistency check.  Checks that:
    // * RAID parity is consistent
    // * Checksums match
    // * RIDT and AllocT are exact inverses
    // * RIDT contains no orphan entries not found in the FSTrees
    // * Spacemaps match actual usage
    pub async fn main(self) -> Result<()> {
        let dev_manager = DevManager::default();
        for dev in self.disks.iter() {
            dev_manager.taste(dev).await.unwrap();
        }

        let db = Arc::new(
            dev_manager
                .import_by_name(self.pool_name)
                .await
                .unwrap_or_else(|_e| {
                    eprintln!("Error: pool not found");
                    exit(1);
                }),
        );
        db.check().await.unwrap();
        // TODO: the other checks
        Ok(())
    }
}

#[derive(Parser, Clone, Debug)]
/// Dump internal filesystem information
struct Dump {
    /// Dump the Forest
    #[clap(long)]
    forest:    bool,
    /// Dump the Free Space Map
    #[clap(short, long)]
    fsm:       bool,
    /// Dump the file system tree
    #[clap(short, long)]
    tree:      bool,
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks:     Vec<PathBuf>,
}

impl Dump {
    async fn dump_forest(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk).await.unwrap();
        }
        let db = dev_manager
            .import_by_name(self.pool_name)
            .await
            .unwrap_or_else(|_e| {
                eprintln!("Error: pool not found");
                exit(1);
            });
        db.dump_forest(&mut std::io::stdout()).await.unwrap()
    }

    async fn dump_fsm(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk).await.unwrap();
        }
        let uuid = dev_manager
            .importable_pools()
            .iter()
            .find(|(name, _uuid)| *name == self.pool_name)
            .unwrap()
            .1;
        let clusters = dev_manager.import_clusters(uuid).await.unwrap();
        for c in clusters {
            println!("{}", c.dump_fsm());
        }
    }

    async fn dump_tree(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk).await.unwrap();
        }
        let db = dev_manager
            .import_by_name(self.pool_name)
            .await
            .unwrap_or_else(|_e| {
                eprintln!("Error: pool not found");
                exit(1);
            });
        let db = Arc::new(db);
        // For now, hardcode tree_id to 0
        let tree_id = TreeID(0);
        db.dump_fs(&mut std::io::stdout(), tree_id).await.unwrap()
    }

    async fn main(self) -> Result<()> {
        if self.forest {
            self.dump_forest().await;
        } else if self.fsm {
            self.dump_fsm().await;
        } else if self.tree {
            self.dump_tree().await
        }
        Ok(())
    }
}

#[derive(Parser, Clone, Debug)]
/// Debugging tools
enum DebugCmd {
    Dump(Dump),
}

mod fs {
    use std::path::Path;

    use super::*;

    /// Create a new file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Create {
        /// File system name
        pub(super) name:       String,
        /// File system properties, comma delimited
        #[clap(
            short = 'o',
            long,
            require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) properties: Vec<String>,
    }

    impl Create {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            let props = self
                .properties
                .iter()
                .map(|ps| {
                    Property::try_from(ps.as_str()).unwrap_or_else(|_e| {
                        eprintln!("Invalid property specification {}", ps);
                        std::process::exit(2);
                    })
                })
                .collect::<Vec<_>>();
            bfffs.fs_create(self.name, props).await.map(drop)
        }
    }

    /// List file systems
    #[derive(Parser, Clone, Debug)]
    pub(super) struct List {
        pub(super) datasets: Vec<String>,
    }

    impl List {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            // TODO: recursion, sorting, properties
            // Sort datasets by name, until other sort options are added
            let mut all = Vec::new();
            for ds in self.datasets.into_iter() {
                bfffs
                    .fs_list(ds, None)
                    .try_for_each(|dsinfo| {
                        all.push(dsinfo);
                        future::ok(())
                    })
                    .await?;
            }
            all.sort_unstable_by(|x, y| x.name.cmp(&y.name));
            for dsinfo in all.into_iter() {
                println!("{}", dsinfo.name);
            }
            Ok(())
        }
    }

    /// Mount a file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Mount {
        /// Mount options, comma delimited
        #[clap(
            short = 'o',
            long,
            require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) options:    Vec<String>,
        /// File system name, including the pool.
        pub(super) name:       String,
        /// Mountpoint
        pub(super) mountpoint: PathBuf,
    }

    impl Mount {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            bfffs.fs_mount(self.name, self.mountpoint).await
        }
    }

    #[derive(Parser, Clone, Debug)]
    /// Create, destroy, and modify file systems
    pub(super) enum FsCmd {
        Create(Create),
        List(List),
        Mount(Mount),
    }
}

mod pool {
    use std::{num::NonZeroU64, sync::Mutex};

    use bfffs_core::{
        cache::Cache,
        cluster::Cluster,
        database::*,
        ddml::DDML,
        idml::IDML,
        pool::Pool,
        BYTES_PER_LBA,
    };

    use super::*;

    /// Create a new storage pool
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Create {
        /// Dataset properties, comma delimited
        #[clap(
            short,
            long,
            require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) properties: Vec<String>,
        /// Simulated zone size in MB
        #[clap(long)]
        pub(super) zone_size:  Option<u64>,
        #[clap(required(true))]
        /// Pool name
        pub(super) pool_name:  String,
        #[clap(required(true))]
        pub(super) vdev:       Vec<String>,
    }

    impl Create {
        pub(super) async fn main(self) -> Result<()> {
            let zone_size = self.zone_size.map(|mbs| {
                let lbas = mbs * 1024 * 1024 / (BYTES_PER_LBA as u64);
                NonZeroU64::new(lbas).expect("zone_size may not be zero")
            });

            let props = self.properties.iter().map(String::as_str);
            let mut builder = Builder::new(self.pool_name, props, zone_size);
            let mut vdev_tokens = self.vdev.iter().map(String::as_str);
            let mut cluster_type = None;
            let mut devs = vec![];
            loop {
                let next = vdev_tokens.next();
                match next {
                    None => {
                        if !devs.is_empty() {
                            match cluster_type {
                                Some("mirror") => {
                                    builder.create_mirror(&devs[..])
                                }
                                Some("raid") => builder.create_raid(&devs[..]),
                                None => assert!(devs.is_empty()),
                                _ => unreachable!(), // LCOV_EXCL_LINE
                            }
                        }
                        break;
                    }
                    Some("mirror") => {
                        if !devs.is_empty() {
                            builder.create_cluster(
                                cluster_type.as_ref().unwrap(),
                                &devs[..],
                            );
                        }
                        devs.clear();
                        cluster_type = Some("mirror")
                    }
                    Some("raid") => {
                        if !devs.is_empty() {
                            builder.create_cluster(
                                cluster_type.as_ref().unwrap(),
                                &devs[..],
                            );
                        }
                        devs.clear();
                        cluster_type = Some("raid")
                    }
                    Some(dev) => {
                        if cluster_type == None {
                            builder.create_single(dev);
                        } else {
                            devs.push(dev);
                        }
                    }
                }
            }
            builder.format().await;
            Ok(())
        }
    }

    struct Builder {
        clusters:   Vec<Cluster>,
        name:       String,
        properties: Vec<Property>,
        zone_size:  Option<NonZeroU64>,
    }

    impl Builder {
        pub fn new<'a, P>(
            name: String,
            propstrings: P,
            zone_size: Option<NonZeroU64>,
        ) -> Self
        where
            P: Iterator<Item = &'a str> + 'a,
        {
            let clusters = Vec::new();
            let properties = propstrings
                .map(|ps| {
                    Property::try_from(ps).unwrap_or_else(|_e| {
                        eprintln!("Invalid property specification {}", ps);
                        std::process::exit(2);
                    })
                })
                .collect::<Vec<_>>();
            Builder {
                clusters,
                name,
                properties,
                zone_size,
            }
        }

        pub fn create_cluster(&mut self, vtype: &str, devs: &[&str]) {
            match vtype {
                "mirror" => self.create_mirror(devs),
                "raid" => self.create_raid(devs),
                _ => panic!("Unsupported vdev type {}", vtype),
            }
        }

        pub fn create_mirror(&mut self, devs: &[&str]) {
            // TODO: allow creating declustered mirrors
            let k = devs.len() as i16;
            let f = devs.len() as i16 - 1;
            self.do_create_cluster(k, f, &devs[2..])
        }

        pub fn create_raid(&mut self, devs: &[&str]) {
            let k = devs[0]
                .parse()
                .expect("Disks per stripe must be an integer");
            let f = devs[1]
                .parse()
                .expect("Redundancy level must be an integer");
            self.do_create_cluster(k, f, &devs[2..])
        }

        pub fn create_single(&mut self, dev: &str) {
            self.do_create_cluster(1, 0, &[dev])
        }

        fn do_create_cluster(&mut self, k: i16, f: i16, devs: &[&str]) {
            let zone_size = self.zone_size;
            let c = Pool::create_cluster(None, k, zone_size, f, devs);
            self.clusters.push(c);
        }

        /// Actually format the disks
        pub async fn format(mut self) {
            let name = self.name.clone();
            let clusters = self.clusters.drain(..).collect();
            let props = self.properties.clone();
            let db = {
                let pool = Pool::create(name, clusters);
                let cache =
                    Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
                let ddml = Arc::new(DDML::new(pool, cache.clone()));
                let idml = Arc::new(IDML::create(ddml, cache));
                Database::create(idml)
            };
            // Create the root file system
            db.create_fs(None, "", props)
                .and_then(|_tree_id| db.sync_transaction())
                .await
                .unwrap()
        }
    }

    #[derive(Parser, Clone, Debug)]
    /// Create, destroy, and modify storage pools
    pub(super) enum PoolCmd {
        Create(Create),
    }
}

#[derive(Parser, Clone, Debug)]
enum SubCommand {
    Check(Check),
    #[clap(subcommand)]
    Debug(DebugCmd),
    #[clap(subcommand)]
    Fs(fs::FsCmd),
    #[clap(subcommand)]
    Pool(pool::PoolCmd),
}

#[derive(Parser, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    /// Path to the bfffsd socket
    #[clap(long, default_value = "/var/run/bfffsd.sock")]
    sock: PathBuf,
    #[clap(subcommand)]
    cmd:  SubCommand,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    match cli.cmd {
        SubCommand::Check(check) => check.main().await,
        SubCommand::Fs(fs::FsCmd::Create(create)) => {
            create.main(&cli.sock).await
        }
        SubCommand::Fs(fs::FsCmd::List(list)) => list.main(&cli.sock).await,
        SubCommand::Fs(fs::FsCmd::Mount(mount)) => mount.main(&cli.sock).await,
        SubCommand::Debug(DebugCmd::Dump(dump)) => dump.main().await,
        SubCommand::Pool(pool::PoolCmd::Create(create)) => create.main().await,
    }
}

#[cfg(test)]
mod t {
    use std::path::Path;

    use clap::ErrorKind::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["bfffs"])]
    #[case(vec!["bfffs", "check"])]
    #[case(vec!["bfffs", "check", "testpool"])]
    #[case(vec!["bfffs", "debug"])]
    #[case(vec!["bfffs", "debug", "dump"])]
    #[case(vec!["bfffs", "debug", "dump", "testpool"])]
    #[case(vec!["bfffs", "fs", "create"])]
    #[case(vec!["bfffs", "fs", "mount", "testpool"])]
    #[case(vec!["bfffs", "pool"])]
    #[case(vec!["bfffs", "pool", "create"])]
    #[case(vec!["bfffs", "pool", "create", "testpool"])]
    fn missing_arg(#[case] args: Vec<&str>) {
        let e = Cli::try_parse_from(args).unwrap_err();
        assert!(
            e.kind() == MissingRequiredArgument ||
                e.kind() == DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn check() {
        let args = vec!["bfffs", "check", "testpool", "/dev/da0", "/dev/da1"];
        let cli = Cli::try_parse_from(args).unwrap();
        assert!(matches!(cli.cmd, SubCommand::Check(_)));
        if let SubCommand::Check(check) = cli.cmd {
            assert_eq!(check.pool_name, "testpool");
            assert_eq!(check.disks[0], Path::new("/dev/da0"));
            assert_eq!(check.disks[1], Path::new("/dev/da1"));
        }
    }

    mod debug {
        use super::*;

        #[test]
        fn dump_fsm() {
            let args = vec![
                "bfffs", "debug", "dump", "-f", "testpool", "/dev/da0",
                "/dev/da1",
            ];
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.cmd, SubCommand::Debug(_)));
            if let SubCommand::Debug(DebugCmd::Dump(debug)) = cli.cmd {
                assert_eq!(debug.pool_name, "testpool");
                assert!(debug.fsm);
                assert!(!debug.tree);
                assert_eq!(debug.disks[0], Path::new("/dev/da0"));
                assert_eq!(debug.disks[1], Path::new("/dev/da1"));
            }
        }

        #[test]
        fn dump_tree() {
            let args = vec![
                "bfffs", "debug", "dump", "-t", "testpool", "/dev/da0",
                "/dev/da1",
            ];
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.cmd, SubCommand::Debug(_)));
            if let SubCommand::Debug(DebugCmd::Dump(debug)) = cli.cmd {
                assert_eq!(debug.pool_name, "testpool");
                assert!(!debug.fsm);
                assert!(debug.tree);
                assert_eq!(debug.disks[0], Path::new("/dev/da0"));
                assert_eq!(debug.disks[1], Path::new("/dev/da1"));
            }
        }
    }

    mod fs {
        use super::*;
        use crate::fs::*;

        mod create {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "create", "testpool/foo"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Create(_))));
                if let SubCommand::Fs(FsCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.name, "testpool/foo");
                    assert!(create.properties.is_empty());
                }
            }

            #[test]
            fn props() {
                let args = vec![
                    "bfffs",
                    "fs",
                    "create",
                    "-o",
                    "atime=off,recsize=65536",
                    "testpool/foo",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Create(_))));
                if let SubCommand::Fs(FsCmd::Create(create)) = cli.cmd {
                    assert_eq!(
                        create.properties,
                        vec!["atime=off", "recsize=65536"]
                    );
                }
            }
        }

        mod mount {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "mount", "testpool", "/mnt"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Mount(_))));
                if let SubCommand::Fs(FsCmd::Mount(mount)) = cli.cmd {
                    assert_eq!(mount.name, "testpool");
                    assert_eq!(mount.mountpoint, Path::new("/mnt"));
                    assert!(mount.options.is_empty());
                }
            }

            #[test]
            fn subfs() {
                let args = vec!["bfffs", "fs", "mount", "testpool/foo", "/mnt"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Mount(_))));
                if let SubCommand::Fs(FsCmd::Mount(mount)) = cli.cmd {
                    assert_eq!(mount.name, "testpool/foo");
                    assert_eq!(mount.mountpoint, Path::new("/mnt"));
                    assert!(mount.options.is_empty());
                }
            }
        }
    }

    mod pool {
        use super::*;
        use crate::pool::*;

        mod create {
            use super::*;

            #[test]
            fn plain() {
                let args =
                    vec!["bfffs", "pool", "create", "testpool", "/dev/da0"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.pool_name, "testpool");
                    assert!(create.properties.is_empty());
                    assert!(create.zone_size.is_none());
                    assert_eq!(create.vdev[0], "/dev/da0");
                }
            }

            #[test]
            fn props() {
                let args = vec![
                    "bfffs",
                    "pool",
                    "create",
                    "-p",
                    "atime=off,recsize=65536",
                    "testpool",
                    "/dev/da0",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(
                        create.properties,
                        vec!["atime=off", "recsize=65536"]
                    );
                }
            }

            #[test]
            fn zone_size() {
                let args = vec![
                    "bfffs",
                    "pool",
                    "create",
                    "--zone-size",
                    "128",
                    "testpool",
                    "/dev/da0",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.zone_size, Some(128));
                }
            }
        }
    }
}
