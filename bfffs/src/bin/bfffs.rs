use bfffs_core::{
    database::TreeID,
    device_manager::DevManager,
    property::Property
};
use clap::{
    Clap,
    crate_version
};
use futures::TryFutureExt;
use std::{
    path::PathBuf,
    process::exit,
    sync::Arc
};
use tokio::runtime::{Builder, Runtime};

fn runtime() -> Runtime {
    Builder::new()
        .threaded_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

#[derive(Clap, Clone, Debug)]
/// Consistency check
struct Check {
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks: Vec<PathBuf>
}

impl Check{
    // Offline consistency check.  Checks that:
    // * RAID parity is consistent
    // * Checksums match
    // * RIDT and AllocT are exact inverses
    // * RIDT contains no orphan entries not found in the FSTrees
    // * Spacemaps match actual usage
    pub fn main(self) {
        let dev_manager = DevManager::default();
        for dev in self.disks.iter()
        {
            dev_manager.taste(dev);
        }

        let mut rt = runtime();
        let handle = rt.handle().clone();
        let db = Arc::new(
            dev_manager.import_by_name(self.pool_name, handle)
            .unwrap_or_else(|_e| {
                eprintln!("Error: pool not found");
                exit(1);
            })
        );
        rt.block_on(async {
            db.check().await
        }).unwrap();
        // TODO: the other checks
    }
}

#[derive(Clap, Clone, Debug)]
/// Dump internal filesystem information
struct Dump {
    /// Dump the Free Space Map
    #[clap(short, long)]
    fsm: bool,
    /// Dump the file system tree
    #[clap(short, long)]
    tree: bool,
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks: Vec<PathBuf>
}

impl Dump {
    fn dump_fsm(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk);
        }
        let uuid = dev_manager.importable_pools().iter()
            .find(|(name, _uuid)| {
                *name == self.pool_name
            }).unwrap().1;
        let mut rt = runtime();
        let clusters = rt.block_on(async move {
            dev_manager.import_clusters(uuid).await
        }).unwrap();
        for c in clusters {
            println!("{}", c.dump_fsm());
        }
    }

    fn dump_tree(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk);
        }
        let rt = runtime();
        let handle = rt.handle().clone();
        let db = Arc::new(
            dev_manager.import_by_name(self.pool_name, handle)
            .unwrap_or_else(|_e| {
                eprintln!("Error: pool not found");
                exit(1);
            })
        );
        // For now, hardcode tree_id to 0
        let tree_id = TreeID::Fs(0);
        db.dump(&mut std::io::stdout(), tree_id).unwrap()
    }

    fn main(self) {
        if self.fsm {
            self.dump_fsm();
        } else if self.tree {
            self.dump_tree();
        }
    }
}

#[derive(Clap, Clone, Debug)]
/// Debugging tools
enum DebugCmd {
    Dump(Dump)
}

mod pool {
    use bfffs_core::{
        BYTES_PER_LBA,
        cache::Cache,
        cluster::Cluster,
        database::*,
        ddml::DDML,
        idml::IDML,
        pool::Pool,
    };
    use std::{
        convert::TryFrom,
        num::NonZeroU64,
        sync::Mutex
    };
    use super::*;

    /// Create a new storage pool
    #[derive(Clap, Clone, Debug)]
    pub(super) struct Create {
        /// Dataset properties, comma delimited
        #[clap(short, long, require_delimiter(true))]
        properties: Vec<String>,
        /// Simulated zone size in MB
        #[clap(long)]
        zone_size: Option<u64>,
        #[clap(required(true))]
        /// Pool name
        pool_name: String,
        #[clap(required(true))]
        vdev: Vec<String>
    }

    impl Create{
        pub(super) fn main(self) {
            let rt = runtime();
            let zone_size = self.zone_size
                .map(|mbs| {
                    let lbas = mbs * 1024 * 1024 / (BYTES_PER_LBA as u64);
                    NonZeroU64::new(lbas).expect("zone_size may not be zero")
                });

            let props = self.properties.iter().map(String::as_str);
            let mut builder = Builder::new(self.pool_name, props, zone_size, rt);
            let mut vdev_tokens = self.vdev.iter().map(String::as_str);
            let mut cluster_type = None;
            let mut devs = vec![];
            loop {
                let next = vdev_tokens.next();
                match next {
                    None => {
                        if !devs.is_empty() {
                            match cluster_type {
                                Some("mirror") =>
                                    builder.create_mirror(&devs[..]),
                                Some("raid") => builder.create_raid(&devs[..]),
                                None => assert!(devs.is_empty()),
                                _ => unreachable!()
                            }
                        }
                        break;
                    },
                    Some("mirror") => {
                        if !devs.is_empty() {
                            builder.create_cluster(
                                cluster_type.as_ref().unwrap(),
                                &devs[..]
                            );
                        }
                        devs.clear();
                        cluster_type = Some("mirror")
                    },
                    Some("raid") => {
                        if !devs.is_empty() {
                            builder.create_cluster(
                                cluster_type.as_ref().unwrap(),
                                &devs[..]);
                        }
                        devs.clear();
                        cluster_type = Some("raid")
                    },
                    Some(dev) => {
                        if cluster_type == None {
                            builder.create_single(dev);
                        } else {
                            devs.push(dev);
                        }
                    }
                }
            }
            builder.format()
        }
    }

    struct Builder {
        clusters: Vec<Cluster>,
        name: String,
        properties: Vec<Property>,
        rt: Runtime,
        zone_size: Option<NonZeroU64>
    }

    impl Builder {
        pub fn new<'a, P>(name: String, propstrings: P,
                   zone_size: Option<NonZeroU64>, rt: Runtime)
            -> Self
            where P: Iterator<Item=&'a str> + 'a
        {
            let clusters = Vec::new();
            let properties = propstrings.map(|ps| {
                    Property::try_from(ps).unwrap_or_else(|_e| {
                        eprintln!("Invalid property specification {}", ps);
                        std::process::exit(2);
                    })
                })
                .collect::<Vec<_>>();
            Builder{clusters, name, properties, rt, zone_size}
        }

        pub fn create_cluster(&mut self, vtype: &str, devs: &[&str]) {
            match vtype {
                "mirror" => self.create_mirror(devs),
                "raid" => self.create_raid(devs),
                _ => panic!("Unsupported vdev type {}", vtype)
            }
        }

        pub fn create_mirror(&mut self, devs: &[&str]) {
            // TODO: allow creating declustered mirrors
            let k = devs.len() as i16;
            let f = devs.len() as i16 - 1;
            self.do_create_cluster(k, f, &devs[2..])
        }

        pub fn create_raid(&mut self, devs: &[&str]) {
            let k = devs[0].parse()
                .expect("Disks per stripe must be an integer");
            let f = devs[1].parse()
                .expect("Disks per stripe must be an integer");
            self.do_create_cluster(k, f, &devs[2..])
        }

        pub fn create_single(&mut self, dev: &str) {
            self.do_create_cluster(1, 0, &[dev])
        }

        fn do_create_cluster(&mut self, k: i16, f: i16, devs: &[&str])
        {
            let zone_size = self.zone_size;
            let c = Pool::create_cluster(None, k, zone_size, f, devs);
            self.clusters.push(c);
        }

        /// Actually format the disks
        pub fn format(&mut self) {
            let name = self.name.clone();
            let clusters = self.clusters.drain(..).collect();
            let handle = self.rt.handle().clone();
            let db = {
                let pool = Pool::create(name, clusters);
                let cache = Arc::new(
                    Mutex::new(Cache::with_capacity(4_194_304))
                );
                let ddml = Arc::new(DDML::new(pool, cache.clone()));
                let idml = Arc::new(IDML::create(ddml, cache));
                Database::create(idml, handle)
            };
            let props = self.properties.clone();
            self.rt.block_on(async move {
                db.new_fs(props)
                .and_then(|_tree_id| db.sync_transaction())
                .await
            }).unwrap();
        }
    }

    #[derive(Clap, Clone, Debug)]
    /// Create, destroy, and modify storage pools
    pub(super) enum PoolCmd {
        Create(Create)
    }

}

#[derive(Clap, Clone, Debug)]
enum SubCommand{
    Check(Check),
    Debug(DebugCmd),
    Pool(pool::PoolCmd)
}

#[derive(Clap, Clone, Debug)]
#[clap(version = crate_version!())]
struct Bfffs {
    #[clap(subcommand)]
    cmd: SubCommand,
}

fn main() {
    let bfffs: Bfffs = Bfffs::parse();
    match bfffs.cmd {
        SubCommand::Check(check) => check.main(),
        SubCommand::Debug(DebugCmd::Dump(dump)) => dump.main(),
        SubCommand::Pool(pool::PoolCmd::Create(create)) => create.main(),
    }
}
