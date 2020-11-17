use bfffs::common::{
    database::TreeID,
    device_manager::DevManager,
    property::Property
};
use clap::crate_version;
use futures::TryFutureExt;
use std::{
    path::Path,
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

mod check {
use super::*;

// Offline consistency check.  Checks that:
// * RAID parity is consistent
// * Checksums match
// * RIDT and AllocT are exact inverses
// * RIDT contains no orphan entries not found in the FSTrees
// * Spacemaps match actual usage
pub fn main(args: &clap::ArgMatches) {
    let poolname = args.value_of("name").unwrap().to_owned();
    let disks = args.values_of("disks").unwrap();
    let dev_manager = DevManager::default();
    for dev in disks.map(str::to_string)
    {
        dev_manager.taste(dev);
    }

    let mut rt = runtime();
    let handle = rt.handle().clone();
    let db = Arc::new(
        dev_manager.import_by_name(poolname, handle)
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

mod debug {
use super::*;

fn dump_fsm<P: AsRef<Path>, S: AsRef<str>>(poolname: S, disks: &[P]) {
    let dev_manager = DevManager::default();
    for disk in disks {
        dev_manager.taste(disk);
    }
    let uuid = dev_manager.importable_pools().iter()
        .find(|(name, _uuid)| {
            *name == poolname.as_ref()
        }).unwrap().1;
    let mut rt = runtime();
    let clusters = rt.block_on(async move {
        dev_manager.import_clusters(uuid).await
    }).unwrap();
    for c in clusters {
        println!("{}", c.dump_fsm());
    }
}

fn dump_tree<P: AsRef<Path>>(poolname: String, disks: &[P]) {
    let dev_manager = DevManager::default();
    for disk in disks {
        dev_manager.taste(disk);
    }
    let rt = runtime();
    let handle = rt.handle().clone();
    let db = Arc::new(
        dev_manager.import_by_name(poolname, handle)
        .unwrap_or_else(|_e| {
            eprintln!("Error: pool not found");
            exit(1);
        })
    );
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    db.dump(&mut std::io::stdout(), tree_id).unwrap()
}

pub fn main(args: &clap::ArgMatches) {
    match args.subcommand() {
        ("dump", Some(args)) => {
            let poolname = args.value_of("name").unwrap();
            let disks = args.values_of("disks").unwrap().collect::<Vec<&str>>();
            if args.is_present("fsm") {
                dump_fsm(&poolname, &disks[..]);
            }
            if args.is_present("tree") {
                dump_tree(poolname.to_string(), &disks[..]);
            }
        },
        _ => {
            println!("Error: subcommand required\n{}", args.usage());
            std::process::exit(2);
        },
    }
}

}

mod pool {
use bfffs::common::BYTES_PER_LBA;
use bfffs::common::cache::Cache;
use bfffs::common::cluster::Cluster;
use bfffs::common::database::*;
use bfffs::common::ddml::DDML;
use bfffs::common::idml::IDML;
use bfffs::common::pool::Pool;
use std::{
    convert::TryFrom,
    num::NonZeroU64,
    str::FromStr,
    sync::Mutex
};
use super::*;

fn create(args: &clap::ArgMatches) {
    let rt = runtime();
    let name = args.value_of("name").unwrap().to_owned();
     let zone_size = args.value_of("zone_size")
        .map(|s| {
            let lbas = u64::from_str(s)
             .expect("zone_size must be a decimal integer")
             * 1024 * 1024 / (BYTES_PER_LBA as u64);
            NonZeroU64::new(lbas).expect("zone_size may not be zero")
        });
    let propstrings = if let Some(it) = args.values_of("property") {
         it.collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut builder = Builder::new(name, propstrings, zone_size, rt);
    let mut vdev_tokens = args.values_of("vdev").unwrap();
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
                    builder.create_cluster(cluster_type.as_ref().unwrap(),
                                           &devs[..]);
                }
                devs.clear();
                cluster_type = Some("mirror")
            },
            Some("raid") => {
                if !devs.is_empty() {
                    builder.create_cluster(cluster_type.as_ref().unwrap(),
                                           &devs[..]);
                }
                devs.clear();
                cluster_type = Some("raid")
            },
            Some(ref dev) => {
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

struct Builder {
    clusters: Vec<Cluster>,
    name: String,
    properties: Vec<Property>,
    rt: Runtime,
    zone_size: Option<NonZeroU64>
}

impl Builder {
    pub fn new(name: String, propstrings: Vec<&str>,
               zone_size: Option<NonZeroU64>, rt: Runtime)
        -> Self
    {
        let clusters = Vec::new();
        let properties = propstrings.into_iter()
            .map(|ps| {
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
        let k = i16::from_str_radix(devs[0], 10)
            .expect("Disks per stripe must be an integer");
        let f = i16::from_str_radix(devs[1], 10)
            .expect("Disks per stripe must be an integer");
        self.do_create_cluster(k, f, &devs[2..])
    }

    pub fn create_single(&mut self, dev: &str) {
        self.do_create_cluster(1, 0, &[&dev])
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
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
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

pub fn main(args: &clap::ArgMatches) {
    match args.subcommand() {
        ("create", Some(create_args)) => create(create_args),
        _ => {
            println!("Error: subcommand required\n{}", args.usage());
            std::process::exit(2);
        },
    }
}

}

fn main() {
    let app = clap::App::new("bfffs")
        .version(crate_version!())
        .subcommand(clap::SubCommand::with_name("check")
            .about("Consistency check")
            .arg(clap::Arg::with_name("name")
                 .help("Pool name")
                 .required(true)
            ).arg(clap::Arg::with_name("disks")
                  .multiple(true)
                  .required(true)
            )
        ).subcommand(clap::SubCommand::with_name("debug")
            .about("Debugging tools")
            .subcommand(clap::SubCommand::with_name("dump")
                .about("Dump internal filesystem information")
                .arg(clap::Arg::with_name("fsm")
                     .help("Dump the Free Space Map")
                     .long("fsm")
                     .short("f")
                ).arg(clap::Arg::with_name("tree")
                     .help("Dump the file system tree")
                     .long("tree")
                     .short("t")
                ).arg(clap::Arg::with_name("name")
                     .help("Pool name")
                     .required(true)
                ).arg(clap::Arg::with_name("disks")
                      .multiple(true)
                      .required(true)
                )
            )
        ).subcommand(clap::SubCommand::with_name("pool")
            .about("create, destroy, and modify storage pools")
            .subcommand(clap::SubCommand::with_name("create")
                .about("create a new storage pool")
                .arg(clap::Arg::with_name("zone_size")
                     .help("Simulated Zone size in MB")
                     .long("zone_size")
                     .takes_value(true)
                ).arg(clap::Arg::with_name("property")
                     .help("Dataset properties, comma delimited")
                     .short("o")
                     .takes_value(true)
                     .multiple(true)
                     .require_delimiter(true)
                ).arg(clap::Arg::with_name("name")
                     .help("Pool name")
                     .required(true)
                ).arg(clap::Arg::with_name("vdev")
                      .multiple(true)
                      .required(true)
                )
            )
        );
    let matches = app.get_matches();
    match matches.subcommand() {
        ("check", Some(args)) => check::main(args),
        ("debug", Some(args)) => debug::main(args),
        ("pool", Some(args)) => pool::main(args),
        _ => {
            println!("Error: subcommand required\n{}", matches.usage());
            std::process::exit(2);
        },
    }
}
