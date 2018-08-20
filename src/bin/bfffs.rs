extern crate bfffs;
#[macro_use] extern crate clap;
extern crate futures;
extern crate tokio;

mod debug {
use bfffs::common::device_manager::DevManager;
use futures::future;
use super::*;
use tokio::runtime::current_thread::Runtime;

fn dump(args: &clap::ArgMatches) {
    let poolname = args.value_of("name").unwrap();
    let disks = args.values_of("disks").unwrap();

    let dev_manager = DevManager::new();
    for disk in disks {
        dev_manager.taste(disk);
    }
    let uuid = dev_manager.importable_pools().iter()
        .filter(|(name, _uuid)| {
            *name == poolname
        }).nth(0).unwrap().1;
    let mut rt = Runtime::new().unwrap();
    let clusters = rt.block_on(future::lazy(move || {
        dev_manager.import_clusters(uuid)
    })).unwrap();
    for c in clusters {
        println!("{}", c.dump_fsm());
    }
}

pub fn main(args: &clap::ArgMatches) {
    match args.subcommand() {
        ("dump", Some(args)) => dump(args),
        _ => {
            println!("Error: subcommand required\n{}", args.usage());
            std::process::exit(2);
        },
    }
}

}

mod pool {
use bfffs::common::BYTES_PER_LBA;
use bfffs::common::LbaT;
use bfffs::common::cache::Cache;
use bfffs::common::database::*;
use bfffs::common::ddml::DDML;
use bfffs::common::idml::IDML;
use bfffs::common::pool::{ClusterProxy, Pool};
use futures::{Future, future};
use std::{
    num::NonZeroU64,
    str::FromStr,
    sync::{Arc, Mutex}
};
use super::*;
use tokio::{
    executor::current_thread::TaskExecutor,
    runtime::current_thread::Runtime
};

// TODO: specify CHUNKSIZE on the command line
const CHUNKSIZE: LbaT = 16;

fn create(args: &clap::ArgMatches) {
    let rt = Runtime::new().unwrap();
    let name = args.value_of("name").unwrap().to_owned();
     let zone_size = args.value_of("zone_size")
        .map(|s| {
            let lbas = u64::from_str(s)
             .expect("zone_size must be a decimal integer")
             * 1024 * 1024 / (BYTES_PER_LBA as u64);
            NonZeroU64::new(lbas).expect("zone_size may not be zero")
        });
    let mut builder = Builder::new(name, zone_size, rt);
    let mut values = args.values_of("vdev").unwrap();
    let mut cluster_type = None;
    let mut devs = vec![];
    loop {
        let next = values.next();
        match next {
            None => {
                if devs.len() > 0 {
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
                if devs.len() > 0 {
                    builder.create_cluster(cluster_type.as_ref().unwrap(),
                                           &devs[..]);
                }
                devs.clear();
                cluster_type = Some("mirror")
            },
            Some("raid") => {
                if devs.len() > 0 {
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
    clusters: Vec<ClusterProxy>,
    name: String,
    rt: Runtime,
    zone_size: Option<NonZeroU64>
}

impl Builder {
    pub fn new(name: String, zone_size: Option<NonZeroU64>, rt: Runtime)
        -> Self
    {
        let clusters = Vec::new();
        Builder{clusters, name, rt, zone_size}
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
        let n = devs.len() as i16;
        let k = devs.len() as i16;
        let f = devs.len() as i16 - 1;
        self.do_create_cluster(n, k, f, &devs[2..])
    }

    pub fn create_raid(&mut self, devs: &[&str]) {
        let n = devs.len() as i16 - 2;
        let k = i16::from_str_radix(devs[0], 10)
            .expect("Disks per stripe must be an integer");
        let f = i16::from_str_radix(devs[1], 10)
            .expect("Disks per stripe must be an integer");
        self.do_create_cluster(n, k, f, &devs[2..])
    }

    pub fn create_single(&mut self, dev: &str) {
        self.do_create_cluster(1, 1, 0, &[&dev])
    }

    fn do_create_cluster(&mut self, n: i16, k: i16, f: i16, devs: &[&str])
    {
        let zone_size = self.zone_size;
        let c = self.rt.block_on(future::lazy(move || {
            Pool::create_cluster(CHUNKSIZE, n, k, zone_size, f, devs)
        })).unwrap();
        self.clusters.push(c);
    }

    /// Actually format the disks
    pub fn format(&mut self) {
        let name = self.name.clone();
        let clusters = self.clusters.drain(..).collect();
        let db = self.rt.block_on(future::lazy(|| {
            Pool::create(name, clusters)
            .map(|pool| {
                let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
                let ddml = Arc::new(DDML::new(pool, cache.clone()));
                let idml = Arc::new(IDML::create(ddml, cache));
                let task_executor = TaskExecutor::current();
                Database::create(idml, task_executor)
            })
        })).unwrap();
        self.rt.block_on(
            db.new_fs().and_then(|_tree_id| db.sync_transaction())
        ).unwrap();
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
        .subcommand(clap::SubCommand::with_name("pool")
            .about("create, destroy, and modify storage pools")
            .subcommand(clap::SubCommand::with_name("create")
                .about("create a new storage pool")
                .arg(clap::Arg::with_name("zone_size")
                     .help("Simulated Zone size in MB")
                     .long("zone_size")
                     .takes_value(true)
                ).arg(clap::Arg::with_name("name")
                     .help("Pool name")
                     .required(true)
                ).arg(clap::Arg::with_name("vdev")
                      .multiple(true)
                      .required(true)
                )
            )
        ).subcommand(clap::SubCommand::with_name("debug")
            .about("Debugging tools")
            .subcommand(clap::SubCommand::with_name("dump")
                .about("Dump internal filesystem information")
                .arg(clap::Arg::with_name("name")
                     .help("Pool name")
                     .required(true)
                ).arg(clap::Arg::with_name("disks")
                      .multiple(true)
                      .required(true)
                )
            )
        );
    let matches = app.get_matches();
    match matches.subcommand() {
        ("pool", Some(args)) => pool::main(args),
        ("debug", Some(args)) => debug::main(args),
        _ => {
            println!("Error: subcommand required\n{}", matches.usage());
            std::process::exit(2);
        },
    }
}
