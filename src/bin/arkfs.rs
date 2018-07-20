extern crate arkfs;
#[macro_use] extern crate clap;
extern crate futures;
extern crate tokio;

mod pool{
use arkfs::common::{LbaT, TxgT};
use arkfs::common::cache::Cache;
use arkfs::common::ddml::DDML;
use arkfs::common::idml::IDML;
use arkfs::common::pool::{Cluster, Pool};
use futures::Future;
use std::sync::{Arc,Mutex};
use super::*;
use tokio::runtime::current_thread;

// TODO: specify CHUNKSIZE on the command line
const CHUNKSIZE: LbaT = 16;

fn create(args: &clap::ArgMatches) {
    let name = args.value_of("name").unwrap();
    let mut values = args.values_of("vdev").unwrap();
    let mut cluster = None;
    let mut clusters = vec![];
    let mut devs = vec![];
    loop {
        let next = values.next();
        match next {
            None => {
                if devs.len() > 0 {
                    match cluster{
                        Some("mirror") =>
                            clusters.push(create_mirror(&devs[..])),
                        Some("raid") => clusters.push(create_raid(&devs[..])),
                        None => assert!(devs.is_empty()),
                        _ => unreachable!()
                    }
                }
                break;
            },
            Some("mirror") => {
                if devs.len() > 0 {
                    let c = create_cluster(cluster.as_ref().unwrap(),
                                           &devs[..]);
                    clusters.push(c);
                }
                devs.clear();
                cluster = Some("mirror")
            },
            Some("raid") => {
                if devs.len() > 0 {
                    let c = create_cluster(cluster.as_ref().unwrap(),
                                           &devs[..]);
                    clusters.push(c);
                }
                devs.clear();
                cluster = Some("raid")
            },
            Some(ref dev) => {
                if cluster == None {
                    let c = create_single(dev);
                    clusters.push(c);
                } else {
                    devs.push(dev);
                }
            }
        }
    }
    let mut rt = current_thread::Runtime::new().unwrap();
    let idml = rt.block_on(
        Pool::create(String::from(name), clusters)
        .map(|pool| {
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            IDML::create(ddml, cache)
        })
    ).unwrap();
    rt.block_on(
        idml.write_label(TxgT::from(0))
    ).unwrap();
}

fn create_cluster(vtype: &str, devs: &[&str]) -> Cluster {
    match vtype {
        "mirror" => create_mirror(devs),
        "raid" => create_raid(devs),
        _ => panic!("Unsupported vdev type {}", vtype)
    }
}

fn create_mirror(devs: &[&str]) -> Cluster {
    // TODO: allow creating declustered mirrors
    let n = devs.len() as i16;
    let k = devs.len() as i16;
    let f = devs.len() as i16 - 1;
    let cluster = Pool::create_cluster(CHUNKSIZE, n, k, None, f, &devs[2..]);
    cluster
}

fn create_raid(devs: &[&str]) -> Cluster {
    let n = devs.len() as i16 - 2;
    let k = i16::from_str_radix(devs[0], 10)
        .expect("Disks per stripe must be an integer");
    let f = i16::from_str_radix(devs[1], 10)
        .expect("Disks per stripe must be an integer");
    let cluster = Pool::create_cluster(CHUNKSIZE, n, k, None, f, &devs[2..]);
    cluster
}

fn create_single(dev: &str) -> Cluster {
    let cluster = Pool::create_cluster(CHUNKSIZE, 1, 1, None, 0, &[&dev]);
    cluster
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
    let app = clap::App::new("arkfs")
        .version(crate_version!())
        .subcommand(clap::SubCommand::with_name("pool")
            .about("create, destroy, and modify storage pools")
            .subcommand(clap::SubCommand::with_name("create")
                .about("create a new storage pool")
                .arg(clap::Arg::with_name("name")
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
        ("pool", Some(args)) => pool::main(args),
        _ => {
            println!("Error: subcommand required\n{}", matches.usage());
            std::process::exit(2);
        },
    }
}
