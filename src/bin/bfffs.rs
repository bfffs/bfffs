extern crate bfffs;
#[macro_use] extern crate clap;
extern crate futures;
extern crate tokio;

mod pool{
use bfffs::common::{LbaT, TxgT};
use bfffs::common::cache::Cache;
use bfffs::common::ddml::DDML;
use bfffs::common::idml::IDML;
use bfffs::common::pool::{ClusterProxy, Pool};
use futures::{Future, future};
use std::sync::{Arc, Mutex};
use super::*;
use tokio::runtime::current_thread::Runtime;

// TODO: specify CHUNKSIZE on the command line
const CHUNKSIZE: LbaT = 16;

fn create(args: &clap::ArgMatches) {
    let name = args.value_of("name").unwrap();
    let mut values = args.values_of("vdev").unwrap();
    let mut cluster = None;
    let mut clusters = vec![];
    let mut devs = vec![];
    let mut rt = Runtime::new().unwrap();
    loop {
        let next = values.next();
        match next {
            None => {
                if devs.len() > 0 {
                    match cluster{
                        Some("mirror") =>
                            clusters.push(create_mirror(&mut rt, &devs[..])),
                        Some("raid") => clusters.push(
                            create_raid(&mut rt, &devs[..])),
                        None => assert!(devs.is_empty()),
                        _ => unreachable!()
                    }
                }
                break;
            },
            Some("mirror") => {
                if devs.len() > 0 {
                    let c = create_cluster(&mut rt, cluster.as_ref().unwrap(),
                                           &devs[..]);
                    clusters.push(c);
                }
                devs.clear();
                cluster = Some("mirror")
            },
            Some("raid") => {
                if devs.len() > 0 {
                    let c = create_cluster(&mut rt, cluster.as_ref().unwrap(),
                                           &devs[..]);
                    clusters.push(c);
                }
                devs.clear();
                cluster = Some("raid")
            },
            Some(ref dev) => {
                if cluster == None {
                    let c = create_single(&mut rt, dev);
                    clusters.push(c);
                } else {
                    devs.push(dev);
                }
            }
        }
    }
    let idml = rt.block_on(future::lazy(|| {
        Pool::create(String::from(name), clusters)
        .map(|pool| {
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            IDML::create(ddml, cache)
        })
    })).unwrap();
    rt.block_on(
        idml.advance_transaction(|_| {
            idml.write_label(TxgT::from(0))
        })
    ).unwrap();
}

fn create_cluster(rt: &mut Runtime, vtype: &str, devs: &[&str]) -> ClusterProxy {
    match vtype {
        "mirror" => create_mirror(rt, devs),
        "raid" => create_raid(rt, devs),
        _ => panic!("Unsupported vdev type {}", vtype)
    }
}

fn create_mirror(rt: &mut Runtime, devs: &[&str]) -> ClusterProxy {
    // TODO: allow creating declustered mirrors
    let n = devs.len() as i16;
    let k = devs.len() as i16;
    let f = devs.len() as i16 - 1;
    do_create_cluster(rt, n, k, f, &devs[2..])
}

fn create_raid(rt: &mut Runtime, devs: &[&str]) -> ClusterProxy {
    let n = devs.len() as i16 - 2;
    let k = i16::from_str_radix(devs[0], 10)
        .expect("Disks per stripe must be an integer");
    let f = i16::from_str_radix(devs[1], 10)
        .expect("Disks per stripe must be an integer");
    do_create_cluster(rt, n, k, f, &devs[2..])
}

fn create_single(rt: &mut Runtime, dev: &str) -> ClusterProxy {
    do_create_cluster(rt, 1, 1, 0, &[&dev])
}

fn do_create_cluster(rt: &mut Runtime, n: i16, k: i16, f: i16, devs: &[&str])
    -> ClusterProxy
{
    rt.block_on(future::lazy(move || {
        Pool::create_cluster(CHUNKSIZE, n, k, None, f, devs)
    })).unwrap()
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
