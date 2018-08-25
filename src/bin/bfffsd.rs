// vim: tw=80

extern crate bfffs;
#[macro_use] extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate futures;
extern crate nix;
extern crate tokio;
extern crate tokio_io_pool;
extern crate tokio_signal;

use bfffs::common::database::*;
use bfffs::common::device_manager::DevManager;
use bfffs::sys::fs::FuseFs;
use futures::{Future, Stream, future};
use std::{
    sync::Arc,
    thread
};
use tokio_signal::unix::{Signal, SIGUSR1};

fn main() {
    env_logger::init();
    let app = clap::App::new("bfffsd")
        .version(crate_version!())
        .arg(clap::Arg::with_name("name")
             .help("Pool name")
             .required(true)
         ).arg(clap::Arg::with_name("mountpoint")
             .required(true)
         ).arg(clap::Arg::with_name("devices")
             .required(true)
             .multiple(true)
         );
    let matches = app.get_matches();
    let poolname = matches.value_of("name").unwrap().to_string();
    let mountpoint = matches.value_of("mountpoint").unwrap().to_string();
    let devices = matches.values_of("devices").unwrap()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let dev_manager = DevManager::new();
    for dev in devices.iter() {
        dev_manager.taste(dev);
    }
    let uuid = dev_manager.importable_pools().iter()
        .filter(|(name, _uuid)| {
            **name == poolname
        }).nth(0).unwrap().1;

    let mut rt = tokio_io_pool::Runtime::new();
    let handle = rt.handle().clone();
    let handle2 = rt.handle().clone();
    let db = Arc::new(rt.block_on(future::lazy(move || {
        dev_manager.import(uuid, handle)
    })).unwrap());
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let db2 = db.clone();
    let thr_handle = thread::spawn(move || {
        let fs = FuseFs::new(db, handle2, tree_id);
        fuse::mount(fs, &mountpoint, &[]).unwrap();
    });

    // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term, this
    // is very handy for debugging the cleaner.
    let sigusr1 = Signal::new(SIGUSR1).flatten_stream();
    rt.spawn(
        sigusr1.map_err(|e| panic!("{:?}", e))
        .for_each(move |_| {
            db2.clean()
        }).map_err(|e| panic!("{:?}", e))
    ).unwrap();

    thr_handle.join().unwrap()
}
