extern crate bfffs;
#[macro_use] extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate futures;
extern crate tokio;
extern crate tokio_io_pool;

use bfffs::common::database::*;
use bfffs::common::device_manager::DevManager;
use bfffs::sys::fs::FuseFs;
use futures::future;
use std::sync::Arc;

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
    let mountpoint = &matches.value_of("mountpoint").unwrap();
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
    let db = rt.block_on(future::lazy(move || {
        dev_manager.import(uuid, handle)
    })).unwrap();
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let fs = FuseFs::new(Arc::new(db), Arc::new(rt), tree_id);
    fuse::mount(fs, mountpoint, &[]).unwrap();
}
