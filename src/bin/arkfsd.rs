extern crate arkfs;
#[macro_use] extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate futures;
extern crate tokio;

use arkfs::common::database::*;
use arkfs::common::device_manager::DevManager;
use arkfs::sys::fs::FS as FS;
use futures::{Future, future};
use std::sync::Arc;
use tokio::runtime::current_thread;

fn main() {
    env_logger::init();
    let app = clap::App::new("arkfsd")
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

    let mut dev_manager = DevManager::new();
    for dev in devices.iter() {
        dev_manager.taste(dev);
    }
    let uuid = dev_manager.importable_pools().filter(|(name, _uuid)| {
        **name == poolname
    }).nth(0).unwrap().1;

    let mut rt = current_thread::Runtime::new().unwrap();

    let db = rt.block_on(future::lazy(|| {
        dev_manager.import(*uuid).map(|idml| {
            Database::new(Arc::new(idml))
        })
    })).unwrap();
    // TODO: use distinct TreeID for each mountpoint
    let fs = FS::new(Arc::new(db), rt, TreeID::Fs(0));
    fuse::mount(fs, mountpoint, &[]).unwrap();
}
