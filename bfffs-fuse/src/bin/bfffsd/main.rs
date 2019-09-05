// vim: tw=80

use bfffs::{
    common::{
        Error,
        database::*,
        device_manager::DevManager,
    },
};
use clap::crate_version;
use futures::{Future, Stream, future};
use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    sync::Arc,
    thread
};
use tokio_signal::unix::{Signal, SIGUSR1};

mod fs;

use crate::fs::FuseFs;

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
        .map(str::to_string)
        .collect::<Vec<_>>();

    let dev_manager = DevManager::default();
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
        dev_manager.import_by_uuid(uuid, handle)
    })).unwrap());
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let db2 = db.clone();
    let thr_handle = thread::spawn(move || {
        let fs = FuseFs::new(db, handle2, tree_id);
        let opts = [
            OsStr::from_bytes(b"-o"),
            OsStr::from_bytes(b"allow_other"),
            OsStr::from_bytes(b"-o"),
            OsStr::from_bytes(b"direct_io"),
        ];
        fs::mount(fs, &mountpoint, &opts[..]).unwrap();
    });

    // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term, this
    // is very handy for debugging the cleaner.
    let sigusr1 = Signal::new(SIGUSR1).flatten_stream();
    rt.spawn(
        sigusr1.map_err(Error::unhandled_canceled)
        .for_each(move |_| {
            db2.clean()
        }).map_err(Error::unhandled)
    ).unwrap();

    thr_handle.join().unwrap()
}
