// vim: tw=80

use bfffs::{
    common::{
        database::*,
        device_manager::DevManager,
    },
};
use clap::crate_version;
use futures::StreamExt;
use std::{
    ffi::OsString,
    sync::Arc,
    thread
};
use tokio::{
    runtime::Builder,
    signal::unix::{signal, SignalKind},
};

mod fs;

use crate::fs::FuseFs;

fn main() {
    env_logger::init();
    let app = clap::App::new("bfffsd")
        .version(crate_version!())
        .arg(clap::Arg::with_name("option")
             .help("Mount options")
             .short("o")
             .takes_value(true)
             .multiple(true)
             .require_delimiter(true)
        ).arg(clap::Arg::with_name("name")
             .help("Pool name")
             .required(true)
         ).arg(clap::Arg::with_name("mountpoint")
             .required(true)
         ).arg(clap::Arg::with_name("devices")
             .required(true)
             .multiple(true)
         );
    let matches = app.get_matches();

    // Unconditionally disable the kernel's buffer cache since BFFFS has its own
    let mut opts = vec![OsString::from("-o"), OsString::from("direct_io")];
    if let Some(it) = matches.values_of("option") {
        for o in it {
            opts.push(OsString::from("-o"));
            opts.push(OsString::from(o));
        }
    };

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

    let mut rt = Builder::new()
        .threaded_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let handle2 = rt.handle().clone();
    let db = Arc::new(rt.block_on(async move {
        dev_manager.import_by_uuid(uuid, handle).await
    }).unwrap());
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let db2 = db.clone();
    let thr_handle = thread::spawn(move || {
        let fs = FuseFs::new(db, handle2, tree_id);
        // We need a separate vec of references :(
        // https://github.com/zargony/rust-fuse/issues/117
        let opt_refs = opts.iter().map(|o| o.as_ref()).collect::<Vec<_>>();
        fs::mount(fs, &mountpoint, &opt_refs[..]).unwrap();
    });

    // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term, this
    // is very handy for debugging the cleaner.
    let sigusr1 = signal(SignalKind::user_defined1()).unwrap();
    rt.spawn(
        sigusr1
        .for_each(move |_| {
            let db3 = db2.clone();
            async move {
                db3.clean().await.unwrap()
            }
        })
    );

    thr_handle.join().unwrap()
}
