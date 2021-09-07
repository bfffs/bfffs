// vim: tw=80

use bfffs_core::{
    database::*,
    device_manager::DevManager,
};
use clap::{
    Clap,
    crate_version
};
use std::{
    ffi::OsString,
    process::exit,
    sync::Arc,
    thread
};
use tokio::{
    runtime::Builder,
    signal::unix::{signal, SignalKind},
};
use tracing_subscriber::EnvFilter;

mod fs;

use crate::fs::FuseFs;

#[derive(Clap, Clone, Debug)]
#[clap(version = crate_version!())]
struct Bfffsd {
    /// Mount options, comma delimited
    #[clap(short = 'o', long, require_delimiter(true))]
    options: Vec<String>,
    /// Pool name
    pool_name: String,
    mountpoint: String,
    #[clap(required(true))]
    devices: Vec<String>
}

fn main() {
    let mut cache_size: Option<usize> = None;
    let mut writeback_size: Option<usize> = None;

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let bfffsd: Bfffsd = Bfffsd::parse();

    let mut opts = vec![
        // Unconditionally disable the kernel's buffer cache; BFFFS has its own
        OsString::from("-o"), OsString::from("direct_io"),
        // Specify the file system type
        OsString::from("-o"), OsString::from("subtype=bfffs"),
    ];
    for o in bfffsd.options.iter() {
        if let Some((name, value)) = o.split_once("=") {
            if name == "cache_size" {
                let v = value.parse()
                    .unwrap_or_else(|_| {
                        eprintln!("cache_size must be numeric");
                        exit(2);
                    });
                cache_size = Some(v);
                continue;
            } else if name == "writeback_size" {
                let v = value.parse()
                    .unwrap_or_else(|_| {
                        eprintln!("writeback_size must be numeric");
                        exit(2);
                    });
                writeback_size = Some(v);
                continue;
            }
            // else, must be a mount_fusefs option
        }
        // Must be a mount_fusefs option
        opts.push(OsString::from("-o"));
        opts.push(OsString::from(o));
    }

    let mut dev_manager = DevManager::default();
    if let Some(cs) = cache_size {
        dev_manager.cache_size(cs);
    }
    if let Some(wbs) = writeback_size {
        dev_manager.writeback_size(wbs);
    }

    for dev in bfffsd.devices.iter() {
        dev_manager.taste(dev);
    }
    let uuid = dev_manager.importable_pools().iter()
        .find(|(name, _uuid)| {
            **name == bfffsd.pool_name
        }).unwrap_or_else(|| {
            eprintln!("error: pool {} not found", bfffsd.pool_name);
            std::process::exit(1);
        }).1;

    let rt = Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let handle2 = rt.handle().clone();
    let db = Arc::new(dev_manager.import_by_uuid(uuid, handle).unwrap());
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let db2 = db.clone();
    let thr_handle = thread::spawn(move || {
        let fs = FuseFs::new(db, handle2, tree_id);
        // We need a separate vec of references :(
        // https://github.com/zargony/rust-fuse/issues/117
        let opt_refs = opts.iter().map(|o| o.as_ref()).collect::<Vec<_>>();
        fs::mount(fs, &bfffsd.mountpoint, &opt_refs[..]).unwrap();
    });

    // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term, this
    // is very handy for debugging the cleaner.
    rt.spawn( async move {
        let mut stream = signal(SignalKind::user_defined1()).unwrap();
        loop {
            stream.recv().await;
            db2.clean().await.unwrap()
        }
    });

    thr_handle.join().unwrap()
}
