// vim: tw=80

use bfffs_core::{
    database::*,
    device_manager::DevManager,
};
use clap::{
    Clap,
    crate_version
};
use fuse3::{
    raw::Session,
    MountOptions
};
use std::{
    process::exit,
    sync::Arc,
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
    #[clap(short = 'o', long, require_delimiter(true), value_delimiter(','))]
    options: Vec<String>,
    /// Pool name
    pool_name: String,
    mountpoint: String,
    #[clap(required(true))]
    devices: Vec<String>
}

#[tokio::main]
async fn main() {
    let mut cache_size: Option<usize> = None;
    let mut writeback_size: Option<usize> = None;

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let bfffsd: Bfffsd = Bfffsd::parse();

    let mut opts = MountOptions::default();
    opts.fs_name("bfffs");
    // Unconditionally disable the kernel's buffer cache; BFFFS has its own
    opts.custom_options("direct_io");
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
        opts.custom_options(o);
    }

    let mut dev_manager = DevManager::default();
    if let Some(cs) = cache_size {
        dev_manager.cache_size(cs);
    }
    if let Some(wbs) = writeback_size {
        dev_manager.writeback_size(wbs);
    }

    for dev in bfffsd.devices.iter() {
        // TODO: taste devices in parallel
        dev_manager.taste(dev).await.unwrap();
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
    let db = Arc::new(dev_manager.import_by_uuid(uuid).await.unwrap());
    // For now, hardcode tree_id to 0
    let tree_id = TreeID::Fs(0);
    let db2 = db.clone();

    rt.block_on(async move {
        // Run the cleaner on receipt of SIGUSR1.  While not ideal long-term,
        // this is very handy for debugging the cleaner.
        tokio::spawn( async move {
            let mut stream = signal(SignalKind::user_defined1()).unwrap();
            loop {
                stream.recv().await;
                db2.clean().await.unwrap()
            }
        });
        let fs = FuseFs::new(db, tree_id).await;
        Session::new(&opts)
            .mount(fs, &bfffsd.mountpoint)
            .await
    }).unwrap();
}

#[cfg(test)]
mod t {
    use clap::ErrorKind::*;
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["bfffsd"])]
    #[case(vec!["bfffsd", "testpool"])]
    #[case(vec!["bfffsd", "testpool", "/mnt"])]
    fn missing_arg(#[case] args: Vec<&str>) {
        let e = Bfffsd::try_parse_from(args).unwrap_err();
        assert!(e.kind == MissingRequiredArgument ||
                e.kind == DisplayHelpOnMissingArgumentOrSubcommand);
    }

    #[test]
    fn options() {
        let args = vec!["bfffsd", "-o", "allow_other,default_permissions",
            "testpool", "/mnt", "/dev/da0"];
        let bfffsd = Bfffsd::try_parse_from(args).unwrap();
        assert_eq!(bfffsd.pool_name, "testpool");
        assert_eq!(bfffsd.mountpoint, "/mnt");
        assert_eq!(bfffsd.options, vec!["allow_other", "default_permissions"]);
        assert_eq!(bfffsd.devices[0], "/dev/da0");
    }

    #[test]
    fn plain() {
        let args = vec!["bfffsd", "testpool", "/mnt", "/dev/da0"];
        let bfffsd = Bfffsd::try_parse_from(args).unwrap();
        assert_eq!(bfffsd.pool_name, "testpool");
        assert_eq!(bfffsd.mountpoint, "/mnt");
        assert!(bfffsd.options.is_empty());
        assert_eq!(bfffsd.devices[0], "/dev/da0");
    }
}
