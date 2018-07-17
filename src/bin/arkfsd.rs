extern crate arkfs;
#[macro_use] extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate futures;
extern crate tokio;

use fuse::Filesystem;
use futures::future::lazy;
use tokio::runtime::current_thread;

struct NullFS;

impl Filesystem for NullFS {}

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

    let mut rt = current_thread::Runtime::new().unwrap();

    let _db = rt.block_on(lazy(|| {
        arkfs::common::database::Database::open(poolname, devices)
    })).unwrap();
    fuse::mount(NullFS, mountpoint, &[]).unwrap();
}
