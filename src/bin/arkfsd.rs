extern crate arkfs;
#[macro_use] extern crate clap;
extern crate fuse;

use fuse::Filesystem;

struct NullFS;

impl Filesystem for NullFS {}

fn main() {
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

    fuse::mount(NullFS, &matches.value_of("mountpoint").unwrap(), &[]).unwrap();
}
