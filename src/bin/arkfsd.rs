extern crate arkfs;
extern crate fuse;

use fuse::Filesystem;
use std::env;

struct NullFS;

impl Filesystem for NullFS {}

fn main() {
    let mountpoint = env::args_os().nth(1).unwrap();
    fuse::mount(NullFS, &mountpoint, &[]).unwrap();
}
