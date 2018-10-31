#![recursion_limit="128"]   // galvanic_test hits the recursion limit
#![cfg_attr(feature = "cargo-clippy", feature(tool_lints))]
// https://github.com/mindsbackyard/galvanic-test/pull/13
#![cfg_attr(feature = "cargo-clippy", allow(clippy::unnecessary_mut_passed))]

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

extern crate bfffs;
extern crate divbuf;
extern crate env_logger;
extern crate fixedbitset;
extern crate futures;
extern crate galvanic_test;
extern crate itertools;
extern crate libc;
extern crate log;
extern crate nix;
extern crate pretty_assertions;
extern crate rand;
extern crate rand_xorshift;
extern crate tempdir;
extern crate time;
extern crate tokio;
extern crate tokio_file;
extern crate tokio_io_pool;
extern crate uuid;

mod common;
mod sys;
