#![cfg_attr(feature = "mocks", feature(plugin))]
#![cfg_attr(feature = "mocks", plugin(mockers_macros))]

extern crate arkfs;
extern crate fixedbitset;
extern crate futures;
#[macro_use] extern crate galvanic_test;
extern crate itertools;
extern crate rand;
#[cfg(feature = "mocks")] extern crate mockers;
extern crate tempdir;
extern crate tokio_core;
extern crate tokio_file;

mod common;
mod sys;
