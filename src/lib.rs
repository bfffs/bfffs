#![cfg_attr(feature = "mocks", feature(plugin))]
#![cfg_attr(feature = "mocks", plugin(mockers_macros))]

extern crate bytes;
extern crate fixedbitset;
extern crate futures;
#[cfg(test)]
#[macro_use] extern crate galvanic_test;
extern crate isa_l;
#[cfg(test)]
#[macro_use]
#[cfg(feature = "mocks")] extern crate mockers;
extern crate modulo;
extern crate nix;
extern crate tokio;
extern crate tokio_file;

pub mod common;
pub mod sys;
