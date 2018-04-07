#![cfg_attr(feature = "mocks", feature(plugin))]
#![cfg_attr(feature = "mocks", plugin(mockers_macros))]

extern crate divbuf;
extern crate fixedbitset;
extern crate futures;
#[cfg(test)]
#[macro_use]
#[cfg(feature = "mocks")] extern crate galvanic_test;
extern crate itertools;
extern crate isa_l;
#[cfg(test)]
#[macro_use]
#[cfg(feature = "mocks")] extern crate mockers;
extern crate modulo;
#[cfg(test)]
extern crate permutohedron;
extern crate nix;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;
extern crate tokio;
extern crate tokio_file;
extern crate uuid;

pub mod common;
pub mod sys;
