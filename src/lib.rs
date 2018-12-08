#![cfg_attr(feature = "mocks", feature(plugin))]

// Disable the range_plus_one lint until this bug is fixed.  It generates many
// false positive in the Tree code.
// https://github.com/rust-lang-nursery/rust-clippy/issues/3307
#![allow(clippy::range_plus_one)]

// I don't find this lint very helpful
#![allow(clippy::type_complexity)]

extern crate atomic;
extern crate bincode;
extern crate bitfield;
extern crate blosc;
extern crate byteorder;
extern crate divbuf;
extern crate downcast;
extern crate enum_primitive_derive;
extern crate fixedbitset;
extern crate fuse;
extern crate futures;
extern crate futures_locks;
#[cfg(test)]
#[cfg(feature = "mocks")] extern crate galvanic_test;
extern crate itertools;
extern crate isa_l;
extern crate lazy_static;
extern crate libc;
extern crate metrohash;
#[cfg(test)]
#[cfg(feature = "mocks")] extern crate mockers;
#[cfg(feature = "mocks")] extern crate mockers_derive;
extern crate modulo;
extern crate num_traits;
#[cfg(test)]
extern crate permutohedron;
#[cfg(test)]
extern crate pretty_assertions;
extern crate nix;
#[cfg(test)]
extern crate rand;
extern crate serde;
extern crate serde_cbor;
extern crate serde_derive;
extern crate serde_yaml;
#[cfg(test)] extern crate simulacrum;
extern crate time;
extern crate tokio;
extern crate tokio_current_thread;
extern crate tokio_file;
extern crate tokio_io_pool;
extern crate uuid;

pub mod common;
pub mod sys;
