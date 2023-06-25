// vim: tw=80

#![cfg_attr(all(feature = "nightly", test), feature(test))]

// Disable the range_plus_one lint until this bug is fixed.  It generates many
// false positive in the Tree code.
// https://github.com/rust-lang-nursery/rust-clippy/issues/3307
#![allow(clippy::range_plus_one)]

// I don't find this lint very helpful
#![allow(clippy::type_complexity)]

// I use a common pattern to substitute mock objects for real ones in test
// builds.  Silence clippy's complaints.
#![allow(clippy::module_inception)]

// I suppose I should probably fix this some day, but I just don't like the look
// of e.g. "Idml" as opposed to "IDML".
#![allow(clippy::upper_case_acronyms)]

// False positive https://github.com/rust-lang/rust-clippy/issues/10498
#![allow(clippy::let_with_type_underscore)]

// error: reached the type-length limit while instantiating std::pin::Pin...
#![type_length_limit="3790758"]
// error: trait bounds overflowed in Database::sync_transaction_priv
#![recursion_limit="256"]

#[cfg(all(feature = "nightly", test))]
extern crate test;

// rstest_reuse must be imported at the crate root for macro reasons
// https://github.com/la10736/rstest/issues/128
#[allow(clippy::single_component_path_imports)]
#[cfg(test)]
use rstest_reuse;

pub mod cache;
pub mod cleaner;
pub mod cluster;
pub mod controller;
pub mod database;
pub mod dataset;
pub mod ddml;
pub mod dml;
pub mod fs;
pub mod fs_tree;
pub mod idml;
pub mod label;
pub mod mirror;
pub mod pool;
pub mod property;
pub mod raid;
pub mod rpc;
pub mod tree;
pub mod types;
pub mod util;
pub mod vdev;
pub mod vdev_block;
pub mod vdev_file;
pub mod writeback;

pub use crate::types::*;
pub use crate::util::*;
