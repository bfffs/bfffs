#![cfg_attr(feature = "nightly", feature(plugin))]
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

// error: reached the type-length limit while instantiating std::pin::Pin...
#![type_length_limit="3198763"]
// error: trait bounds overflowed in Database::sync_transaction_priv
#![recursion_limit="256"]

#[cfg(all(feature = "nightly", test))]
extern crate test;

pub mod common;
