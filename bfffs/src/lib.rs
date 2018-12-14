#![cfg_attr(feature = "mocks", feature(plugin))]

// Disable the range_plus_one lint until this bug is fixed.  It generates many
// false positive in the Tree code.
// https://github.com/rust-lang-nursery/rust-clippy/issues/3307
#![allow(clippy::range_plus_one)]

// I don't find this lint very helpful
#![allow(clippy::type_complexity)]

pub mod common;
pub mod sys;

#[macro_export]
macro_rules! boxfut {
    ( $v:expr ) => {
        Box::new($v) as Box<dyn Future<Item=_, Error=_> + Send>
    };
    ( $v:expr, $e:ty ) => {
        Box::new($v) as Box<dyn Future<Item=_, Error=$e> + Send>
    };
    ( $v:expr, $i:ty, $e:ty ) => {
        Box::new($v) as Box<dyn Future<Item=$i, Error=$e> + Send>
    };
    ( $v:expr, $i:ty, $e:ty, $lt:lifetime ) => {
        Box::new($v) as Box<dyn Future<Item=$i, Error=$e> + $lt>
    };
}

#[macro_export]
macro_rules! boxstream {
    ( $v:expr ) => {
        Box::new($v) as Box<dyn Stream<Item=_, Error=_> + Send>
    };
}
