// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use cfg_if::cfg_if;
use crate::common::{*, dml::*};
use serde::de::DeserializeOwned;
use std::ops::Range;

mod node;
use self::node::*;
// Node must be visible for the IDML's unit tests
pub(super) use self::node::Node;

pub use self::node::{Addr, Key, MinValue, Value};

pub(super) mod tree;

cfg_if! {
    if #[cfg(test)]{
        //mod tree_mock;
        //pub use self::tree::MockRangeQuery as RangeQuery;
        //pub use self::tree_mock::MockTree as Tree;
    } else {
        pub use self::tree::RangeQuery;
        pub use self::tree::Tree;
    }
}

/// A version of `Inner` that is serializable
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
struct InnerOnDisk<A: Addr> {
    height: u64,
    limits: Limits,
    root: A,
    txgs: Range<TxgT>,
}

#[cfg(test)]
impl<A: Addr + Default> Default for InnerOnDisk<A> {
    fn default() -> Self {
        InnerOnDisk {
            height: u64::default(),
            limits: Limits::default(),
            root: A::default(),
            txgs: TxgT(0)..TxgT(1),
        }
    }
}

/// The serialized, on-disk representation of a `Tree`
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
#[cfg_attr(test, derive(Default))]
pub struct TreeOnDisk<A: Addr>(InnerOnDisk<A>);

impl<A: Addr> TypicalSize for TreeOnDisk<A> {
    // Verified in common::tree::tests::io::serialize_forest
    const TYPICAL_SIZE: usize = 32 + A::TYPICAL_SIZE;
}

impl<A: Addr> Value for TreeOnDisk<A> {}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
    use super::*;

    // pet kcov
    #[test]
    fn debug() {
        let tod = TreeOnDisk::<RID>::default();
        format!("{:?}", tod);
    }
}
// LCOV_EXCL_STOP
