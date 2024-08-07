// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use cfg_if::cfg_if;
use crate::types::*;
use serde_derive::{Deserialize, Serialize};
use serde::{de::DeserializeOwned, ser::{Serialize, Serializer}};
use std::ops::Range;

mod node;
use self::node::NodeId;
pub use self::node::{Addr, Key, MinValue, Value};

// Node is _supposed_ to be private, but these types must technically be public
// for the sake of the integration tests.
pub use self::node::{
    IntData, IntElem, LeafData, Limits, Node, NodeData,
    TreePtr, TreeReadGuard, TreeWriteGuard
};


pub(super) mod tree;

cfg_if! {
    if #[cfg(test)]{
        mod tree_mock;
        pub use self::tree::MockRangeQuery as RangeQuery;
        pub use self::tree_mock::MockTree as Tree;
        pub use self::tree_mock::OPEN_MTX;
    } else {
        pub use self::tree::RangeQuery;
        pub use self::tree::Tree;
    }
}

/// Describes how much WriteBack credit is needed for various Tree operations,
/// in the worst-case
#[derive(Clone, Copy, Debug)]
pub struct CreditRequirements {
    /// Credit required for a worst-case insertion, *excluding* credit
    /// requirements of individual `Value`s.
    pub insert: usize,
    pub range_delete: usize,
    pub remove: usize
}

fn serialize_reserved<S>(reserved: &[u8; 7], s: S)
    -> std::result::Result<S::Ok, S::Error>
    where S: Serializer
{
    if s.is_human_readable() {
        // When dumping to YAML, print on a single line
        0u64.serialize(s)
    } else {
        // but for Bincode, use the default representation as bytes
        (*reserved).serialize(s)
    }
}

/// A version of `Inner` that is serializable
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
struct InnerOnDisk<A: Addr> {
    // 8 bits of tree height is sufficient for a tree that can contain more data
    // than will ever be created by mankind, even with fanout of 2.
    height: u8,
    // Makes the rest of the structure line up nicely in a hexdump
    #[serde(serialize_with = "serialize_reserved")]
    _reserved: [u8; 7],
    limits: Limits,
    root: A,
    txgs: Range<TxgT>,
}

#[cfg(test)]
impl<A: Addr + Default> Default for InnerOnDisk<A> {
    fn default() -> Self {
        InnerOnDisk {
            height: Default::default(),
            _reserved: Default::default(),
            limits: Default::default(),
            root: Default::default(),
            txgs: TxgT(0)..TxgT(1),
        }
    }
}

/// The serialized, on-disk representation of a `Tree`
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
#[cfg_attr(test, derive(Default))]
pub struct TreeOnDisk<A: Addr>(InnerOnDisk<A>);

impl<A: Addr> TypicalSize for TreeOnDisk<A> {
    // Verified in tree::tests::io::serialize_forest
    const TYPICAL_SIZE: usize = 32 + A::TYPICAL_SIZE;
}

impl<A: Addr> Value for TreeOnDisk<A> { }
