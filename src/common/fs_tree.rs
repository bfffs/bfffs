// vim: tw=80

//! Data types used by trees representing filesystems

use common::tree::*;

// TODO: define real keys and values for filesystems
/// The per-object portion of a `FSKey`
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum ObjKey {
    DirEntry(u64)
}

/// B-Tree keys for a Filesystem tree
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub struct FSKey {
    object: u64,
    /// Rust doesn't support true bitfields, so we have to pack them manually
    bitfields: u64
}

impl MinValue for FSKey {
    fn min_value() -> Self {
        FSKey{object: 0, bitfields: 0}
    }
}

pub type FSValue = u32;

