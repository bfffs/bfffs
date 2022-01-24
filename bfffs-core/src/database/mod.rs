// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use crate::{
    tree::{Key, MinValue},
    types::*,
};
use mockall_double::*;
use serde_derive::{Deserialize, Serialize};

mod database;

#[double]
pub use self::database::Database;

pub use self::database::ReadOnlyFilesystem;
pub use self::database::ReadWriteFilesystem;

/// Unique identifier for a tree, like a ZFS guid
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
// NB: might need to make this cryptographic, to support send/recv
pub struct TreeID(pub u64);

impl TreeID {
    /// Get the sequentially next Tree ID
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(TreeID)
    }
}

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub struct ForestKey {
    /// The TreeID of this tree's parent, or 0 if this is the root file system
    tree_id: TreeID,
    /// A hash of the child's name
    offset: u64
}

impl ForestKey {
    /// Construct a key to lookup a tree with a known id
    pub fn tree(tree_id: TreeID) -> Self  {
        ForestKey{ tree_id, offset: 0}
    }
}

impl Key for ForestKey {
    const USES_CREDIT: bool = false;
}

impl TypicalSize for ForestKey {
    const TYPICAL_SIZE: usize = 16;
}

impl MinValue for ForestKey {
    fn min_value() -> Self {
        Self {
            tree_id: TreeID(u64::min_value()),
            offset: 0
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
mod forest_key {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn typical_size() {
        let key = ForestKey::min_value();
        let size = bincode::serialized_size(&key).unwrap() as usize;
        assert_eq!(ForestKey::TYPICAL_SIZE, size);
    }
}
}
