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

impl Key for TreeID {
    const USES_CREDIT: bool = false;
}

impl TypicalSize for TreeID {
    const TYPICAL_SIZE: usize = 8;
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID(u64::min_value())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
mod treeid {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn typical_size() {
        assert_eq!(TreeID::TYPICAL_SIZE,
                   bincode::serialized_size(&TreeID(0)).unwrap() as usize);
    }
}
}
