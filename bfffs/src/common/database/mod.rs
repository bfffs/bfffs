// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use cfg_if::cfg_if;
use crate::common::{
    *,
    tree::MinValue
};

mod database;

cfg_if! {
    if #[cfg(test)]{
        pub use self::database::MockDatabase as Database;
    } else {
        pub use self::database::Database;
    }
}

pub use self::database::ReadOnlyFilesystem;
pub use self::database::ReadWriteFilesystem;

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum TreeID {
    /// A filesystem, snapshot, or clone
    Fs(u32)
}

impl TypicalSize for TreeID {
    const TYPICAL_SIZE: usize = 8;
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID::Fs(u32::min_value())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
mod treeid {
    use bincode;
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn typical_size() {
        assert_eq!(TreeID::TYPICAL_SIZE,
                   bincode::serialized_size(&TreeID::Fs(0)).unwrap() as usize);
    }
}
}
