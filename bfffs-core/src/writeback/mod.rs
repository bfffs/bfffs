// vim: tw=80
//! A writeback cache accountant
//!
//! The cached data is actually owned by the tree module.  This module just
//! limits its size.

mod writeback;

pub use self::writeback::Credit;
pub use self::writeback::WriteBack;
