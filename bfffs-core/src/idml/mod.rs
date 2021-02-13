// vim: tw=80
///! Indirect Data Management Layer
///
/// Interface for working with indirect records.  An indirect record is a record
/// that is referenced by an immutable Record ID, rather than a disk address.
/// Unlike a direct record, it may be duplicated, by through snapshots, clones,
/// or deduplication.

use crate::{
    *,
    ddml::*,
    // Import tree::tree::Tree rather than tree::Tree so we can use the real
    // object and not the mock one, even in test mode.
    tree::tree::Tree,
    tree::Value
};
use mockall_double::*;

mod idml;

#[double]
pub use self::idml::IDML;

pub use crate::ddml::ClosedZone;

pub type DTree<K, V> = Tree<DRP, DDML, K, V>;

/// Value type for the RIDT table.  Should not be used outside of this module
/// except by the fanout calculator.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[cfg_attr(test, derive(Default))]
pub struct RidtEntry {
    drp: DRP,
    refcount: u64
}

impl RidtEntry {
    pub fn new(drp: DRP) -> Self {
        RidtEntry{drp, refcount: 1}
    }
}

impl TypicalSize for RidtEntry {
    const TYPICAL_SIZE: usize = 35;
}

impl Value for RidtEntry {}
