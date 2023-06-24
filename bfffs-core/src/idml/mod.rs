// vim: tw=80
//! Indirect Data Management Layer
//!
//! Interface for working with indirect records.  An indirect record is a record
//! that is referenced by an immutable Record ID, rather than a disk address.
//! Unlike a direct record, it may be duplicated, by through snapshots, clones,
//! or deduplication.

use crate::{
    cache::Cache,
    ddml::*,
    label::LabelReader,
    // Import tree::tree::Tree rather than tree::Tree so we can use the real
    // object and not the mock one, even in test mode.
    tree::tree::Tree,
    tree::Value,
    types::*,
};
use std::{
    path::Path,
    sync::{Arc, Mutex}
};
use mockall_double::*;
use serde_derive::{Deserialize, Serialize};

mod idml;

#[double]
pub use self::idml::IDML;

pub type ClosedZone = crate::ddml::ClosedZone;

pub type DTree<K, V> = Tree<DRP, DDML, K, V>;

/// Value type for the RIDT table.  Should not be used outside of this module
/// except by the fanout calculator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager(crate::ddml::Manager);

impl Manager {
    /// Import a pool that is already known to exist
    pub async fn import(&mut self, uuid: Uuid, cache: Arc<Mutex<Cache>>, wbs: usize)
        -> Result<(IDML, LabelReader)>
    {
        let (ddml, label_reader) = self.0.import(uuid, cache.clone()).await?;
        Ok(IDML::open(Arc::new(ddml), cache, wbs, label_reader))
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        self.0.importable_pools()
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<()> {
        self.0.taste(p).await
    }
}
