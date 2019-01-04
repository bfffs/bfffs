// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `vdev_block` and
//! provide RAID-like functionality.

use crate::common::{
    *,
    label::LabelReader,
    raid::vdev_raid::VdevBlockLike,
};
#[cfg(not(test))] use crate::common::vdev::Vdev;
use std::{
    collections::BTreeMap,
    rc::Rc
};

mod codec;
mod declust;
mod null_locator;
mod prime_s;
mod sgcursor;
mod vdev_raid;
mod vdev_raid_api;

pub use self::vdev_raid::VdevRaid;
pub use self::vdev_raid_api::VdevRaidApi;

#[derive(Serialize, Deserialize, Debug)]
pub enum Label {
    // TODO: VdevMirror
    // TODO: VdevOneDisk
    VdevRaid(self::vdev_raid::Label)
}

impl Label {
    pub fn iter_children(&self) -> impl Iterator<Item=&Uuid> {
        match self {
            Label::VdevRaid(l) => l.children.iter()
        }
    }

    pub fn uuid(&self) -> Uuid {
        match self {
            Label::VdevRaid(l) => l.uuid
        }
    }
}

/// Open some kind of RAID `Vdev` from its components `Vdev`s.
///
/// # Parameters
///
/// * `uuid`:       Uuid of the desired `Vdev`, if present.  If `None`,
///                 then it will not be verified.
/// * `combined`:   An array of pairs of `VdevBlock`s and their
///                 associated `LabelReader`.  The labels of each will be
///                 verified.
pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlockLike, LabelReader)>)
    -> (Rc<dyn VdevRaidApi>, LabelReader)
{
    let mut label_pair = None;
    let all_blockdevs = combined.into_iter()
        .map(|(vdev_block, mut label_reader)| {
        let label: Label = label_reader.deserialize().unwrap();
        if let Some(u) = uuid {
            assert_eq!(u, label.uuid(), "Opening disk from wrong cluster");
        }
        if label_pair.is_none() {
            label_pair = Some((label, label_reader));
        }
        (vdev_block.uuid(), vdev_block)
    }).collect::<BTreeMap<Uuid, VdevBlockLike>>();
    let (label, label_reader) = label_pair.unwrap();
    let vr = match label {
        Label::VdevRaid(vrl) => VdevRaid::open(vrl, all_blockdevs)
    };
    (Rc::new(vr), label_reader)
}
