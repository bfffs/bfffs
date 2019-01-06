// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `vdev_block` and
//! provide RAID-like functionality.

use crate::common::{
    *,
    label::LabelReader,
    vdev::*
};
#[cfg(not(test))] use crate::common::vdev_block::*;
#[cfg(test)] use crate::common::label::LabelWriter;
use std::{
    collections::BTreeMap,
    iter::once,
    rc::Rc
};
#[cfg(not(test))] use std::{
    num::NonZeroU64,
    path::Path,
};

mod codec;
mod declust;
mod prime_s;
mod sgcursor;
mod vdev_onedisk;
mod vdev_raid;
mod vdev_raid_api;

pub use self::vdev_onedisk::VdevOneDisk;
pub use self::vdev_raid::VdevRaid;
pub use self::vdev_raid_api::VdevRaidApi;

#[cfg(test)]
/// Only exists so mockers can replace VdevBlock
pub trait VdevBlockTrait : Vdev {
    // Note: The return values are Boxed traits instead of impl Traits because:
    // 1) Mockers can only mock traits, not structs, and traits cannot "impl
    //    Trait"
    // 2) Simulacrum use mocked methods' return types as generic method
    //    parameters, and "impl Trait" is not allowed there.
    // But this is ok, since a Boxed trait does satisfy "impl Trait".  The only
    // problem is that the Boxed trait can do a few things that the real "impl
    // Trait" can't, pack into a container with other Boxed traits.
    fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut>;
    fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut>;
    fn open_zone(&self, lba: LbaT) -> Box<VdevFut>;
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut>;
    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut>;
    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
    fn write_label(&self, labeller: LabelWriter) -> Box<VdevFut>;
    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> Box<VdevFut>;
    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevFut>;
}
#[cfg(test)]
type VdevBlockLike = Box<dyn VdevBlockTrait>;
#[cfg(not(test))]
#[doc(hidden)]
type VdevBlockLike = VdevBlock;

#[derive(Serialize, Deserialize, Debug)]
pub enum Label {
    // TODO: VdevMirror
    OneDisk(self::vdev_onedisk::Label),
    Raid(self::vdev_raid::Label)
}

impl<'a> Label {
    pub fn iter_children(&'a self) -> Box<dyn Iterator<Item=&Uuid> + 'a> {
        match self {
            Label::Raid(l) => Box::new(l.children.iter()),
            Label::OneDisk(l) => Box::new(once(&l.child))
        }
    }

    pub fn uuid(&self) -> Uuid {
        match self {
            Label::Raid(l) => l.uuid,
            Label::OneDisk(l) => l.uuid
        }
    }
}

/// Create a raid-like `Vdev` from its components.
///
///
/// * `chunksize`:          RAID chunksize in LBAs, if specified.  This is the
///                         largest amount of data that will be read/written to
///                         a single device before the `Locator` switches to the
///                         next device.
/// * `disks_per_stripe`:   Number of data plus parity chunks in each
///                         self-contained RAID stripe.  Must be less than or
///                         equal to the number of disks in `paths`.
/// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
///                         simulated zones on devices that don't have
///                         native zones.
/// * `redundancy`:         Degree of RAID redundancy.  Up to this many
///                         disks may fail before the array becomes
///                         inoperable.
/// * `paths`:              Slice of pathnames of files and/or devices
#[cfg(not(test))]
pub fn create<P: AsRef<Path>>(chunksize: Option<NonZeroU64>,
                              disks_per_stripe: i16,
                              lbas_per_zone: Option<NonZeroU64>,
                              redundancy: i16,
                              paths: &[P]) -> Rc<dyn VdevRaidApi>
{
    if paths.len() == 1 {
        assert_eq!(disks_per_stripe, 1);
        assert_eq!(redundancy, 0);
        Rc::new(VdevOneDisk::create(lbas_per_zone, &paths[0]))
    } else {
        Rc::new(VdevRaid::create(chunksize, disks_per_stripe, lbas_per_zone,
                                 redundancy, paths))
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
    let vdev = match label {
        Label::Raid(l) => {
            Rc::new(VdevRaid::open(l, all_blockdevs)) as Rc<dyn VdevRaidApi>
        },
        Label::OneDisk(l) => {
            Rc::new(VdevOneDisk::open(l, all_blockdevs)) as Rc<dyn VdevRaidApi>
        },
    };
    (vdev, label_reader)
}
