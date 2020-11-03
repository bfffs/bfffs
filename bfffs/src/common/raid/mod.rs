// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `vdev_block` and
//! provide RAID-like functionality.

#[cfg(test)] use async_trait::async_trait;
use crate::common::{
    *,
    label::*,
    vdev::*,
};
#[cfg(test)] use mockall::*;
use std::{
    collections::BTreeMap,
    iter::once,
    num::NonZeroU64,
    path::Path,
    sync::Arc
};

#[cfg(test)]
use crate::common::vdev_block::MockVdevBlock as VdevBlock;
#[cfg(not(test))]
use crate::common::vdev_block::VdevBlock;

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
pub fn create<P>(chunksize: Option<NonZeroU64>, disks_per_stripe: i16,
    lbas_per_zone: Option<NonZeroU64>, redundancy: i16,
    mut paths: Vec<P>) -> Arc<dyn VdevRaidApi>
    where P: AsRef<Path> + 'static
{
    if paths.len() == 1 {
        assert_eq!(disks_per_stripe, 1);
        assert_eq!(redundancy, 0);
        Arc::new(VdevOneDisk::create(lbas_per_zone, paths.pop().unwrap()))
    } else {
        Arc::new(VdevRaid::create(chunksize, disks_per_stripe, lbas_per_zone,
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
pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
    -> (Arc<dyn VdevRaidApi>, LabelReader)
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
    }).collect::<BTreeMap<Uuid, VdevBlock>>();
    let (label, label_reader) = label_pair.unwrap();
    let vdev = match label {
        Label::Raid(l) => {
            Arc::new(VdevRaid::open(l, all_blockdevs)) as Arc<dyn VdevRaidApi>
        },
        Label::OneDisk(l) => {
            Arc::new(VdevOneDisk::open(l, all_blockdevs)) as Arc<dyn VdevRaidApi>
        },
    };
    (vdev, label_reader)
}

#[cfg(test)]
mock!{
    pub VdevRaid {}
    impl Vdev for VdevRaid {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
    #[async_trait]
    impl VdevRaidApi for VdevRaid {
        fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn flush_zone(&self, zone: ZoneT) -> (LbaT, BoxVdevFut);
        fn open_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut;
        fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> BoxVdevFut;
        fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            -> BoxVdevFut;
    }
}
