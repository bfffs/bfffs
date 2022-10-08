// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `mirror` and
//! provide RAID-like functionality.

#[cfg(test)] use async_trait::async_trait;
use crate::{
    label::*,
    types::*,
    vdev::*,
};
#[cfg(test)] use mockall::*;
use mockall_double::double;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    iter::once,
    num::NonZeroU64,
    sync::Arc
};

#[double]
use crate::mirror::Mirror;

mod codec;
mod declust;
mod prime_s;
mod sgcursor;
mod null_raid;
mod vdev_raid;
mod vdev_raid_api;

pub use self::null_raid::NullRaid;
pub use self::vdev_raid::VdevRaid;
pub use self::vdev_raid_api::VdevRaidApi;

#[derive(Serialize, Deserialize, Debug)]
pub enum Label {
    NullRaid(self::null_raid::Label),
    Raid(self::vdev_raid::Label),
}

impl<'a> Label {
    pub fn iter_children(&'a self) -> Box<dyn Iterator<Item=&Uuid> + 'a> {
        match self {
            Label::Raid(l) => Box::new(l.children.iter()),
            Label::NullRaid(l) => Box::new(once(&l.child)),
        }
    }

    pub fn uuid(&self) -> Uuid {
        match self {
            Label::Raid(l) => l.uuid,
            Label::NullRaid(l) => l.uuid,
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
/// * `redundancy`:         Degree of RAID redundancy.  Up to this many
///                         disks may fail before the array becomes
///                         inoperable.
/// * `mirrors`:            Already labeled Mirror devices
pub fn create(chunksize: Option<NonZeroU64>, disks_per_stripe: i16,
    redundancy: i16, mut mirrors: Vec<Mirror>) -> Arc<dyn VdevRaidApi>
{
    if mirrors.len() == 1 {
        assert_eq!(disks_per_stripe, 1);
        assert_eq!(redundancy, 0);
        Arc::new(NullRaid::create(mirrors.pop().unwrap()))
    } else {
        Arc::new(VdevRaid::create(chunksize, disks_per_stripe, redundancy,
                                  mirrors))
    }
}

/// Open some kind of RAID `Vdev` from its components `Vdev`s.
///
/// # Parameters
///
/// * `uuid`:       Uuid of the desired `Vdev`, if present.  If `None`,
///                 then it will not be verified.
/// * `combined`:   An array of pairs of `Mirror`s and their
///                 associated `LabelReader`.  The labels of each will be
///                 verified.
pub fn open(uuid: Option<Uuid>, combined: Vec<(Mirror, LabelReader)>)
    -> (Arc<dyn VdevRaidApi>, LabelReader)
{
    let mut label_pair = None;
    let all_mirrors = combined.into_iter()
        .map(|(mirror, mut label_reader)| {
        let label: Label = label_reader.deserialize().unwrap();
        if let Some(u) = uuid {
            assert_eq!(u, label.uuid(), "Opening disk from wrong cluster");
        }
        if label_pair.is_none() {
            label_pair = Some((label, label_reader));
        }
        (mirror.uuid(), mirror)
    }).collect::<BTreeMap<Uuid, Mirror>>();
    let (label, label_reader) = label_pair.unwrap();
    let vdev = match label {
        Label::Raid(l) => {
            Arc::new(VdevRaid::open(l, all_mirrors)) as Arc<dyn VdevRaidApi>
        },
        Label::NullRaid(l) => {
            Arc::new(NullRaid::open(l, all_mirrors)) as Arc<dyn VdevRaidApi>
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
        fn read_at(self: Arc<Self>, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut;
        fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> BoxVdevFut;
        fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            -> BoxVdevFut;
    }
}
