// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `mirror` and
//! provide RAID-like functionality.

#[cfg(test)] use async_trait::async_trait;
use crate::{
    label::*,
    types::*,
    mirror,
    vdev::*,
};
#[cfg(test)]
use divbuf::DivBufShared;
use futures::Future;
#[cfg(not(test))] use futures::{
    FutureExt,
    StreamExt,
    future,
    stream::FuturesUnordered
};
#[cfg(test)] use mockall::*;
use mockall_double::double;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    iter::once,
    num::NonZeroU64,
    path::Path,
    sync::Arc
};
#[cfg(test)]
use std::pin::Pin;

#[double]
use crate::mirror::Mirror;

mod codec;
mod declust;
mod prime_s;
mod sgcursor;
mod null_raid;
mod vdev_raid;
mod vdev_raid_api;

use declust::{Locator, ChunkId, Chunkloc};

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

    pub fn nchildren(&self) -> usize {
        match self {
            Label::NullRaid(_) => 1,
            Label::Raid(rl) => rl.children.len()
        }
    }

    pub fn uuid(&self) -> Uuid {
        match self {
            Label::Raid(l) => l.uuid,
            Label::NullRaid(l) => l.uuid,
        }
    }
}

#[derive(Clone)]
#[enum_dispatch::enum_dispatch]
enum LocatorImpl {
    PrimeS(prime_s::PrimeS)
}

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager {
    mm: crate::mirror::Manager,
    raids: BTreeMap<Uuid, Label>,
}

impl Manager {
    /// Import a RAID device that is already known to exist
    #[cfg(not(test))]
    pub fn import(&mut self, uuid: Uuid)
        -> impl Future<Output=Result<(Arc<dyn VdevRaidApi>, LabelReader)>>
    {
        let rl = match self.raids.remove(&uuid) {
            Some(rl) => rl,
            None => return future::err(Error::ENOENT).boxed()
        };
        rl.iter_children()
            .map(move |child_uuid| self.mm.import(*child_uuid))
            .collect::<FuturesUnordered<_>>()
            // Could use Iterator::try_collect if it stabilizes.
            // https://github.com/rust-lang/rust/issues/94047
            .collect::<Vec<_>>()
            .map(move |v| {
                let mut pairs = Vec::with_capacity(rl.nchildren());
                let mut error = Error::ENOENT;
                for r in v.into_iter() {
                    match r {
                        Ok(pair) => pairs.push(pair),
                        Err(e) => {
                            error = e;
                        }
                    }
                };
                if pairs.is_empty() {
                    Err(error)
                } else {
                    Ok(open(Some(uuid), pairs))
                }
            }).boxed()
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<LabelReader> {
        let mut reader = self.mm.taste(p).await?;
        let rl: Label = reader.deserialize().unwrap();
        self.raids.insert(rl.uuid(), rl);
        Ok(reader)
    }
}

/// Return value of [`VdevRaidApi::status`]
#[derive(Clone, Debug)]
pub struct Status {
    pub health: Health,
    pub codec: String,
    pub mirrors: Vec<mirror::Status>,
    pub uuid: Uuid
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
fn open(uuid: Option<Uuid>, combined: Vec<(Mirror, LabelReader)>)
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
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
    #[async_trait]
    impl VdevRaidApi for VdevRaid {
        fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn fault(&mut self, uuid: Uuid) -> Result<()>;
        fn flush_zone(&self, zone: ZoneT) -> (LbaT, BoxVdevFut);
        fn open_zone(&self, zone: ZoneT) -> BoxVdevFut;
        fn read_at(self: Arc<Self>, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        fn read_long(&self, len: LbaT, lba: LbaT)
            -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>;
        fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut;
        fn status(&self) -> Status;
        fn uuid(&self) -> Uuid;
        fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> BoxVdevFut;
        fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            -> BoxVdevFut;
    }
}
