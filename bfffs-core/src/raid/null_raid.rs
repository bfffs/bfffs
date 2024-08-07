// vim: tw=80

use std::{
    collections::BTreeMap,
    pin::Pin,
};

use crate::{
    BYTES_PER_LBA,
    ZERO_REGION,
    label::*,
    types::*,
    vdev::*,
};
use divbuf::DivBufShared;
use futures::{Future, FutureExt, TryFutureExt, future};
use mockall_double::double;
use serde_derive::{Deserialize, Serialize};
use super::{
    Status,
    vdev_raid_api::*,
};

#[double]
use crate::mirror::Mirror;

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:   Uuid,
    /// UUID of the wrapped `Mirror`
    pub child:  Uuid
}

/// `NullRaid`: RAID-level passthrough Virtual Device
///
/// This Vdev adapts a Cluster to a single disk or a single mirror, without
/// providing any additional redundancy.
pub struct NullRaid {
    /// Underlying mirror.
    mirror: Mirror,

    uuid: Uuid,
}

impl NullRaid {
    /// Create a new NullRaid from an unused file or device
    ///
    /// * `mirror`:             Already labeled Mirror device
    // Hide from docs.  The public API should just be raid::create, but this
    // function technically needs to be public for testing purposes.
    #[doc(hidden)]
    pub fn create(mirror: Mirror) -> Self
    {
        let uuid = Uuid::new_v4();
        NullRaid{mirror, uuid}
    }

    /// Open an existing `NullRaid`
    ///
    /// # Parameters
    ///
    /// * `label`:      The `NullRaid`'s label
    /// * `mirrors`:  A map containing a single `Mirror`, indexed by UUID
    pub(super) fn open(label: Label, mirrors: BTreeMap<Uuid, Mirror>) -> Self
    {
        assert_eq!(mirrors.len(), 1);
        let mirror = mirrors.into_iter().next().unwrap().1;
        NullRaid{uuid: label.uuid, mirror}
    }
}

impl Vdev for NullRaid {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.mirror.lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.mirror.optimum_queue_depth()
    }

    fn size(&self) -> LbaT {
        self.mirror.size()
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.mirror.zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.mirror.zones()
    }
}

impl VdevRaidApi for NullRaid {
    fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.mirror.zone_limits(zone);
        Box::pin(self.mirror.erase_zone(limits.0, limits.1 - 1))
    }

    fn fault(&mut self, uuid: Uuid) -> Result<()> {
        if uuid == self.uuid {
            Err(Error::EINVAL)
        } else {
            self.mirror.fault(uuid)
        }
    }

    fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.mirror.zone_limits(zone);
        Box::pin(self.mirror.finish_zone(limits.0, limits.1 - 1))
    }

    fn flush_zone(&self, _zone: ZoneT) -> (LbaT, BoxVdevFut) {
        (0, Box::pin(future::ok(())))
    }

    fn open_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.mirror.zone_limits(zone);
        Box::pin(self.mirror.open_zone(limits.0))
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        Box::pin(self.mirror.read_at(buf, lba))
    }

    fn read_long(&self, len: LbaT, lba: LbaT)
        -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>
    {
        self.mirror.read_long(len, lba)
            .map_ok(|r| Box::new(r) as Box<dyn Iterator<Item=DivBufShared> + Send>)
            .boxed()
    }

    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut
    {
        Box::pin(self.mirror.read_spacemap(buf, idx))
    }

    fn reopen_zone(&self, _zone: ZoneT, _allocated: LbaT) -> BoxVdevFut
    {
        Box::pin(future::ok(()))
    }

    fn status(&self) -> Status {
        let codec = String::from("NonRedundant");
        let child = self.mirror.status();
        Status {
            health: child.health,
            codec,
            mirrors: vec![child],
            uuid: self.uuid
        }
    }

    fn sync_all(&self) -> BoxVdevFut {
        self.mirror.sync_all()
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn write_at(&self, buf: IoVec, _zone: ZoneT, lba: LbaT) -> BoxVdevFut
    {
        // Pad up to a whole number of LBAs.  Upper layers don't do this because
        // VdevRaidApi doesn't have a writev_at method.  But Mirror does, so
        // the raid layer is the most efficient place to pad.
        let partial = buf.len() % BYTES_PER_LBA;
        if partial == 0 {
            Box::pin(self.mirror.write_at(buf, lba))
        } else {
            let remainder = BYTES_PER_LBA - partial;
            let zbuf = ZERO_REGION.try_const().unwrap().slice_to(remainder);
            let sglist = vec![buf, zbuf];
            Box::pin(self.mirror.writev_at(sglist, lba))
        }
    }

    fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut
    {
        let nullraid_label = Label {
            uuid: self.uuid,
            child: self.mirror.uuid()
        };
        let label = super::Label::NullRaid(nullraid_label);
        labeller.serialize(&label).unwrap();
        Box::pin(self.mirror.write_label(labeller))
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> BoxVdevFut
    {
        Box::pin(self.mirror.write_spacemap(sglist, idx, block))
    }
}
