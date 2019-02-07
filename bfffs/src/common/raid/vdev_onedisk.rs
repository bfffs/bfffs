// vim: tw=80

use crate::{
    boxfut,
    common::{
        *,
        label::*,
        vdev::*,
    }
};
use futures::{Future, IntoFuture};
use std::collections::BTreeMap;
#[cfg(not(test))] use std::{
    num::NonZeroU64,
    path::Path
};
use super::{
    vdev_raid_api::*,
};

#[cfg(test)]
use crate::common::vdev_block::MockVdevBlock as VdevBlock;
#[cfg(not(test))]
use crate::common::vdev_block::VdevBlock;

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:   Uuid,
    /// UUID of the wrapped `VdevFile`
    pub child:  Uuid
}

/// `VdevOneDisk`: RAID-level Virtual Device for single-disk clusters
///
/// This Vdev adapts a Cluster to a single disk, without providing any
/// redundancy.
pub struct VdevOneDisk {
    /// Underlying block device.
    blockdev: VdevBlock,

    uuid: Uuid,
}

impl VdevOneDisk {
    /// Create a new VdevOneDisk from an unused file or device
    ///
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `path`:               Pathnames of file or device
    #[cfg(not(test))]
    // Hide from docs.  The public API should just be raid::create, but this
    // function technically needs to be public for testing purposes.
    #[doc(hidden)]
    pub fn create<P>(lbas_per_zone: Option<NonZeroU64>, path: P) -> Self
        where P: AsRef<Path> + 'static
    {
        let uuid = Uuid::new_v4();
        let blockdev = VdevBlock::create(path, lbas_per_zone).unwrap();
        VdevOneDisk{uuid, blockdev}
    }

    /// Open an existing `VdevOneDisk`
    ///
    /// # Parameters
    ///
    /// * `label`:      The `VdevOneDisk`'s label
    /// * `blockdevs`:  A map containing a single `VdevBlock`, indexed by UUID
    pub(super) fn open(label: Label, blockdevs: BTreeMap<Uuid, VdevBlock>)
        -> Self
    {
        assert_eq!(blockdevs.len(), 1);
        let blockdev = blockdevs.into_iter().next().unwrap().1;
        VdevOneDisk{uuid: label.uuid, blockdev}
    }

}

impl Vdev for VdevOneDisk {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.blockdev.lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.blockdev.optimum_queue_depth()
    }

    fn size(&self) -> LbaT {
        self.blockdev.size()
    }

    fn sync_all(&self) -> Box<VdevFut> {
        self.blockdev.sync_all()
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.blockdev.zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.blockdev.zones()
    }
}

impl VdevRaidApi for VdevOneDisk {
    fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.blockdev.zone_limits(zone);
        boxfut!(self.blockdev.erase_zone(limits.0, limits.1 - 1), _, _, 'static)
    }

    fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.blockdev.zone_limits(zone);
        let fut = self.blockdev.finish_zone(limits.0, limits.1 - 1);
        boxfut!(fut, _, _, 'static)
    }

    fn flush_zone(&self, _zone: ZoneT) -> (LbaT, BoxVdevFut) {
        (0, boxfut!(Ok(()).into_future(), _, _, 'static))
    }

    fn open_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let limits = self.blockdev.zone_limits(zone);
        boxfut!(self.blockdev.open_zone(limits.0), _, _, 'static)
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        boxfut!(self.blockdev.read_at(buf, lba), _, _, 'static)
    }

    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut {
        boxfut!(self.blockdev.read_spacemap(buf, idx), _, _, 'static)
    }

    fn reopen_zone(&self, _zone: ZoneT, _allocated: LbaT) -> BoxVdevFut {
        boxfut!(Ok(()).into_future(), _, _, 'static)
    }

    fn write_at(&self, buf: IoVec, _zone: ZoneT, lba: LbaT) -> BoxVdevFut {
        // Pad up to a whole number of LBAs.  Upper layers don't do this because
        // VdevRaidApi doesn't have a writev_at method.  But VdevBlock does, so
        // the raid layer is the most efficient place to pad.
        let partial = buf.len() % BYTES_PER_LBA;
        if partial == 0 {
            boxfut!(self.blockdev.write_at(buf, lba), _, _, 'static)
        } else {
            let remainder = BYTES_PER_LBA - partial;
            let zbuf = ZERO_REGION.try_const().unwrap().slice_to(remainder);
            let sglist = vec![buf, zbuf];
            boxfut!(self.blockdev.writev_at(sglist, lba), _, _, 'static)
        }
    }

    fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut {
        let onedisk_label = Label {
            uuid: self.uuid,
            child: self.blockdev.uuid()
        };
        let label = super::Label::OneDisk(onedisk_label);
        labeller.serialize(&label).unwrap();
        boxfut!(self.blockdev.write_label(labeller), _, _, 'static)
    }

    // Allow &Vec arguments so we can clone them.
    // TODO: pass by value instead of reference, to eliminate the clone
    #[allow(clippy::ptr_arg)]
    fn write_spacemap(&self, sglist: &SGList, idx: u32, block: LbaT)
        -> Box<VdevFut>
    {
        let fut = self.blockdev.write_spacemap(sglist.clone(), idx, block);
        boxfut!(fut, _, _, 'static)
    }
}

#[cfg(test)]
mod t {

use super::*;
// pet kcov
#[test]
fn debug() {
    let label = Label {
        uuid: Uuid::new_v4(),
        child: Uuid::new_v4()
    };
    format!("{:?}", label);
}

}
