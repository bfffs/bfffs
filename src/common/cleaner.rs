// vim: tw=80

use common::ddml::{DDML, ClosedZone};
use futures::{
    Future,
    stream::{self, Stream}
};
use nix::Error;

/// Garbage collector.
///
/// Cleans old Zones by moving their data to empty zones and erasing them.
pub struct Cleaner {
    /// Handle to the DML.
    // TODO: use the IDML once that's ready
    ddml: DDML,

    /// Dirtiness threshold.  Zones with less than this percentage of freed
    /// space will not be cleaned.
    threshold: f32,
}

impl<'a> Cleaner {
    /// Clean zones in the foreground, blocking the task
    pub fn clean_now(&'a self) -> impl Future<Item=(), Error=Error>  + 'a {
        // Outline:
        // 1) Get a list of mostly-free zones
        // 2) For each zone:
        //    let offset = 0
        //    while offset < sizeof(zone)
        //        let record = find_record(zone, offset)
        //        ddml.move(record)
        //        offset += sizeof(record)
        let stream = stream::iter_ok::<_, ()>(self.select_zones());
        stream.map_err(|_| unreachable!())
            .for_each(move |zone| {
            self.clean_zone(zone)
        })
    }

    /// Immediately clean the given zone in the foreground
    fn clean_zone(&'a self, zone: ClosedZone)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        self.ddml.list_records(zone).for_each(move |record| {
            self.ddml.move_record(record)
        })
    }

    /// Select which zones to clean and return them sorted by cleanliness:
    /// dirtiest zones first.
    fn select_zones(&self) -> Vec<ClosedZone> {
        let mut zones = self.ddml.list_closed_zones()
            .filter(|z| {
                let dirtiness = z.freed_blocks as f32 / z.total_blocks as f32;
                dirtiness >= self.threshold
            }).collect::<Vec<_>>();
        // Sort by highest percentage of free space to least
        // TODO: optimize for the case where all zones have equal size, removing
        // the division.
        zones.sort_unstable_by(|a, b| {
            // Annoyingly, f32 only implements PartialOrd, not Ord.  So we have
            // to define a comparator function.
            let afrac = -(a.freed_blocks as f32 / a.total_blocks as f32);
            let bfrac = -(b.freed_blocks as f32 / b.total_blocks as f32);
            afrac.partial_cmp(&bfrac).unwrap()
        });
        zones
    }
}
