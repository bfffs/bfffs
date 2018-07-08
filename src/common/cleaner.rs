// vim: tw=80

use common::*;
use common::idml::ClosedZone;
use futures::{
    Future,
    stream::{self, Stream}
};
use nix::{Error, errno};
use std::sync::Arc;

#[cfg(not(test))] use common::idml::IDML;
#[cfg(test)] use common::idml_mock::IDMLMock as IDML;

/// Garbage collector.
///
/// Cleans old Zones by moving their data to empty zones and erasing them.
pub struct Cleaner {
    /// Handle to the DML.
    idml: Arc<IDML>,

    /// Dirtiness threshold.  Zones with less than this percentage of freed
    /// space will not be cleaned.
    threshold: f32,
}

impl<'a> Cleaner {
    const DEFAULT_THRESHOLD: f32 = 0.5;

    /// Clean zones in the foreground, blocking the task
    pub fn clean_now(&'a self) -> impl Future<Item=(), Error=Error>  + 'a {
        // Outline:
        // 1) Get a list of mostly-free zones
        // 2) For each zone:
        //    let offset = 0
        //    while offset < sizeof(zone)
        //        let record = find_record(zone, offset)
        //        idml.move(record)
        //        offset += sizeof(record)
        let stream = stream::iter_ok::<_, ()>(self.select_zones());
        stream.map_err(|_| unreachable!())
            .for_each(move |zone| {
                self.idml.txg()
                    .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    .and_then(move |txg_guard| {
                        self.clean_zone(zone, *txg_guard)
                    })
            })
    }

    /// Immediately clean the given zone in the foreground
    fn clean_zone(&'a self, zone: ClosedZone, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        self.idml.clean_zone(zone, txg)
    }

    pub fn new(idml: Arc<IDML>, thresh: Option<f32>) -> Self {
        Cleaner{idml, threshold: thresh.unwrap_or(Cleaner::DEFAULT_THRESHOLD)}
    }

    /// Select which zones to clean and return them sorted by cleanliness:
    /// dirtiest zones first.
    fn select_zones(&self) -> Vec<ClosedZone> {
        let mut zones = self.idml.list_closed_zones()
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

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use futures::future;
use nix::Error;
use simulacrum::*;
use super::*;
use tokio::runtime::current_thread;

/// No zone is less dirty than the threshold
#[test]
fn no_sufficiently_dirty_zones() {
    let mut idml = IDML::new();
    idml.expect_list_closed_zones()
        .called_once()
        .returning(|_| {
            let czs = vec![
                ClosedZone{freed_blocks: 1, total_blocks: 100,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(czs.into_iter())
        });
    idml.expect_txg().called_never();
    idml.expect_clean_zone().called_never();
    let cleaner = Cleaner::new(Arc::new(idml), Some(0.5));
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

#[test]
fn one_sufficiently_dirty_zone() {
    const TXG: TxgT = TxgT(42);

    let mut idml = IDML::new();
    idml.expect_list_closed_zones()
        .called_once()
        .returning(|_| {
            let czs = vec![
                ClosedZone{freed_blocks: 55, total_blocks: 100,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(czs.into_iter())
        });
    idml.expect_txg()
        .called_once()
        .returning(|_| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .called_once()
        .with(passes(move |args: &(ClosedZone, TxgT)| {
            args.0.pba == PBA::new(0, 0) &&
            args.1 == TXG
        }))
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let cleaner = Cleaner::new(Arc::new(idml), Some(0.5));
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

#[test]
fn two_sufficiently_dirty_zones() {
    const TXG: TxgT = TxgT(42);

    let mut idml = IDML::new();
    idml.expect_list_closed_zones()
        .called_once()
        .returning(|_| {
            let czs = vec![
                ClosedZone{freed_blocks: 55, total_blocks: 100,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)},
                ClosedZone{freed_blocks: 25, total_blocks: 100,
                    pba: PBA::new(1, 0), txgs: TxgT::from(0)..TxgT::from(1)},
                ClosedZone{freed_blocks: 75, total_blocks: 100,
                    pba: PBA::new(2, 0), txgs: TxgT::from(1)..TxgT::from(2)},
            ];
            Box::new(czs.into_iter())
        });
    idml.expect_txg()
        .called_once()
        .returning(|_| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .called_once()
        .with(passes(move |args: &(ClosedZone, TxgT)| {
            args.0.pba == PBA::new(2, 0) &&
            args.1 == TXG
        }))
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    idml.then().expect_txg()
        .called_once()
        .returning(|_| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .called_once()
        .with(passes(move |args: &(ClosedZone, TxgT)| {
            args.0.pba == PBA::new(0, 0) &&
            args.1 == TXG
        }))
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let cleaner = Cleaner::new(Arc::new(idml), Some(0.5));
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

}
// LCOV_EXCL_STOP
