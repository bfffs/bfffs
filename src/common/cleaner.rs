// vim: tw=80

use common::*;
use common::idml::ClosedZone;
use futures::{
    Future,
    Sink,
    future,
    stream::{self, Stream},
    sync::mpsc
};
use std::sync::Arc;
use tokio::executor::Executor;

#[cfg(not(test))] use common::idml::IDML;
#[cfg(test)] use common::idml_mock::IDMLMock as IDML;

struct SyncCleaner {
    /// Handle to the DML.
    idml: Arc<IDML>,

    /// Dirtiness threshold.  Zones with less than this percentage of freed
    /// space will not be cleaned.
    threshold: f32,
}

impl SyncCleaner {
    /// Clean zones in the foreground, blocking the task
    pub fn clean_now(&self) -> impl Future<Item=(), Error=Error> + Send {
        // Outline:
        // 1) Get a list of mostly-free zones
        // 2) For each zone:
        //    let offset = 0
        //    while offset < sizeof(zone)
        //        let record = find_record(zone, offset)
        //        idml.move(record)
        //        offset += sizeof(record)
        let idml2 = self.idml.clone();
        self.select_zones()
        .and_then(move |zones| {
            stream::iter_ok(zones.into_iter())
            .for_each(move |zone| {
                let idml3 = idml2.clone();
                idml2.txg()
                    .map_err(|_| Error::EPIPE)
                    .and_then(move |txg_guard| {
                        idml3.clean_zone(zone, *txg_guard)
                    })
            })
        })
    }

    pub fn new(idml: Arc<IDML>, threshold: f32) -> Self {
        SyncCleaner{idml, threshold}
    }

    /// Select which zones to clean and return them sorted by cleanliness:
    /// dirtiest zones first.
    fn select_zones(&self)
        -> impl Future<Item=Vec<ClosedZone>, Error=Error> + Send
    {
        let threshold = self.threshold;
        self.idml.list_closed_zones()
        .filter(move |z| {
            let dirtiness = z.freed_blocks as f32 / z.total_blocks as f32;
            dirtiness >= threshold
        }).collect()
        .map(|mut zones: Vec<ClosedZone>| {
            // Sort by highest percentage of free space to least
            // TODO: optimize for the case where all zones have equal size,
            // removing the division.
            zones.sort_unstable_by(|a, b| {
                // Annoyingly, f32 only implements PartialOrd, not Ord.  So we
                // have to define a comparator function.
                let afrac = -(a.freed_blocks as f32 / a.total_blocks as f32);
                let bfrac = -(b.freed_blocks as f32 / b.total_blocks as f32);
                afrac.partial_cmp(&bfrac).unwrap()
            });
            zones
        })
    }
}

/// Garbage collector.
///
/// Cleans old Zones by moving their data to empty zones and erasing them.
pub struct Cleaner {
    tx: mpsc::Sender<()>
}

impl Cleaner {
    const DEFAULT_THRESHOLD: f32 = 0.5;

    pub fn clean(&self) -> impl Future<Item=(), Error=Error> {
        self.tx.clone()
            .send(())
            .map(|_| ())
            .map_err(|e| panic!("{:?}", e))
    }

    pub fn new<E>(handle: E, idml: Arc<IDML>, thresh: Option<f32>) -> Self
        where E: Executor + 'static
    {
        let (tx, rx) = mpsc::channel(1);
        Cleaner::run(handle, idml, thresh.unwrap_or(Cleaner::DEFAULT_THRESHOLD),
                     rx);
        Cleaner{tx}
    }

    // Start a task that will clean the system in the background, whenever
    // requested.
    fn run<E>(mut handle: E, idml: Arc<IDML>, thresh: f32,
              rx: mpsc::Receiver<()>)
        where E: Executor + 'static
    {
        handle.spawn(Box::new(future::lazy(move || {
            let sync_cleaner = SyncCleaner::new(idml, thresh);
            rx.for_each(move |_| {
                sync_cleaner.clean_now()
                    .map_err(|e| panic!("{:?}", e))
            })
        }))).unwrap()
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use futures::future;
use simulacrum::*;
use super::*;
use tokio::runtime::current_thread;
use tokio::executor::current_thread::TaskExecutor;

/// Clean in the background
#[test]
fn background() {
    let mut idml = IDML::new();
    idml.expect_list_closed_zones()
        .called_once()
        .returning(|_| {
            let czs = vec![
                ClosedZone{freed_blocks: 0, total_blocks: 100,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg().called_never();
    idml.expect_clean_zone().called_never();

    let mut rt = current_thread::Runtime::new().unwrap();
    rt.spawn(future::lazy(|| {
        let te = TaskExecutor::current();
        let cleaner = Cleaner::new(te, Arc::new(idml), None);
        cleaner.clean()
            .map_err(|e| panic!("{:?}", e))
    }));
    rt.run().unwrap();
}

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
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg().called_never();
    idml.expect_clean_zone().called_never();
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
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
            Box::new(stream::iter_ok(czs.into_iter()))
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
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
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
            Box::new(stream::iter_ok(czs.into_iter()))
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
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

}
// LCOV_EXCL_STOP
