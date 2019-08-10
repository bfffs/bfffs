// vim: tw=80

use crate::common::{
    *,
    idml::{ClosedZone, IDML}
};
use futures::{
    Future,
    future,
    stream::{self, Stream},
    sync::{mpsc, oneshot}
};
use std::sync::Arc;
use tokio::executor::Executor;

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
    tx: Option<mpsc::Sender<oneshot::Sender<()>>>
}

impl Cleaner {
    const DEFAULT_THRESHOLD: f32 = 0.5;

    /// Clean zones immediately.  Does not wait for the result to be polled!
    ///
    /// The returned `Receiver` will deliver notification when cleaning is
    /// complete.  However, there is no requirement to poll it.  The client may
    /// drop it, and cleaning will continue in the background.
    pub fn clean(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.tx.as_ref().unwrap().clone().try_send(tx) {
            if e.is_full() {
                // No worries; cleaning is idempotent
            } else {
                panic!("{:?}", e);
            }
        }
        rx
    }

    pub fn new<E>(handle: E, idml: Arc<IDML>, thresh: Option<f32>) -> Self
        where E: Executor + 'static
    {
        let (tx, rx) = mpsc::channel(1);
        Cleaner::run(handle, idml, thresh.unwrap_or(Cleaner::DEFAULT_THRESHOLD),
                     rx);
        Cleaner{tx: Some(tx)}
    }

    // Start a task that will clean the system in the background, whenever
    // requested.
    fn run<E>(mut handle: E, idml: Arc<IDML>, thresh: f32,
              rx: mpsc::Receiver<oneshot::Sender<()>>)
        where E: Executor + 'static
    {
        handle.spawn(Box::new(future::lazy(move || {
            let sync_cleaner = SyncCleaner::new(idml, thresh);
            rx.for_each(move |tx| {
                sync_cleaner.clean_now()
                    .map_err(Error::unhandled)
                    .map(move |_| {
                        // Ignore errors.  An error here indicates that the
                        // client doesn't want to be notified.
                        let _result = tx.send(());
                    })
            })
        }))).unwrap()
    }

    // Shutdown the Cleaner's background task
    pub fn shutdown(&mut self) -> impl Future<Item=(), Error=()> + Send {
        // Ignore return value.  An error indicates that the Cleaner is already
        // shut down.
        let _ = self.tx.take();
        future::ok::<(), ()>(())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use futures::future;
use mockall::Sequence;
use super::*;
use tokio::runtime::current_thread;
use tokio_current_thread::TaskExecutor;

/// Clean in the background
#[test]
fn background() {
    let mut idml = IDML::default();
    idml.expect_list_closed_zones()
        .once()
        .returning(|| {
            let czs = vec![
                ClosedZone{freed_blocks: 0, total_blocks: 100, zid: 0,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg().never();
    idml.expect_clean_zone().never();

    let mut rt = current_thread::Runtime::new().unwrap();
    rt.spawn(future::lazy(|| {
        let te = TaskExecutor::current();
        let cleaner = Cleaner::new(te, Arc::new(idml), None);
        cleaner.clean()
            .map_err(Error::unhandled)
    }));
    rt.run().unwrap();
}

/// No zone is less dirty than the threshold
#[test]
fn no_sufficiently_dirty_zones() {
    let mut idml = IDML::default();
    idml.expect_list_closed_zones()
        .once()
        .returning(|| {
            let czs = vec![
                ClosedZone{freed_blocks: 1, total_blocks: 100, zid: 0,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg().never();
    idml.expect_clean_zone().never();
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

#[test]
fn one_sufficiently_dirty_zone() {
    const TXG: TxgT = TxgT(42);

    let mut idml = IDML::default();
    idml.expect_list_closed_zones()
        .once()
        .returning(|| {
            let czs = vec![
                ClosedZone{freed_blocks: 55, total_blocks: 100, zid: 0,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)}
            ];
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg()
        .once()
        .returning(|| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .once()
        .withf(move |zone, txg| {
            zone.pba == PBA::new(0, 0) &&
            *txg == TXG
        }).returning(|_, _| Box::new(future::ok::<(), Error>(())));
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

#[test]
fn two_sufficiently_dirty_zones() {
    const TXG: TxgT = TxgT(42);

    let mut seq = Sequence::new();
    let mut idml = IDML::default();
    idml.expect_list_closed_zones()
        .once()
        .returning(|| {
            let czs = vec![
                ClosedZone{freed_blocks: 55, total_blocks: 100, zid: 0,
                    pba: PBA::new(0, 0), txgs: TxgT::from(0)..TxgT::from(1)},
                ClosedZone{freed_blocks: 25, total_blocks: 100, zid: 1,
                    pba: PBA::new(1, 0), txgs: TxgT::from(0)..TxgT::from(1)},
                ClosedZone{freed_blocks: 75, total_blocks: 100, zid: 2,
                    pba: PBA::new(2, 0), txgs: TxgT::from(1)..TxgT::from(2)},
            ];
            Box::new(stream::iter_ok(czs.into_iter()))
        });
    idml.expect_txg()
        .once()
        .in_sequence(&mut seq)
        .returning(|| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .once()
        .withf(move |zone, txg| {
            zone.pba == PBA::new(2, 0) &&
            *txg == TXG
        }).returning(|_, _| Box::new(future::ok::<(), Error>(())));
    idml.expect_txg()
        .once()
        .in_sequence(&mut seq)
        .returning(|| Box::new(future::ok::<&'static TxgT, Error>(&TXG)));
    idml.expect_clean_zone()
        .once()
        .withf(move |zone, txg| {
            zone.pba == PBA::new(0, 0) &&
            *txg == TXG
        }).returning(|_, _| Box::new(future::ok::<(), Error>(())));
    let cleaner = SyncCleaner::new(Arc::new(idml), 0.5);
    current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
        cleaner.clean_now()
    })).unwrap();
}

}
// LCOV_EXCL_STOP
