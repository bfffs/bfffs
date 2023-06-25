// vim: tw=80

use crate::{
    cluster,
    label::*,
    types::*,
    util::*,
    vdev::*
};
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    stream::FuturesUnordered,
    task::{Context, Poll}
};
use pin_project::pin_project;
#[cfg(test)] use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::{
    ops::Range,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc
    }
};
use std::{
    collections::BTreeMap,
    path::Path
};

#[cfg(test)]
use crate::cluster::MockCluster as Cluster;
#[cfg(not(test))]
use crate::cluster::Cluster;

/// Public representation of a closed zone
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosedZone {
    /// Number of freed blocks in this zone
    pub freed_blocks: LbaT,

    /// Physical address of the start of the zone
    pub pba: PBA,

    /// Total number of blocks in this zone
    pub total_blocks: LbaT,

    /// Range of transactions included in this zone
    pub txgs: Range<TxgT>,

    /// Index of the closed zone
    pub zid: ZoneT
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Human-readable name
    pub name:               String,

    /// Pool UUID, fixed at format time
    pub uuid:               Uuid,

    /// `UUID`s of all component `VdevRaid`s
    pub children:           Vec<Uuid>,
}

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager {
    cm: crate::cluster::Manager,
    pools: BTreeMap<Uuid, Label>,
}

impl Manager {
    /// Import a pool that is already known to exist
    #[cfg(not(test))]
    pub async fn import(&mut self, uuid: Uuid) -> Result<(Pool, LabelReader)>
    {
        let pl = match self.pools.remove(&uuid) {
            Some(l) => l,
            None => return Err(Error::ENOENT)
        };
        let cc = pl.children.into_iter()
            .map(move |child_uuid| self.cm.import(child_uuid))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>().await?;
        Ok(Pool::open(Some(uuid), cc))
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        self.pools.values()
            .map(|label| {
                (label.name.clone(), label.uuid)
            }).collect::<Vec<_>>()
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<()> {
        let mut reader = self.cm.taste(p).await?;
        let pl: Label = reader.deserialize().unwrap();
        self.pools.insert(pl.uuid, pl);
        Ok(())
    }
}

struct Stats {
    /// The queue depth of each `Cluster`, including both commands that have
    /// been sent to the disks, and commands that are pending in `VdevBlock`
    queue_depth: Vec<AtomicU32>,

    /// "Best" number of commands to queue to each VdevRaid
    optimum_queue_depth: Vec<f64>,

    /// The total size of each `Cluster`
    size: Vec<LbaT>,

    /// The total amount of used space across all `Cluster`s, excluding space
    /// that has already been freed but not erased.
    used_space: AtomicU64
}

impl Stats {
    /// The approximate usable size of the Pool
    fn size(&self) -> LbaT {
        self.size.iter().sum()
    }

    fn used(&self) -> LbaT {
        self.used_space.load(Ordering::Relaxed)
    }
}

/// Return value of [`Pool::status`]
#[derive(Clone, Debug)]
pub struct Status {
    pub name: String,
    pub clusters: Vec<cluster::Status>
}

/// An BFFFS storage pool
pub struct Pool {
    clusters: Vec<Cluster>,

    /// Human-readable pool name.  Must be unique on any one system.
    name: String,

    stats: Arc<Stats>,

    uuid: Uuid,
}

#[cfg_attr(test, automock)]
impl Pool {
    /// Cleanup stuff from the previous transaction.
    pub fn advance_transaction(&self, txg: TxgT)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        self.clusters.iter()
        .map(|cl| cl.advance_transaction(txg))
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop)
    }

    /// Assert that the given zone was clean as of the given transaction
    #[cfg(debug_assertions)]
    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) {
        self.clusters[cluster as usize].assert_clean_zone(zone, txg)
    }

    /// Choose the best Cluster for the next write
    ///
    /// This decision is subjective, but should strive to:
    /// 1) Balance capacity utilization amongst all Clusters
    /// 2) Balance IOPs amongst all Clusters
    /// 3) Run quickly
    fn choose_cluster(&self) -> ClusterT {
        // This simple implementation weighs both capacity utilization and IOPs,
        // though above 95% utilization it switches to weighing by capacity
        // utilization only.  It's slow because it iterates through all clusters
        // on every write.  A better implementation might perform the full
        // calculation only occasionally, to update coefficients, and perform a
        // quick calculation on each write.
        (0..self.clusters.len())
        .map(|i| {
            let alloc = self.clusters[i].allocated() as f64;
            let space_util = alloc / (self.stats.size[i] as f64);
            let qdepth = self.stats.queue_depth[i]
                .load(Ordering::Relaxed) as f64;
            let queue_fraction = qdepth / self.stats.optimum_queue_depth[i];
            let q_coeff = if 0.95 > space_util {0.95 - space_util} else {0.0};
            let weight = q_coeff * queue_fraction + space_util;
            (i, weight)
        })
        .min_by(|&(_, x), &(_, y)| x.partial_cmp(&y).unwrap())
        .map(|(i, _)| i)
        .unwrap() as ClusterT
    }

    /// Create a new `Pool` from some freshly created `Cluster`s.
    pub fn create(name: String, clusters: Vec<Cluster>) -> Self
    {
        Pool::new(name, Uuid::new_v4(), clusters)
    }

    pub fn dump_fsm(&self) -> Vec<String> {
        self.clusters.iter()
            .map(|cluster| cluster.dump_fsm())
            .collect::<Vec<_>>()
    }

    pub fn flush(&self, idx: u32)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        self.clusters.iter()
        .map(|cl| cl.flush(idx))
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop)
    }

    /// Mark `length` LBAs beginning at PBA `pba` as unused, but do not delete
    /// them from the underlying storage.
    ///
    /// Freeing data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, BFFFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Pool.
    pub fn free(&self, pba: PBA, length: LbaT) -> BoxVdevFut
    {
        self.stats.used_space.fetch_sub(length, Ordering::Relaxed);
        Box::pin(self.clusters[pba.cluster as usize].free(pba.lba, length))
    }

    /// Construct a new `Pool` from some already constructed
    /// [`Cluster`](struct.Cluster.html)s.
    #[allow(clippy::new_ret_no_self)]
    fn new(name: String, uuid: Uuid, clusters: Vec<Cluster>) -> Self
    {
        let size = clusters.iter()
            .map(Cluster::size)
            .collect::<Vec<_>>();
        let optimum_queue_depth = clusters.iter()
            .map(|cluster| f64::from(cluster.optimum_queue_depth()))
            .collect::<Vec<_>>();
        let queue_depth: Vec<_> = clusters.iter()
            .map(|_| AtomicU32::new(0))
            .collect();
        let used_space = clusters.iter()
            .map(|cluster| cluster.used())
            .sum::<u64>()
            .into();
        let stats = Arc::new(Stats{
            queue_depth,
            optimum_queue_depth,
            size,
            used_space,
        });
        Pool{clusters, name, stats, uuid}
    }

    /// Find the next closed zone in the pool.
    ///
    /// Returns the next cluster and zone to query as well as ClosedZone.
    ///
    /// # Returns
    ///
    /// * `(Some(c), Some(x))` - `c` is a `ClosedZone`.  Pass `x` on the next
    ///                          call.
    /// * `(None, Some(x))`    - No closed zone this call.  Repeat the call,
    ///                          supplying `x`
    /// * `(None, None)`       - No more closed zones in this pool.
    #[allow(clippy::collapsible_if)]
    pub fn find_closed_zone(&self, clust: ClusterT, zid: ZoneT)
        -> (Option<ClosedZone>, Option<(ClusterT, ZoneT)>)
    {
        let nclusters = self.clusters.len() as ClusterT;
        let r = self.clusters[clust as usize].find_closed_zone(zid);
        if let Some(cclz) = r {
            // convert cluster::ClosedZone to pool::ClosedZone
            let pclz = ClosedZone {
                freed_blocks: cclz.freed_blocks,
                pba: PBA::new(clust, cclz.start),
                total_blocks: cclz.total_blocks,
                txgs: cclz.txgs,
                zid: cclz.zid};
            (Some(pclz), Some((clust, cclz.zid + 1)))
        } else {
            // No more closed zones on this cluster
            if clust < nclusters - 1 {
                // Try the next cluster
                (None, Some((clust + 1, 0)))
            } else {
                // No more clusters
                (None, None)
            }
        }
    }

    /// Return the `Pool`'s name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open an existing `Pool` from its component devices.
    ///
    /// Returns a new `Pool` object and a `LabelReader`
    ///
    /// # Parameters
    ///
    /// * `uuid`:       Uuid of the desired `Pool`, if present.  If `None`, then
    ///                 it will not be verified.
    /// * `combined`:   An array of pairs of `Cluster`s and their
    ///                 associated `LabelReader`.  The labels of each will be
    ///                 verified.
    pub fn open(uuid: Option<Uuid>, combined: Vec<(Cluster, LabelReader)>)
        -> (Self, LabelReader)
    {
        let mut label_pair = None;
        let mut all_clusters = combined.into_iter()
            .map(|(cluster, mut label_reader)| {
            let label: Label = label_reader.deserialize().unwrap();
            if let Some(u) = uuid {
                assert_eq!(u, label.uuid, "Opening cluster from wrong pool");
            }
            if label_pair.is_none() {
                label_pair = Some((label, label_reader));
            }
            (cluster.uuid(), cluster)
        }).collect::<BTreeMap<Uuid, Cluster>>();
        let (label, label_reader) = label_pair.unwrap();
        assert_eq!(all_clusters.len(), label.children.len(),
            "Missing clusters");
        let children = label.children.iter().map(|uuid| {
            all_clusters.remove(uuid).unwrap()
        }).collect::<Vec<_>>();
        (Pool::new(label.name, label.uuid, children), label_reader)
    }

    /// Asynchronously read from the pool
    pub fn read(&self, buf: IoVecMut, pba: PBA) -> BoxVdevFut
    {
        let cidx = pba.cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let stats2 = self.stats.clone();
        let fut = self.clusters[pba.cluster as usize].read(buf, pba.lba)
            .map(move |r| {
                stats2.queue_depth[cidx].fetch_sub(1, Ordering::Relaxed);
                r
            });
        Box::pin(fut)
    }

    /// Return approximately the Pool's usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.stats.size()
    }

    pub fn status(&self) -> Status {
        Status {
            name: self.name().to_string(),
            clusters: self.clusters.iter()
                .map(Cluster::status)
                .collect::<Vec<_>>()
        }
    }

    /// Sync the `Pool`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&self) -> BoxVdevFut {
        let fut = self.clusters.iter()
        .map(Cluster::sync_all)
        .collect::<FuturesUnordered<BoxVdevFut>>()
        .try_collect::<Vec<()>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    /// How many blocks have been allocated and are still in used?
    pub fn used(&self) -> LbaT {
        self.stats.used()
    }

    /// Return the `Pool`'s UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Write a buffer to the pool
    ///
    /// # Returns
    ///
    /// The `PBA` where the data was written
    pub fn write(&self, buf: IoVec, txg: TxgT) ->
        impl Future<Output=Result<PBA>> + Send
    {
        let cluster = self.choose_cluster();
        let cidx = cluster as usize;
        let space = div_roundup(buf.len(), BYTES_PER_LBA) as LbaT;
        let stats2 = self.stats.clone();
        match self.clusters[cidx].write(buf, txg) {
            Ok((lba, wfut)) => {
                self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
                let pba = PBA::new(cluster, lba);
                Write::Write(wfut, stats2, cidx, space, pba)
            },
            Err(e) => Write::EarlyErr(e)
        }
    }

    /// Asynchronously write this `Pool`'s label to all component devices
    pub fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut
    {
        let cluster_uuids = self.clusters.iter().map(Cluster::uuid)
            .collect::<Vec<_>>();
        let label = Label {
            name: self.name.clone(),
            uuid: self.uuid,
            children: cluster_uuids,
        };
        labeller.serialize(&label).unwrap();
        let fut = self.clusters.iter()
        .map(|cluster| cluster.write_label(labeller.clone()))
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }
}

/// Return value of `Pool::write`
#[pin_project(project = WriteProj)]
enum Write {
    Write(#[pin] BoxVdevFut, Arc<Stats>, usize, LbaT, PBA),
    EarlyErr(Error)
}

impl Future for Write {
    type Output = Result<PBA>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(match self.as_mut().project() {
            WriteProj::Write(wfut, stats, cidx, space, pba) => {
                let r = futures::ready!(wfut.poll(cx));
                stats.queue_depth[*cidx].fetch_sub(1, Ordering::Relaxed);
                r.map(|_| {
                    stats.used_space.fetch_add(*space, Ordering::Relaxed);
                    *pba
                })
            },
            WriteProj::EarlyErr(e) => Err(*e)
        })
    }
}


// LCOV_EXCL_START
#[cfg(test)]
mod t {

mod label {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{name: "Foo".to_owned(),
            uuid: Uuid::new_v4(),
            children: vec![]
        };
        format!("{label:?}");
    }
}

mod pool {
    use super::super::*;
    use crate::cluster;
    use divbuf::DivBufShared;
    use futures::future;
    use mockall::predicate::*;
    use pretty_assertions::assert_eq;

    fn mock_cluster(allocated: u64, size: u64, used: u64) -> Cluster {
        let mut c = Cluster::default();
        c.expect_allocated().return_const(allocated);
        c.expect_size().return_const(size);
        c.expect_optimum_queue_depth().return_const(10u32);
        c.expect_used().return_const(used);
        c.expect_uuid().return_const(Uuid::new_v4());
        c
    }

    #[tokio::test]
    async fn advance_transaction() {
        let txg = TxgT::from(42);
        let mut cl0 = mock_cluster(0, 1000, 0);
        cl0.expect_advance_transaction()
            .once()
            .with(eq(txg))
            .return_once(move |_| Box::pin(future::ok(())));
        let mut cl1 = mock_cluster(0, 1000, 0);
        cl1.expect_advance_transaction()
            .once()
            .with(eq(txg))
            .return_once(move |_| Box::pin(future::ok(())));
        let clusters = vec![cl0, cl1];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        pool.advance_transaction(txg).await.unwrap();
    }

    /// Two clusters, one full and one empty.  Choose the empty one
    #[test]
    fn choose_cluster_empty() {
        let clusters = vec![
            mock_cluster(0, 1000, 0),
            mock_cluster(1000, 1000, 0)
        ];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        assert_eq!(pool.choose_cluster(), 0);

        // Try the reverse, too
        let clusters = vec![
            mock_cluster(1000, 1000, 0),
            mock_cluster(0, 1000, 0)
        ];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        assert_eq!(pool.choose_cluster(), 1);
    }

    /// Two clusters, one busy and one idle.  Choose the idle one
    #[test]
    fn choose_cluster_queue_depth() {
        let clusters = vec![mock_cluster(0, 1000, 0), mock_cluster(0, 1000, 0)];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        pool.stats.queue_depth[0].store(0, Ordering::Relaxed);
        pool.stats.queue_depth[1].store(10, Ordering::Relaxed);
        assert_eq!(pool.choose_cluster(), 0);

        // Try the reverse, too
        pool.stats.queue_depth[0].store(10, Ordering::Relaxed);
        pool.stats.queue_depth[1].store(0, Ordering::Relaxed);
        assert_eq!(pool.choose_cluster(), 1);
    }

    /// Two clusters, one nearly full and idle, the other busy but not very
    /// full.  Choose the not very full one.
    #[test]
    fn choose_cluster_nearly_full() {
        let clusters = vec![
            mock_cluster(960, 1000, 10),
            mock_cluster(50, 1000, 10)
        ];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        pool.stats.queue_depth[0].store(0, Ordering::Relaxed);
        pool.stats.queue_depth[1].store(10, Ordering::Relaxed);
        assert_eq!(pool.choose_cluster(), 1);

        // Try the reverse, too
        let clusters = vec![
            mock_cluster(50, 1000, 10),
            mock_cluster(960, 1000, 10)
        ];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        pool.stats.queue_depth[0].store(10, Ordering::Relaxed);
        pool.stats.queue_depth[1].store(0, Ordering::Relaxed);
        assert_eq!(pool.choose_cluster(), 0);
    }

    #[test]
    fn find_closed_zone() {
        let cluster = || {
            let mut c = mock_cluster(0, 32_768_000, 0);
            c.expect_optimum_queue_depth().return_const(10u32);
            c.expect_find_closed_zone()
                .with(eq(0))
                .return_const(Some(cluster::ClosedZone {
                    zid: 1,
                    start: 10,
                    freed_blocks: 5,
                    total_blocks: 10,
                    txgs: TxgT::from(0)..TxgT::from(1)
                }));
            c.expect_find_closed_zone()
                .with(eq(2))
                .return_const(Some(cluster::ClosedZone {
                    zid: 3,
                    start: 30,
                    freed_blocks: 6,
                    total_blocks: 10,
                    txgs: TxgT::from(2)..TxgT::from(3)
                }));
            c.expect_find_closed_zone()
                .with(eq(4))
                .return_const(None);
            c
        };
        let clusters = vec![ cluster(), cluster() ];
        let pool =  Pool::new("foo".to_string(), Uuid::new_v4(), clusters);

        let r0 = pool.find_closed_zone(0, 0);
        assert_eq!(r0.0, Some(ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1), zid: 1}));

        let (clust, zid) = r0.1.unwrap();
        let r1 = pool.find_closed_zone(clust, zid);
        assert_eq!(r1.0, Some(ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3), zid: 3}));

        let (clust, zid) = r1.1.unwrap();
        let r2 = pool.find_closed_zone(clust, zid);
        assert!(r2.0.is_none());

        let (clust, zid) = r2.1.unwrap();
        let r3 = pool.find_closed_zone(clust, zid);
        assert_eq!(r3.0, Some(ClosedZone{pba: PBA::new(1, 10), freed_blocks: 5,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1), zid: 1}));

        let (clust, zid) = r3.1.unwrap();
        let r4 = pool.find_closed_zone(clust, zid);
        assert_eq!(r4.0, Some(ClosedZone{pba: PBA::new(1, 30), freed_blocks: 6,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3), zid: 3}));

        let (clust, zid) = r4.1.unwrap();
        let r5 = pool.find_closed_zone(clust, zid);
        assert_eq!(r5, (None, None));
    }

    #[tokio::test]
    async fn free() {
        let c0 = mock_cluster(100, 32_768_000, 50);
        let mut c1 = mock_cluster(100, 32_768_000, 50);
        c1.expect_free()
            .with(eq(12345), eq(16))
            .once()
            .return_once(|_, _| Box::pin(future::ok(())));

        let clusters = vec![ c0, c1 ];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);

        assert!(pool.free(PBA::new(1, 12345), 16).await.is_ok());
        // Freeing bytes should not decrease allocated.  Only erasing should.
        assert_eq!(pool.used(), 84);
    }

    // optimum_queue_depth is always a smallish integer, so exact equality
    // testing is ok.
    #[allow(clippy::float_cmp)]
    #[test]
    fn new() {
        let clusters = vec![
            mock_cluster(500, 1000, 400),
            mock_cluster(500, 1000, 400),
        ];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);
        assert_eq!(pool.stats.optimum_queue_depth[0], 10.0);
        assert_eq!(pool.stats.optimum_queue_depth[1], 10.0);
        assert_eq!(pool.size(), 2000);
        assert_eq!(pool.used(), 800);
    }

    #[tokio::test]
    async fn read() {
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_read()
            .with(always(), eq(10))
            .once()
            .returning(|mut iovec, _lba| {
                iovec.copy_from_slice(&vec![99; 4096][..]);
                Box::pin(future::ok(()))
            });

        let clusters = vec![ cluster, ];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let dbm0 = dbs.try_mut().unwrap();
        let pba = PBA::new(0, 10);
        let result = pool.read(dbm0, pba).await;
        assert!(result.is_ok());
        let db0 = dbs.try_const().unwrap();
        assert_eq!(&db0[..], &vec![99u8; 4096][..]);
    }

    #[tokio::test]
    async fn read_error() {
        let e = Error::EIO;
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_read()
            .once()
            .return_once(move |_, _| Box::pin(future::err(e)));

        let clusters = vec![cluster];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let dbm0 = dbs.try_mut().unwrap();
        let pba = PBA::new(0, 10);
        let result = pool.read(dbm0, pba).await;
        assert_eq!(result.unwrap_err(), e);
    }

    #[tokio::test]
    async fn sync_all() {
        let cluster = || {
            let mut c = mock_cluster(0, 32_768_000, 0);
            c.expect_sync_all()
                .once()
                .return_once(|| Box::pin(future::ok(())));
            c
        };

        let clusters = vec![cluster(), cluster()];
        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), clusters);

        assert!(pool.sync_all().await.is_ok());
    }

    #[tokio::test]
    async fn write() {
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_write()
            .withf(|buf, txg| {
                buf.len() == BYTES_PER_LBA && *txg == TxgT::from(42)
            }).once()
            .return_once(|_, _| Ok((0, Box::pin(future::ok(())))));

        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), vec![cluster]);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result =  pool.write(db0, TxgT::from(42)).await;
        assert_eq!(result.unwrap(), PBA::new(0, 0));
        assert_eq!(pool.used(), 1);
    }

    #[tokio::test]
    async fn write_async_error() {
        let e = Error::EIO;
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_write()
            .once()
            .return_once(move |_, _| Ok((0, Box::pin(future::err(e)))));

        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), vec![cluster]);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result =  pool.write(db0, TxgT::from(42)).await;
        assert_eq!(result.unwrap_err(), e);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn write_sync_error() {
        let e = Error::ENOSPC;
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_write()
            .once()
            .return_once(move |_, _| Err(e));

        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), vec![cluster]);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result =  pool.write(db0, TxgT::from(42)).await;
        assert_eq!(result.unwrap_err(), e);
        assert_eq!(pool.used(), 0);
    }

    // Make sure allocated space accounting is symmetric
    #[tokio::test]
    async fn write_and_free() {
        let mut cluster = mock_cluster(0, 32_768_000, 0);
        cluster.expect_write()
            .once()
            .return_once(|_, _| Ok((0, Box::pin(future::ok(())))));
        cluster.expect_free()
            .once()
            .return_once(|_, _| Box::pin(future::ok(())));

        let pool = Pool::new("foo".to_string(), Uuid::new_v4(), vec![cluster]);

        let dbs = DivBufShared::from(vec![0u8; 1024]);
        let db0 = dbs.try_const().unwrap();
        let drp =  pool.write(db0, TxgT::from(42)).await.unwrap();
         pool.free(drp, 1).await.unwrap();
        assert_eq!(pool.used(), 0);
    }
}
}
// LCOV_EXCL_STOP
