// vim: tw=80

use crate::{
    common::{*, label::*}
};
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    StreamExt,
    TryStreamExt,
    future,
    channel::{
        mpsc,
        oneshot::{self, Canceled}
    },
    task::{Context, Poll}
};
#[cfg(test)] use mockall::automock;
use std::{
    ops::Range,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc
    }
};
#[cfg(not(test))] use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};
use std::collections::BTreeMap;

#[cfg(test)]
use crate::common::cluster::MockCluster as Cluster;
#[cfg(not(test))]
use crate::common::cluster::Cluster;

/// Communication type used between `ClusterProxy` and `ClusterServer`
#[derive(Debug)]
enum Rpc {
    Allocated(oneshot::Sender<LbaT>),
    FindClosedZone(ZoneT, oneshot::Sender<Option<cluster::ClosedZone>>),
    Flush(u32, oneshot::Sender<Result<(), Error>>),
    Free(LbaT, LbaT, oneshot::Sender<Result<(), Error>>),
    OptimumQueueDepth(oneshot::Sender<u32>),
    Read(IoVecMut, LbaT, oneshot::Sender<Result<(), Error>>),
    Shutdown(),
    Size(oneshot::Sender<LbaT>),
    SyncAll(oneshot::Sender<Result<(), Error>>),
    Write(IoVec, TxgT, oneshot::Sender<Result<LbaT, Error>>),
    WriteLabel(LabelWriter, oneshot::Sender<Result<(), Error>>),
    #[cfg(debug_assertions)]
    AssertCleanZone(ZoneT, TxgT),
}

/// RPC server for `Cluster` objects
///
/// As `Cluster` is neither `Send` nor `Sync` it cannot be directly accessed
/// from other threads.  The `ClusterServer` fixes that.  Bound to a single
/// thread, it owns a `Cluster` and serves RPC requests from its own and other
/// threads.
///
/// TODO: Since the upgrade to Tokio 0.3, all Futures beneath Cluster are now
/// Send + Sync.  So it would be possible to make Cluster Send + Sync, too.
/// Consider eliminating ClusterServer.
struct ClusterServer {
    cluster: Cluster
}

impl ClusterServer {
    fn new(cluster: Cluster) -> Self {
        ClusterServer{cluster}
    }

    /// Start the `ClusterServer` in the background, in the current thread
    fn run(cs: Arc<ClusterServer>, rx: mpsc::UnboundedReceiver<Rpc>) {
        let fut = async move {
            // In Futures 0.2, use try_for_each_concurrent instead
            rx.map(|r| Ok(r)).try_for_each(move |rpc| cs.dispatch(rpc))
            // If we get here, the ClusterProxy was dropped
        };
        tokio::spawn(fut);
    }

    fn dispatch(&self, rpc: Rpc)
        -> Pin<Box<dyn Future<Output=Result<(), ()>> + Send>>
    {
        match rpc {
            #[cfg(debug_assertions)]
            Rpc::AssertCleanZone(zone, txg) => {
                self.cluster.assert_clean_zone(zone, txg);
                Box::pin(future::ok(()))
            },
            Rpc::Allocated(tx) => {
                tx.send(self.cluster.allocated()).unwrap();
                Box::pin(future::ok(()))
            },
            Rpc::FindClosedZone(zid, tx) => {
                tx.send(self.cluster.find_closed_zone(zid)).unwrap();
                Box::pin(future::ok(()))
            },
            Rpc::Flush(idx, tx) => {
                let fut = self.cluster.flush(idx)
                .map(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::pin(fut)
            },
            Rpc::Free(lba, length, tx) => {
                let fut = self.cluster.free(lba, length)
                .map(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::pin(fut)
            }
            Rpc::OptimumQueueDepth(tx) => {
                tx.send(self.cluster.optimum_queue_depth()).unwrap();
                Box::pin(future::ok(()))
            },
            Rpc::Read(buf, lba, tx) => {
                let fut = self.cluster.read(buf, lba)
                .map(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::pin(fut)
            },
            Rpc::Shutdown() => {
                // Returning an error will cause the service loop to shut down
                Box::pin(future::err::<(), ()>(()))
            },
            Rpc::Size(tx) => {
                tx.send(self.cluster.size()).unwrap();
                Box::pin(future::ok(()))
            },
            Rpc::SyncAll(tx) => {
                let fut = self.cluster.sync_all()
                .map(|r: Result<(), Error>| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::pin(fut)
            },
            Rpc::Write(buf, txg, tx) => {
                match self.cluster.write(buf, txg) {
                    Ok((lba, wfut)) => {
                        let txfut = wfut
                            .map(move |r| {
                                match r {
                                    Ok(_) => tx.send(Ok(lba)),
                                    Err(e) => tx.send(Err(e))
                                }.unwrap();
                                Ok(())
                            });
                        Box::pin(txfut)
                    },
                    Err(e) => {
                        tx.send(Err(e)).unwrap();
                        Box::pin(future::ok(()))
                    }
                }
            },
            Rpc::WriteLabel(label_writer, tx) => {
                let fut = self.cluster.write_label(label_writer)
                .map(|r: Result<(), Error>| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::pin(fut)
            },
        }
    }
}

/// Return type of `ClusterProxy::write`
struct ClusterProxyWrite {
    rx: oneshot::Receiver<Result<LbaT, Error>>
}

impl Future for ClusterProxyWrite
{
    type Output = Result<LbaT, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // TODO: can this be replaced by self.rx.poll(cx) ?
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => {
                match r {
                    Ok(lba) => Poll::Ready(Ok(lba)),
                    Err(e) => Poll::Ready(Err(e))
                }
            },
            Poll::Ready(Err(Canceled)) => Poll::Ready(Err(Error::EPIPE)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// `Send`able, `Clone`able handle to a `ClusterServer`
#[derive(Debug)]
pub struct ClusterProxy {
    server: mpsc::UnboundedSender<Rpc>,
    // Copy of the underlying Cluster's uuid
    uuid: Uuid
}

impl ClusterProxy {
    fn allocated(&self) -> impl Future<Output = Result<LbaT, Error>> {
        let (tx, rx) = oneshot::channel::<LbaT>();
        let rpc = Rpc::Allocated(tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    #[cfg(debug_assertions)]
    fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT) {
        let rpc = Rpc::AssertCleanZone(zone, txg);
        self.server.unbounded_send(rpc).unwrap();
    }

    fn flush(&self, idx: u32) -> impl Future<Output=Result<(), Error>> + Send {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Flush(idx, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn free(&self, lba: LbaT, length: LbaT)
        -> impl Future<Output=Result<(), Error>>
    {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Free(lba, length, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn find_closed_zone(&self, zid: ZoneT)
        -> impl Future<Output=Result<Option<cluster::ClosedZone>, Error>>
    {
        let (tx, rx) = oneshot::channel::<Option<cluster::ClosedZone>>();
        let rpc = Rpc::FindClosedZone(zid, tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    fn optimum_queue_depth(&self) -> impl Future<Output=Result<u32, Error>> {
        let (tx, rx) = oneshot::channel::<u32>();
        let rpc = Rpc::OptimumQueueDepth(tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    /// Create a new ClusterServer/ClusterProxy pair, start the server, and
    /// return the proxy
    pub fn new(cluster: Cluster) -> Self {
        let (tx, rx) = mpsc::unbounded();
        let uuid = cluster.uuid();
        let cs = Arc::new(ClusterServer::new(cluster));
        ClusterServer::run(cs, rx);
        ClusterProxy{server: tx, uuid}
    }

    fn read(&self, buf: IoVecMut, lba: LbaT)
        -> impl Future<Output=Result<(), Error>>
    {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Read(buf, lba, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    /// Helper that collapses a Future<Output=Result<_, _>>.
    fn rx_unit_result(rx: oneshot::Receiver<Result<(), Error>>)
        -> impl Future<Output=Result<(), Error>>
    {
        rx.map_err(|_| Error::EPIPE)
            .and_then(|result| future::ready(result))
    }

    fn shutdown(&self) {
        let rpc = Rpc::Shutdown();
        // Ignore errors.  An error indicates that the ClusterServer is already
        // shut down.
        let _ = self.server.unbounded_send(rpc);
    }

    fn size(&self) -> impl Future<Output=Result<LbaT, Error>> {
        let (tx, rx) = oneshot::channel::<LbaT>();
        let rpc = Rpc::Size(tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    fn sync_all(&self) -> impl Future<Output=Result<(), Error>> {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::SyncAll(tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn write(&self, buf: IoVec, txg: TxgT) -> ClusterProxyWrite
    {
        let (tx, rx) = oneshot::channel::<Result<LbaT, Error>>();
        let rpc = Rpc::Write(buf, txg, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxyWrite{rx}
    }

    fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Output=Result<(), Error>>
    {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::WriteLabel(labeller, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }
}

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

struct Stats {
    /// The queue depth of each `Cluster`, including both commands that have
    /// been sent to the disks, and commands that are pending in `VdevBlock`
    queue_depth: Vec<AtomicU32>,

    /// "Best" number of commands to queue to each VdevRaid
    optimum_queue_depth: Vec<f64>,

    /// The total size of each `Cluster`
    size: Vec<LbaT>,

    /// The total amount of allocated space in each `Cluster`, excluding
    /// space that has already been freed but not erased.
    allocated_space: Vec<AtomicU64>,
}

impl Stats {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    fn allocated(&self) -> LbaT {
        self.allocated_space.iter()
            .map(|alloc| alloc.load(Ordering::Relaxed))
            .sum()
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
        // on every write.  A better implementation would perform the full
        // calculation only occasionally, to update coefficients, and perform a
        // quick calculation on each write.
        (0..self.size.len()).map(|i| {
            let alloc = self.allocated_space[i].load(Ordering::Relaxed) as f64;
            let space_util = alloc / (self.size[i] as f64);
            let qdepth = self.queue_depth[i].load(Ordering::Relaxed) as f64;
            let queue_fraction = qdepth / self.optimum_queue_depth[i];
            let q_coeff = if 0.95 > space_util {0.95 - space_util} else {0.0};
            let weight = q_coeff * queue_fraction + space_util;
            (i, weight)
        })
        .min_by(|&(_, x), &(_, y)| x.partial_cmp(&y).unwrap())
        .map(|(i, _)| i)
        .unwrap() as ClusterT
    }

    /// The approximate usable size of the Pool
    fn size(&self) -> LbaT {
        self.size.iter().sum()
    }
}

/// Return type of `Pool::write`
struct Write {
    cpfut: ClusterProxyWrite,
    stats: Arc<Stats>,
    cidx: usize,
    space: LbaT,
    cluster: ClusterT
}

impl Write {
    fn new(cpfut: ClusterProxyWrite, stats: Arc<Stats>,
           cidx: usize, space: LbaT, cluster: ClusterT) -> Self
    {
        Write{cpfut, stats, cidx, space, cluster}
    }
}

impl Future for Write
{
    type Output = Result<PBA, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.cpfut).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(lba)) => {
                self.stats.queue_depth[self.cidx]
                    .fetch_sub(1, Ordering::Relaxed);
                self.stats.allocated_space[self.cidx]
                    .fetch_add(self.space, Ordering::Relaxed);
                let pba = PBA::new(self.cluster, lba);
                Poll::Ready(Ok(pba))
            },
            Poll::Ready(Err(e)) => {
                self.stats.queue_depth[self.cidx]
                    .fetch_sub(1, Ordering::Relaxed);
                Poll::Ready(Err(e))
            }
        }
    }
}


/// An BFFFS storage pool
pub struct Pool {
    clusters: Vec<ClusterProxy>,

    /// Human-readable pool name.  Must be unique on any one system.
    name: String,

    stats: Arc<Stats>,

    uuid: Uuid,
}

#[cfg_attr(test, automock)]
impl Pool {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.stats.allocated()
    }

    /// Assert that the given zone was clean as of the given transaction
    #[cfg(debug_assertions)]
    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) {
        self.clusters[cluster as usize].assert_clean_zone(zone, txg)
    }

    /// Create a new `Cluster` from unused files or devices.
    ///
    /// Must be called from within the context of a Tokio Runtime.  Once
    /// created, the `Cluster` will be permanently bound to the thread of its
    /// creation.
    ///
    /// * `chunksize`:          RAID chunksize in LBAs, if specified.  This is
    ///                         the largest amount of data that will be
    ///                         read/written to a single device before the
    ///                         `Locator` switches to the next device.
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than
    ///                         or equal to the size of `paths`.
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `paths`:              Slice of pathnames of files and/or devices
    #[cfg(not(test))]
    pub fn create_cluster<P>(chunksize: Option<NonZeroU64>,
                             disks_per_stripe: i16,
                             lbas_per_zone: Option<NonZeroU64>,
                             redundancy: i16,
                             paths: &[P])
        -> impl Future<Output=Result<ClusterProxy, ()>>
        where P: AsRef<Path> + Sync
    {
        let (tx, rx) = oneshot::channel();
        // tokio::spawn needs 'static futures; we must copy the Paths
        let owned_paths = paths.iter()
            .map(|p| p.as_ref().to_owned())
            .collect::<Vec<PathBuf>>();
        tokio::spawn(async move {
            let c = Cluster::create(chunksize, disks_per_stripe,
                    lbas_per_zone, redundancy, owned_paths);
            tx.send(ClusterProxy::new(c)).unwrap();
        });
        rx.map_err(|_| panic!("Closed Runtime while creating Cluster?"))
    }

    /// Create a new `Pool` from some freshly created `Cluster`s.
    ///
    /// Must be called from within the context of a Tokio Runtime.
    pub fn create(name: String, clusters: Vec<ClusterProxy>)
        -> impl Future<Output=Result<Self, Error>>
    {
        Pool::new(name, Uuid::new_v4(), clusters)
    }

    pub fn flush(&self, idx: u32) -> impl Future<Output=Result<(), Error>> + Send {
        future::try_join_all(
            self.clusters.iter()
            .map(|cp| cp.flush(idx))
            .collect::<Vec<_>>()
        ).map_ok(drop)
    }

    /// Mark `length` LBAs beginning at PBA `pba` as unused, but do not delete
    /// them from the underlying storage.
    ///
    /// Freeing data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, BFFFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Pool.
    pub fn free(&self, pba: PBA, length: LbaT)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        let idx = pba.cluster as usize;
        self.stats.allocated_space[idx].fetch_sub(length, Ordering::Relaxed);
        self.clusters[pba.cluster as usize].free(pba.lba, length)
    }

    /// Construct a new `Pool` from some already constructed
    /// [`Cluster`](struct.Cluster.html)s.
    ///
    /// Must be called from within the context of a Tokio Runtime.
    #[allow(clippy::new_ret_no_self)]
    fn new(name: String, uuid: Uuid, clusters: Vec<ClusterProxy>)
        -> impl Future<Output=Result<Self, Error>>
    {
        let size_fut = future::try_join_all(clusters.iter()
            .map(ClusterProxy::size)
            .collect::<Vec<_>>()
        );
        let allocated_fut = future::try_join_all(clusters.iter()
            .map(|cluster| cluster.allocated()
                .map_ok(AtomicU64::new)
            ).collect::<Vec<_>>()
        );
        let oqd_fut = future::try_join_all(clusters.iter()
            .map(|cluster| cluster.optimum_queue_depth()
                .map_ok(f64::from)
            ).collect::<Vec<_>>()
        );
        let queue_depth: Vec<_> = clusters.iter()
            .map(|_| AtomicU32::new(0))
            .collect();
        future::try_join3(size_fut, allocated_fut, oqd_fut)
        .map_ok(move |(size, allocated_space, optimum_queue_depth)| {
            let stats = Arc::new(Stats{
                allocated_space,
                optimum_queue_depth,
                queue_depth,
                size
            });
            Pool{name, clusters, stats, uuid}
        })
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
        -> impl Future<Output=Result<(Option<ClosedZone>,
                                      Option<(ClusterT, ZoneT)>),
                       Error>> + Send
    {
        let nclusters = self.clusters.len() as ClusterT;
        self.clusters[clust as usize].find_closed_zone(zid)
            .map_ok(move |r| {
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
            })
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
    /// * `combined`:   An array of pairs of `ClusterProxy`s and their
    ///                 associated `LabelReader`.  The labels of each will be verified.
    pub fn open(uuid: Option<Uuid>, combined: Vec<(ClusterProxy, LabelReader)>)
        -> impl Future<Output=Result<(Self, LabelReader), Error>>
    {
        let mut label_pair = None;
        let mut all_clusters = combined.into_iter()
            .map(|(cluster_proxy, mut label_reader)| {
            let label: Label = label_reader.deserialize().unwrap();
            if let Some(u) = uuid {
                assert_eq!(u, label.uuid, "Opening cluster from wrong pool");
            }
            if label_pair.is_none() {
                label_pair = Some((label, label_reader));
            }
            (cluster_proxy.uuid(), cluster_proxy)
        }).collect::<BTreeMap<Uuid, ClusterProxy>>();
        let (label, label_reader) = label_pair.unwrap();
        assert_eq!(all_clusters.len(), label.children.len(),
            "Missing clusters");
        let children = label.children.iter().map(|uuid| {
            all_clusters.remove(&uuid).unwrap()
        }).collect::<Vec<_>>();
        Pool::new(label.name, label.uuid, children)
            .map_ok(|pool| (pool, label_reader))
    }

    /// Asynchronously read from the pool
    pub fn read(&self, buf: IoVecMut, pba: PBA)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        let cidx = pba.cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let stats2 = self.stats.clone();
        self.clusters[pba.cluster as usize].read(buf, pba.lba)
            .map(move |r| {
                stats2.queue_depth[cidx].fetch_sub(1, Ordering::Relaxed);
                r
            })
    }

    /// Shutdown all background tasks.
    pub fn shutdown(&self) {
        for c in self.clusters.iter() {
            c.shutdown();
        }
    }

    /// Return approximately the Pool's usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.stats.size()
    }

    /// Sync the `Pool`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&self) -> impl Future<Output=Result<(), Error>> + Send {
        future::try_join_all(
            self.clusters.iter()
            .map(ClusterProxy::sync_all)
            .collect::<Vec<_>>()
        ).map_ok(drop)
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
    pub fn write(&self, buf: IoVec, txg: TxgT)
        -> impl Future<Output=Result<PBA, Error>> + Send
    {
        let cluster = self.stats.choose_cluster();
        let cidx = cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let space = div_roundup(buf.len(), BYTES_PER_LBA) as LbaT;
        let stats2 = self.stats.clone();
        let cpfut = self.clusters[cidx].write(buf, txg);
        Write::new(cpfut, stats2, cidx, space, cluster)
    }

    /// Asynchronously write this `Pool`'s label to all component devices
    pub fn write_label(&self, mut labeller: LabelWriter)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        let cluster_uuids = self.clusters.iter().map(ClusterProxy::uuid)
            .collect::<Vec<_>>();
        let label = Label {
            name: self.name.clone(),
            uuid: self.uuid,
            children: cluster_uuids,
        };
        labeller.serialize(&label).unwrap();
        let futs = self.clusters.iter().map(|cluster| {
            cluster.write_label(labeller.clone())
        }).collect::<Vec<_>>();
        future::try_join_all(futs).map_ok(drop)
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
        format!("{:?}", label);
    }
}

mod pool {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::{IntoFuture, future};
    use mockall::predicate::*;
    use pretty_assertions::assert_eq;
    use tokio::runtime::current_thread;

    // pet kcov
    #[test]
    fn debug() {
        let mut c = Cluster::default();
        c.expect_uuid().return_const(Uuid::new_v4());
        let mut rt = current_thread::Runtime::new().unwrap();
        rt.block_on(future::lazy(|| {
            let cluster_proxy = ClusterProxy::new(c);
            format!("{:?}", cluster_proxy);
            future::ok::<(), ()>(())
        })).unwrap();
    }

    #[test]
    fn find_closed_zone() {
        let cluster = || {
            let mut c = Cluster::default();
            c.expect_allocated().return_const(0u64);
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
            c.expect_size().return_const(32_768_000u64);
            c.expect_uuid().return_const(Uuid::new_v4());
            c
        };
        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(cluster()),
                ClusterProxy::new(cluster())
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        let r0 = rt.block_on(pool.find_closed_zone(0, 0)).unwrap();
        assert_eq!(r0.0, Some(ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1), zid: 1}));

        let (clust, zid) = r0.1.unwrap();
        let r1 = rt.block_on(pool.find_closed_zone(clust, zid)).unwrap();
        assert_eq!(r1.0, Some(ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3), zid: 3}));

        let (clust, zid) = r1.1.unwrap();
        let r2 = rt.block_on(pool.find_closed_zone(clust, zid)).unwrap();
        assert!(r2.0.is_none());

        let (clust, zid) = r2.1.unwrap();
        let r3 = rt.block_on(pool.find_closed_zone(clust, zid)).unwrap();
        assert_eq!(r3.0, Some(ClosedZone{pba: PBA::new(1, 10), freed_blocks: 5,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1), zid: 1}));

        let (clust, zid) = r3.1.unwrap();
        let r4 = rt.block_on(pool.find_closed_zone(clust, zid)).unwrap();
        assert_eq!(r4.0, Some(ClosedZone{pba: PBA::new(1, 30), freed_blocks: 6,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3), zid: 3}));

        let (clust, zid) = r4.1.unwrap();
        let r5 = rt.block_on(pool.find_closed_zone(clust, zid)).unwrap();
        assert_eq!(r5, (None, None));
    }

    #[test]
    fn free() {
        let cluster = || {
            let mut c = Cluster::default();
            c.expect_allocated().return_const(0u64);
            c.expect_optimum_queue_depth().return_const(10u32);
            c.expect_size().return_const(32_768_000u64);
            c.expect_uuid().return_const(Uuid::new_v4());
            c
        };
        let c0 = cluster();
        let mut c1 = cluster();
        c1.expect_free()
            .with(eq(12345), eq(16))
            .once()
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(c0),
                ClusterProxy::new(c1)
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        assert!(rt.block_on(pool.free(PBA::new(1, 12345), 16)).is_ok());
    }

    // optimum_queue_depth is always a smallish integer, so exact equality
    // testing is ok.
    #[allow(clippy::float_cmp)]
    #[test]
    fn new() {
        let cluster = || {
            let mut c = Cluster::default();
            c.expect_optimum_queue_depth().return_const(10u32);
            c.expect_allocated().return_const(500u64);
            c.expect_size().return_const(1000u64);
            c.expect_uuid().return_const(Uuid::new_v4());
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(cluster()),
                ClusterProxy::new(cluster())
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();
        assert_eq!(pool.stats.allocated_space[0].load(Ordering::Relaxed), 500);
        assert_eq!(pool.stats.allocated_space[1].load(Ordering::Relaxed), 500);
        assert_eq!(pool.stats.optimum_queue_depth[0], 10.0);
        assert_eq!(pool.stats.optimum_queue_depth[1], 10.0);
        assert_eq!(pool.allocated(), 1000);
        assert_eq!(pool.size(), 2000);
    }

    #[test]
    fn read() {
        let mut cluster = Cluster::default();
        cluster.expect_allocated().return_const(0u64);
        cluster.expect_optimum_queue_depth().return_const(10u32);
        cluster.expect_size().return_const(32_768_000u64);
        cluster.expect_uuid().return_const(Uuid::new_v4());
        cluster.expect_read()
            .with(always(), eq(10))
            .once()
            .returning(|mut iovec, _lba| {
                iovec.copy_from_slice(&vec![99; 4096][..]);
                Box::new( future::ok::<(), Error>(()))
            });

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(move || {
            let clusters = vec![
                ClusterProxy::new(cluster),
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let dbm0 = dbs.try_mut().unwrap();
        let pba = PBA::new(0, 10);
        let result = rt.block_on(pool.read(dbm0, pba));
        assert!(result.is_ok());
        let db0 = dbs.try_const().unwrap();
        assert_eq!(&db0[..], &vec![99u8; 4096][..]);
    }

    #[test]
    fn read_error() {
        let e = Error::EIO;
        let mut cluster = Cluster::default();
        cluster.expect_allocated().return_const(0u64);
        cluster.expect_optimum_queue_depth().return_const(10u32);
        cluster.expect_size().return_const(32_768_000u64);
        cluster.expect_uuid().return_const(Uuid::new_v4());
        cluster.expect_read()
            .once()
            .return_once(move |_, _| Box::new(Err(e).into_future()));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(move || {
            let clusters = vec![
                ClusterProxy::new(cluster),
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let dbm0 = dbs.try_mut().unwrap();
        let pba = PBA::new(0, 10);
        let result = rt.block_on(pool.read(dbm0, pba));
        assert_eq!(result.unwrap_err(), e);
    }

    #[test]
    fn sync_all() {
        let cluster = || {
            let mut c = Cluster::default();
            c.expect_allocated().return_const(0u64);
            c.expect_optimum_queue_depth().return_const(10u32);
            c.expect_size().return_const(32_768_000u64);
            c.expect_uuid().return_const(Uuid::new_v4());
            c.expect_sync_all()
                .once()
                .return_once(|| Box::new(future::ok::<(), Error>(())));
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(cluster()),
                ClusterProxy::new(cluster())
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        assert!(rt.block_on(pool.sync_all()).is_ok());
    }

    #[test]
    fn write() {
        let mut cluster = Cluster::default();
            cluster.expect_allocated().return_const(0u64);
            cluster.expect_optimum_queue_depth().return_const(10u32);
            cluster.expect_size().return_const(32_768_000u64);
            cluster.expect_uuid().return_const(Uuid::new_v4());
            cluster.expect_write()
                .withf(|buf, txg| {
                    buf.len() == BYTES_PER_LBA && *txg == TxgT::from(42)
                }).once()
                .return_once(|_, _| Ok((0, Box::new(future::ok::<(), Error>(())))));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(cluster)])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap(), PBA::new(0, 0));
    }

    #[test]
    fn write_async_error() {
        let e = Error::EIO;
        let mut cluster = Cluster::default();
            cluster.expect_allocated().return_const(0u64);
            cluster.expect_optimum_queue_depth().return_const(10u32);
            cluster.expect_size().return_const(32_768_000u64);
            cluster.expect_uuid().return_const(Uuid::new_v4());
            cluster.expect_write()
                .once()
                .return_once(move |_, _| Ok((0, Box::new(Err(e).into_future()))));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(cluster)])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap_err(), e);
    }

    #[test]
    fn write_sync_error() {
        let e = Error::ENOSPC;
        let mut cluster = Cluster::default();
            cluster.expect_allocated().return_const(0u64);
            cluster.expect_optimum_queue_depth().return_const(10u32);
            cluster.expect_size().return_const(32_768_000u64);
            cluster.expect_uuid().return_const(Uuid::new_v4());
            cluster.expect_write()
                .once()
                .return_once(move |_, _| Err(e));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(cluster)])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap_err(), e);
    }

    // Make sure allocated space accounting is symmetric
    #[test]
    fn write_and_free() {
        let mut cluster = Cluster::default();
        cluster.expect_allocated().return_const(0u64);
        cluster.expect_optimum_queue_depth().return_const(10u32);
        cluster.expect_size().return_const(32_768_000u64);
        cluster.expect_uuid().return_const(Uuid::new_v4());
        cluster.expect_write()
            .once()
            .return_once(|_, _| Ok((0, Box::new(future::ok::<(), Error>(())))));
        cluster.expect_free()
            .once()
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(cluster)])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 1024]);
        let db0 = dbs.try_const().unwrap();
        let drp = rt.block_on( pool.write(db0, TxgT::from(42))).unwrap();
        assert!(pool.stats.allocated_space[0].load(Ordering::Relaxed) > 0);
        rt.block_on( pool.free(drp, 1)).unwrap();
        assert_eq!(pool.stats.allocated_space[0].load(Ordering::Relaxed), 0);
    }
}

mod rpc {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let dbs = DivBufShared::from(Vec::new());
        let lw = LabelWriter::new(0);
        format!("{:?}", Rpc::Allocated(oneshot::channel().0));
        format!("{:?}", Rpc::FindClosedZone(0, oneshot::channel().0));
        format!("{:?}", Rpc::Flush(0, oneshot::channel().0));
        format!("{:?}", Rpc::Free(0, 0, oneshot::channel().0));
        format!("{:?}", Rpc::OptimumQueueDepth(oneshot::channel().0));
        format!("{:?}", Rpc::Read(dbs.try_mut().unwrap(), 0,
            oneshot::channel().0));
        format!("{:?}", Rpc::Size(oneshot::channel().0));
        format!("{:?}", Rpc::SyncAll(oneshot::channel().0));
        format!("{:?}", Rpc::Write(dbs.try_const().unwrap(), TxgT(0),
            oneshot::channel().0));
        format!("{:?}", Rpc::WriteLabel(lw, oneshot::channel().0));
        #[cfg(debug_assertions)]
        format!("{:?}", Rpc::AssertCleanZone(0, TxgT(0)));
    }
}

mod stats {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn allocated() {
        let stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![AtomicU32::new(0), AtomicU32::new(0)],
            size: vec![1000, 1000],
            allocated_space: vec![AtomicU64::new(10), AtomicU64::new(900)]
        };
        assert_eq!(stats.allocated(), 910);
    }

    #[test]
    fn choose_cluster_empty() {
        // Two clusters, one full and one empty.  Choose the empty one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![AtomicU32::new(0), AtomicU32::new(0)],
            size: vec![1000, 1000],
            allocated_space: vec![AtomicU64::new(0), AtomicU64::new(1000)]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.allocated_space = vec![AtomicU64::new(1000), AtomicU64::new(0)];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_queue_depth() {
        // Two clusters, one busy and one idle.  Choose the idle one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![AtomicU32::new(0), AtomicU32::new(10)],
            size: vec![1000, 1000],
            allocated_space: vec![AtomicU64::new(0), AtomicU64::new(0)]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.queue_depth = vec![AtomicU32::new(10), AtomicU32::new(0)];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_nearly_full() {
        // Two clusters, one nearly full and idle, the other busy but not very
        // full.  Choose the not very full one.
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![AtomicU32::new(0), AtomicU32::new(10)],
            size: vec![1000, 1000],
            allocated_space: vec![AtomicU64::new(960), AtomicU64::new(50)]
        };
        assert_eq!(stats.choose_cluster(), 1);

        // Try the reverse, too
        stats.queue_depth = vec![AtomicU32::new(10), AtomicU32::new(0)];
        stats.allocated_space = vec![AtomicU64::new(50), AtomicU64::new(960)];
        assert_eq!(stats.choose_cluster(), 0);
    }
}
}
// LCOV_EXCL_STOP
