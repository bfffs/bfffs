// vim: tw=80

use atomic::{Atomic, Ordering};
use crate::common::{*, label::*};
#[cfg(not(test))] use crate::common::cluster;
use futures::{
    Future,
    IntoFuture,
    Stream,
    future,
    sync::{mpsc, oneshot}
};
use std::{
    ops::Range,
    rc::Rc,
    sync::Arc
};
use std::collections::BTreeMap;
#[cfg(not(test))] use std::{
    num::NonZeroU64,
    path::{Path, PathBuf}
};
use tokio::executor;
#[cfg(not(test))] use tokio::executor::{DefaultExecutor, Executor};
use uuid::Uuid;

pub type PoolFut = Future<Item = (), Error = Error>;

#[cfg(test)]
/// Only exists so mockers can replace Cluster
/// XXX note that the signatures for some methods have different lifetime
/// specifiers than in the non-test versions.  This is because mockers doesn't
/// work with parameterized traits.
pub trait ClusterTrait {
    fn allocated(&self) -> LbaT;
    fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT);
    fn find_closed_zone(&self, zid: ZoneT) -> Option<cluster::ClosedZone>;
    fn flush(&self, idx: u32) -> Box<PoolFut>;
    fn free(&self, lba: LbaT, length: LbaT) -> Box<PoolFut>;
    fn optimum_queue_depth(&self) -> u32;
    fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut>;
    fn size(&self) -> LbaT;
    fn sync_all(&self) -> Box<PoolFut>;
    fn uuid(&self) -> Uuid;
    fn write(&self, buf: IoVec, txg: TxgT)
        -> Result<(LbaT, Box<PoolFut>), Error>;
    fn write_label(&self, labeller: LabelWriter) -> Box<PoolFut>;
}
#[cfg(test)]
pub type ClusterLike = Box<ClusterTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type ClusterLike = cluster::Cluster;

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
struct ClusterServer {
    cluster: ClusterLike
}

impl<'a> ClusterServer {
    fn new(cluster: ClusterLike) -> Self {
        ClusterServer{cluster}
    }

    /// Start the `ClusterServer` in the background, in the current thread
    fn run(cs: Rc<ClusterServer>, rx: mpsc::UnboundedReceiver<Rpc>) {
        let fut = future::lazy(move || {
            // In Futures 0.2, use for_each_concurrent instead
            rx.for_each(move |rpc| cs.dispatch(rpc))
            // If we get here, the ClusterProxy was dropped
        });
        executor::current_thread::TaskExecutor::current().spawn_local(
            Box::new(fut)
        ).unwrap();
    }

    fn dispatch(&self, rpc: Rpc) -> impl Future<Item=(), Error=()>
    {
        match rpc {
            #[cfg(debug_assertions)]
            Rpc::AssertCleanZone(zone, txg) => {
                self.cluster.assert_clean_zone(zone, txg);
                Box::new(future::ok::<(), ()>(()))
                    as Box<Future<Item=(), Error=()>>
            },
            Rpc::Allocated(tx) => {
                tx.send(self.cluster.allocated()).unwrap();
                Box::new(future::ok::<(), ()>(()))
                    as Box<Future<Item=(), Error=()>>
            },
            Rpc::FindClosedZone(zid, tx) => {
                tx.send(self.cluster.find_closed_zone(zid)).unwrap();
                Box::new(future::ok::<(), ()>(()))
                    as Box<Future<Item=(), Error=()>>
            },
            Rpc::Flush(idx, tx) => {
                let fut = self.cluster.flush(idx)
                .then(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::new(fut) as Box<Future<Item=(), Error=()>>
            },
            Rpc::Free(lba, length, tx) => {
                let fut = self.cluster.free(lba, length)
                .then(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::new(fut) as Box<Future<Item=(), Error=()>>
            }
            Rpc::OptimumQueueDepth(tx) => {
                tx.send(self.cluster.optimum_queue_depth()).unwrap();
                Box::new(future::ok::<(), ()>(()))
                    as Box<Future<Item=(), Error=()>>
            },
            Rpc::Read(buf, lba, tx) => {
                let fut = self.cluster.read(buf, lba)
                .then(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::new(fut) as Box<Future<Item=(), Error=()>>
            },
            Rpc::Shutdown() => {
                Box::new(future::err::<(), ()>(()))
            },
            Rpc::Size(tx) => {
                tx.send(self.cluster.size()).unwrap();
                Box::new(future::ok::<(), ()>(()))
                    as Box<Future<Item=(), Error=()>>
            },
            Rpc::SyncAll(tx) => {
                let fut = self.cluster.sync_all()
                .then(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::new(fut) as Box<Future<Item=(), Error=()>>
            },
            Rpc::Write(buf, txg, tx) => {
                match self.cluster.write(buf, txg) {
                    Ok((lba, wfut)) => {
                        let txfut = wfut
                            .then(move |r| {
                                match r {
                                    Ok(_) => tx.send(Ok(lba)),
                                    Err(e) => tx.send(Err(e))
                                }.unwrap();
                                Ok(())
                            });
                        Box::new(txfut) as Box<Future<Item=(), Error=()>>
                    },
                    Err(e) => {
                        tx.send(Err(e)).unwrap();
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=()>>
                    }
                }
            },
            Rpc::WriteLabel(label_writer, tx) => {
                let fut = self.cluster.write_label(label_writer)
                .then(|r| {
                    tx.send(r).unwrap();
                    Ok(())
                });
                Box::new(fut) as Box<Future<Item=(), Error=()>>
            },
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

impl<'a> ClusterProxy {
    fn allocated(&self) -> impl Future<Item = LbaT, Error = Error> {
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

    fn flush(&self, idx: u32) -> impl Future<Item=(), Error=Error> {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Flush(idx, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn free(&self, lba: LbaT, length: LbaT)
        -> impl Future<Item=(), Error=Error>
    {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Free(lba, length, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn find_closed_zone(&self, zid: ZoneT)
        -> impl Future<Item=Option<cluster::ClosedZone>, Error=Error>
    {
        let (tx, rx) = oneshot::channel::<Option<cluster::ClosedZone>>();
        let rpc = Rpc::FindClosedZone(zid, tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    fn optimum_queue_depth(&self) -> impl Future<Item=u32, Error=Error> {
        let (tx, rx) = oneshot::channel::<u32>();
        let rpc = Rpc::OptimumQueueDepth(tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    /// Create a new ClusterServer/ClusterProxy pair, start the server, and
    /// return the proxy
    pub fn new(cluster: ClusterLike) -> Self {
        let (tx, rx) = mpsc::unbounded();
        let uuid = cluster.uuid();
        let cs = Rc::new(ClusterServer::new(cluster));
        ClusterServer::run(cs, rx);
        ClusterProxy{server: tx, uuid}
    }

    fn read(&self, buf: IoVecMut, lba: LbaT)
        -> impl Future<Item=(), Error=Error>
    {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::Read(buf, lba, tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    /// Helper that collapses a Future<Item=Result<_>, Error=_>.
    fn rx_unit_result(rx: oneshot::Receiver<Result<(), Error>>)
        -> impl Future<Item=(), Error=Error>
    {
        rx.map_err(|_| Error::EPIPE)
            .and_then(|result| result.into_future())
    }

    fn shutdown(&self) {
        let rpc = Rpc::Shutdown();
        // Ignore errors.  An error indicates that the ClusterServer is already
        // shut down.
        let _ = self.server.unbounded_send(rpc);
    }

    fn size(&self) -> impl Future<Item = LbaT, Error = Error> {
        let (tx, rx) = oneshot::channel::<LbaT>();
        let rpc = Rpc::Size(tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
    }

    fn sync_all(&self) -> impl Future<Item = (), Error = Error> {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        let rpc = Rpc::SyncAll(tx);
        self.server.unbounded_send(rpc).unwrap();
        ClusterProxy::rx_unit_result(rx)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn write(&self, buf: IoVec, txg: TxgT)
        -> impl Future<Item = LbaT, Error = Error>
    {
        let (tx, rx) = oneshot::channel::<Result<LbaT, Error>>();
        let rpc = Rpc::Write(buf, txg, tx);
        self.server.unbounded_send(rpc).unwrap();
        rx.map_err(|_| Error::EPIPE)
            .and_then(|result| result.into_future())
    }

    fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error>
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
    // NB: 32 bits would be preferable for queue_depth, once stable Rust
    // supports atomic 32-bit ints
    // https://github.com/rust-lang/rust/issues/32976
    queue_depth: Vec<Atomic<u64>>,

    /// "Best" number of commands to queue to each VdevRaid
    optimum_queue_depth: Vec<f64>,

    /// The total size of each `Cluster`
    size: Vec<LbaT>,

    /// The total amount of allocated space in each `Cluster`, excluding
    /// space that has already been freed but not erased.
    allocated_space: Vec<Atomic<LbaT>>,
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

/// An BFFFS storage pool
pub struct Pool {
    clusters: Vec<ClusterProxy>,

    /// Human-readable pool name.  Must be unique on any one system.
    name: String,

    stats: Arc<Stats>,

    uuid: Uuid,
}

impl<'a> Pool {
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
    /// * `num_disks`:          Total number of disks in the array
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than
    ///                         or equal to `num_disks`.
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `paths`:              Slice of pathnames of files and/or devices
    #[cfg(not(test))]
    pub fn create_cluster<P: AsRef<Path> + Sync>(chunksize: Option<NonZeroU64>,
                               num_disks: i16,
                               disks_per_stripe: i16,
                               lbas_per_zone: Option<NonZeroU64>,
                               redundancy: i16,
                               paths: &[P])
        -> impl Future<Item=ClusterProxy, Error=()>
    {
        let (tx, rx) = oneshot::channel();
        // DefaultExecutor needs 'static futures; we must copy the Paths
        let owned_paths = paths.iter()
            .map(|p| p.as_ref().to_owned())
            .collect::<Vec<PathBuf>>();
        DefaultExecutor::current().spawn(Box::new(future::lazy(move || {
            let c = cluster::Cluster::create(chunksize, num_disks,
                disks_per_stripe, lbas_per_zone, redundancy, &owned_paths);
            tx.send(ClusterProxy::new(c)).unwrap();
            Ok(())
        }))).unwrap();
        rx.map_err(|_| panic!("Closed Runtime while creating Cluster?"))
    }

    /// Create a new `Pool` from some freshly created `Cluster`s.
    ///
    /// Must be called from within the context of a Tokio Runtime.
    #[cfg(not(test))]
    pub fn create(name: String, clusters: Vec<ClusterProxy>)
        -> impl Future<Item=Self, Error=Error>
    {
        Pool::new(name, Uuid::new_v4(), clusters)
    }

    pub fn flush(&self, idx: u32) -> impl Future<Item=(), Error=Error> {
        future::join_all(
            self.clusters.iter()
            .map(|cp| cp.flush(idx))
            .collect::<Vec<_>>()
        ).map(drop)
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
        -> impl Future<Item=(), Error=Error>
    {
        let idx = pba.cluster as usize;
        self.stats.allocated_space[idx].fetch_sub(length, Ordering::Relaxed);
        self.clusters[pba.cluster as usize].free(pba.lba, length)
    }

    /// Construct a new `Pool` from some already constructed
    /// [`Cluster`](struct.Cluster.html)s.
    ///
    /// Must be called from within the context of a Tokio Runtime.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::new_ret_no_self))]
    fn new(name: String, uuid: Uuid, clusters: Vec<ClusterProxy>)
        -> impl Future<Item=Self, Error=Error>
    {
        let size_fut = future::join_all(clusters.iter()
            .map(ClusterProxy::size)
            .collect::<Vec<_>>()
        );
        let allocated_fut = future::join_all(clusters.iter()
            .map(|cluster| cluster.allocated()
                .map(Atomic::new)
            ).collect::<Vec<_>>()
        );
        let oqd_fut = future::join_all(clusters.iter()
            .map(|cluster| cluster.optimum_queue_depth()
                .map(f64::from)
            ).collect::<Vec<_>>()
        );
        let queue_depth: Vec<_> = clusters.iter()
            .map(|_| Atomic::new(0))
            .collect();
        size_fut.join3(allocated_fut, oqd_fut)
        .map(move |(size, allocated_space, optimum_queue_depth)| {
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
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::collapsible_if))]
    pub fn find_closed_zone(&self, clust: ClusterT, zid: ZoneT)
        -> impl Future<Item=(Option<ClosedZone>, Option<(ClusterT, ZoneT)>),
                       Error=Error>
    {
        let nclusters = self.clusters.len() as ClusterT;
        self.clusters[clust as usize].find_closed_zone(zid)
            .map(move |r| {
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
        -> impl Future<Item = (Self, LabelReader), Error = Error>
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
            .map(|pool| (pool, label_reader))
    }

    /// Asynchronously read from the pool
    pub fn read(&self, buf: IoVecMut, pba: PBA)
        -> impl Future<Item=(), Error=Error>
    {
        let cidx = pba.cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let stats2 = self.stats.clone();
        self.clusters[pba.cluster as usize].read(buf, pba.lba)
            .then(move |r| {
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
    pub fn sync_all(&self) -> impl Future<Item=(), Error=Error> {
        future::join_all(
            self.clusters.iter()
            .map(ClusterProxy::sync_all)
            .collect::<Vec<_>>()
        ).map(drop)
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
        -> impl Future<Item = PBA, Error=Error>
    {
        let cluster = self.stats.choose_cluster();
        let cidx = cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let space = div_roundup(buf.len(), BYTES_PER_LBA) as LbaT;
        let stats2 = self.stats.clone();
        let stats3 = self.stats.clone();
        self.clusters[cidx].write(buf, txg)
            .then(move |r| {
                stats2.queue_depth[cidx].fetch_sub(1, Ordering::Relaxed);
                r
            }).map(move |lba| {
                stats3.allocated_space[cidx]
                    .fetch_add(space, Ordering::Relaxed);
                PBA::new(cluster, lba)
            })
    }

    /// Asynchronously write this `Pool`'s label to all component devices
    pub fn write_label(&self, mut labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error>
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
        future::join_all(futs).map(drop)
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

#[cfg(feature = "mocks")]
mod pool {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::{IntoFuture, future};
    use mockers::{Scenario, check, matchers};
    use mockers_derive::mock;
    use tokio::runtime::current_thread;

    mock!{
        MockCluster,
        self,
        trait ClusterTrait {
            fn allocated(&self) -> LbaT;
            fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT);
            fn find_closed_zone(&self, zid: ZoneT)
                -> Option<cluster::ClosedZone>;
            fn flush(&self, idx: u32) -> Box<Future<Item=(), Error=Error>>;
            fn free(&self, lba: LbaT, length: LbaT)
                -> Box<Future<Item=(), Error=Error>>;
            fn optimum_queue_depth(&self) -> u32;
            fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut>;
            fn size(&self) -> LbaT;
            fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
            fn uuid(&self) -> Uuid;
            fn write(&self, buf: IoVec, txg: TxgT)
                -> Result<(LbaT, Box<PoolFut>), Error>;
            fn write_label(&self, labeller: LabelWriter) -> Box<PoolFut>;
        }
    }

    // pet kcov
    #[test]
    fn debug() {
        let s = Scenario::new();
        let c = s.create_mock::<MockCluster>();
        s.expect(c.uuid_call().and_return(Uuid::new_v4()));
        let mut rt = current_thread::Runtime::new().unwrap();
        rt.block_on(future::lazy(|| {
            let cluster_proxy = ClusterProxy::new(Box::new(c));
            format!("{:?}", cluster_proxy);
            future::ok::<(), ()>(())
        })).unwrap();
    }

    #[test]
    fn find_closed_zone() {
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.allocated_call().and_return(0));
            s.expect(c.optimum_queue_depth_call().and_return(10));
            s.expect(c.find_closed_zone_call(0)
                .and_return(Some(cluster::ClosedZone {
                    zid: 1,
                    start: 10,
                    freed_blocks: 5,
                    total_blocks: 10,
                    txgs: TxgT::from(0)..TxgT::from(1)
                })));
            s.expect(c.find_closed_zone_call(2)
                .and_return(Some(cluster::ClosedZone {
                    zid: 3,
                    start: 30,
                    freed_blocks: 6,
                    total_blocks: 10,
                    txgs: TxgT::from(2)..TxgT::from(3)
                })));
            s.expect(c.find_closed_zone_call(4).and_return(None));
            s.expect(c.size_call().and_return(32_768_000));
            s.expect(c.uuid_call().and_return(Uuid::new_v4()));
            c
        };
        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(Box::new(cluster())),
                ClusterProxy::new(Box::new(cluster()))
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
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.allocated_call().and_return(0));
            s.expect(c.optimum_queue_depth_call().and_return(10));
            s.expect(c.size_call().and_return(32_768_000));
            s.expect(c.uuid_call().and_return(Uuid::new_v4()));
            c
        };
        let c0 = cluster();
        let c1 = cluster();
        s.expect(c1.free_call(12345, 16)
            .and_return(Box::new(Ok(()).into_future())));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(Box::new(c0)),
                ClusterProxy::new(Box::new(c1))
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        assert!(rt.block_on(pool.free(PBA::new(1, 12345), 16)).is_ok());
    }

    // optimum_queue_depth is always a smallish integer, so exact equality
    // testing is ok.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::float_cmp))]
    #[test]
    fn new() {
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.optimum_queue_depth_call().and_return(10));
            s.expect(c.allocated_call().and_return(500));
            s.expect(c.size_call().and_return(1000));
            s.expect(c.uuid_call().and_return(Uuid::new_v4()));
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(Box::new(cluster())),
                ClusterProxy::new(Box::new(cluster()))
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
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
        s.expect(cluster.allocated_call().and_return(0));
        s.expect(cluster.optimum_queue_depth_call().and_return(10));
        s.expect(cluster.size_call().and_return(32_768_000));
        s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
        s.expect(cluster.read_call(matchers::ANY, 10)
            .and_call(|mut iovec: IoVecMut, _lba: LbaT| {
                iovec.copy_from_slice(&vec![99; 4096][..]);
                Box::new( future::ok::<(), Error>(()))
            })
        );

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(move || {
            let clusters = vec![
                ClusterProxy::new(Box::new(cluster)),
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
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
        s.expect(cluster.allocated_call().and_return(0));
        s.expect(cluster.optimum_queue_depth_call().and_return(10));
        s.expect(cluster.size_call().and_return(32_768_000));
        s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
        s.expect(cluster.read_call(matchers::ANY, matchers::ANY)
            .and_return(Box::new(Err(e).into_future()))
        );

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(move || {
            let clusters = vec![
                ClusterProxy::new(Box::new(cluster)),
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
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.allocated_call().and_return(0));
            s.expect(c.optimum_queue_depth_call().and_return(10));
            s.expect(c.size_call().and_return(32_768_000));
            s.expect(c.uuid_call().and_return(Uuid::new_v4()));
            s.expect(c.sync_all_call()
                .and_return(Box::new(future::ok::<(), Error>(())))
            );
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            let clusters = vec![
                ClusterProxy::new(Box::new(cluster())),
                ClusterProxy::new(Box::new(cluster()))
            ];
            Pool::new("foo".to_string(), Uuid::new_v4(), clusters)
        })).unwrap();

        assert!(rt.block_on(pool.sync_all()).is_ok());
    }

    #[test]
    fn write() {
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
            s.expect(cluster.allocated_call().and_return(0));
            s.expect(cluster.optimum_queue_depth_call().and_return(10));
            s.expect(cluster.size_call().and_return(32_768_000));
            s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
            s.expect(cluster.write_call(check!(move |buf: &IoVec| {
                    buf.len() == BYTES_PER_LBA
                }), TxgT::from(42))
                .and_return(Ok((0, Box::new(future::ok::<(), Error>(())))))
        );

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(Box::new(cluster))])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap(), PBA::new(0, 0));
    }

    #[test]
    fn write_async_error() {
        let s = Scenario::new();
        let e = Error::EIO;
        let cluster = s.create_mock::<MockCluster>();
            s.expect(cluster.allocated_call().and_return(0));
            s.expect(cluster.optimum_queue_depth_call().and_return(10));
            s.expect(cluster.size_call().and_return(32_768_000));
            s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
            s.expect(cluster.write_call(matchers::ANY, matchers::ANY)
                .and_return(Ok((0, Box::new(Err(e).into_future()))))
        );

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(Box::new(cluster))])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap_err(), e);
    }

    #[test]
    fn write_sync_error() {
        let s = Scenario::new();
        let e = Error::ENOSPC;
        let cluster = s.create_mock::<MockCluster>();
            s.expect(cluster.allocated_call().and_return(0));
            s.expect(cluster.optimum_queue_depth_call().and_return(10));
            s.expect(cluster.size_call().and_return(32_768_000));
            s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
            s.expect(cluster.write_call(matchers::ANY, matchers::ANY)
                .and_return(Err(e)));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(Box::new(cluster))])
        })).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let result = rt.block_on( pool.write(db0, TxgT::from(42)));
        assert_eq!(result.unwrap_err(), e);
    }

    // Make sure allocated space accounting is symmetric
    #[test]
    fn write_and_free() {
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
        s.expect(cluster.allocated_call().and_return(0));
        s.expect(cluster.optimum_queue_depth_call().and_return(10));
        s.expect(cluster.size_call().and_return(32_768_000));
        s.expect(cluster.uuid_call().and_return(Uuid::new_v4()));
        s.expect(cluster.write_call(matchers::ANY, matchers::ANY)
            .and_return(Ok((0, Box::new(future::ok::<(), Error>(()))))));
        s.expect(cluster.free_call(matchers::ANY, matchers::ANY)
            .and_return(Box::new(Ok(()).into_future())));

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(future::lazy(|| {
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![ClusterProxy::new(Box::new(cluster))])
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
    use super::super::*;

    #[test]
    fn allocated() {
        let stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![Atomic::new(0), Atomic::new(0)],
            size: vec![1000, 1000],
            allocated_space: vec![Atomic::new(10), Atomic::new(900)]
        };
        assert_eq!(stats.allocated(), 910);
    }

    #[test]
    fn choose_cluster_empty() {
        // Two clusters, one full and one empty.  Choose the empty one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![Atomic::new(0), Atomic::new(0)],
            size: vec![1000, 1000],
            allocated_space: vec![Atomic::new(0), Atomic::new(1000)]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.allocated_space = vec![Atomic::new(1000), Atomic::new(0)];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_queue_depth() {
        // Two clusters, one busy and one idle.  Choose the idle one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![Atomic::new(0), Atomic::new(10)],
            size: vec![1000, 1000],
            allocated_space: vec![Atomic::new(0), Atomic::new(0)]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.queue_depth = vec![Atomic::new(10), Atomic::new(0)];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_nearly_full() {
        // Two clusters, one nearly full and idle, the other busy but not very
        // full.  Choose the not very full one.
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![Atomic::new(0), Atomic::new(10)],
            size: vec![1000, 1000],
            allocated_space: vec![Atomic::new(960), Atomic::new(50)]
        };
        assert_eq!(stats.choose_cluster(), 1);

        // Try the reverse, too
        stats.queue_depth = vec![Atomic::new(10), Atomic::new(0)];
        stats.allocated_space = vec![Atomic::new(50), Atomic::new(960)];
        assert_eq!(stats.choose_cluster(), 0);
    }
}
}
// LCOV_EXCL_STOP
