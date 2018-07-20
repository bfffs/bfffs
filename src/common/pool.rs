// vim: tw=80

use atomic::{Atomic, Ordering};
use common::{*, label::*};
#[cfg(not(test))] use common::cluster;
use futures::{Future, Stream, future, stream};
use nix::Error;
#[cfg(not(test))] use nix::errno;
use std::ops::Range;
#[cfg(not(test))] use std::{collections::BTreeMap, path::Path};
use uuid::Uuid;

pub type PoolFut<'a> = Future<Item = (), Error = Error> + 'a;

#[cfg(test)]
/// Only exists so mockers can replace Cluster
/// XXX note that the signatures for some methods have different lifetime
/// specifiers than in the non-test versions.  This is because mockers doesn't
/// work with parameterized traits.
pub trait ClusterTrait {
    fn allocated(&self) -> Box<Future<Item=LbaT, Error=Error>>;
    fn free(&self, lba: LbaT, length: LbaT)
        -> Box<Future<Item=(), Error=Error>>;
    fn list_closed_zones(&self)
        -> Box<Stream<Item=cluster::ClosedZone, Error=Error>>;
    fn optimum_queue_depth(&self) -> Box<Future<Item=u32, Error=Error>>;
    fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut<'static>>;
    fn size(&self) -> Box<Future<Item=LbaT, Error=Error>>;
    fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
    fn uuid(&self) -> Uuid;
    fn write(&self, buf: IoVec, txg: TxgT)
        -> Result<(LbaT, Box<PoolFut<'static>>), Error>;
    fn write_label(&self, labeller: LabelWriter) -> Box<PoolFut<'static>>;
}
#[cfg(test)]
pub type ClusterLike = Box<ClusterTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type ClusterLike = cluster::Cluster;

/// Opaque helper type used for `Pool::create`
#[cfg(not(test))]
pub struct Cluster(cluster::Cluster);

/// Public representation of a closed zone
#[derive(Debug, Eq, PartialEq)]
pub struct ClosedZone {
    /// Number of freed blocks in this zone
    pub freed_blocks: LbaT,

    /// Physical address of the start of the zone
    pub pba: PBA,

    /// Total number of blocks in this zone
    pub total_blocks: LbaT,

    /// Range of transactions included in this zone
    pub txgs: Range<TxgT>
}

// LCOV_EXCL_START
#[derive(Serialize, Deserialize, Debug)]
struct Label {
    /// Human-readable name
    name:               String,

    /// Pool UUID, fixed at format time
    uuid:               Uuid,

    /// `UUID`s of all component `VdevRaid`s
    children:           Vec<Uuid>,
}
// LCOV_EXCL_STOP

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

/// An ArkFS storage pool
pub struct Pool {
    clusters: Vec<ClusterLike>,

    /// Human-readable pool name.  Must be unique on any one system.
    name: String,

    stats: Stats,

    uuid: Uuid,
}

impl<'a> Pool {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.stats.allocated()
    }

    /// Create a new `Cluster` from unused files or devices
    ///
    /// * `chunksize`:          RAID chunksize in LBAs.  This is the largest
    ///                         amount of data that will be read/written to a
    ///                         single device before the `Locator` switches to
    ///                         the next device.
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
    pub fn create_cluster<P: AsRef<Path>>(chunksize: LbaT,
                               num_disks: i16,
                               disks_per_stripe: i16,
                               lbas_per_zone: Option<LbaT>,
                               redundancy: i16,
                               paths: &[P]) -> Cluster
    {
        Cluster(cluster::Cluster::create(chunksize, num_disks, disks_per_stripe,
            lbas_per_zone, redundancy, paths))
    }

    #[cfg(not(test))]
    pub fn create(name: String, clusters: Vec<Cluster>)
        -> impl Future<Item=Self, Error=Error>
    {
        Pool::new(name, Uuid::new_v4(),
                  clusters.into_iter().map(|c| c.0).collect::<Vec<_>>())
    }

    /// Mark `length` LBAs beginning at PBA `pba` as unused, but do not delete
    /// them from the underlying storage.
    ///
    /// Freeing data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, ArkFS should double-check that
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
    /// [`Cluster`](struct.Cluster.html)s
    #[cfg(any(not(test), feature = "mocks"))]
    fn new(name: String, uuid: Uuid, clusters: Vec<ClusterLike>)
        -> impl Future<Item=Self, Error=Error>
    {
        let size_fut = future::join_all(clusters.iter()
            .map(|cluster| cluster.size())
            .collect::<Vec<_>>()
        );
        let allocated_fut = future::join_all(clusters.iter()
            .map(|cluster| cluster.allocated()
                 .map(|allocated| Atomic::new(allocated))
            ).collect::<Vec<_>>()
        );
        let oqd_fut = future::join_all(clusters.iter()
            .map(|cluster| cluster.optimum_queue_depth()
                 .map(|oqd| oqd as f64)
            ).collect::<Vec<_>>()
        );
        let queue_depth: Vec<_> = clusters.iter()
            .map(|_| Atomic::new(0))
            .collect();
        size_fut.join3(allocated_fut, oqd_fut)
        .map(move |(size, allocated_space, optimum_queue_depth)| {
            let stats = Stats{
                allocated_space,
                optimum_queue_depth,
                queue_depth,
                size
            };
            Pool{name, clusters, stats, uuid}
        })
    }

    /// List all closed zones in this Pool in no particular order
    pub fn list_closed_zones(&'a self)
        -> impl Stream<Item=ClosedZone, Error=Error> + 'a
    {
        stream::iter_ok::<_, Error>(self.clusters.iter().enumerate())
        .map(|(i, cluster)| {
            cluster.list_closed_zones()
            .map(move |clz| ClosedZone {
                freed_blocks: clz.freed_blocks,
                pba: PBA::new(i as ClusterT, clz.start),
                total_blocks: clz.total_blocks,
                txgs: clz.txgs
            })
        }).flatten()
    }

    /// Return the `Pool`'s name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open an existing `Pool` by name
    ///
    /// Returns a new `Pool` object
    ///
    /// * `name`:   Name of the desired `Pool`
    /// * `paths`:  Pathnames to search for the `Pool`.  All child devices
    ///             must be present.
    #[cfg(not(test))]
    pub fn open<P>(name: String, paths: Vec<P>)
        -> impl Future<Item=(Self, LabelReader), Error=Error>
        where P: AsRef<Path> + 'static
    {
         // Outline:
         // 1) Discover all `Cluster`s.
         // 2) Search among the `Cluster`s for one that has a `Pool` label
         //    matching `name`.
         // 3) Construct a `Pool` with all required `Cluster`s.
         // 4) `drop` any unwanted `Cluster`s.

        cluster::Cluster::open_all(paths).and_then(|v| {
            let mut label = None;
            let mut all_clusters = v.into_iter()
                                    .map(|(cluster, mut label_reader)| {
                if label.is_none() {
                    let l: Label = label_reader.deserialize().unwrap();
                    if l.name == name {
                        label = Some(l);
                    }
                }
                (cluster.uuid(), (cluster, label_reader))
            }).collect::<BTreeMap<Uuid, (cluster::Cluster, LabelReader)>>();

            match label {
                Some(label) => {
                    let num_clusters = label.children.len();
                    let mut clusters = Vec::with_capacity(num_clusters);
                    let mut label_reader = None;
                    for uuid in label.children {
                        match all_clusters.remove(&uuid) {
                            Some((cluster, reader)) => {
                                clusters.push(cluster);
                                label_reader = Some(reader)
                            },
                            None => break
                        }
                    }
                    if clusters.len() == num_clusters {
                        Ok((Pool::new(name, label.uuid, clusters),
                            label_reader.take().unwrap()))
                    } else {
                        Err(Error::Sys(errno::Errno::ENOENT))
                    }
                },
                None => Err(Error::Sys(errno::Errno::ENOENT))
            }
        }).and_then(|(pool_fut, label_reader)| {
            pool_fut.map(move |pool| (pool, label_reader))
        })
    }


    /// Asynchronously read from the pool
    pub fn read(&'a self, buf: IoVecMut, pba: PBA)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        let cidx = pba.cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        self.clusters[pba.cluster as usize].read(buf, pba.lba)
                 .then(move |r| {
            self.stats.queue_depth[cidx].fetch_sub(1, Ordering::Relaxed);
            r
        })
    }

    /// Return approximately the Pool's usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.stats.size()
    }

    /// Sync the `Pool`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&'a self) -> impl Future<Item=(), Error=Error> + 'a {
        future::join_all(
            self.clusters.iter()
            .map(|bd| bd.sync_all())
            .collect::<Vec<_>>()
        ).map(|_| ())
    }

    /// Return the `Pool`'s UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Write a buffer to the pool
    ///
    /// # Returns
    ///
    /// The `PBA` where the data will be written, and a `Future` for the
    /// operation in progress.
    pub fn write(&'a self, buf: IoVec, txg: TxgT)
        -> Result<(PBA, Box<PoolFut<'a>>), Error>
    {
        let cluster = self.stats.choose_cluster();
        let cidx = cluster as usize;
        self.stats.queue_depth[cidx].fetch_add(1, Ordering::Relaxed);
        let space = (buf.len() / BYTES_PER_LBA) as LbaT;
        self.clusters[cidx].write(buf, txg)
            .map(|(lba, wfut)| {
                self.stats.allocated_space[cidx]
                    .fetch_add(space, Ordering::Relaxed);
                let fut: Box<PoolFut> = Box::new(wfut.then(move |r| {
                    self.stats.queue_depth[cidx].fetch_sub(1, Ordering::Relaxed);
                    r
                }));
                (PBA::new(cluster, lba), fut)
            })
    }

    /// Asynchronously write this `Pool`'s label to all component devices
    pub fn write_label(&'a self, mut labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        let cluster_uuids = self.clusters.iter().map(|cluster| cluster.uuid())
            .collect::<Vec<_>>();
        let label = Label {
            name: self.name.clone(),
            uuid: self.uuid,
            children: cluster_uuids,
        };
        labeller.serialize(label).unwrap();
        let futs = self.clusters.iter().map(|cluster| {
            cluster.write_label(labeller.clone())
        }).collect::<Vec<_>>();
        future::join_all(futs).map(|_| ())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

#[cfg(feature = "mocks")]
mod pool {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::{IntoFuture, future};
    use mockers::Scenario;
    use mockers_derive::mock;
    use tokio::runtime::current_thread;

    mock!{
        MockCluster,
        self,
        trait ClusterTrait {
            fn allocated(&self) -> Box<Future<Item=LbaT, Error=Error>>;
            fn free(&self, lba: LbaT, length: LbaT)
                -> Box<Future<Item=(), Error=Error>>;
            fn list_closed_zones(&self)
                -> Box<Stream<Item=cluster::ClosedZone, Error=Error>>;
            fn optimum_queue_depth(&self) -> Box<Future<Item=u32, Error=Error>>;
            fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut<'static>>;
            fn size(&self) -> Box<Future<Item=LbaT, Error=Error>>;
            fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
            fn uuid(&self) -> Uuid;
            fn write(&self, buf: IoVec, txg: TxgT)
                -> Result<(LbaT, Box<PoolFut<'static>>), Error>;
            fn write_label(&self, labeller: LabelWriter) -> Box<PoolFut<'static>>;
        }
    }

    #[test]
    fn list_closed_zones() {
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.allocated_call()
                .and_return(Box::new(Ok(0).into_future())));
            s.expect(c.optimum_queue_depth_call()
                .and_return(Box::new(Ok(10).into_future())));
            s.expect(c.list_closed_zones_call()
                .and_return(Box::new(stream::iter_ok::<_, Error>(
                    vec![
                        cluster::ClosedZone {
                            zid: 1,
                            start: 10,
                            freed_blocks: 5,
                            total_blocks: 10,
                            txgs: TxgT::from(0)..TxgT::from(1)
                        },
                        cluster::ClosedZone {
                            zid: 3,
                            start: 30,
                            freed_blocks: 6,
                            total_blocks: 10,
                            txgs: TxgT::from(2)..TxgT::from(3)
                        },
                    ].into_iter()
                ))));
            s.expect(c.size_call()
                .and_return(Box::new(Ok(32768000).into_future())));
            c
        };
        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(
            Pool::new("foo".to_string(), Uuid::new_v4(),
                vec![Box::new(cluster()), Box::new(cluster())])
        ).unwrap();
        let closed_zones = rt.block_on(
            pool.list_closed_zones().collect()
        ).unwrap();
        let expected = vec![
            ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5, total_blocks: 10,
                       txgs: TxgT::from(0)..TxgT::from(1)},
            ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6, total_blocks: 10,
                       txgs: TxgT::from(2)..TxgT::from(3)},
            ClosedZone{pba: PBA::new(1, 10), freed_blocks: 5, total_blocks: 10,
                       txgs: TxgT::from(0)..TxgT::from(1)},
            ClosedZone{pba: PBA::new(1, 30), freed_blocks: 6, total_blocks: 10,
                       txgs: TxgT::from(2)..TxgT::from(3)},
        ];
        assert_eq!(closed_zones, expected);
    }

    #[test]
    fn new() {
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.optimum_queue_depth_call()
                     .and_return(Box::new(Ok(10).into_future())));
            s.expect(c.allocated_call()
                     .and_return(Box::new(Ok(500).into_future())));
            s.expect(c.size_call()
                     .and_return(Box::new(Ok(1000).into_future())));
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![Box::new(cluster()), Box::new(cluster())])
        ).unwrap();
        assert_eq!(pool.stats.allocated_space[0].load(Ordering::Relaxed), 500);
        assert_eq!(pool.stats.allocated_space[1].load(Ordering::Relaxed), 500);
        assert_eq!(pool.stats.optimum_queue_depth[0], 10.0);
        assert_eq!(pool.stats.optimum_queue_depth[1], 10.0);
        assert_eq!(pool.size(), 2000);
    }

    #[test]
    fn sync_all() {
        let s = Scenario::new();
        let cluster = || {
            let c = s.create_mock::<MockCluster>();
            s.expect(c.allocated_call()
                .and_return(Box::new(Ok(0).into_future())));
            s.expect(c.optimum_queue_depth_call()
                .and_return(Box::new(Ok(10).into_future())));
            s.expect(c.size_call()
                .and_return(Box::new(Ok(32768000).into_future())));
            s.expect(c.sync_all_call()
                .and_return(Box::new(future::ok::<(), Error>(())))
            );
            c
        };

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![Box::new(cluster()), Box::new(cluster())])
        ).unwrap();

        assert!(rt.block_on(pool.sync_all()).is_ok());
    }

    #[test]
    fn write() {
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
            s.expect(cluster.allocated_call()
                .and_return(Box::new(Ok(0).into_future())));
        s.expect(cluster.optimum_queue_depth_call()
                 .and_return(Box::new(Ok(10).into_future())));
        s.expect(cluster.size_call()
                .and_return(Box::new(Ok(32768000).into_future())));
        s.expect(cluster.write_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }), TxgT::from(42))
            .and_return(Ok((0, Box::new(future::ok::<(), Error>(())))))
        );

        let mut rt = current_thread::Runtime::new().unwrap();
        let pool = rt.block_on(
            Pool::new("foo".to_string(), Uuid::new_v4(),
                      vec![Box::new(cluster)])
        ).unwrap();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let result = rt.block_on(future::lazy(|| {
            let (pba, fut) = pool.write(db0, TxgT::from(42))
                .expect("write failed early");
            fut.map(move |_| pba)
        }));
        assert_eq!(result.unwrap(), PBA::new(0, 0));
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
