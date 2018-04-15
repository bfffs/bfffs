// vim: tw=80

use common::*;
#[cfg(not(test))]
use common::cluster;
use common::label::*;
use futures::{Future, future};
use nix::Error;
#[cfg(not(test))]
use nix::errno;
use std::cell::RefCell;
#[cfg(not(test))]
use std::collections::BTreeMap;
#[cfg(not(test))]
use std::path::Path;
#[cfg(not(test))]
use tokio::reactor::Handle;
use uuid::Uuid;

pub type PoolFut<'a> = Future<Item = (), Error = Error> + 'a;

#[cfg(test)]
/// Only exists so mockers can replace Cluster
/// XXX note that the signature for `write` has a different lifetime specifier
/// than in the non-test version.  This is because mockers doesn't work with
/// parameterized traits.
pub trait ClusterTrait {
    fn erase_zone(&mut self, zone: ZoneT) -> Box<PoolFut<'static>>;
    fn free(&self, lba: LbaT, length: LbaT);
    fn optimum_queue_depth(&self) -> u32;
    fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut<'static>>;
    fn size(&self) -> LbaT;
    fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
    fn uuid(&self) -> Uuid;
    fn write(&self, buf: IoVec) -> Result<(LbaT, Box<PoolFut<'static>>), Error>;
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

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    /// Human-readable name
    name:               String,

    /// Pool UUID, fixed at format time
    uuid:               Uuid,

    /// `UUID`s of all component `VdevRaid`s
    children:           Vec<Uuid>
}

struct Stats {
    /// The queue depth of each `Cluster`, including both commands that have
    /// been sent to the disks, and commands that are pending in `VdevBlock`
    queue_depth: Vec<i32>,

    /// "Best" number of commands to queue to each VdevRaid
    optimum_queue_depth: Vec<f64>,

    /// The total size of each `Cluster`
    size: Vec<LbaT>,

    /// The total percentage of allocated space in each `Cluster`, excluding
    /// space that has already been freed but not erased.
    allocated_space: Vec<u64>,
}

impl Stats {
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
            let space_util = (self.allocated_space[i] as f64) /
                             (self.size[i] as f64);
            let queue_fraction = (self.queue_depth[i] as f64) /
                                  self.optimum_queue_depth[i];
            let q_coeff = if 0.95 > space_util {0.95 - space_util} else {0.0};
            let weight = q_coeff * queue_fraction + space_util;
            (i, weight)
        })
        .min_by(|&(_, x), &(_, y)| x.partial_cmp(&y).unwrap())
        .map(|(i, _)| i)
        .unwrap() as ClusterT
    }
}

/// An ArkFS storage pool
pub struct Pool {
    clusters: Vec<ClusterLike>,

    /// Human-readable pool name.  Must be unique on any one system.
    name: String,

    stats: RefCell<Stats>,

    uuid: Uuid,
}

impl<'a> Pool {
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
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `paths`:              Slice of pathnames of files and/or devices
    /// * `handle`:             Handle to the Tokio reactor that will be used to
    ///                         service this vdev.
    #[cfg(not(test))]
    pub fn create_cluster<P: AsRef<Path>>(chunksize: LbaT,
                               num_disks: i16,
                               disks_per_stripe: i16,
                               redundancy: i16,
                               paths: &[P],
                               handle: Handle) -> Cluster {
        Cluster(cluster::Cluster::create(chunksize, num_disks, disks_per_stripe,
                                         redundancy, paths, handle))
    }

    #[cfg(not(test))]
    pub fn create(name: String, clusters: Vec<Cluster>) -> Self {
        Pool::new(name, Uuid::new_v4(),
                  clusters.into_iter().map(|c| c.0).collect::<Vec<_>>())
    }

    /// Mark `length` LBAs beginning at LBA `lba` on cluster `cluster` as
    /// unused, but do not delete them from the underlying storage.
    ///
    /// Freeing data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, ArkFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Pool.
    pub fn free(&self, cluster: ClusterT, lba: LbaT, length: LbaT) {
        self.stats.borrow_mut().allocated_space[cluster as usize] -= length;
        self.clusters[cluster as usize].free(lba, length)
    }

    /// Construct a new `Pool` from some already constructed
    /// [`Cluster`](struct.Cluster.html)s
    #[cfg(any(not(test), feature = "mocks"))]
    fn new(name: String, uuid: Uuid, clusters: Vec<ClusterLike>) -> Self {
        let size: Vec<_> = clusters.iter()
            .map(|cluster| cluster.size())
            .collect();
        let allocated_space: Vec<_> = clusters.iter().map(|_| 0).collect();
        let optimum_queue_depth: Vec<_> = clusters.iter().map(|cluster| {
            cluster.optimum_queue_depth() as f64
        }).collect();
        let queue_depth: Vec<_> = clusters.iter().map(|_| 0).collect();
        let stats = RefCell::new(Stats{
            allocated_space,
            optimum_queue_depth,
            queue_depth,
            size
        });
        Pool{name, clusters, stats, uuid}
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
    /// * `h`:      Handle to the Tokio reactor that will be used to service
    ///             this `Pool`.
    #[cfg(not(test))]
    pub fn open<P>(name: String, paths: Vec<P>, handle: Handle)
        -> Box<Future<Item=Self, Error=Error>>
        where P: AsRef<Path> + 'static {
         // Outline:
         // 1) Discover all `Cluster`s.
         // 2) Search among the `Cluster`s for one that has a `Pool` label
         //    matching `name`.
         // 3) Construct a `Pool` with all required `Cluster`s.
         // 4) `drop` any unwanted `Cluster`s.

        Box::new(cluster::Cluster::open_all(paths, handle).and_then(|v| {
            let mut label = None;
            let mut all_clusters = v.into_iter()
                                    .map(|(cluster, mut label_reader)| {
                if label.is_none() {
                    let l: Label = label_reader.deserialize().unwrap();
                    if l.name == name {
                        label = Some(l);
                    }
                }
                (cluster.uuid(), cluster)
            }).collect::<BTreeMap<Uuid, cluster::Cluster>>();

            match label {
                Some(label) => {
                    let num_clusters = label.children.len();
                    let mut clusters = Vec::with_capacity(num_clusters);
                    for uuid in label.children {
                        match all_clusters.remove(&uuid) {
                            Some(cluster) => clusters.push(cluster),
                            None => break
                        }
                    }
                    if clusters.len() == num_clusters {
                        Ok(Pool::new(name, label.uuid, clusters))
                    } else {
                        Err(Error::Sys(errno::Errno::ENOENT))
                    }
                },
                None => Err(Error::Sys(errno::Errno::ENOENT))
            }
        }))
    }


    /// Asynchronously read from the pool
    pub fn read(&'a self, buf: IoVecMut, cluster: ClusterT,
                lba: LbaT) -> Box<PoolFut<'a>> {
        let mut stats = self.stats.borrow_mut();
        stats.queue_depth[cluster as usize] += 1;
        Box::new(self.clusters[cluster as usize].read(buf, lba).then(move |r| {
            stats.queue_depth[cluster as usize] -= 1;
            r
        }))
    }

    /// Sync the `Pool`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&'a self) -> Box<PoolFut<'a>> {
        Box::new(
            future::join_all(
                self.clusters.iter()
                .map(|bd| bd.sync_all())
                .collect::<Vec<_>>()
            ).map(|_| ())
        )
    }

    /// Return the `Pool`'s UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Write a buffer to the pool
    ///
    /// # Returns
    ///
    /// The Cluster and LBA where the data will be written, and a `Future` for
    /// the operation in progress.
    pub fn write(&'a self, buf: IoVec) -> Result<(ClusterT, LbaT,
                                                  Box<PoolFut<'a>>), Error> {
        let cluster = self.stats.borrow().choose_cluster();
        let mut stats = self.stats.borrow_mut();
        stats.queue_depth[cluster as usize] += 1;
        let space = (buf.len() / BYTES_PER_LBA) as LbaT;
        self.clusters[cluster as usize].write(buf)
            .map(|(lba, wfut)| {
                stats.allocated_space[cluster as usize] += space;
                let fut: Box<PoolFut> = Box::new(wfut.then(move |r| {
                    stats.queue_depth[cluster as usize] -= 1;
                    r
                }));
                (cluster, lba, fut)
            })
    }

    /// Asynchronously write this `Pool`'s label to all component devices
    pub fn write_label(&self) -> Box<PoolFut> {
        let mut labeller = LabelWriter::new();
        let cluster_uuids = self.clusters.iter().map(|cluster| cluster.uuid())
            .collect::<Vec<_>>();
        let label = Label {
            name: self.name.clone(),
            uuid: self.uuid,
            children: cluster_uuids
        };
        let dbs = labeller.serialize(label);
        let futs = self.clusters.iter().map(|cluster| {
            cluster.write_label(labeller.clone())
        }).collect::<Vec<_>>();
        Box::new(future::join_all(futs).map(move |_| {
            let _ = dbs;    // needs to live this long
        }))
    }
}

#[cfg(test)]
mod t {

#[cfg(feature = "mocks")]
mod pool {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use mockers::Scenario;
    use tokio::executor::current_thread;

    mock!{
        MockCluster,
        self,
        trait ClusterTrait {
            fn erase_zone(&mut self, zone: ZoneT) -> Box<PoolFut<'static>>;
            fn free(&self, lba: LbaT, length: LbaT);
            fn optimum_queue_depth(&self) -> u32;
            fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<PoolFut<'static>>;
            fn size(&self) -> LbaT;
            fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
            fn uuid(&self) -> Uuid;
            fn write(&self, buf: IoVec) -> Result<(LbaT, Box<PoolFut<'static>>), Error>;
            fn write_label(&self, labeller: LabelWriter) -> Box<PoolFut<'static>>;
        }
    }

    #[test]
    fn write() {
        let s = Scenario::new();
        let cluster = s.create_mock::<MockCluster>();
        s.expect(cluster.optimum_queue_depth_call()
                 .and_return_clone(10)
                 .times(..));
        s.expect(cluster.size_call().and_return_clone(32768000).times(..));
        s.expect(cluster.write_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }))
            .and_return(Ok((0, Box::new(future::ok::<(), Error>(())))))
        );

        let pool = Pool::new("foo".to_string(), Uuid::new_v4(),
                             vec![Box::new(cluster)]);

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let result = current_thread::block_on_all(future::lazy(|| {
            let (cluster, lba, fut) = pool.write(db0).expect("write failed early");
            fut.map(move |_| (cluster, lba))
        }));
        assert_eq!(result.unwrap(), (0, 0));
    }
}

mod stats {
    use super::super::*;

    #[test]
    fn choose_cluster_empty() {
        // Two clusters, one full and one empty.  Choose the empty one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![0, 0],
            size: vec![1000, 1000],
            allocated_space: vec![0, 1000]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.allocated_space = vec![1000, 0];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_queue_depth() {
        // Two clusters, one busy and one idle.  Choose the idle one
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![0, 10],
            size: vec![1000, 1000],
            allocated_space: vec![0, 0]
        };
        assert_eq!(stats.choose_cluster(), 0);

        // Try the reverse, too
        stats.queue_depth = vec![10, 0];
        assert_eq!(stats.choose_cluster(), 1);
    }

    #[test]
    fn choose_cluster_nearly_full() {
        // Two clusters, one nearly full and idle, the other busy but not very
        // full.  Choose the not very full one.
        let mut stats = Stats {
            optimum_queue_depth: vec![10.0, 10.0],
            queue_depth: vec![0, 10],
            size: vec![1000, 1000],
            allocated_space: vec![960, 50]
        };
        assert_eq!(stats.choose_cluster(), 1);

        // Try the reverse, too
        stats.queue_depth = vec![10, 0];
        stats.allocated_space = vec![50, 960];
        assert_eq!(stats.choose_cluster(), 0);
    }
}
}
