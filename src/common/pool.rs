// vim: tw=80

use common::*;
use common::dva::*;
use common::cluster::*;
use futures::Future;
use nix::Error;
use std::cell::RefCell;

pub type PoolFut<'a> = Future<Item = (), Error = Error> + 'a;

#[cfg(test)]
/// Only exists so mockers can replace Cluster
/// XXX note that the signature for `write` has a different lifetime specifier
/// than in the non-test version.  This is because mockers doesn't work with
/// parameterized traits.
pub trait ClusterTrait {
    fn erase_zone(&mut self, zone: ZoneT) -> Box<ClusterFut<'static>>;
    fn free(&self, lba: LbaT, length: LbaT);
    fn optimum_queue_depth(&self) -> u32;
    fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<ClusterFut<'static>>;
    fn size(&self) -> LbaT;
    fn write(&self, buf: IoVec) -> Result<(LbaT, Box<ClusterFut<'static>>), Error>;
}
#[cfg(test)]
pub type ClusterLike = Box<ClusterTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type ClusterLike = Cluster;

struct Stats {
    /// The queue depth of each `Cluster`, including both commands that have
    /// been sent to the disks, and commands that are pending in `VdevBlock`
    queue_depth: Vec<i32>,

    /// "Best" number of commands to queue to each VdevRaid
    optimum_queue_depth: Vec<f64>,

    /// The total size of each `Cluster`
    size: Vec<LbaT>,

    /// The total percentage of allocated space in each `Cluster`, excluding space
    /// that has already been freed but not erased.
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

    stats: RefCell<Stats>
}

impl<'a> Pool {
    /// Mark `length` LBAs beginning at LBA `lba` on cluster `cluster` as
    /// unused, but do not delete them from the underlying storage.
    ///
    /// Freeing data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, ArkFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Pool.
    pub fn free(&mut self, cluster: ClusterT, lba: LbaT, length: LbaT) {
        self.stats.borrow_mut().allocated_space[cluster as usize] -= length;
        self.clusters[cluster as usize].free(lba, length)
    }

    /// Construct a new `Pool` from a some already constructed
    /// [`Cluster`](struct.Cluster.html)s
    pub fn new(clusters: Vec<ClusterLike>) -> Self {
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
        Pool{clusters, stats}
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
            fn erase_zone(&mut self, zone: ZoneT) -> Box<ClusterFut<'static>>;
            fn free(&self, lba: LbaT, length: LbaT);
            fn optimum_queue_depth(&self) -> u32;
            fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<ClusterFut<'static>>;
            fn size(&self) -> LbaT;
            fn write(&self, buf: IoVec) -> Result<(LbaT, Box<ClusterFut<'static>>), Error>;
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

        let pool = Pool::new(vec![Box::new(cluster)]);

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
