// vim: tw=80

use common::{ cluster, pool, vdev::Vdev, vdev_raid};
#[cfg(not(test))] use common::{ cache, ddml, idml, vdev_block};
use futures::{Future, future};
#[cfg(not(test))] use futures::{stream, sync::oneshot};
#[cfg(not(test))] use futures::Stream;
#[cfg(not(test))] use nix::{Error, errno};
use std::{
    borrow::{Borrow, ToOwned},
    collections::BTreeMap,
    path::{Path, PathBuf},
};
#[cfg(not(test))] use std::sync::{Arc, Mutex};
use sys::vdev_file;
use uuid::Uuid;
use tokio::runtime::current_thread;
#[cfg(not(test))] use tokio::executor::{self, DefaultExecutor, Executor};

pub struct DevManager {
    leaves: BTreeMap<Uuid, PathBuf>,
    raids: BTreeMap<Uuid, vdev_raid::Label>,
    pools: BTreeMap<Uuid, pool::Label>,
}

impl DevManager {
    /// Import a pool by its UUID
    #[cfg(not(test))]
    pub fn import<'a>(&'a self, uuid: Uuid)
        -> impl Future<Item = idml::IDML, Error = Error> + 'a
    {
        let proxies = self.pools[&uuid].children.iter().map(move |child_uuid| {
            let raid = &self.raids[&child_uuid];
            let raid_uuid = raid.uuid.clone();
            let leaf_paths = raid.children.iter()
                .map(|uuid| self.leaves[&uuid].clone())
                .collect::<Vec<_>>();
            let (tx, rx) = oneshot::channel();
            // The top-level Executor spawn puts each Cluster onto a different
            // thread, when using tokio-io-pool
            DefaultExecutor::current().spawn(Box::new(future::lazy(move || {
                let fut = stream::iter_ok(leaf_paths.into_iter())
                    .and_then(|path| {
                    vdev_file::VdevFile::open(path)
                }).map(|(leaf, reader)| {
                    (vdev_block::VdevBlock::new(leaf), reader)
                }).collect()
                .map(move |vdev_blocks| {
                    let (vdev_raid, reader) =
                        vdev_raid::VdevRaid::open(Some(raid_uuid), vdev_blocks);
                    let (cluster, reader) = cluster::Cluster::open(vdev_raid,
                                                                   reader);
                    let proxy = pool::ClusterProxy::new(cluster);
                    tx.send((proxy, reader))
                        .ok().expect("channel dropped too soon");
                }).map_err(|e| panic!("{:?}", e));
                executor::current_thread::TaskExecutor::current().spawn_local(
                    Box::new(fut)
                ).map_err(|e| panic!("{:?}", e))
            }))
            ).unwrap();
            rx
        });
        future::join_all(proxies).map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |proxies| {
                pool::Pool::open(Some(uuid), proxies)
            }).map(|(pool, label_reader)| {
                let cache = cache::Cache::with_capacity(1_000_000_000);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                idml::IDML::open(ddml, arc_cache, label_reader)
            })
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> impl Iterator<Item=(&str, &Uuid)> {
        self.pools.iter()
            .map(|(_uuid, label)| {
                (label.name.borrow(), &label.uuid)
            })
    }

    pub fn new() -> Self {
        DevManager{ leaves: BTreeMap::new(),
            raids: BTreeMap::new(),
            pools: BTreeMap::new()
        }
    }

    /// Taste the device identified by `p` for an ArkFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    pub fn taste<P: AsRef<Path>>(&mut self, p: P) {
        // taste should be called from the synchronous domain, so it needs to
        // create its own temporary Runtime
        let mut rt = current_thread::Runtime::new().unwrap();
        rt.block_on(future::lazy(move || {
            let pathbuf = p.as_ref().to_owned();
            vdev_file::VdevFile::open(p)
                .map(move |(vdev_file, mut reader)| {
                    self.leaves.insert(vdev_file.uuid(), pathbuf);
                    let rl: vdev_raid::Label = reader.deserialize().unwrap();
                    self.raids.insert(rl.uuid, rl);
                    let _cl: cluster::Label = reader.deserialize().unwrap();
                    let pl: pool::Label = reader.deserialize().unwrap();
                    self.pools.insert(pl.uuid, pl);
                })
        })).unwrap();
    }
}
