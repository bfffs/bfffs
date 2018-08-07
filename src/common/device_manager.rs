// vim: tw=80

use common::{ cluster, pool, vdev::Vdev, vdev_raid};
#[cfg(not(test))] use common::{ cache, database, ddml, idml, vdev_block};
use futures::{Future, future};
#[cfg(not(test))] use futures::{stream, sync::oneshot};
#[cfg(not(test))] use futures::Stream;
#[cfg(not(test))] use nix::{Error, errno};
use std::{
    borrow::ToOwned,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex
};
#[cfg(not(test))] use std::sync::Arc;
use sys::vdev_file;
use uuid::Uuid;
use tokio::runtime::current_thread;
#[cfg(not(test))] use tokio::executor::{self, DefaultExecutor, Executor};

struct Inner {
    leaves: BTreeMap<Uuid, PathBuf>,
    raids: BTreeMap<Uuid, vdev_raid::Label>,
    pools: BTreeMap<Uuid, pool::Label>,
}

pub struct DevManager {
    inner: Mutex<Inner>
}

impl DevManager {
    /// Import a pool by its UUID
    #[cfg(not(test))]
    pub fn import(&self, uuid: Uuid)
        -> impl Future<Item = database::Database, Error = Error>
    {
        let (_pool, raids, mut leaves) = {
            let mut inner = self.inner.lock().unwrap();
            let pool = inner.pools.remove(&uuid).unwrap();
            let raids = pool.children.iter()
                .map(|child_uuid| inner.raids.remove(child_uuid).unwrap())
                .collect::<Vec<_>>();
            let leaves = raids.iter().map(|raid| {
                let leaves = raid.children.iter().map(|uuid| {
                    inner.leaves.remove(&uuid).unwrap()
                }).collect::<Vec<_>>();
                (raid.uuid.clone(), leaves)
            }).collect::<BTreeMap<_, _>>();
            // Drop the self.inner mutex
            (pool, raids, leaves)
        };
        let proxies = raids.into_iter().map(move |raid| {
            let raid_uuid = raid.uuid.clone();
            let leaf_paths = leaves.remove(&raid_uuid).unwrap();
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
                let (idml, label_reader) = idml::IDML::open(ddml, arc_cache,
                                                            label_reader);
                database::Database::open(Arc::new(idml), label_reader)
            })
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        let inner = self.inner.lock().unwrap();
        inner.pools.iter()
            .map(|(_uuid, label)| {
                (label.name.clone(), label.uuid)
            }).collect::<Vec<_>>()
    }

    pub fn new() -> Self {
        let inner = Inner{ leaves: BTreeMap::new(),
            raids: BTreeMap::new(),
            pools: BTreeMap::new()
        };
        DevManager{inner: Mutex::new(inner)}
    }

    /// Taste the device identified by `p` for an BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    pub fn taste<P: AsRef<Path>>(&self, p: P) {
        // taste should be called from the synchronous domain, so it needs to
        // create its own temporary Runtime
        let mut rt = current_thread::Runtime::new().unwrap();
        rt.block_on(future::lazy(move || {
            let pathbuf = p.as_ref().to_owned();
            vdev_file::VdevFile::open(p)
                .map(move |(vdev_file, mut reader)| {
                    let mut inner = self.inner.lock().unwrap();
                    inner.leaves.insert(vdev_file.uuid(), pathbuf);
                    let rl: vdev_raid::Label = reader.deserialize().unwrap();
                    inner.raids.insert(rl.uuid, rl);
                    let _cl: cluster::Label = reader.deserialize().unwrap();
                    let pl: pool::Label = reader.deserialize().unwrap();
                    inner.pools.insert(pl.uuid, pl);
                })
        })).unwrap();
    }
}
