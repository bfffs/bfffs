// vim: tw=80

use crate::common::{pool, vdev::Vdev, vdev_raid};
#[cfg(not(test))]
use crate::common::{Error, cache, cluster, database, ddml, idml, label,
    vdev_block};
use futures::{Future, future};
#[cfg(not(test))] use futures::{
    Stream,
    stream,
    sync::oneshot
};
use std::{
    borrow::ToOwned,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex
};
#[cfg(not(test))] use std::sync::{Arc, MutexGuard};
use crate::sys::vdev_file;
use uuid::Uuid;
use tokio::runtime::current_thread;
#[cfg(not(test))] use tokio::executor::{self, DefaultExecutor, Executor};

#[derive(Default)]
struct Inner {
    leaves: BTreeMap<Uuid, PathBuf>,
    raids: BTreeMap<Uuid, vdev_raid::Label>,
    pools: BTreeMap<Uuid, pool::Label>,
}

#[derive(Default)]
pub struct DevManager {
    inner: Mutex<Inner>
}

impl DevManager {
    /// Import a pool by its pool name
    // It would be nice to return a Future instead of a Result<Future>.  But if
    // we do that, then we can't automatically infer whether the result should
    // be Send (it's based on whether E: Send).  So until specialization is
    // available, we must do it like this.
    #[cfg(not(test))]
    pub fn import_by_name<E, S>(&self, name: S, handle: E)
        -> Result<impl Future<Item = database::Database, Error = Error>, Error>
        where E: Clone + Executor + 'static,
              S: AsRef<str>
    {
        let inner = self.inner.lock().unwrap();
        inner.pools.iter()
        .filter_map(|(uuid, label)| {
            if label.name == name.as_ref() {
                Some(*uuid)
            } else {
                None
            }
        })
        .nth(0)
        .ok_or(Error::ENOENT)
        .map(move |uuid| {
            self.import(uuid, handle, inner)
        })
    }

    /// Import a pool by its UUID
    // TODO: handle the ENOENT case
    #[cfg(not(test))]
    pub fn import_by_uuid<E>(&self, uuid: Uuid, handle: E)
        -> impl Future<Item = database::Database, Error = Error>
        where E: Clone + Executor + 'static
    {
        let inner = self.inner.lock().unwrap();
        self.import(uuid, handle, inner)
    }

    /// Import a pool that is already known to exist
    #[cfg(not(test))]
    fn import<E>(&self, uuid: Uuid, handle: E, inner: MutexGuard<Inner>)
        -> impl Future<Item = database::Database, Error = Error>
        where E: Clone + Executor + 'static
    {
        let (_pool, raids, mut leaves) = self.open_labels(uuid, inner);
        let proxies = raids.into_iter().map(move |raid| {
            let leaf_paths: Vec<PathBuf> = leaves.remove(&raid.uuid).unwrap();
            let (tx, rx) = oneshot::channel();
            // The top-level Executor spawn puts each Cluster onto a different
            // thread, when using tokio-io-pool
            DefaultExecutor::current().spawn(Box::new(future::lazy(move || {
                let fut = DevManager::open_cluster(leaf_paths, raid.uuid)
                .map(move |(cluster, reader)| {
                    let proxy = pool::ClusterProxy::new(cluster);
                    tx.send((proxy, reader))
                        .ok().expect("channel dropped too soon");
                }).map_err(Error::unhandled);
                executor::current_thread::TaskExecutor::current().spawn_local(
                    Box::new(fut)
                ).map_err(Error::unhandled)
            }))
            ).expect("DefaultExecutor::spawn failed");
            rx
        });
        future::join_all(proxies).map_err(|_| Error::EPIPE)
            .and_then(move |proxies| {
                pool::Pool::open(Some(uuid), proxies)
            }).map(|(pool, label_reader)| {
                let cache = cache::Cache::with_capacity(1_000_000_000);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                let (idml, label_reader) = idml::IDML::open(ddml, arc_cache,
                                                            label_reader);
                database::Database::open(Arc::new(idml), handle, label_reader)
            })
    }

    /// Import all of the clusters from a Pool.  For debugging purposes only.
    #[doc(hidden)]
    #[cfg(not(test))]
    pub fn import_clusters(&self, uuid: Uuid)
        -> impl Future<Item = Vec<cluster::Cluster>, Error = Error>
    {
        let inner = self.inner.lock().unwrap();
        let (_pool, raids, mut leaves) = self.open_labels(uuid, inner);
        let cfuts = raids.into_iter().map(move |raid| {
            let leaf_paths = leaves.remove(&raid.uuid).unwrap();
            DevManager::open_cluster(leaf_paths, raid.uuid)
            .map(|(cluster, _reader)| cluster)
        });
        future::join_all(cfuts)
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        let inner = self.inner.lock().unwrap();
        inner.pools.iter()
            .map(|(_uuid, label)| {
                (label.name.clone(), label.uuid)
            }).collect::<Vec<_>>()
    }

    #[cfg(not(test))]
    fn open_cluster(leaf_paths: Vec<PathBuf>, uuid: Uuid)
        -> impl Future<Item=(cluster::Cluster, label::LabelReader), Error=Error>
    {
        DevManager::open_vdev_blocks(leaf_paths)
        .and_then(move |vdev_blocks| {
            let (vdev_raid, reader) =
                vdev_raid::VdevRaid::open(Some(uuid), vdev_blocks);
            cluster::Cluster::open(vdev_raid)
            .map(move |cluster| (cluster, reader))
        })
    }

    #[cfg(not(test))]
    fn open_labels(&self, uuid: Uuid, mut inner: MutexGuard<Inner>)
        -> (pool::Label, Vec<vdev_raid::Label>, BTreeMap<Uuid, Vec<PathBuf>>)
    {
        let pool = inner.pools.remove(&uuid).unwrap();
        let raids = pool.children.iter()
            .map(|child_uuid| inner.raids.remove(child_uuid).unwrap())
            .collect::<Vec<_>>();
        let leaves = raids.iter().map(|raid| {
            let leaves = raid.children.iter().map(|uuid| {
                inner.leaves.remove(&uuid).unwrap()
            }).collect::<Vec<_>>();
            (raid.uuid, leaves)
        }).collect::<BTreeMap<_, _>>();
        // Drop the self.inner mutex
        (pool, raids, leaves)
    }

    #[cfg(not(test))]
    fn open_vdev_blocks(leaf_paths: Vec<PathBuf>)
        -> impl Future<Item=Vec<(vdev_block::VdevBlock, label::LabelReader)>,
                       Error=Error>
    {
        stream::iter_ok(leaf_paths.into_iter())
        .and_then(vdev_file::VdevFile::open)
        .map(|(leaf, reader)| {
            (vdev_block::VdevBlock::new(leaf), reader)
        }).collect()
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
                    let pl: pool::Label = reader.deserialize().unwrap();
                    inner.pools.insert(pl.uuid, pl);
                })
        })).unwrap();
    }
}