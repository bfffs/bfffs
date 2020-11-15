// vim: tw=80

#[cfg(not(test))]
use crate::common::vdev::Vdev;
use crate::common::{Error, Uuid, cache, database, ddml, idml, label, pool,
                    raid};
use futures::{
    Future,
    FutureExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    channel::oneshot,
    stream,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard}
};
#[cfg(not(test))]
use std::{
    borrow::ToOwned,
    path::Path
};
#[cfg(not(test))]
use tokio::runtime;
use tokio::runtime::Handle;

#[cfg(not(test))] use crate::common::pool::Pool;
#[cfg(test)] use crate::common::pool::MockPool as Pool;

#[cfg(not(test))] use crate::common::cluster::Cluster;
#[cfg(test)] use crate::common::cluster::MockCluster as Cluster;

#[cfg(not(test))] use crate::common::vdev_block::VdevBlock;
#[cfg(test)] use crate::common::vdev_block::MockVdevBlock as VdevBlock;

#[cfg(not(test))] use crate::common::vdev_file::VdevFile;
#[cfg(test)] use crate::common::vdev_file::MockVdevFile as VdevFile;

#[derive(Default)]
struct Inner {
    leaves: BTreeMap<Uuid, PathBuf>,
    raids: BTreeMap<Uuid, raid::Label>,
    pools: BTreeMap<Uuid, pool::Label>,
}

#[derive(Default)]
pub struct DevManager {
    inner: Mutex<Inner>
}

impl DevManager {
    /// Import a pool by its pool name
    pub fn import_by_name<S>(&self, name: S, handle: Handle)
        -> impl Future<Output=Result<database::Database, Error>>
        where S: AsRef<str>
    {
        let inner = self.inner.lock().unwrap();
        let r = inner.pools.iter()
        .filter_map(|(uuid, label)| {
            if label.name == name.as_ref() {
                Some(*uuid)
            } else {
                None
            }
        }).nth(0);
        match r {
            Some(uuid) => self.import(uuid, handle, inner).boxed(),
            None => future::err(Error::ENOENT).boxed()
       }
    }

    /// Import a pool by its UUID
    // TODO: handle the ENOENT case
    pub fn import_by_uuid(&self, uuid: Uuid, handle: Handle)
        -> impl Future<Output=Result<database::Database, Error>>
    {
        let inner = self.inner.lock().unwrap();
        self.import(uuid, handle, inner)
    }

    /// Import a pool that is already known to exist
    fn import(&self, uuid: Uuid, handle: Handle, inner: MutexGuard<Inner>)
        -> impl Future<Output=Result<database::Database, Error>>
    {
        let h2 = handle.clone();
        let (_pool, raids, mut leaves) = self.open_labels(uuid, inner);
        let proxies = raids.into_iter().map(move |raid| {
            let leaf_paths: Vec<PathBuf> = leaves.remove(&raid.uuid()).unwrap();
            let (tx, rx) = oneshot::channel();
            handle.spawn(async move {
                let fut = DevManager::open_cluster(leaf_paths, raid.uuid())
                .map_ok(move |(cluster, reader)| {
                    let proxy = pool::ClusterProxy::new(cluster);
                    tx.send((proxy, reader))
                        .ok().expect("channel dropped too soon");
                }).map_err(Error::unhandled);
                fut
                .await
            });
            rx
        });
        future::try_join_all(proxies).map_err(|_| Error::EPIPE)
            .and_then(move |proxies| {
                Pool::open(Some(uuid), proxies)
            }).map_ok(|(pool, label_reader)| {
                let cache = cache::Cache::with_capacity(1_000_000_000);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                let (idml, label_reader) = idml::IDML::open(ddml, arc_cache,
                                                            label_reader);
                database::Database::open(Arc::new(idml), h2, label_reader)
            })
    }

    /// Import all of the clusters from a Pool.  For debugging purposes only.
    #[doc(hidden)]
    pub fn import_clusters(&self, uuid: Uuid)
        -> impl Future<Output=Result<Vec<Cluster>, Error>>
    {
        let inner = self.inner.lock().unwrap();
        let (_pool, raids, mut leaves) = self.open_labels(uuid, inner);
        let cfuts = raids.into_iter().map(move |raid| {
            let leaf_paths = leaves.remove(&raid.uuid()).unwrap();
            DevManager::open_cluster(leaf_paths, raid.uuid())
            .map_ok(|(cluster, _reader)| cluster)
        });
        future::try_join_all(cfuts)
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        let inner = self.inner.lock().unwrap();
        inner.pools.iter()
            .map(|(_uuid, label)| {
                (label.name.clone(), label.uuid)
            }).collect::<Vec<_>>()
    }

    fn open_cluster(leaf_paths: Vec<PathBuf>, uuid: Uuid)
        -> impl Future<Output=Result<(Cluster, label::LabelReader), Error>>
    {
        DevManager::open_vdev_blocks(leaf_paths)
        .and_then(move |vdev_blocks| {
            let (vdev_raid_api, reader) = raid::open(Some(uuid), vdev_blocks);
            Cluster::open(vdev_raid_api)
            .map_ok(move |cluster| (cluster, reader))
        })
    }

    fn open_labels(&self, uuid: Uuid, mut inner: MutexGuard<Inner>)
        -> (pool::Label, Vec<raid::Label>, BTreeMap<Uuid, Vec<PathBuf>>)
    {
        let pool = inner.pools.remove(&uuid).unwrap();
        let raids = pool.children.iter()
            .map(|child_uuid| inner.raids.remove(child_uuid).unwrap())
            .collect::<Vec<_>>();
        let leaves = raids.iter().map(|raid| {
            let leaves = raid.iter_children().map(|uuid| {
                inner.leaves.remove(&uuid).unwrap()
            }).collect::<Vec<_>>();
            (raid.uuid(), leaves)
        }).collect::<BTreeMap<_, _>>();
        // Drop the self.inner mutex
        (pool, raids, leaves)
    }

    fn open_vdev_blocks(leaf_paths: Vec<PathBuf>)
        -> impl Future<Output=Result<Vec<(VdevBlock, label::LabelReader)>,
                       Error>>
    {
        stream::iter(leaf_paths.into_iter())
        .map(|path| Ok(path))
        .and_then(VdevFile::open)
        .map_ok(|(leaf, reader)| {
            (VdevBlock::new(leaf), reader)
        }).try_collect()
    }

    /// Taste the device identified by `p` for an BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    // Disable in test mode because MockVdevFile::Open requires P: 'static
    // TODO: add a method for tasting disks in parallel.
    #[cfg(not(test))]
    pub fn taste<P: AsRef<Path>>(&self, p: P) {
        // taste should be called from the synchronous domain, so it needs to
        // create its own temporary Runtime
        let mut rt = runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async move {
            let pathbuf = p.as_ref().to_owned();
            VdevFile::open(p)
            .map_ok(move |(vdev_file, mut reader)| {
                let mut inner = self.inner.lock().unwrap();
                inner.leaves.insert(vdev_file.uuid(), pathbuf);
                let rl: raid::Label = reader.deserialize().unwrap();
                inner.raids.insert(rl.uuid(), rl);
                let pl: pool::Label = reader.deserialize().unwrap();
                inner.pools.insert(pl.uuid, pl);
            }).await
        }).unwrap();
    }
}
