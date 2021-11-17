// vim: tw=80

#[cfg(not(test))]
use crate::vdev::Vdev;
use crate::{Error, Uuid, cache, database, ddml, idml, label, pool, raid};
use futures::{
    Future,
    FutureExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::{self, FuturesOrdered},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex}
};
#[cfg(not(test))]
use std::{
    borrow::ToOwned,
    path::Path
};

#[cfg(not(test))] use crate::pool::Pool;
#[cfg(test)] use crate::pool::MockPool as Pool;

#[cfg(not(test))] use crate::cluster::Cluster;
#[cfg(test)] use crate::cluster::MockCluster as Cluster;

#[cfg(not(test))] use crate::vdev_block::VdevBlock;
#[cfg(test)] use crate::vdev_block::MockVdevBlock as VdevBlock;

#[cfg(not(test))] use crate::vdev_file::VdevFile;
#[cfg(test)] use crate::vdev_file::MockVdevFile as VdevFile;

#[derive(Default)]
struct Inner {
    leaves: BTreeMap<Uuid, PathBuf>,
    raids: BTreeMap<Uuid, raid::Label>,
    pools: BTreeMap<Uuid, pool::Label>,
}

#[derive(Default)]
pub struct DevManager {
    cache_size: Option<usize>,
    inner: Mutex<Inner>,
    writeback_size: Option<usize>
}

impl DevManager {
    /// Set the maximum size in bytes of the Cache
    pub fn cache_size(&mut self, cache_size: usize) {
        self.cache_size = Some(cache_size);
    }

    /// Import a pool by its pool name
    pub async fn import_by_name<S>(&self, name: S)
        -> Result<database::Database, Error>
        where S: AsRef<str>
    {
        let r = self.inner.lock().unwrap().pools.iter()
        .filter_map(|(uuid, label)| {
            if label.name == name.as_ref() {
                Some(*uuid)
            } else {
                None
            }
        }).next();
        match r {
            Some(uuid) => self.import(uuid).await,
            None => Err(Error::ENOENT)
       }
    }

    /// Import a pool by its UUID
    pub async fn import_by_uuid(&self, uuid: Uuid)
        -> Result<database::Database, Error>
    {
        self.import(uuid).await
    }

    /// Import a pool that is already known to exist
    async fn import(&self, uuid: Uuid) -> Result<database::Database, Error>
    {
        let (_pool, raids, mut leaves) = self.open_labels(uuid)?;
        let combined_clusters = raids.into_iter()
        .map(move |raid| {
            let leaf_paths = leaves.remove(&raid.uuid()).unwrap();
            DevManager::open_cluster(leaf_paths, raid.uuid())
        }).collect::<FuturesOrdered<_>>()
        .try_collect::<Vec<_>>().await?;
        let (pool, label_reader) = Pool::open(Some(uuid), combined_clusters);
        let cs = self.cache_size.unwrap_or(1_073_741_824);
        let wbs = self.writeback_size.unwrap_or(268_435_456);
        let cache = cache::Cache::with_capacity(cs);
        let arc_cache = Arc::new(Mutex::new(cache));
        let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
        let (idml, label_reader) = idml::IDML::open(ddml, arc_cache,
            wbs, label_reader);
        Ok(database::Database::open(Arc::new(idml), label_reader))
    }

    /// Import all of the clusters from a Pool.  For debugging purposes only.
    #[doc(hidden)]
    pub fn import_clusters(&self, uuid: Uuid)
        -> impl Future<Output=Result<Vec<Cluster>, Error>>
    {
        let (_pool, raids, mut leaves) = match self.open_labels(uuid) {
            Ok((p, r, l)) => (p, r, l),
            Err(e) => return future::err(e).boxed()
        };
        raids.into_iter()
        .map(move |raid| {
            let leaf_paths = leaves.remove(&raid.uuid()).unwrap();
            DevManager::open_cluster(leaf_paths, raid.uuid())
            .map_ok(|(cluster, _reader)| cluster)
        }).collect::<FuturesOrdered<_>>()
        .try_collect::<Vec<_>>()
        .boxed()
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

    fn open_labels(&self, uuid: Uuid)
        -> Result<
            (pool::Label, Vec<raid::Label>, BTreeMap<Uuid, Vec<PathBuf>>),
            Error
        >
    {
        let mut inner = self.inner.lock().unwrap();
        inner.pools.remove(&uuid)
        .map(|pool| {
            let raids = pool.children.iter()
                .map(|child_uuid| inner.raids.remove(child_uuid).unwrap())
                .collect::<Vec<_>>();
            let leaves = raids.iter().map(|raid| {
                let leaves = raid.iter_children().map(|uuid| {
                    inner.leaves.remove(uuid).unwrap()
                }).collect::<Vec<_>>();
                (raid.uuid(), leaves)
            }).collect::<BTreeMap<_, _>>();
            // Drop the self.inner mutex
            (pool, raids, leaves)
        }).ok_or(Error::ENOENT)
    }

    fn open_vdev_blocks(leaf_paths: Vec<PathBuf>)
        -> impl Future<Output=Result<Vec<(VdevBlock, label::LabelReader)>,
                       Error>>
    {
        stream::iter(leaf_paths.into_iter())
        .map(Ok)
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
    pub async fn taste<P: AsRef<Path>>(&self, p: P) -> Result<(), Error> {
        let pathbuf = p.as_ref().to_owned();
        let (vdev_file, mut reader) = VdevFile::open(p).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.leaves.insert(vdev_file.uuid(), pathbuf);
        let rl: raid::Label = reader.deserialize().unwrap();
        inner.raids.insert(rl.uuid(), rl);
        let pl: pool::Label = reader.deserialize().unwrap();
        inner.pools.insert(pl.uuid, pl);
        Ok(())
    }

    /// Set the maximum amount of dirty cached data, in bytes.
    ///
    /// This is independent of [`cache_size`].
    pub fn writeback_size(&mut self, writeback_size: usize) {
        self.writeback_size = Some(writeback_size);
    }

}
