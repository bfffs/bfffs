// vim: tw=80

use crate::{
    Error, Result, Uuid, vdev::Vdev, cache, database, ddml, idml, label, mirror,
    pool, raid
};
use futures::{
    Future,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    stream::{self, FuturesOrdered, FuturesUnordered},
};
use mockall_double::double;
use std::{
    borrow::ToOwned,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex}
};

#[double] use crate::pool::Pool;
#[double] use crate::cluster::Cluster;
#[double] use crate::mirror::Mirror;
#[double] use crate::vdev_block::VdevBlock;
#[double] use crate::vdev_file::VdevFile;

/// Holds cached labels detected during tasting.
// NB: these labels may be out-of-date because we don't open devices exclusively
// until import time.
#[derive(Default)]
struct Inner {
    leaves: BTreeMap<Uuid, PathBuf>,
    mirrors: BTreeMap<Uuid, mirror::Label>,
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
        -> Result<database::Database>
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
        -> Result<database::Database>
    {
        self.import(uuid).await
    }

    /// Import a pool that is already known to exist
    async fn import(&self, uuid: Uuid) -> Result<database::Database>
    {
        let (_pool, raids, mut mirrors, mut leaves) = self.open_labels(uuid)?;
        let combined_clusters = raids.into_iter()
        .map(move |raid| {
            let mirror_labels = mirrors.remove(&raid.uuid()).unwrap();
            mirror_labels.iter()
                .map(|mirror_label| {
                    let leaf_paths = leaves.remove(&mirror_label.uuid).unwrap();
                    DevManager::open_mirror(mirror_label.uuid, leaf_paths)
                }).collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .and_then(move |mirrors| DevManager::open_cluster(mirrors, raid.uuid()))
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
    pub async fn import_clusters(&self, uuid: Uuid) -> Result<Vec<Cluster>>
    {
        let (_pool, raids, mut mirrors, mut leaves) = self.open_labels(uuid)?;
        raids.into_iter()
        .map(move |raid| {
            let mirror_labels = mirrors.remove(&raid.uuid()).unwrap();
            mirror_labels.iter()
                .map(|mirror_label| {
                    let leaf_paths = leaves.remove(&mirror_label.uuid).unwrap();
                    DevManager::open_mirror(mirror_label.uuid, leaf_paths)
                }).collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .and_then(move |mirrors| DevManager::open_cluster(mirrors, raid.uuid()))
        }).collect::<FuturesOrdered<_>>()
        .map_ok(|(cluster, _reader)| cluster)
        .try_collect::<Vec<_>>().await
    }

    /// List every pool that hasn't been imported, but can be
    pub fn importable_pools(&self) -> Vec<(String, Uuid)> {
        let inner = self.inner.lock().unwrap();
        inner.pools.values()
            .map(|label| {
                (label.name.clone(), label.uuid)
            }).collect::<Vec<_>>()
    }

    fn open_cluster(
        mirrors: Vec<(Mirror, label::LabelReader)>,
        uuid: Uuid
    ) -> impl Future<Output=Result<(Cluster, label::LabelReader)>>
    {
        let (vdev_raid_api, reader) = raid::open(Some(uuid), mirrors);
        Cluster::open(vdev_raid_api)
            .map_ok(move |cluster| (cluster, reader))
    }

    fn open_mirror(uuid: Uuid, leaf_paths: Vec<PathBuf>)
        -> impl Future<Output=Result<(Mirror, label::LabelReader)>>
    {
        DevManager::open_vdev_blocks(leaf_paths)
        .map_ok(move |vdev_blocks| {
            Mirror::open(Some(uuid), vdev_blocks)
        })
    }

    fn open_labels(&self, uuid: Uuid)
        -> Result<(
            pool::Label,
            Vec<raid::Label>,
            BTreeMap<Uuid, Vec<mirror::Label>>,
            BTreeMap<Uuid, Vec<PathBuf>>
        )>
    {
        let mut inner = self.inner.lock().unwrap();
        inner.pools.remove(&uuid)
        .map(|pool| {
            let raids = pool.children.iter()
                .map(|child_uuid| inner.raids.remove(child_uuid).unwrap())
                .collect::<Vec<_>>();
            let mirrors = raids.iter().map(|raid| {
                let mirrors = raid.iter_children().map(|uuid| {
                    inner.mirrors.remove(uuid).unwrap()
                }).collect::<Vec<_>>();
                (raid.uuid(), mirrors)
            }).collect::<BTreeMap<_, _>>();
            let mut leaves = BTreeMap::new();
            for vmirrors in mirrors.values() {
                for mirror in vmirrors {
                    let vleaves = mirror.children.iter().map(|uuid| {
                        inner.leaves.remove(uuid).unwrap()
                    }).collect::<Vec<_>>();
                    leaves.insert(mirror.uuid, vleaves);
                }
            }
            // Drop the self.inner mutex
            (pool, raids, mirrors, leaves)
        }).ok_or(Error::ENOENT)
    }

    fn open_vdev_blocks(leaf_paths: Vec<PathBuf>)
        -> impl Future<Output=Result<Vec<(VdevBlock, label::LabelReader)>>>
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
    // TODO: add a method for tasting disks in parallel.
    pub async fn taste<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        let pathbuf = p.as_ref().to_owned();
        let (vdev_file, mut reader) = VdevFile::open(p).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.leaves.insert(vdev_file.uuid(), pathbuf);
        let ml: mirror::Label = reader.deserialize().unwrap();
        inner.mirrors.insert(ml.uuid, ml);
        let rl: raid::Label = reader.deserialize().unwrap();
        inner.raids.insert(rl.uuid(), rl);
        let pl: pool::Label = reader.deserialize().unwrap();
        inner.pools.insert(pl.uuid, pl);
        Ok(())
    }

    /// Set the maximum amount of dirty cached data, in bytes.
    ///
    /// This is independent of [`self.cache_size`].
    pub fn writeback_size(&mut self, writeback_size: usize) {
        self.writeback_size = Some(writeback_size);
    }

}
