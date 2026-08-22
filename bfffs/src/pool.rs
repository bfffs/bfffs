use std::{
    mem,
    num::NonZeroU64,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bfffs_core::{
    cache::Cache,
    cluster::Cluster,
    controller::Controller,
    database::Database,
    ddml::DDML,
    idml::IDML,
    mirror::Mirror,
    pool::Pool,
    property::Property,
    raid,
};

use super::{Error, Result};

#[derive(Debug, Clone)]
struct ClusterSpec {
    k:       i16,
    f:       i16,
    mirrors: Vec<Vec<PathBuf>>,
}

impl ClusterSpec {
    fn new<T: Into<Vec<Vec<PathBuf>>>>(k: i16, f: i16, mirrors: T) -> Self {
        Self {
            k,
            f,
            mirrors: mirrors.into(),
        }
    }
}

/// Used to construct new pools.  A bfffsd connection is not required.
///
/// # Examples
///
/// Create a pool equivalent to this command line:
/// `bfffs pool create -p atime=off tank raid 3 1 mirror a b mirror c d mirror e f mirror g h
/// mirror i j`
/// ```
/// use bfffs::{Property, pool};
/// use std::{env, fs};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let td = tempfile::tempdir().unwrap();
/// env::set_current_dir(td.path()).unwrap();
/// for fname in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
///      fs::File::create_new(fname).unwrap().set_len(64 << 20);
/// }
///
/// pool::Builder::new("tank")
///     .set_prop(Property::Atime(false))
///     .add_mirror(["a", "b"])
///     .add_mirror(["c", "d"])
///     .add_mirror(["e", "f"])
///     .add_mirror(["g", "h"])
///     .add_mirror(["i", "j"])
///     .add_raid_cluster(3, 1)
///     .build()
///     .await
///     .unwrap();
/// # }
/// ```
#[derive(Default)]
pub struct Builder {
    clusters:   Vec<ClusterSpec>,
    mirrors:    Vec<Vec<PathBuf>>,
    name:       String,
    properties: Vec<Property>,
    zone_size:  Option<NonZeroU64>,
}

impl Builder {
    /// Create a new Builder to construct a pool named `name`.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Add a non-mirrored disk, for use later with [`add_raid_cluster']
    pub fn add_disk<P: Into<PathBuf>>(&mut self, dev: P) -> &mut Self {
        self.mirrors.push(vec![dev.into()]);
        self
    }

    /// Add a mirror vdev, for use later with [`add_raid_cluster`]
    pub fn add_mirror<I, P>(&mut self, devs: I) -> &mut Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.mirrors
            .push(devs.into_iter().map(|p| p.into()).collect());
        self
    }

    /// Add a mirrored cluster, without raid
    pub fn add_mirror_cluster<I, P>(&mut self, devs: I) -> &mut Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        let v: Vec<PathBuf> = devs.into_iter().map(|p| p.into()).collect();
        self.clusters.push(ClusterSpec::new(1, 0, [v]));
        self
    }

    /// Add a cluster that is a single device; no mirror, no raid
    pub fn add_nonredundant_cluster<P: Into<PathBuf>>(
        &mut self,
        dev: P,
    ) -> &mut Self {
        self.clusters
            .push(ClusterSpec::new(1, 0, [vec![dev.into()]]));
        self
    }

    /// Add a RAID cluster.  It will consume all of the vdevs previously added by [`add_disk`] and
    /// [`add_mirror`]
    ///
    /// # Arguments
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than or
    ///                         equal to the number of disks in `paths`.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    // TODO: allow setting the chunksize
    pub fn add_raid_cluster(
        &mut self,
        disks_per_stripe: i16,
        redundancy: i16,
    ) -> &mut Self {
        let mirrors = mem::take(&mut self.mirrors);
        self.clusters.push(ClusterSpec::new(
            disks_per_stripe,
            redundancy,
            mirrors,
        ));
        self
    }

    pub async fn build(&mut self) -> Result<()> {
        let mut clusters = Vec::with_capacity(self.clusters.len());
        for cl in self.clusters.drain(..) {
            let mut mirrors = Vec::with_capacity(cl.mirrors.len());
            for m in cl.mirrors.into_iter() {
                mirrors.push(Mirror::create(&m, self.zone_size)?);
            }
            let raid = raid::create(None, cl.k, cl.f, mirrors);
            clusters.push(Cluster::create(raid));
        }
        let pool = Pool::create(self.name.clone(), clusters);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::create(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        let db = Database::create(idml);
        let controller = Controller::new(db);
        // Create the root file system
        controller.create_fs(&self.name).await?;
        for prop in self.properties.drain(..) {
            controller.set_prop(&self.name, prop).await?;
        }
        controller.sync_transaction().await.map_err(Error::from)
    }

    /// Set properties to be assigned to the root dataset
    pub fn set_prop(&mut self, prop: Property) -> &mut Self {
        self.properties.push(prop);
        self
    }

    /// Set the simulated zone size on the pool, if using storage that isn't natively zoned.
    pub fn set_zone_size(&mut self, zone_size: NonZeroU64) -> &mut Self {
        self.zone_size = Some(zone_size);
        self
    }
}
