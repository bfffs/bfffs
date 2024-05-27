// vim: tw=80
//! The Black Footed Ferret File System
//!
//! This library is for programmatic access to BFFFS.  It is intended to be A
//! stable API.

use std::{
    collections::VecDeque,
    io,
    path::{Path, PathBuf},
};

pub use bfffs_core::{
    controller::TreeID,
    property::{Property, PropertyName},
};
use bfffs_core::{rpc, Uuid};
use futures::{stream, FutureExt, Stream, StreamExt, TryFutureExt};
use thiserror::Error as ThisError;
use tokio_seqpacket::UnixSeqpacket;

#[derive(Clone, Debug, ThisError, Eq, PartialEq)]
pub enum Error {
    #[error("Corrupt response from server")]
    CorruptResponse,
    #[error("No such device")]
    NoDevice,
    #[error("Server did not send response")]
    NoResponse,
    #[error("Server sent unexpectedly large response {0} bytes")]
    TooLarge(usize),
    #[error(transparent)]
    Other(#[from] bfffs_core::Error),
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Other(e.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Represents a single BFFFS vdev.  Could be a either a leaf device, or a
/// Mirror, or a Raid, or a Cluster.
pub enum Device {
    Path(PathBuf),
    Uuid(Uuid),
}

impl Device {
    fn into_uuid(path: PathBuf, status: rpc::pool::PoolStatus) -> Result<Uuid> {
        for clust in status.clusters.iter() {
            for mirror in clust.mirrors.iter() {
                for leaf in mirror.leaves.iter() {
                    if leaf.path == path {
                        return Ok(leaf.uuid);
                    }
                }
            }
        }
        Err(Error::NoDevice)
    }
}

impl From<Uuid> for Device {
    fn from(uuid: Uuid) -> Self {
        Device::Uuid(uuid)
    }
}

impl From<PathBuf> for Device {
    fn from(path: PathBuf) -> Self {
        Device::Path(path)
    }
}

/// A connection to the bfffsd server
#[derive(Debug)]
pub struct Bfffs {
    peer: UnixSeqpacket,
}

impl Bfffs {
    /// Connect to the server at the default address
    pub async fn default() -> Self {
        Self::new(Path::new("/var/run/bfffsd.sock")).await.unwrap()
    }

    /// Drop all in-memory caches, for testing or debugging purposes
    pub async fn drop_cache(&self) -> Result<()> {
        let req = rpc::Request::DebugDropCache;
        self.call(req)
            .await?
            .into_debug_drop_cache()
            .map_err(Error::from)
    }

    /// Create a new file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the new file system, including the pool
    /// `props`     -   Any non-default properties to set on the file system
    pub async fn fs_create(
        &self,
        fsname: String,
        props: Vec<Property>,
    ) -> Result<TreeID> {
        let req = rpc::fs::create(fsname, props);
        self.call(req).await?.into_fs_create().map_err(Error::from)
    }

    /// Destroy a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system, including the pool
    pub async fn fs_destroy(&self, fsname: String) -> Result<()> {
        let req = rpc::fs::destroy(fsname);
        self.call(req).await?.into_fs_destroy().map_err(Error::from)
    }

    /// List the given dataset and all of its children
    ///
    /// # Arguments
    ///
    /// `fsname`    -   The dataset to list, including pool name
    /// `props`     -   Properties to look up
    /// `offs`      -   A stream resume token.  It must be either `None` or the
    ///                 value returned from a previous call to this function.
    ///                 Children will be returned beginning after the entry
    ///                 whose offset is `offs`.
    /// `depth`     -   Only return datasets this far underneath the named
    ///                 ones.  `0` means return the named ones only, `1` means
    ///                 return the named ones and its children, `usize::MAX`
    ///                 means fully recurse.
    // Modeled after getdirentries(2).
    pub fn fs_list(
        &self,
        name: String,
        props: Vec<PropertyName>,
        offs: Option<u64>,
        depth: usize,
    ) -> impl Stream<Item = Result<rpc::fs::DsInfo>> + '_ {
        struct State {
            offs:     Option<u64>,
            // The usize is the depth of children to list beneath this dataset
            datasets: VecDeque<(usize, String)>,
            results:  VecDeque<rpc::fs::DsInfo>,
            // The depth of children to list beneath the stuff in results
            depth:    usize,
        }

        let req = rpc::fs::stat(name.clone(), props.clone());
        let parent_fut = self.call(req).map(|r| {
            match r {
                Ok(resp) => resp.into_fs_stat().map_err(Error::from),
                Err(e) => Err(e),
            }
        });
        let parent_stream = stream::once(parent_fut);
        let datasets = if depth > 0 {
            VecDeque::from(vec![(depth, name)])
        } else {
            VecDeque::new()
        };
        let state = State {
            offs,
            datasets,
            results: VecDeque::new(),
            depth: depth.saturating_sub(1),
        };
        let children_stream = stream::try_unfold(state, move |mut state| {
            let props2 = props.clone();
            async move {
                if state.results.is_empty() {
                    loop {
                        let dsname =
                            if let Some((depth, n)) = state.datasets.front() {
                                debug_assert!(*depth > 0);
                                state.depth = *depth;
                                n.to_owned()
                            } else {
                                return Ok(None);
                            };
                        let props3 = props2.clone();
                        let req = rpc::fs::list(dsname, props3, state.offs);
                        state.results =
                            self.call(req).await?.into_fs_list()?.into();
                        if state.results.is_empty() {
                            state.offs = None;
                            state.datasets.pop_front();
                            continue;
                        }
                        break;
                    }
                }
                let x = state.results.pop_front().map(|dsinfo| {
                    if state.depth > 1 {
                        state
                            .datasets
                            .push_back((state.depth - 1, dsinfo.name.clone()));
                    }
                    state.offs = Some(dsinfo.offset);
                    (dsinfo, state)
                });
                Ok(x)
            }
        });
        parent_stream.chain(children_stream)
    }

    /// Mount a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system to mount, including the pool
    pub async fn fs_mount(&self, fsname: String) -> Result<()> {
        let req = rpc::fs::mount(fsname);
        self.call(req).await?.into_fs_mount().map_err(Error::from)
    }

    /// Set properties on a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system to mount, including the pool
    /// `props`     -   Properties to set
    pub async fn fs_set(
        &self,
        fsname: String,
        props: Vec<Property>,
    ) -> Result<()> {
        let req = rpc::fs::set(fsname, props);
        self.call(req).await?.into_fs_set().map_err(Error::from)
    }

    /// Unmount a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system to mount, including the pool
    /// `force`     -   Forcibly unmount the file system, even if still in use.
    pub async fn fs_unmount(&self, fsname: &str, force: bool) -> Result<()> {
        let req = rpc::fs::unmount(fsname.to_owned(), force);
        self.call(req).await?.into_fs_unmount().map_err(Error::from)
    }

    /// Connect to the server whose socket is at this path
    pub async fn new(sock: &Path) -> Result<Self> {
        let peer = UnixSeqpacket::connect(sock).await.map_err(Error::from)?;
        Ok(Self { peer })
    }

    /// Clean freed space on a pool
    pub async fn pool_clean(&self, pool: String) -> Result<()> {
        let req = rpc::pool::clean(pool);
        self.call(req).await?.into_pool_clean().map_err(Error::from)
    }

    /// Mark a disk or mirror as faulted.
    ///
    /// Disks may be specified by either device path or UUID.  Mirrors may only
    /// be specified by UUID.
    pub async fn pool_fault<D>(&self, pool: String, d: D) -> Result<()>
    where
        D: Into<Device>,
    {
        let dev = d.into();
        let uuid = match dev {
            Device::Uuid(uuid) => uuid,
            Device::Path(path) => {
                let status = self.pool_status(pool.clone()).await?;
                Device::into_uuid(path, status)?
            }
        };
        let req = rpc::pool::fault(pool, uuid);
        self.call(req).await?.into_pool_fault().map_err(Error::from)
    }

    /// List one or more active pools
    ///
    /// # Arguments
    ///
    /// `pool`      -   Optional pool to list.  If not given, list all.
    /// `offs`      -   A stream resume token.  It must be either `None` or the
    ///                 value returned from a previous call to this function.
    ///                 Children will be returned beginning after the entry
    ///                 whose offset is `offs`.
    pub fn pool_list(
        &self,
        pool: Option<String>,
        _offs: Option<u64>,
    ) -> impl Stream<Item = Result<rpc::pool::PoolInfo>> + '_ {
        let req = rpc::pool::list(pool, None);
        self.call(req)
            .map(|r| {
                match r {
                    Ok(resp) => resp.into_pool_list().map_err(Error::from),
                    Err(e) => Err(e),
                }
            })
            .map_ok(|v: Vec<rpc::pool::PoolInfo>| {
                stream::iter(v.into_iter().map(Ok))
            })
            .try_flatten_stream()
    }

    pub async fn pool_status(
        &self,
        pool: String,
    ) -> Result<rpc::pool::PoolStatus> {
        let req = rpc::pool::status(pool);
        self.call(req)
            .await?
            .into_pool_status()
            .map_err(Error::from)
    }

    /// Submit an RPC request to the server
    async fn call(&self, req: rpc::Request) -> Result<rpc::Response> {
        const BUFSIZ: usize = 4096;

        let encoded: Vec<u8> = bincode::serialize(&req).unwrap();
        let nwrite = self.peer.send(&encoded).await.map_err(Error::from)?;
        assert_eq!(nwrite, encoded.len());

        let mut buf = vec![0u8; BUFSIZ];
        let nread = self.peer.recv(&mut buf).await.map_err(Error::from)?;
        if nread == 0 {
            Err(Error::NoResponse)
        } else if nread >= BUFSIZ {
            Err(Error::TooLarge(nread))
        } else {
            buf.truncate(nread);
            let resp = bincode::deserialize::<rpc::Response>(&buf[..])
                .map_err(|_| Error::CorruptResponse)?;
            Ok(resp)
        }
    }
}

#[cfg(test)]
mod t {
    use super::*;

    mod into_uuid {
        use bfffs_core::{
            rpc::pool::{ClusterStatus, LeafStatus, MirrorStatus, PoolStatus},
            vdev::Health,
        };

        use super::*;

        #[test]
        fn disk() {
            let device_uuid = Uuid::new_v4();
            let path = PathBuf::from("/dev/da0");

            let stat = PoolStatus {
                health:   Health::Online,
                name:     "TestPool".to_string(),
                clusters: vec![ClusterStatus {
                    health:  Health::Online,
                    codec:   "NonRedundant".to_string(),
                    mirrors: vec![MirrorStatus {
                        health: Health::Online,
                        leaves: vec![LeafStatus {
                            health: Health::Online,
                            path:   PathBuf::from("/dev/da0"),
                            uuid:   device_uuid,
                        }],
                        uuid:   Uuid::default(),
                    }],
                    uuid:    Uuid::default(),
                }],
                uuid:     Uuid::default(),
            };

            assert_eq!(Ok(device_uuid), Device::into_uuid(path, stat));
        }

        #[test]
        fn enoent() {
            let device_uuid = Uuid::new_v4();
            let path = PathBuf::from("/dev/da1");

            let stat = PoolStatus {
                health:   Health::Online,
                name:     "TestPool".to_string(),
                clusters: vec![ClusterStatus {
                    health:  Health::Online,
                    codec:   "NonRedundant".to_string(),
                    mirrors: vec![MirrorStatus {
                        health: Health::Online,
                        leaves: vec![LeafStatus {
                            health: Health::Online,
                            path:   PathBuf::from("/dev/da0"),
                            uuid:   device_uuid,
                        }],
                        uuid:   Uuid::default(),
                    }],
                    uuid:    Uuid::default(),
                }],
                uuid:     Uuid::default(),
            };

            assert_eq!(Err(Error::NoDevice), Device::into_uuid(path, stat));
        }
    }
}
