// vim: tw=80
//! The Black Footed Ferret File System
//!
//! This library is for programmatic access to BFFFS.  It is intended to be A
//! stable API.

use std::{collections::VecDeque, path::Path};

use bfffs_core::rpc;
pub use bfffs_core::{
    controller::TreeID,
    property::{Property, PropertyName},
    Error,
    Result,
};
use futures::{stream, Stream, StreamExt, TryFutureExt};
use tokio_seqpacket::UnixSeqpacket;

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
        self.call(req).await.unwrap().into_debug_drop_cache()
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
        self.call(req).await.unwrap().into_fs_create()
    }

    /// Destroy a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system, including the pool
    pub async fn fs_destroy(&self, fsname: String) -> Result<()> {
        let req = rpc::fs::destroy(fsname);
        self.call(req).await.unwrap().into_fs_destroy()
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
        let parent_fut = self
            .call(req)
            .map_ok(rpc::Response::into_fs_stat)
            .map_ok(Result::unwrap);
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
        self.call(req).await.unwrap().into_fs_mount()
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
        self.call(req).await.unwrap().into_fs_set()
    }

    /// Unmount a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system to mount, including the pool
    /// `force`     -   Forcibly unmount the file system, even if still in use.
    pub async fn fs_unmount(&self, fsname: &str, force: bool) -> Result<()> {
        let req = rpc::fs::unmount(fsname.to_owned(), force);
        self.call(req).await.unwrap().into_fs_unmount()
    }

    /// Connect to the server whose socket is at this path
    pub async fn new(sock: &Path) -> Result<Self> {
        let peer = UnixSeqpacket::connect(sock).await.map_err(Error::from)?;
        Ok(Self { peer })
    }

    /// Clean freed space on a pool
    pub async fn pool_clean(&self, pool: String) -> Result<()> {
        let req = rpc::pool::clean(pool);
        self.call(req).await.unwrap().into_pool_clean()
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
            eprintln!("Server did not send response");
            Err(Error::EIO)
        } else if nread >= BUFSIZ {
            eprintln!("Server sent unexpectedly large response {nread} bytes");
            Err(Error::EIO)
        } else {
            buf.truncate(nread);
            let resp = bincode::deserialize::<rpc::Response>(&buf[..])
                .expect("Corrupt response from server");
            Ok(resp)
        }
    }
}
