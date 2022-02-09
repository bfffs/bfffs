// vim: tw=80
//! The Black Footed Ferret File System
//!
//! This library is for programmatic access to BFFFS.  It is intended to be A
//! stable API.

use std::path::{Path, PathBuf};

use bfffs_core::rpc;
pub use bfffs_core::{controller::TreeID, property::Property, Error, Result};
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

    /// Mount a file system
    ///
    /// # Arguments
    ///
    /// `fsname`    -   Name of the file system to mount, including the pool
    /// `mountpoint` -  Path to mount the file system.
    pub async fn fs_mount(
        &self,
        fsname: String,
        mountpoint: PathBuf,
    ) -> Result<()> {
        let req = rpc::fs::mount(mountpoint, fsname);
        self.call(req).await.unwrap().into_fs_mount()
    }

    /// Connect to the server whose socket is at this path
    pub async fn new(sock: &Path) -> Result<Self> {
        let peer = UnixSeqpacket::connect(sock).await.map_err(Error::from)?;
        Ok(Self { peer })
    }

    /// Submit an RPC request to the server
    async fn call(&self, req: rpc::Request) -> Result<rpc::Response> {
        const BUFSIZ: usize = 128;

        let encoded: Vec<u8> = bincode::serialize(&req).unwrap();
        let nwrite = self.peer.send(&encoded).await.map_err(Error::from)?;
        assert_eq!(nwrite, encoded.len());

        let mut buf = vec![0u8; BUFSIZ];
        let nread = self.peer.recv(&mut buf).await.map_err(Error::from)?;
        if nread == 0 {
            eprintln!("Server did not send response");
            Err(Error::EIO)
        } else if nread >= BUFSIZ {
            eprintln!(
                "Server sent unexpectedly large response {} bytes",
                nread
            );
            Err(Error::EIO)
        } else {
            buf.truncate(nread);
            let resp = bincode::deserialize::<rpc::Response>(&buf[..])
                .expect("Corrupt response from server");
            Ok(resp)
        }
    }
}
