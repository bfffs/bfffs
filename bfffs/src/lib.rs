// vim: tw=80
//! The Black Footed Ferret File System
//!
//! This library is for programmatic access to BFFFS.  It is intended to be A
//! stable API.

use bfffs_core::rpc;
use std::path::Path;
use tokio_seqpacket::UnixSeqpacket;

pub use bfffs_core::Error;

// TODO: move definition into bfffs_core after
// https://gitlab.com/cardoe/enum-primitive-derive/-/issues/8 is fixed.
pub type Result<T> = ::std::result::Result<T, Error>;

/// A connection to the bfffsd server
#[derive(Debug)]
pub struct Bfffs {
    peer: UnixSeqpacket
}

impl Bfffs {
    /// Connect to the server at the default address
    pub async fn default() -> Self {
        Self::new(Path::new("/var/run/bfffsd.sock")).await.unwrap()
    }

    /// Connect to the server whose socket is at this path
    pub async fn new(sock: &Path) -> Result<Self> {
        let peer = UnixSeqpacket::connect(sock).await
            .map_err(Error::from)?;
        Ok(Self{peer})
    }

    /// Submit an RPC request to the server
    pub async fn call(&self, req: rpc::Request) -> Result<rpc::Response> {
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
                nread);
            Err(Error::EIO)
        } else {
            buf.truncate(nread);
            let resp = bincode::deserialize::<rpc::Response>(&buf[..])
                .expect("Corrupt response from server");
            Ok(resp)
        }
    }
}
