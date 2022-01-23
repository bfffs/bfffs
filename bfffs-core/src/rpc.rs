// vim: tw=80
//! RPC definitions for communication between bfffs commands and the daemon
// Even though this stuff isn't consumed anywhere in bfffs-core, it must reside
// here rather than in the bfffs crate because it may need to be compiled with
// or without no_std.

use crate::{
    Error,
    controller::TreeID
};
use serde_derive::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct FsMount {
    pub mountpoint: PathBuf,
    /// Comma-separated mount options
    pub opts: String,
    pub tree_id: TreeID
}

/// An RPC request from bfffs to bfffsd
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    FsMount(FsMount)
}

impl Request {
    pub fn fs_mount(mountpoint: PathBuf, tree_id: TreeID) -> Self {
        Self::FsMount(FsMount {
            mountpoint,
            opts: String::new(),    // TODO
            tree_id
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    FsMount(Result<(), Error>)
}
