// vim: tw=80
//! RPC definitions for communication between bfffs commands and the daemon
// Even though this stuff isn't consumed anywhere in bfffs-core, it must reside
// here rather than in the bfffs crate because it may need to be compiled with
// or without no_std.

use crate::{
    controller::TreeID,
    Result
};
use serde_derive::{Deserialize, Serialize};

pub mod fs {
    use crate::property::Property;
    use std::path::PathBuf;
    use super::Request;
    use serde_derive::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Create {
        pub name: String,
        pub props: Vec<Property>,
    }

    pub fn create(name: String, props: Vec<Property>) -> Request {
        Request::FsCreate(Create{name, props})
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct DsInfo {
        pub name:   String,
        pub props:  Vec<Property>,
        pub offset: u64
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct List {
        pub name: String,
        pub offset: Option<u64>
    }

    pub fn list(name: String, offset: Option<u64>) -> Request {
        Request::FsList(List{name, offset})
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Mount {
        pub mountpoint: PathBuf,
        /// Comma-separated mount options
        pub opts: String,
        /// File system name, including with the pool
        pub name: String,
    }

    pub fn mount(mountpoint: PathBuf, name: String) -> Request {
        Request::FsMount(Mount {
            mountpoint,
            opts: String::new(),    // TODO
            name
        })
    }
}

/// An RPC request from bfffs to bfffsd
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    FsCreate(fs::Create),
    FsList(fs::List),
    FsMount(fs::Mount)
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    FsCreate(Result<TreeID>),
    FsList(Result<Vec<fs::DsInfo>>),
    FsMount(Result<()>)
}

impl Response {
    pub fn into_fs_create(self) -> Result<TreeID> {
        match self {
            Response::FsCreate(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }

    pub fn into_fs_list(self) -> Result<Vec<fs::DsInfo>> {
        match self {
            Response::FsList(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }

    pub fn into_fs_mount(self) -> Result<()> {
        match self {
            Response::FsMount(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }
}
