// vim: tw=80
//! RPC definitions for communication between bfffs commands and the daemon
// Even though this stuff isn't consumed anywhere in bfffs-core, it must reside
// here rather than in the bfffs crate because it may need to be compiled with
// or without no_std.

use crate::{
    controller::TreeID,
    Result,
};
use serde_derive::{Deserialize, Serialize};

pub mod fs {
    use crate::property::{Property, PropertyName, PropertySource};
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
    pub struct Destroy {
        pub name: String,
    }

    pub fn destroy(name: String) -> Request {
        Request::FsDestroy(Destroy{name})
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct DsInfo {
        pub name:   String,
        pub props:  Vec<(Property, PropertySource)>,
        pub offset: u64
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct List {
        pub name: String,
        pub props: Vec<PropertyName>,
        pub offset: Option<u64>
    }

    /// Like `readdirplus`, list all of a dataset's children with the requested
    /// properties.
    ///
    /// The named dataset itself will not be included.  If `offset` is provided,
    /// it can be used to resume a previous listing, as in `getdirentries`.
    pub fn list(name: String, props: Vec<PropertyName>, offset: Option<u64>)
        -> Request
    {
        Request::FsList(List{name, props, offset})
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Mount {
        /// Comma-separated mount options
        pub opts: String,
        /// File system name, including the pool
        pub name: String,
    }

    pub fn mount(name: String) -> Request {
        Request::FsMount(Mount {
            opts: String::new(),    // TODO
            name
        })
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Set {
        /// File system name, including the pool
        pub name: String,
        /// Dataset properties
        pub props: Vec<Property>
    }

    pub fn set(name: String, props: Vec<Property>) -> Request {
        Request::FsSet(Set {
            name,
            props
        })
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Stat {
        pub name: String,
        pub props: Vec<PropertyName>,
    }

    /// Lookup the requested properties for a single dataset
    pub fn stat(name: String, props: Vec<PropertyName>) -> Request {
        Request::FsStat(Stat{name, props})
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Unmount {
        /// Forcibly unmount, even if in-use
        pub force: bool,
        /// File system name, including the pool
        pub name: String,
    }

    pub fn unmount(name: String, force: bool) -> Request {
        Request::FsUnmount(Unmount {
            name,
            force
        })
    }

}

pub mod pool {
    use std::path::PathBuf;
    use super::Request;
    use serde_derive::{Deserialize, Serialize};
    use crate::{vdev::Health, Uuid};

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    pub struct PoolInfo {
        /// Pool name.
        pub name: String,
        /// Stream resume token
        pub offs: u64
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    pub struct LeafStatus {
        pub health: Health,
        pub path: PathBuf,
        pub uuid: Uuid
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    pub struct MirrorStatus {
        pub leaves: Vec<LeafStatus>,
        pub health: Health,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    pub struct ClusterStatus {
        pub health: Health,
        pub codec: String,
        pub mirrors: Vec<MirrorStatus>
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    pub struct PoolStatus {
        pub health: Health,
        pub name: String,
        pub clusters: Vec<ClusterStatus>
    }


    #[derive(Debug, Deserialize, Serialize)]
    pub struct Clean {
        pub pool: String
    }

    pub fn clean(pool: String) -> Request {
        Request::PoolClean(Clean {
            pool
        })
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct List {
        pub pool: Option<String>,
        pub offset: Option<u64>
    }

    /// Like `readdirplus`, list one or all imported pools with the requested
    /// properties.
    ///
    /// If `offset` is provided, it can be used to resume a previous listing, as
    /// in `getdirentries`.
    pub fn list(pool: Option<String>, offset: Option<u64>) -> Request {
        Request::PoolList(List { pool, offset })
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Status {
        pub pool: String
    }

    /// Get the health and configuration of a single pool.
    pub fn status(pool: String) -> Request {
        Request::PoolStatus(Status { pool})
    }
}

/// An RPC request from bfffs to bfffsd
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    DebugDropCache,
    FsCreate(fs::Create),
    FsDestroy(fs::Destroy),
    FsList(fs::List),
    FsMount(fs::Mount),
    FsSet(fs::Set),
    FsStat(fs::Stat),
    FsUnmount(fs::Unmount),
    PoolClean(pool::Clean),
    PoolList(pool::List),
    PoolStatus(pool::Status)
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    DebugDropCache(Result<()>),
    FsCreate(Result<TreeID>),
    FsDestroy(Result<()>),
    FsList(Result<Vec<fs::DsInfo>>),
    FsMount(Result<()>),
    FsSet(Result<()>),
    FsStat(Result<fs::DsInfo>),
    FsUnmount(Result<()>),
    PoolClean(Result<()>),
    PoolList(Result<Vec<pool::PoolInfo>>),
    PoolStatus(Result<pool::PoolStatus>),
}

impl Response {
    pub fn into_debug_drop_cache(self) -> Result<()> {
        match self {
            Response::DebugDropCache(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_create(self) -> Result<TreeID> {
        match self {
            Response::FsCreate(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_destroy(self) -> Result<()> {
        match self {
            Response::FsDestroy(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_list(self) -> Result<Vec<fs::DsInfo>> {
        match self {
            Response::FsList(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_mount(self) -> Result<()> {
        match self {
            Response::FsMount(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_set(self) -> Result<()> {
        match self {
            Response::FsSet(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_stat(self) -> Result<fs::DsInfo> {
        match self {
            Response::FsStat(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_pool_clean(self) -> Result<()> {
        match self {
            Response::PoolClean(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_pool_list(self) -> Result<Vec<pool::PoolInfo>> {
        match self {
            Response::PoolList(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_pool_status(self) -> Result<pool::PoolStatus> {
        match self {
            Response::PoolStatus(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }

    pub fn into_fs_unmount(self) -> Result<()> {
        match self {
            Response::FsUnmount(r) => r,
            x => panic!("Unexpected response type {x:?}")
        }
    }
}
