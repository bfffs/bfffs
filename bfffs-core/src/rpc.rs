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
    use super::Request;
    use serde_derive::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Clean {
        pub pool: String
    }

    pub fn clean(pool: String) -> Request {
        Request::PoolClean(Clean {
            pool
        })
    }
}

/// An RPC request from bfffs to bfffsd
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    FsCreate(fs::Create),
    FsDestroy(fs::Destroy),
    FsList(fs::List),
    FsMount(fs::Mount),
    FsUnmount(fs::Unmount),
    PoolClean(pool::Clean)
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    FsCreate(Result<TreeID>),
    FsDestroy(Result<()>),
    FsList(Result<Vec<fs::DsInfo>>),
    FsMount(Result<()>),
    FsUnmount(Result<()>),
    PoolClean(Result<()>),
}

impl Response {
    pub fn into_fs_create(self) -> Result<TreeID> {
        match self {
            Response::FsCreate(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }

    pub fn into_fs_destroy(self) -> Result<()> {
        match self {
            Response::FsDestroy(r) => r,
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

    pub fn into_pool_clean(self) -> Result<()> {
        match self {
            Response::PoolClean(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }

    pub fn into_fs_unmount(self) -> Result<()> {
        match self {
            Response::FsUnmount(r) => r,
            x => panic!("Unexpected response type {:?}", x)
        }
    }
}
