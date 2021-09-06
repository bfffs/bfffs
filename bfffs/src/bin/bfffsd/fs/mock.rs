// vim: tw=80
// LCOV_EXCL_START
//! Mock objects for bfffsd::fs

// rust-fuse contains many functions with > 7 arguments that we must redefine
// here for our mocks.
//#![allow(clippy::too_many_arguments)]

use bfffs_core::{
    database::{Database, TreeID},
    fs::{ExtAttr, ExtAttrNamespace, FileData, GetAttr, SetAttr},
    property::Property,
    SGList,
    RID,
};
use divbuf::DivBuf;
use mockall::mock;
use std::{
    ffi::{OsStr, OsString},
    sync::Arc,
};
use tokio::runtime::Handle;

/*
 * Mock BFFFS structs
 * ==================
 * We need to use mock! here instead of automock because bfffs is technically in
 * another crate, so it doesn't get built in test mode when bfffs-fuse does
 */
mock! {
    pub Fs {
        pub fn create(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub fn deleteextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<(), i32>;
        pub fn inactive(&self, fd: FileData);
        pub fn fsync(&self, fd: &FileData) -> Result<(), i32>;
        pub fn getattr(&self, fd: &FileData) -> Result<GetAttr, i32>;
        pub fn getextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr)
            -> Result<DivBuf, i32>;
        pub fn getextattrlen(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<u32, i32>;
        pub fn link(&self, parent: &FileData, fd: &FileData, name: &OsStr)
            -> Result<(), i32>;
        pub fn lookup<'a>(&self, grandparent: Option<&'a FileData>,
            parent: &'a FileData, name: &OsStr) -> Result<FileData, i32>;
        pub fn listextattr<F>(&self, fd: &FileData, size: u32, f: F)
            -> Result<Vec<u8>, i32>
            where F: Fn(&mut Vec<u8>, &ExtAttr<RID>) + Send + 'static;
        pub fn listextattrlen<F>(&self, fd: &FileData, f: F) -> Result<u32, i32>
            where F: Fn(&ExtAttr<RID>) -> u32 + Send + 'static;
        pub fn mkdir(&self, parent: &FileData, name: &OsStr, perm: u16, 
            id: u32, gid: u32) -> Result<FileData, i32>;
        pub fn mkblock(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, rdev: u32) -> Result<FileData, i32>;
        pub fn mkchar(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, rdev: u32) -> Result<FileData, i32>;
        pub fn mkfifo(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub fn mksock(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub fn new(database: Arc<Database>, handle: Handle, tree: TreeID)
            -> Self;
        pub fn read(&self, fd: &FileData, offset: u64, size: usize)
            -> Result<SGList, i32>;
        pub fn readdir(&self, fd: &FileData, soffs: i64)
            -> impl Iterator<Item=Result<(libc::dirent, i64), i32>> + Send;
        pub fn readlink(&self, fd: &FileData) -> Result<OsString, i32>;
        pub fn rename<'a>(&self, parent: &'a FileData, fd: &'a FileData,
            name: &'a OsStr, newparent: &'a FileData, newino: Option<u64>,
            newname: &'a OsStr)
            -> Result<u64, i32>;
        pub fn rmdir(&self, parent: &FileData, name: &OsStr) -> Result<(), i32>;
        pub fn root(&self) -> FileData;
        pub fn setattr(&self, fd: &FileData, mut attr: SetAttr)
            -> Result<(), i32>;
        pub fn setextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> Result<(), i32>;
        pub fn set_props(&mut self, props: Vec<Property>);
        pub fn statvfs(&self) -> Result<libc::statvfs, i32>;
        pub fn symlink(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, link: &OsStr) -> Result<FileData, i32>;
        pub fn sync(&self);
        pub fn unlink<'a>(&self, parent: &'a FileData, fd: Option<&'a FileData>,
            name: &'a OsStr)
            -> Result<(), i32>;
        // Change write's signature slightly.  The real write takes a IU:
        // Into<UIO>, but Mockall can't mock non-'static, non-reference
        // arguments.  So we change the argument to &[u8], which is how
        // bfffs-fuse uses it anyway.
        pub fn write(&self, fd: &FileData, offset: u64, data: &[u8],
            _flags: u32) -> Result<u32, i32>;
        //pub fn write<IU>(&self, fd: &FileData, offset: u64, data: IU,
            //_flags: u32) -> Result<u32, i32>
            //where IU: Into<bfffs::fs::Uio>;
    }
}
// LCOV_EXCL_STOP
