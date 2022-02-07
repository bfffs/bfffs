// vim: tw=80
// LCOV_EXCL_START
//! Mock objects for bfffsd::fs

use std::ffi::{OsStr, OsString};

use bfffs_core::{
    fs::{ExtAttr, ExtAttrNamespace, FileData, GetAttr, SeekWhence, SetAttr},
    property::Property,
    SGList,
};
use divbuf::DivBuf;
use futures::Stream;
use mockall::mock;

/*
 * Mock BFFFS structs
 * ==================
 * We need to use mock! here instead of automock because Fs is technically in
 * the bfffs-core crate, so it doesn't get built in test mode when bfffs does.
 */
mock! {
    pub Fs {
        pub async fn create(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub async fn deallocate(&self, fd: &FileData, offset: u64, len: u64)
            -> Result<(), i32>;
        pub async fn deleteextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<(), i32>;
        pub async fn inactive(&self, fd: FileData);
        pub async fn fsync(&self, fd: &FileData) -> Result<(), i32>;
        pub async fn getattr(&self, fd: &FileData) -> Result<GetAttr, i32>;
        pub async fn getextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr)
            -> Result<DivBuf, i32>;
        pub async fn getextattrlen(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<u32, i32>;
        pub async fn ilookup(&self, ino: u64) -> Result<FileData, i32>;
        pub async fn link(&self, parent: &FileData, fd: &FileData, name: &OsStr)
            -> Result<(), i32>;
        pub async fn lookup<'a>(&self, grandparent: Option<&'a FileData>,
            parent: &'a FileData, name: &OsStr) -> Result<FileData, i32>;
        pub async fn listextattr<F>(&self, fd: &FileData, size: u32, f: F)
            -> Result<Vec<u8>, i32>
            where F: Fn(&mut Vec<u8>, &ExtAttr) + Send + 'static;
        pub async fn listextattrlen<F>(&self, fd: &FileData, f: F) -> Result<u32, i32>
            where F: Fn(&ExtAttr) -> u32 + Send + 'static;
        pub async fn lseek(&self, fd: &FileData, mut offset: u64,
            whence: SeekWhence) -> Result<u64, i32>;
        pub async fn mkdir(&self, parent: &FileData, name: &OsStr, perm: u16,
            id: u32, gid: u32) -> Result<FileData, i32>;
        pub async fn mkblock(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, rdev: u32) -> Result<FileData, i32>;
        pub async fn mkchar(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, rdev: u32) -> Result<FileData, i32>;
        pub async fn mkfifo(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub async fn mksock(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32) -> Result<FileData, i32>;
        pub async fn read(&self, fd: &FileData, offset: u64, size: usize)
            -> Result<SGList, i32>;
        pub fn readdir(&self, fd: &FileData, soffs: i64)
            -> impl Stream<Item=Result<(libc::dirent, i64), i32>> + Send;
        pub async fn readlink(&self, fd: &FileData) -> Result<OsString, i32>;
        pub async fn rename<'a>(&self, parent: &'a FileData, fd: &'a FileData,
            name: &'a OsStr, newparent: &'a FileData, newino: Option<u64>,
            newname: &'a OsStr)
            -> Result<u64, i32>;
        pub async fn rmdir(&self, parent: &FileData, name: &OsStr) -> Result<(), i32>;
        pub fn root(&self) -> FileData;
        pub async fn setattr(&self, fd: &FileData, mut attr: SetAttr)
            -> Result<(), i32>;
        pub async fn setextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> Result<(), i32>;
        pub async fn set_props(&mut self, props: Vec<Property>);
        pub async fn statvfs(&self) -> Result<libc::statvfs, i32>;
        pub async fn symlink(&self, parent: &FileData, name: &OsStr, perm: u16,
            uid: u32, gid: u32, link: &OsStr) -> Result<FileData, i32>;
        pub async fn sync(&self);
        pub async fn unlink<'a>(&self, parent: &'a FileData, fd: Option<&'a FileData>,
            name: &'a OsStr)
            -> Result<(), i32>;
        // Change write's signature slightly.  The real write takes a IU:
        // Into<UIO>, but Mockall can't mock non-'static, non-reference
        // arguments.  So we change the argument to &[u8], which is how
        // bfffs-fuse uses it anyway.
        pub async fn write(&self, fd: &FileData, offset: u64, data: &[u8],
            _flags: u32) -> Result<u32, i32>;
        //pub async fn write<IU>(&self, fd: &FileData, offset: u64, data: IU,
            //_flags: u32) -> Result<u32, i32>
            //where IU: Into<bfffs::fs::Uio>;
    }
}
// LCOV_EXCL_STOP
