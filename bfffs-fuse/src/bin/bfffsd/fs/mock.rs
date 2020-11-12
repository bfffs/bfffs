// vim: tw=80
// LCOV_EXCL_START
//! Mock objects for bfffsd::fs
use bfffs::common::{
    database::{Database, TreeID},
    fs::{ExtAttr, ExtAttrNamespace, FileData, GetAttr, SetAttr},
    property::Property,
    SGList,
    RID,
};
use divbuf::DivBuf;
use fuse::{FileAttr, FileType};
use libc::c_int;
use mockall::mock;
use std::{
    ffi::{OsStr, OsString},
    io,
    path::Path,
    sync::Arc,
};
use time::Timespec;
use tokio::runtime::Handle;

/*
 * Mock BFFFS structs
 * ==================
 * We need to use mock! here instead of automock because bfffs is technically in
 * another crate, so it doesn't get built in test mode when bfffs-fuse does
 */
mock! {
    pub Fs {
        fn create(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>;
        fn deleteextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<(), i32>;
        fn inactive(&self, fd: FileData);
        fn fsync(&self, fd: &FileData) -> Result<(), i32>;
        fn getattr(&self, fd: &FileData) -> Result<GetAttr, i32>;
        fn getextattr(&self, fd: &FileData, ns: ExtAttrNamespace, name: &OsStr)
            -> Result<DivBuf, i32>;
        fn getextattrlen(&self, fd: &FileData, ns: ExtAttrNamespace,
            name: &OsStr) -> Result<u32, i32>;
        fn link(&self, parent: &FileData, fd: &FileData, name: &OsStr)
            -> Result<(), i32>;
        fn lookup<'a>(&self, grandparent: Option<&'a FileData>,
            parent: &'a FileData, name: &OsStr) -> Result<FileData, i32>;
        fn listextattr<F>(&self, fd: &FileData, size: u32, f: F)
            -> Result<Vec<u8>, i32>
            where F: Fn(&mut Vec<u8>, &ExtAttr<RID>) + Send + 'static;
        fn listextattrlen<F>(&self, fd: &FileData, f: F) -> Result<u32, i32>
            where F: Fn(&ExtAttr<RID>) -> u32 + Send + 'static;
        fn mkdir(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                 gid: u32) -> Result<FileData, i32>;
        fn mkblock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, rdev: u32) -> Result<FileData, i32>;
        fn mkchar(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32, rdev: u32) -> Result<FileData, i32>;
        fn mkfifo(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>;
        fn mksock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>;
        fn new(database: Arc<Database>, handle: Handle, tree: TreeID) -> Self;
        fn read(&self, fd: &FileData, offset: u64, size: usize)
            -> Result<SGList, i32>;
        fn readdir(&self, fd: &FileData, soffs: i64)
            -> impl Iterator<Item=Result<(libc::dirent, i64), i32>>;
        fn readlink(&self, fd: &FileData) -> Result<OsString, i32>;
        fn rename<'a>(&self, parent: &'a FileData, fd: &'a FileData,
            name: &'a OsStr, newparent: &'a FileData, newino: Option<u64>,
            newname: &'a OsStr)
            -> Result<u64, i32>;
        fn rmdir(&self, parent: &FileData, name: &OsStr) -> Result<(), i32>;
        fn root(&self) -> FileData;
        fn setattr(&self, fd: &FileData, mut attr: SetAttr) -> Result<(), i32>;
        fn setextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> Result<(), i32>;
        fn set_props(&mut self, props: Vec<Property>);
        fn statvfs(&self) -> Result<libc::statvfs, i32>;
        fn symlink(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, link: &OsStr) -> Result<FileData, i32>;
        fn sync(&self);
        fn unlink<'a>(&self, parent: &'a FileData, fd: Option<&'a FileData>,
            name: &'a OsStr)
            -> Result<(), i32>;
        // Change write's signature slightly.  The real write takes a IU:
        // Into<UIO>, but Mockall can't mock non-'static, non-reference
        // arguments.  So we change the argument to &[u8], which is how
        // bfffs-fuse uses it anyway.
        fn write(&self, fd: &FileData, offset: u64, data: &[u8], _flags: u32)
            -> Result<u32, i32>;
        //fn write<IU>(&self, fd: &FileData, offset: u64, data: IU, _flags: u32)
            //-> Result<u32, i32>
            //where IU: Into<bfffs::common::fs::Uio>;
    }
}

/*
 * Mock rust-fuse structs
 * ======================
 * Rust-fuse structs are pretty opaque, but easy enough to mock
 */
mock! {
    pub ReplyAttr {
        fn attr(self, ttl: &Timespec, attr: &FileAttr);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyBmap{
        fn bmap(self, block: u64);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyCreate {
        fn created(self, ttl: &Timespec, attr: &FileAttr, generation: u64,
                   fh: u64, flags: u32);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyData {
        fn data(self, data: &[u8]);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyDirectory {
        fn add<T>(&mut self, ino: u64, offset: i64, kind: FileType, name: T)
            -> bool
            where T: AsRef<OsStr> + 'static;
        fn ok(self);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyEmpty {
        fn ok(self);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyEntry {
        fn entry(self, ttl: &Timespec, attr: &FileAttr, generation: u64);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyLock{
        fn locked(self, start: u64, end: u64, typ: u32, pid: u32);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyOpen{
        fn opened(self, fh: u64, flags: u32);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyStatfs{
        fn statfs(self, blocks: u64, bfree: u64, bavail: u64, files: u64,
                  ffree: u64, bsize: u32, namelen: u32, frsize: u32);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyWrite{
        fn written(self, size: u32);
        fn error(self, err: c_int);
    }
}

mock! {
    pub ReplyXattr {
        fn size(self, size: u32);
        fn data(self, data: &[u8]);
        fn error(self, err: c_int);
    }
}

mock! {
    pub Request {
        fn unique(&self) -> u64;
        fn uid(&self) -> u32;
        fn gid(&self) -> u32;
        fn pid(&self) -> u32;
    }
}

pub trait Filesystem
{
    fn init(&mut self, _: &MockRequest) -> Result<(), c_int>
    {
        Ok(())
    }
    fn destroy(&mut self, _: &MockRequest) {}
    fn lookup(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEntry,
    )
    {
    }
    fn forget(&mut self, _: &MockRequest, _: u64, _: u64) {}
    fn getattr(&mut self, _: &MockRequest, _: u64, _reply: MockReplyAttr) {}
    fn setattr(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u64>,
        _: Option<Timespec>,
        _: Option<Timespec>,
        _: Option<u64>,
        _: Option<Timespec>,
        _: Option<Timespec>,
        _: Option<Timespec>,
        _: Option<u32>,
        _reply: MockReplyAttr,
    )
    {
    }
    fn readlink(&mut self, _: &MockRequest, _: u64, _reply: MockReplyData) {}
    fn mknod(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u32,
        _: u32,
        _reply: MockReplyEntry,
    )
    {
    }
    fn mkdir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u32,
        _reply: MockReplyEntry,
    )
    {
    }
    fn unlink(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn rmdir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn symlink(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: &Path,
        _reply: MockReplyEntry,
    )
    {
    }
    fn rename(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn link(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEntry,
    )
    {
    }
    fn open(&mut self, _: &MockRequest, _: u64, _: u32, _reply: MockReplyOpen)
    {
    }
    fn read(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: i64,
        _: u32,
        _reply: MockReplyData,
    )
    {
    }
    fn write(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: i64,
        _: &[u8],
        _: u32,
        _reply: MockReplyWrite,
    )
    {
    }
    fn flush(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: u64,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn release(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: u32,
        _: u64,
        _: bool,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn fsync(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: bool,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn opendir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u32,
        _reply: MockReplyOpen,
    )
    {
    }
    fn readdir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: i64,
        _reply: MockReplyDirectory,
    )
    {
    }
    fn releasedir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: u32,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn fsyncdir(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: bool,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn statfs(&mut self, _: &MockRequest, _: u64, _reply: MockReplyStatfs) {}
    fn setxattr(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: &[u8],
        _: u32,
        _: u32,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn getxattr(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u32,
        _reply: MockReplyXattr,
    )
    {
    }
    fn listxattr(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u32,
        _reply: MockReplyXattr,
    )
    {
    }
    fn removexattr(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn access(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u32,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn create(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u32,
        _: u32,
        _reply: MockReplyCreate,
    )
    {
    }
    fn getlk(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
        _reply: MockReplyLock,
    )
    {
    }
    fn setlk(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
        _: bool,
        _reply: MockReplyEmpty,
    )
    {
    }
    fn bmap(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: u32,
        _: u64,
        _reply: MockReplyBmap,
    )
    {
    }
    #[cfg(target_os = "macos")]
    fn setvolname(&mut self, _: &MockRequest, _: &OsStr, _reply: MockReplyEmpty)
    {
    }
    #[cfg(target_os = "macos")]
    fn exchange(
        &mut self,
        _: &MockRequest,
        _: u64,
        _: &OsStr,
        _: u64,
        _: &OsStr,
        _: u64,
        _reply: MockReplyEmpty,
    )
    {
    }
    #[cfg(target_os = "macos")]
    fn getxtimes(&mut self, _: &MockRequest, _: u64, _reply: MockReplyXTimes) {}
}

pub fn mount<FS, P>(_: FS, _: P, _: &[&OsStr]) -> io::Result<()>
where
    FS: Filesystem,
    P: AsRef<Path>,
{
    unimplemented!()
}
// LCOV_EXCL_STOP
