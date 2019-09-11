// vim: tw=80
use bfffs::common::{
    database::TreeID,
    device_manager::DevManager,
    fs::{FileData, Fs},
    Error,
};
use futures::{future, Future, IntoFuture};
use lazy_static::lazy_static;
use memoffset::offset_of;
use std::{
    borrow::Borrow,
    ffi::{CStr, OsStr},
    mem,
    os::unix::ffi::OsStrExt,
    ptr,
    slice,
    sync::{Arc, Mutex},
};
use tokio_io_pool::Runtime;

mod ffi;

use crate::ffi::*;

impl fio_option
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: &[u8],
        lname: &[u8],
        type_: fio_opt_type,
        off1: usize,
        help: &[u8],
        def: &[u8],
        category: opt_category,
        group: opt_category_group,
    ) -> Self
    {
        fio_option {
            name: name as *const _ as *const libc::c_char,
            lname: lname as *const _ as *const libc::c_char,
            type_,
            off1: off1 as libc::c_uint,
            help: help as *const _ as *const libc::c_char,
            def: def as *const _ as *const libc::c_char,
            category: u64::from(category),
            group,
            ..unsafe { mem::zeroed() }
        }
    }
}

impl flist_head
{
    const fn zeroed() -> Self
    {
        flist_head {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

#[repr(C)]
struct BfffsOptions
{
    /// silly fio can't handle an offset of 0
    _pad: u32,
    pool_name: *const libc::c_char,
    /// The name of the device that backs the pool.  If there are multiple
    /// devices, then they should be space-separated
    vdev: *const libc::c_char,
}

static mut OPTIONS: Option<[fio_option; 3]> = None;

#[link_section = ".init_array"]
#[used] //  Don't allow the optimizer to eliminate this symbol!
pub static INITIALIZE: extern "C" fn() = rust_ctor;

/// Initialize global statics.
///
/// If mem::zeroed were a const_fn, then we wouldn't need this.
#[no_mangle]
pub extern "C" fn rust_ctor()
{
    unsafe {
        OPTIONS = Some([
            fio_option::new(
                b"pool\0",
                b"pool\0",
                fio_opt_type_FIO_OPT_STR_STORE,
                offset_of!(BfffsOptions, pool_name),
                b"Name of BFFFS pool\0",
                b"\0",
                opt_category___FIO_OPT_C_ENGINE,
                opt_category_group_FIO_OPT_G_INVALID,
            ),
            fio_option::new(
                b"vdev\0",
                b"vdev\0",
                fio_opt_type_FIO_OPT_STR_STORE,
                offset_of!(BfffsOptions, vdev),
                b"Name of BFFFS vdev\0",
                b"\0",
                opt_category___FIO_OPT_C_ENGINE,
                opt_category_group_FIO_OPT_G_INVALID,
            ),
            mem::zeroed(),
        ]);
        IOENGINE.options = OPTIONS.as_ref().unwrap() as *const _ as *mut _;
    }
}

lazy_static! {
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new());
    // TODO: remove the Mutex once there is a fs::Handle that is Sync
    static ref FS: Mutex<Option<Fs>> = Mutex::new(None);
}

#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_close(
    _td: *mut thread_data,
    f: *mut fio_file,
) -> libc::c_int
{
    (*f).fd = -1;
    0
}

#[no_mangle]
pub extern "C" fn fio_bfffs_commit(_td: *mut thread_data) -> libc::c_int
{
    1
}

#[no_mangle]
pub extern "C" fn fio_bfffs_event(
    _td: *mut thread_data,
    _event: libc::c_int,
) -> *mut io_u
{
    ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn fio_bfffs_getevents(
    _td: *mut thread_data,
    _min: libc::c_uint,
    _max: libc::c_uint,
    _t: *const libc::timespec,
) -> libc::c_int
{
    0
}

#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_init(td: *mut thread_data) -> libc::c_int
{
    let mut fs = FS.lock().unwrap();
    if fs.is_none() {
        let mut rt = RUNTIME.lock().unwrap();
        let opts = (*td).eo as *mut BfffsOptions;
        if opts as isize != -1 {
            let (pool, vdev) = {
                let pool = if (*opts).pool_name.is_null() {
                    eprintln!("Error: pool option is required");
                    return 1;
                } else {
                    CStr::from_ptr((*opts).pool_name).to_string_lossy()
                };
                let vdev = if (*opts).vdev.is_null() {
                    eprintln!("Error: vdev option is required");
                    return 1;
                } else {
                    CStr::from_ptr((*opts).vdev).to_string_lossy()
                };
                (pool, vdev)
            };
            let dev_manager = DevManager::default();
            // TODO: allow using multiple vdevs
            let borrowed_vdev: &str = vdev.borrow();
            dev_manager.taste(borrowed_vdev);
            let handle = rt.handle().clone();
            let r = rt.block_on(future::lazy(move || {
                if let Ok(fut) = dev_manager.import_by_name(pool, handle) {
                    Box::new(fut) as Box<dyn Future<Item = _, Error = _> + Send>
                } else {
                    Box::new(Err(Error::ENOENT).into_future())
                        as Box<dyn Future<Item = _, Error = _> + Send>
                }
            }));
            if let Ok(db) = r {
                let adb = Arc::new(db);
                // For now, hardcode tree_id to 0
                let tree_id = TreeID::Fs(0);
                let root_fs = Fs::new(adb, rt.handle().clone(), tree_id);
                *fs = Some(root_fs);
                0
            } else {
                eprintln!("Pool not found");
                1
            }
        } else {
            eprintln!("Upgrade fio to 3.12 or later");
            1
        }
    } else {
        0
    }
}

#[no_mangle]
pub extern "C" fn fio_bfffs_invalidate(
    _td: *mut thread_data,
    _f: *mut fio_file,
) -> libc::c_int
{
    // TODO: invalidate cache
    0
}

#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_open(
    _td: *mut thread_data,
    f: *mut fio_file,
) -> libc::c_int
{
    let file_name =
        OsStr::from_bytes(CStr::from_ptr((*f).file_name).to_bytes());
    let mut fs_opt = FS.lock().unwrap();
    let fs = fs_opt.as_mut().unwrap();
    let root = fs.root();
    let r = fs
        .lookup(&root, file_name)
        .or_else(|_| fs.create(&root, file_name, 0o600, 0, 0));
    fs.inactive(root);
    match r {
        Ok(fd) => {
            // Store the inode number where fio would put its file
            // descriptor
            (*f).fd = fd.ino() as i32;
            0
        }
        Err(e) => {
            eprintln!("fio_bfffs_open: {:?}", e);
            1
        }
    }
}

#[no_mangle]
#[allow(non_upper_case_globals)]
pub unsafe extern "C" fn fio_bfffs_queue(
    _td: *mut thread_data,
    io_u: *mut io_u,
) -> fio_q_status
{
    let fs_opt = FS.lock().unwrap();
    let fs = fs_opt.as_ref().unwrap();

    let (ddir, fd, offset, data) = {
        let data: &[u8] = slice::from_raw_parts(
            (*io_u).xfer_buf as *mut u8,
            (*io_u).xfer_buflen as usize,
        );
        let ino = (*(*io_u).file).fd as u64;
        // XXX The FIO API requires us to be able to uniquely identify a
        // FileData based on the contents of a c_int (they assume it's a file
        // descriptor).  For now, we just cram the inode number in there.  That
        // will have to change at some point in the future.
        let fd = FileData::new_for_tests(ino);
        ((*io_u).ddir, fd, (*io_u).offset, data)
    };

    match ddir {
        fio_ddir_DDIR_READ => {
            fs.read(&fd, offset, data.len()).unwrap();
        }
        fio_ddir_DDIR_WRITE => {
            fs.write(&fd, offset, data, 0).unwrap();
        }
        fio_ddir_DDIR_SYNC => {
            // Until we support fsync, we must sync the entire filesystem
            fs.sync()
        }
        _ => unimplemented!(),
    }

    fio_q_status_FIO_Q_COMPLETED
}

#[export_name = "ioengine"]
pub static mut IOENGINE: ioengine_ops = ioengine_ops {
    name:               b"bfffs\0" as *const _ as *const libc::c_char,
    version:            FIO_IOOPS_VERSION as i32,
    init:               Some(fio_bfffs_init),
    flags:              fio_ioengine_flags_FIO_SYNCIO as i32,
    setup:              None,
    prep:               None,
    queue:              Some(fio_bfffs_queue),
    commit:             Some(fio_bfffs_commit),
    getevents:          Some(fio_bfffs_getevents),
    event:              Some(fio_bfffs_event),
    errdetails:         None,
    cancel:             None,
    cleanup:            None,
    open_file:          Some(fio_bfffs_open),
    close_file:         Some(fio_bfffs_close),
    invalidate:         Some(fio_bfffs_invalidate),
    unlink_file:        None,
    get_file_size:      None,
    terminate:          None,
    iomem_alloc:        None,
    iomem_free:         None,
    io_u_init:          None,
    io_u_free:          None,
    list:               flist_head::zeroed(),
    option_struct_size: mem::size_of::<BfffsOptions>() as i32,
    options:            ptr::null_mut(),
};
