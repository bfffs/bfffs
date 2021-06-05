// vim: tw=80

// https://github.com/Gilnaa/memoffset/issues/34
#![allow(clippy::unneeded_field_pattern)]

use bfffs_core::{
    database::TreeID,
    device_manager::DevManager,
    fs::{FileData, Fs},
};
use lazy_static::lazy_static;
use memoffset::offset_of;
use std::{
    borrow::Borrow,
    collections::hash_map::HashMap,
    ffi::{CStr, OsStr},
    mem,
    os::unix::ffi::OsStrExt,
    ptr,
    slice,
    sync::{Arc, Mutex, RwLock},
};
use tokio::runtime::{Builder, Runtime};
use tracing_subscriber::EnvFilter;

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
#[derive(Debug)]
struct BfffsOptions
{
    /// silly fio can't handle an offset of 0
    _pad:      u32,
    cache_size: usize,
    pool_name: *const libc::c_char,
    /// The name of the device(s) that backs the pool.  If there are multiple
    /// devices, then they should be space-separated
    vdevs:     *const libc::c_char,
    writeback_size: usize
}

static mut OPTIONS: Option<[fio_option; 5]> = None;

#[link_section = ".init_array"]
#[used] //  Don't allow the optimizer to eliminate this symbol!
pub static INITIALIZE: extern "C" fn() = rust_ctor;

/// Initialize global statics.
///
/// If mem::zeroed were a const_fn, then we wouldn't need this.
#[no_mangle]
pub extern "C" fn rust_ctor()
{
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    unsafe {
        OPTIONS = Some([
            fio_option::new(
                b"cache_size\0",
                b"cache_size\0",
                fio_opt_type_FIO_OPT_INT,
                offset_of!(BfffsOptions, cache_size),
                b"Cache size in bytes\0",
                b"0\0",
                opt_category___FIO_OPT_C_ENGINE,
                opt_category_group_FIO_OPT_G_INVALID,
            ),
            fio_option::new(
                b"pool\0",
                b"pool\0",
                fio_opt_type_FIO_OPT_STR_STORE,
                offset_of!(BfffsOptions, pool_name),
                b"Name of BFFFS pool\0",
                b"0\0",
                opt_category___FIO_OPT_C_ENGINE,
                opt_category_group_FIO_OPT_G_INVALID,
            ),
            fio_option::new(
                b"vdevs\0",
                b"vdevs\0",
                fio_opt_type_FIO_OPT_STR_STORE,
                offset_of!(BfffsOptions, vdevs),
                b"Name of BFFFS vdev(s)\0",
                b"\0",
                opt_category___FIO_OPT_C_ENGINE,
                opt_category_group_FIO_OPT_G_INVALID,
            ),
            fio_option::new(
                b"writeback_size\0",
                b"writeback_size\0",
                fio_opt_type_FIO_OPT_INT,
                offset_of!(BfffsOptions, writeback_size),
                b"Writeback cache size in bytes\0",
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
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(
        Builder::new()
        .threaded_scheduler()
        .enable_time()
        .enable_io()
        .build()
        .unwrap()
    );
    static ref FS: RwLock<Option<Fs>> = RwLock::default();
    static ref ROOT: RwLock<Option<FileData>> = RwLock::default();
    static ref FILES: RwLock<HashMap<libc::c_int, FileData>> = RwLock::default();
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_close(
    _td: *mut thread_data,
    f: *mut fio_file,
) -> libc::c_int
{
    let fs_opt = FS.read().unwrap();
    let fs = fs_opt.as_ref().unwrap();
    let fd = FILES.write().unwrap().remove(&(*f).fd).unwrap();
    fs.inactive(fd);
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

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_init(td: *mut thread_data) -> libc::c_int
{
    let mut fs = FS.write().unwrap();
    if fs.is_none() {
        let rt = RUNTIME.lock().unwrap();
        let opts = (*td).eo as *mut BfffsOptions;
        if opts as isize != -1 {
            let (pool, vdevs) = {
                let pool = if (*opts).pool_name.is_null() {
                    eprintln!("Error: pool option is required");
                    return 1;
                } else {
                    CStr::from_ptr((*opts).pool_name).to_string_lossy()
                };
                let vdevs = if (*opts).vdevs.is_null() {
                    eprintln!("Error: vdevs option is required");
                    return 1;
                } else {
                    CStr::from_ptr((*opts).vdevs).to_string_lossy()
                };
                (pool, vdevs)
            };
            let mut dev_manager = DevManager::default();
            if (*opts).cache_size != 0 {
                dev_manager.cache_size((*opts).cache_size);
            }
            if (*opts).writeback_size != 0 {
                dev_manager.writeback_size((*opts).writeback_size);
            }
            for vdev in vdevs.split_whitespace() {
                let borrowed_vdev: &str = vdev.borrow();
                dev_manager.taste(borrowed_vdev);
            }
            let handle = rt.handle().clone();
            let r = dev_manager.import_by_name(pool, handle);
            if let Ok(db) = r {
                let adb = Arc::new(db);
                // For now, hardcode tree_id to 0
                let tree_id = TreeID::Fs(0);
                let root_fs = Fs::new(adb, rt.handle().clone(), tree_id);
                let root = root_fs.root();
                *fs = Some(root_fs);
                *ROOT.write().unwrap() = Some(root);
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

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_open(
    _td: *mut thread_data,
    f: *mut fio_file,
) -> libc::c_int
{
    let file_name =
        OsStr::from_bytes(CStr::from_ptr((*f).file_name).to_bytes());
    let fs_opt = FS.read().unwrap();
    let fs = fs_opt.as_ref().unwrap();
    let root_opt = ROOT.read().unwrap();
    let root = root_opt.as_ref().unwrap();
    let r = fs
        .lookup(None, root, file_name)
        .or_else(|_| fs.create(root, file_name, 0o600, 0, 0));
    match r {
        Ok(fd) => {
            // Store the inode number where fio would put its file
            // descriptor
            let filedesc = fd.ino() as i32;
            (*f).fd = filedesc;
            FILES.write().unwrap().insert(filedesc, fd);
            0
        }
        Err(e) => {
            eprintln!("fio_bfffs_open: {:?}", e);
            1
        }
    }
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
#[allow(non_upper_case_globals)]
pub unsafe extern "C" fn fio_bfffs_queue(
    _td: *mut thread_data,
    io_u: *mut io_u,
) -> fio_q_status
{
    let fs_opt = FS.read().unwrap();
    let fs = fs_opt.as_ref().unwrap();
    let files = FILES.read().unwrap();

    let (ddir, fd, offset, data) = {
        let data: &[u8] = slice::from_raw_parts(
            (*io_u).xfer_buf as *mut u8,
            (*io_u).xfer_buflen as usize,
        );
        let filedesc = (*(*io_u).file).fd;
        let fd = files.get(&filedesc).unwrap();
        ((*io_u).ddir, fd, (*io_u).offset, data)
    };

    match ddir {
        fio_ddir_DDIR_READ => {
            fs.read(fd, offset, data.len()).unwrap();
        }
        fio_ddir_DDIR_WRITE => {
            fs.write(fd, offset, data, 0).unwrap();
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
    cancel:             None,
    cleanup:            None,
    close_file:         Some(fio_bfffs_close),
    commit:             Some(fio_bfffs_commit),
    dlhandle:           ptr::null_mut(),
    errdetails:         None,
    event:              Some(fio_bfffs_event),
    flags:              fio_ioengine_flags_FIO_SYNCIO as i32,
    get_file_size:      None,
    getevents:          Some(fio_bfffs_getevents),
    get_zoned_model:    None,
    init:               Some(fio_bfffs_init),
    invalidate:         Some(fio_bfffs_invalidate),
    io_u_free:          None,
    io_u_init:          None,
    iomem_alloc:        None,
    iomem_free:         None,
    list:               flist_head::zeroed(),
    name:               b"bfffs\0" as *const _ as *const libc::c_char,
    open_file:          Some(fio_bfffs_open),
    option_struct_size: mem::size_of::<BfffsOptions>() as i32,
    options:            ptr::null_mut(),
    post_init:          None,
    prep:               None,
    prepopulate_file:   None,
    report_zones:       None,
    reset_wp:           None,
    queue:              Some(fio_bfffs_queue),
    setup:              None,
    terminate:          None,
    unlink_file:        None,
    version:            FIO_IOOPS_VERSION as i32,
};
