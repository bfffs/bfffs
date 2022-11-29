// vim: tw=80

// https://github.com/rust-lang/rust-clippy/issues/9986
#![allow(clippy::unnecessary_safety_doc)]

use std::{
    borrow::Borrow,
    collections::hash_map::HashMap,
    ffi::{CStr, OsStr},
    mem,
    os::unix::ffi::OsStrExt,
    pin::Pin,
    ptr,
    slice,
    sync::{Arc, RwLock},
};

use bfffs_core::{
    device_manager::DevManager,
    fs::{FileDataMut, Fs},
    IoVec,
};
use futures::{
    future::{Future, FutureExt, TryFutureExt},
    stream::{futures_unordered::FuturesUnordered, StreamExt},
};
use lazy_static::lazy_static;
use memoffset::offset_of;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinError,
};
use tracing_subscriber::EnvFilter;

mod ffi;

use crate::ffi::*;

impl fio_option {
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
    ) -> Self {
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

impl flist_head {
    const fn zeroed() -> Self {
        flist_head {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

/// Like *mut io_u, but Send
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
struct IoU(*mut io_u);

// There's no good reason for pointers not to be Send and Sync
unsafe impl Send for IoU {}
unsafe impl Sync for IoU {}

type BfffsIoIopResult = Result<Result<(IoU, u32), i32>, JoinError>;

#[derive(Debug, Default)]
struct BfffsIoOpsData {
    pub futs: FuturesUnordered<Pin<Box<dyn Future<Output = BfffsIoIopResult>>>>,
    pub ready_events: Vec<IoU>,
}

#[repr(C)]
#[derive(Debug)]
struct BfffsOptions {
    /// silly fio can't handle an offset of 0
    _pad:           u32,
    cache_size:     usize,
    pool_name:      *const libc::c_char,
    /// The name of the device(s) that backs the pool.  If there are multiple
    /// devices, then they should be space-separated
    vdevs:          *const libc::c_char,
    writeback_size: usize,
}

static mut OPTIONS: Option<[fio_option; 5]> = None;

#[link_section = ".init_array"]
#[used] //  Don't allow the optimizer to eliminate this symbol!
pub static INITIALIZE: extern "C" fn() = rust_ctor;

/// Initialize global statics.
///
/// If mem::zeroed were a const_fn, then we wouldn't need this.
#[no_mangle]
pub extern "C" fn rust_ctor() {
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
    static ref RUNTIME: RwLock<Runtime> = RwLock::new(
        Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap()
    );
    static ref FILES: RwLock<HashMap<libc::c_int, FileDataMut>> =
        RwLock::default();
}
static FS: RwLock<Option<Arc<Fs>>> = RwLock::new(None);
static ROOT: RwLock<Option<FileDataMut>> = RwLock::new(None);

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_cleanup(td: *mut thread_data) {
    if !(*td).io_ops_data.is_null() {
        drop(Box::from_raw((*td).io_ops_data as *mut BfffsIoOpsData));
        (*td).io_ops_data = ptr::null_mut();
    }
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_close(
    _td: *mut thread_data,
    f: *mut fio_file,
) -> libc::c_int {
    let rt = RUNTIME.read().unwrap();
    let fs_opt = FS.read().unwrap();
    let fs = fs_opt.as_ref().unwrap();
    let fd = FILES.write().unwrap().remove(&(*f).fd).unwrap();
    rt.block_on(async { fs.inactive(fd).await });
    (*f).fd = -1;
    0
}

/// # Safety
///
/// Safe because the pointer argument is unused.
#[no_mangle]
pub extern "C" fn fio_bfffs_commit(_td: *mut thread_data) -> libc::c_int {
    unimplemented!()
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_event(
    td: *mut thread_data,
    event: libc::c_int,
) -> *mut io_u {
    let mut io_ops_data =
        Box::from_raw((*td).io_ops_data as *mut BfffsIoOpsData);
    let io_u = mem::replace(
        &mut io_ops_data.ready_events[event as usize],
        IoU(ptr::null_mut()),
    );
    assert!(!io_u.0.is_null());

    Box::into_raw(io_ops_data);
    io_u.0
}

/// Not that it's documented anywhere, but as best as I can tell fio will call
/// .getevents() once, then call .event() n times, where n is the return value
/// of .getevents().  .getevents should sleep until at least `min` events are
/// ready.
///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_getevents(
    td: *mut thread_data,
    min: libc::c_uint,
    _max: libc::c_uint,
    t: *const libc::timespec,
) -> libc::c_int {
    let rt = RUNTIME.read().unwrap();
    let mut io_ops_data =
        Box::from_raw((*td).io_ops_data as *mut BfffsIoOpsData);
    assert!(t.is_null(), "timeout is unimplemented");
    assert!(!io_ops_data.futs.is_empty());

    io_ops_data.ready_events.clear();

    rt.block_on(async {
        for _ in 0..min {
            let r = io_ops_data.futs.next().await;
            let (io_u, retval) =
                r.unwrap().unwrap().expect("I/O failed inside of BFFFS");
            (*io_u.0).resid = (*io_u.0).xfer_buflen - retval as u64;
            io_ops_data.ready_events.push(io_u);
        }
    });

    let retval = io_ops_data.ready_events.len() as i32;
    Box::into_raw(io_ops_data);
    retval
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
pub unsafe extern "C" fn fio_bfffs_init(td: *mut thread_data) -> libc::c_int {
    let io_ops_data = BfffsIoOpsData::default();
    let io_ops_data = Box::new(io_ops_data);
    (*td).io_ops_data = Box::into_raw(io_ops_data) as *mut libc::c_void;

    let mut fs = FS.write().unwrap();
    if fs.is_none() {
        let rt = RUNTIME.read().unwrap();
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
            rt.block_on(async {
                for vdev in vdevs.split_whitespace() {
                    let borrowed_vdev: &str = vdev.borrow();
                    dev_manager.taste(borrowed_vdev).await.unwrap();
                }
                let r = dev_manager.import_by_name(pool).await;
                if let Ok(db) = r {
                    let adb = Arc::new(db);
                    let tree_id = adb.lookup_fs("").await.unwrap().1.unwrap();
                    let root_fs = Fs::new(adb, tree_id).await;
                    let root = root_fs.root();
                    *fs = Some(Arc::new(root_fs));
                    *ROOT.write().unwrap() = Some(root);
                    0
                } else {
                    eprintln!("Pool not found");
                    1
                }
            })
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
) -> libc::c_int {
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
) -> libc::c_int {
    let rt = RUNTIME.read().unwrap();
    let file_name =
        OsStr::from_bytes(CStr::from_ptr((*f).file_name).to_bytes());
    let fs_opt = FS.read().unwrap();
    let fs = fs_opt.as_ref().unwrap();
    let root_opt = ROOT.read().unwrap();
    let root = root_opt.as_ref().unwrap().handle();
    rt.block_on(async {
        let r = fs
            .lookup(None, &root, file_name)
            .or_else(|_| fs.create(&root, file_name, 0o600, 0, 0))
            .await;
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
    })
}

///
/// # Safety
///
/// Caller must ensure validity of the pointer arguments
#[no_mangle]
#[allow(non_upper_case_globals)]
pub unsafe extern "C" fn fio_bfffs_queue(
    td: *mut thread_data,
    io_u: *mut io_u,
) -> fio_q_status {
    let rt = RUNTIME.read().unwrap();
    let fs = FS.read().unwrap().clone().unwrap();
    let files = FILES.read().unwrap();
    assert!(!((*td).io_ops_data.is_null()));
    let io_ops_data = Box::from_raw((*td).io_ops_data as *mut BfffsIoOpsData);

    let (ddir, fd, offset, data) = {
        let data: &[u8] = slice::from_raw_parts(
            (*io_u).xfer_buf as *mut u8,
            (*io_u).xfer_buflen as usize,
        );
        let filedesc = (*(*io_u).file).fd;
        let fd = files.get(&filedesc).unwrap().handle();
        ((*io_u).ddir, fd, (*io_u).offset, data)
    };

    let io_u = IoU(io_u);
    let jh = rt
        .spawn(async move {
            match ddir {
                fio_ddir_DDIR_READ => {
                    // Throw away the data
                    fs.read(&fd, offset, data.len())
                        .map_ok(|sglist| {
                            (
                                io_u,
                                sglist.iter().map(IoVec::len).sum::<usize>()
                                    as u32,
                            )
                        })
                        .await
                }
                fio_ddir_DDIR_WRITE => {
                    fs.write(&fd, offset, data, 0).map_ok(|r| (io_u, r)).await
                }
                fio_ddir_DDIR_SYNC => {
                    fs.fsync(&fd).map_ok(|_| (io_u, 0u32)).await
                }
                _ => unimplemented!(),
            }
        })
        .boxed();
    io_ops_data.futs.push(jh);

    Box::into_raw(io_ops_data);
    fio_q_status_FIO_Q_QUEUED
}

#[export_name = "ioengine"]
pub static mut IOENGINE: ioengine_ops = ioengine_ops {
    cancel:             None,
    cleanup:            Some(fio_bfffs_cleanup),
    close_file:         Some(fio_bfffs_close),
    commit:             None,
    dlhandle:           ptr::null_mut(),
    errdetails:         None,
    event:              Some(fio_bfffs_event),
    flags:              fio_ioengine_flags_FIO_ASYNCIO_SYNC_TRIM as i32,
    get_file_size:      None,
    getevents:          Some(fio_bfffs_getevents),
    get_max_open_zones: None,
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
