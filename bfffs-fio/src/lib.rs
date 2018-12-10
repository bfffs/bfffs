use bfffs::common::fs::Fs;
use lazy_static::lazy_static;
use std::{
    ptr,
    sync::Mutex,
};
use tokio_io_pool::Runtime;

mod ffi;

use crate::ffi::*;

impl flist_head {
    const fn zeroed() -> Self {
        flist_head {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

impl value_pair {
    const fn zeroed() -> Self {
        value_pair {
            ival: ptr::null(),
            oval: 0,
            help: ptr::null(),
            orval: 0,
            cb: ptr::null_mut(),
        }
    }
}
const FIO_BFFFS_OPTIONS: [fio_option; 1] = [
    fio_option {
        name: ptr::null(),
        lname: ptr::null(),
        alias: ptr::null(),
        type_: fio_opt_type_FIO_OPT_INVALID,
        off1: 0,
        off2: 0,
        off3: 0,
        off4: 0,
        off5: 0,
        off6: 0,
        maxval: 0,
        minval: 0,
        maxfp: 0.0,
        minfp: 0.0,
        interval: 0,
        maxlen: 0,
        neg: 0,
        prio: 0,
        cb: ptr::null_mut(),
        help: ptr::null(),
        def: ptr::null(),
        posval: [
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
            value_pair::zeroed(),
        ],
        parent: ptr::null(),
        hide: 0,
        hide_on_set: 0,
        inverse: ptr::null(),
        inv_opt: ptr::null_mut(),
        verify: None,
        prof_name: ptr::null(),
        prof_opts: ptr::null_mut(),
        category: 0,
        group: 0,
        gui_data: ptr::null_mut(),
        is_seconds: 0,
        is_time: 0,
        no_warn_def: 0,
        pow2: 0,
        no_free: 0,
    }
];

lazy_static! {
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new());
    // TODO: remove the Mutex once there is a fs::Handle that is Sync
    static ref FS: Mutex<Fs> = {
        let handle = {
            RUNTIME.lock().unwrap().handle().clone()
        };

        unimplemented!()
    };
}

#[no_mangle]
pub extern fn fio_bfffs_close(_td: *mut thread_data, _f: *mut fio_file)
    -> libc::c_int
{
    0
}

#[no_mangle]
pub extern fn fio_bfffs_commit(_td: *mut thread_data) -> libc::c_int
{
    0
}

#[no_mangle]
pub extern fn fio_bfffs_event(_td: *mut thread_data, _event: libc::c_int)
    -> *mut io_u
{
    ptr::null_mut()
}

#[no_mangle]
pub extern fn fio_bfffs_getevents(_td: *mut thread_data, _min: libc::c_uint,
                                  _max: libc::c_uint, _t: *const libc::timespec)
    -> libc::c_int
{
    0
}

#[no_mangle]
pub extern fn fio_bfffs_init(_td: *mut thread_data) -> libc::c_int {
    0
}

#[no_mangle]
pub extern fn fio_bfffs_open(_td: *mut thread_data, _f: *mut fio_file)
    -> libc::c_int
{
    -1
}

#[no_mangle]
pub extern fn fio_bfffs_queue(_td: *mut thread_data, _io_u: *mut io_u)
    -> libc::c_uint
{
    0
}

#[export_name = "ioengine"]
pub static mut IOENGINE: ioengine_ops = ioengine_ops {
    name: &b"bfffs" as *const _ as *const libc::c_char,
    version: FIO_IOOPS_VERSION as i32,
    init: Some(fio_bfffs_init),
    flags: 0,   // TODO
    setup: None,
    prep: None,
    queue: Some(fio_bfffs_queue),
    commit: Some(fio_bfffs_commit),
    getevents: Some(fio_bfffs_getevents),
    event: Some(fio_bfffs_event),
    errdetails: None,
    cancel: None,
    cleanup: None,
    open_file: Some(fio_bfffs_open),
    close_file: Some(fio_bfffs_close),
    invalidate: None,
    unlink_file: None,
    get_file_size: None,
    terminate: None,
    iomem_alloc: None,
    iomem_free: None,
    io_u_init: None,
    io_u_free: None,
    list: flist_head::zeroed(),
    option_struct_size: 0,  // TODO
    options: &FIO_BFFFS_OPTIONS as *const _ as *mut _
};
