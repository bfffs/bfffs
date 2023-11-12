#! /bin/sh

FIOPATH=/usr/local/include/fio

cat > src/ffi.rs << HERE
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused)]
#![cfg_attr(test, allow(deref_nullptr))]
#![allow(clippy::all)]
use libc::timespec;
HERE

# Disable layout tests due to this bindgen bug:
# https://github.com/rust-lang/rust-bindgen/issues/867
bindgen --allowlist-type 'fio_file' \
	--allowlist-type 'fio_option' \
	--allowlist-type 'fio_opt_type.*' \
	--allowlist-type 'ioengine_ops' \
	--allowlist-type 'io_u' \
	--allowlist-type 'opt_category.*' \
	--allowlist-type 'thread_data' \
	--allowlist-type 'fio_ioengine_flags' \
	--blocklist-type 'timespec' \
	--allowlist-var 'FIO_IOOPS_VERSION' \
	--ctypes-prefix libc \
	--opaque-type FILE \
	--opaque-type clat_prio_stat \
	--opaque-type disk_util \
	--opaque-type fio_flow \
	--opaque-type fio_lfsr \
	--opaque-type fio_rb_node \
	--opaque-type fio_sem \
	--opaque-type frand_state \
	--opaque-type gauss_state \
	--opaque-type io_log \
	--opaque-type io_piece \
	--opaque-type io_u_queue \
	--opaque-type io_u_ring \
	--opaque-type os_aiocb_t \
	--opaque-type prof_io_ops \
	--opaque-type pthread_cond_t \
	--opaque-type pthread_mutex_t \
	--opaque-type pthread_t \
	--opaque-type rb_root \
	--opaque-type rusage \
	--opaque-type steadystate_data \
	--opaque-type thread_io_list \
	--opaque-type thread_options \
	--opaque-type thread_stat \
	--opaque-type value_pair \
	--opaque-type workqueue \
	--opaque-type workqueue_work \
	--opaque-type zipf_state \
	--opaque-type zone_split_index \
	src/ffi.h -- -I$FIOPATH >> src/ffi.rs
