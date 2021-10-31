#! /bin/sh

# fio doesn't install the necessary headers, so we have to reference its source
# directory
FIOPATH="/usr/home/somers/src/freebsd.org/ports/benchmarks/fio/work/fio-3.28"

cat > src/ffi.rs << HERE
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused)]
#![allow(clippy::all)]
use libc::timespec;
HERE

# Disable layout tests due to this bindgen bug:
# https://github.com/rust-lang/rust-bindgen/issues/867
bindgen --no-layout-tests \
	--whitelist-function 'generic_.*_file' \
	--whitelist-type 'fio_file' \
	--whitelist-type 'fio_option' \
	--whitelist-type 'fio_opt_type.*' \
	--whitelist-type 'ioengine_ops' \
	--whitelist-type 'io_u' \
	--whitelist-type 'opt_category.*' \
	--whitelist-type 'thread_data' \
	--whitelist-type 'fio_ioengine_flags' \
	--blacklist-type 'timespec' \
	--whitelist-var 'FIO_IOOPS_VERSION' \
	--ctypes-prefix libc \
	src/ffi.h -- -I$FIOPATH >> src/ffi.rs
