#! /bin/sh

# fio doesn't install the necessary headers, so we have to reference its source
# directory
FIOPATH="/usr/home/somers/freebsd/ports/head/benchmarks/fio/work/fio-3.16"

cat > src/ffi.rs << HERE
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused)]
#![allow(clippy::all)]
use libc::timespec;
HERE

# Use 1.0 compatibility mode to workaround a rustfmt bug involving unions:
# https://github.com/rust-lang/rust-bindgen/issues/1120
bindgen --no-rustfmt-bindings \
	--no-layout-tests \
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
	--rust-target 1.27 \
	src/ffi.h -- -I$FIOPATH >> src/ffi.rs
rustup run nightly rustfmt --edition 2018 src/ffi.rs
