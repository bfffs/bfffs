#!/bin/sh

# Used to manually regenerate FFI bindings to ISA-L

BINDGEN=$HOME/.cargo/bin/bindgen
HEADER=/usr/local/include/isa-l.h
OUTPUT=`dirname $0`/../isa-l/src/ffi/mod.rs

if [ ! -f "$HEADER" ]; then
	echo "Must install ISA-L first!  Try \"pkg install isa-l\"."
	exit 1
fi

if [ ! -x "${BINDGEN}" ]; then
	echo "Must install rust-bindgen first!  Try \"cargo install bindgen\"."
	exit 1
fi

# ISA-L is a large library.  Only bind the symbols that we may need.
${BINDGEN} -o ${OUTPUT} \
	--whitelist-function gf_gen_cauchy1_matrix \
	--whitelist-function gf_gen_rs_matrix \
	--whitelist-function ec_init_tables \
	--whitelist-function ec_encode_data \
	--whitelist-function gf_invert_matrix \
	--whitelist-function ec_encode_data_update \
	--whitelist-var ISAL_MAJOR_VERSION \
	--whitelist-var ISAL_MINOR_VERSION \
	--whitelist-var ISAL_PATCH_VERSION \
	${HEADER} -- -I /usr/local/include
