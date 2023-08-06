#! /bin/sh -e
#
# Generate a BFFFS code coverage report
#
# Requirements:
# sudo pkg install fio grcov
# cargo install grcov
# rustup component add llvm-tools-preview
#
# Usage:
# tools/coverage.sh

export LLVM_PROFILE_FILE="bfffs-%p-%m.profraw"
export RUSTFLAGS="-Cinstrument-coverage"
TOOLCHAIN=nightly
cargo +$TOOLCHAIN build --all-features
cargo +$TOOLCHAIN test --all-features

truncate -s 1g /tmp/bfffs.img
cargo +$TOOLCHAIN run --all-features --bin bfffs -- pool create testpool /tmp/bfffs.img
fio bfffs-fio/data/ci.fio

grcov . --binary-path $PWD/target/debug -s . -t html --branch \
	--ignore-not-existing \
	--excl-line LCOV_EXCL_LINE \
	--excl-start LCOV_EXCL_START \
	--excl-stop LCOV_EXCL_STOP \
	--ignore "*/tests/*" \
	--ignore "*/src/*/tests.rs" \
	--ignore bfffs/src/bin/bfffsd/fs/mock.rs \
	--ignore bfffs-core/src/dataset/dataset_mock.rs \
	--ignore bfffs-core/src/tree/tree_mock.rs \
	--ignore "*/examples/*" \
	-o ./coverage/
