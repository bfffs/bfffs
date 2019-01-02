#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely
# Use --all-targets to include benchmarks (in debug mode)
cargo +nightly test --all-features --all --all-targets

# It should also work on stable >= 1.30.0
cargo +stable test --all

# bfffs-fio should stay in consistent style.  The other crates can't, because
# rustfmt screws them up.
cargo +nightly fmt --package bfffs-fio -- --check

# Measure test coverage, too.  Only measure test coverage for the main crate,
# because cargo-kcov doesn't work with workspaces that have features.
# https://github.com/kennytm/cargo-kcov/issues/39
if which -s kcov ; then
	cd bfffs;
	env CARGO_TARGET_DIR=/localhome/somers/src/rust/bfffs/target_cov cargo +nightly kcov --all -v --features nightly -- --include-path="src"
fi
