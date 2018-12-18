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

# Measure test coverage, too
which -s kcov && \
	env CARGO_TARGET_DIR=/localhome/somers/src/rust/bfffs/target_cov cargo +nightly kcov --all -v --features mocks -- --include-path="bfffs/src,isa-l/src"
