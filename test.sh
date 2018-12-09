#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

cargo +nightly test --all-features --all

# Check that benchmarks run, but don't care about results
cargo +nightly bench

# It should also work on stable >= 1.30.0
cargo +stable test --all

# Measure test coverage, too
which -s kcov && \
	env CARGO_TARGET_DIR=/localhome/somers/src/rust/bfffs/target_cov cargo +nightly kcov --all -v --features mocks -- --include-path="bfffs/src,isa-l/src"
