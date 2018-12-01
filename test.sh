#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

# Mockers is very picky about rust versions.  Mockers 0.12.2 is known to work
# with rust-nightly 1.30.0 as of Sep-6-2018.
cargo +nightly-2018-10-15-x86_64-unknown-freebsd test --all-features --all

# Check that benchmarks run, but don't care about results
rustup run nightly-2018-10-15-x86_64-unknown-freebsd cargo bench

# It should also work on stable >= 1.30.0
cargo +stable test --all

# Measure test coverage, too
which -s kcov && \
	env CARGO_TARGET_DIR=/localhome/somers/src/rust/bfffs/target_cov cargo +nightly-2018-10-15-x86_64 kcov --all -v --features mocks -- --include-path="src,isa-l/src"
