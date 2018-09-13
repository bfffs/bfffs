#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

# Mockers is very picky about rust versions.  Mockers 0.12.2 is known to work
# with rust-nightly 1.30.0 as of Sep-6-2018.
cargo +nightly-2018-09-06-x86_64-unknown-freebsd test --all-features

# Check that benchmarks run, but don't care about results
rustup run nightly-2018-09-06-x86_64-unknown-freebsd cargo bench

# It should also work on stable >= 1.29.0
cargo +stable test

# Measure test coverage, too
which -s kcov && \
	env CARGO_TARGET_DIR=/localhome/somers/src/rust/arkfs/target_cov cargo +nightly-2018-09-06-x86_64 kcov -v --features mocks -- --include-path="src"
