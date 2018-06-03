#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

# Mockers is very picky about rust versions.  Mockers 0.11.1 is known to work
# with rust-nightly 1.28.0 as of Jun-1-2018.
cargo +nightly-2018-06-01-x86_64-unknown-freebsd test --all-features

# Check that benchmarks run, but don't care about results
rustup run nightly-2018-06-01-x86_64-unknown-freebsd cargo bench

# It should also work on stable >= 1.26.0
cargo test
