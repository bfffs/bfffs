#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

# Mockers only works with this specific toolchain
rustup run nightly-2017-10-14-x86_64-unknown-freebsd cargo test --all-features

# Check that benchmarks run, but don't care about results
rustup run nightly cargo bench

# It should also work on stable and recent nightly
rustup run stable cargo test -j1
rustup run nightly cargo test
