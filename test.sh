#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely

# Mockers is very picky about rust versions.  Mockers 0.9.4 is known to work
# with rust-nightly 1.26.0 as of Feb-26-2018.
rustup run nightly cargo test --all-features

# Check that benchmarks run, but don't care about results
rustup run nightly cargo bench

# It should also work on stable
rustup run stable cargo test -j1
