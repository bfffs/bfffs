#! /bin/sh
set -e

# Run all of the tests in order of most likely to fail to least likely
# Use --all-targets to include benchmarks (in debug mode)
cargo +nightly test --all-features --all --all-targets

# It should also work on stable >= 1.42.0
cargo +1.42.0 test --all

# bfffs-fio should stay in consistent style.  The other crates can't, because
# rustfmt screws them up.
cargo +nightly fmt --package bfffs-fio -- --check
