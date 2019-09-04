#! /bin/sh

# Render a graph of the use relationships between the source files, much like a
# C++ include graph
#
# Usage: Run the script. "use_graph.svg" will be created in PWD.

TEMPFILE=`mktemp -t use_graph.dot`
# Until cargo-modules gets released with Rust 2018 support, use my local build
/usr/home/somers/src/rust/cargo-modules/target/debug/cargo-modules --enable-edition-2018 modules graph > $TEMPFILE
sed -E '
# Exclude test modules
/::t\>/d
/::benches\>/d
/::tests\>/d
/_mock\>/d
#
# Exclude boring modules
/"bfffs"/d
/"bfffs::common"/d
/"bfffs::common::raid::sgcursor"/d
/"bfffs::common::tree::tree::atomic_u64_serializer"/d
/"bfffs::common::tree::tree::tree_root_serializer"/d
/"bfffs::common::tree::node::node_serializer"/d
/"bfffs::common::fs_tree::dbs_serializer"/d
/"bfffs::common::vdev"/d
' < $TEMPFILE | dot -Tsvg -o use_graph.svg
rm $TEMPFILE
