#! /bin/sh

# Render a graph of the use relationships between the source files, much like a
# C++ include graph
#
# Usage: Run the script. "use_graph.svg" will be created in PWD.

TEMPFILE=`mktemp -t use_graph.dot`
cargo +nightly-2018-06-01-x86_64-unknown-freebsd modules graph > $TEMPFILE
sed '
# Exclude test modules
/::t::/d
/::t"/d
#
# Exclude boring modules
/"::bfffs"/d
/"::common"/d
/"::common::cache_mock"/d
/"::common::ddml_mock"/d
/"::common::idml_mock"/d
/"::common::tree_mock"/d
/"::common::dva"/d
/"::common::sgcursor"/d
/"::common::tree::atomic_u64_serializer"/d
/"::common::tree::tree_root_serializer"/d
/"::common::fs_tree::dbs_serializer"/d
/"::common::vdev"/d
#
# Exclude submodule edges, leaving just the use edges
/->.*weight=100/d
' < $TEMPFILE | dot -Tsvg -o use_graph.svg
rm $TEMPFILE
