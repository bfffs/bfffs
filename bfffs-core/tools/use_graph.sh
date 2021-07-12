#! /bin/sh

# Render a graph of the use relationships between the source files, much like a
# C++ include graph
#
# Usage: Run the script. "use_graph.svg" will be created in PWD.

# The output is still a little wrong due to cargo-modules bugs.  See
# https://github.com/regexident/cargo-modules/issues/79
# https://github.com/regexident/cargo-modules/issues/80

TEMPFILE=`mktemp -t use_graph.dot`
cargo modules generate graph --layout dot --with-uses --package bfffs-core > $TEMPFILE
sed -E '
# Exclude test modules
/::t\>/d
/::benches\>/d
/::tests\>/d
/node::has_dirty_children\>/d
/node::serialization\>/d
/_mock\>/d
#
# Exclude boring modules
/"bfffs_core::types"/d
/"bfffs_core::util"/d
/"bfffs_core::vdev"/d
/_serializer"/d
# Uncomment to exclude owns edges
#/label=.owns/d
' < $TEMPFILE | dot -Tsvg -o use_graph.svg
rm $TEMPFILE
