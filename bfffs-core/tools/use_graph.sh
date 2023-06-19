#! /bin/sh

# Render a graph of the use relationships between the source files, much like a
# C++ include graph
#
# Usage: Run the script. "use_graph.svg" will be created in PWD.

# The output is still a little wrong due to cargo-modules bugs.  See
# https://github.com/regexident/cargo-modules/issues/79
# https://github.com/regexident/cargo-modules/issues/80

VERBOSE=0
while getopts "vh" arg; do
case $arg in
	h)
		echo "usage: $0 [OPTS]"
		echo "	-v	Include more stuff.  Can specify multiple times"
		exit 2
		;;
	v)
		VERBOSE=$(($VERBOSE + 1))
		;;
esac
done

TEMPFILE=`mktemp -t use_graph.dot`
cargo modules generate graph --layout dot --with-uses --package bfffs-core > $TEMPFILE
sed -I "" -E '
# Exclude test modules
/::t\>/d
/::benches\>/d
/::tests\>/d
/node::has_dirty_children\>/d
/node::serialization\>/d
/_mock\>/d
#
# Exclude edges for utility modules and trait definitions that are used at many
# levels of the stack
/-> "bfffs_core::types"/d
/-> "bfffs_core::util/d
/-> "bfffs_core::vdev"/d
#/-> "bfffs_core::dml"/d
#
# Exclude some constants that really dont contribute
/"bfffs_core::(fs_tree|label|property|util|vdev_block|vdev_file::ffi)::[A-Z]/d
#
# Exclude serde serializer modules, which are tiny and only used by proc macros,
# which cargo-modules does not understand.
/_serializer"/d
#
# Exclude ownership edges.  They are confusing when combined with use edges.
/label=.owns/d
s/label="uses", //
#
# Fixup certain edges that cargo-modules creates incorrectly due to
# https://github.com/regexident/cargo-modules/issues/79
s/cluster" -> "bfffs_core::raid::vdev_raid_api"/cluster" -> "bfffs_core::raid"/
s/tree::node" -> "bfffs_core::cache"/tree::node" -> "bfffs_core::dml"/
#
# Use "use" edges for rank ordering
s/constraint=false/constraint=true/
' $TEMPFILE

if [ "$VERBOSE" -lt 2 ]; then
	# De-double some modules.  They are doubled to facilitate mocking, but
	# that is not relevant for the use graph.
	sed -I "" -E '
	/^digraph/a \
	concentrate=true
	/splines="line",/d
	' $TEMPFILE

	for mod in cache database dataset ddml idml tree writeback; do
		sed -I "" -E "
		/mod\|$mod::$mod/d
		s/$mod::$mod/$mod/
		/\"bfffs_core::$mod\" -> \"bfffs_core::$mod\"/d
		" $TEMPFILE
	done

	# Inline the tree::node module, which is supposed to be private to tree
	# The only reason it doesn't appear that way is because of
	# https://github.com/regexident/cargo-modules/issues/79
	sed -I "" -E "
	/mod\|tree::node\"/d
	s/tree::node/tree/
	/\"bfffs_core::tree\" -> \"bfffs_core::tree\"/d
	" $TEMPFILE
fi

if [ "$VERBOSE" -lt 1 ]; then
	# Exclude some traits and control-path modules that are used all over
	# the place
	sed -I "" -E '
		/-> "bfffs_core::cache/d
		/"bfffs_core::cache::cache"/d
		/"bfffs_core::device_manager" ->/d
		/"bfffs_core::dml"/d
		/-> "bfffs_core::raid::vdev_raid_api"/d
		/-> "bfffs_core::label"/d
		/-> "bfffs_core::property::/d
		/-> "bfffs_core::writeback"/d
		/"bfffs_core::writeback::writeback"/d
	' $TEMPFILE
fi
dot -Tsvg -o use_graph.svg $TEMPFILE
rm $TEMPFILE
