#! /bin/sh

# Render a graph of the use relationships between the source files, much like a
# C++ include graph
#
# It can handle most variations of use statement, but not all.  Multi-line
# statements are a problem.  There are probably others.
#
# Usage: Modify EXTERNAL_CRATES as appropriate.  Those relationships will be
# excluded from the graph.  Then run the script.  "use_graph.svg" will be
# created in PWD.

EXTERNAL_CRATES='(std|byteorder|divbuf|fixedbitset|futures|isa_l|itertools|metrohash|mio|mockers|modulo|permutohedron|nix|rand|serde|serde_cbor|super|tempdir|tokio|tokio_file|uuid)'
BORING_FILES='^\<(common)\>$'

EXTERNAL_PAT="^use.${EXTERNAL_CRATES}"
SRC=`dirname $0`/../src
TEMPFILE=`mktemp -t use_graph.dot`
{
echo "strict digraph use_graph {"
for f in `find $SRC -name '*.rs'`; do
	basename=${f#${SRC}/}
	mod=${basename%.rs}
	uses=`awk -v external_pat=${EXTERNAL_PAT} '
		$0 ~ external_pat {next;}
		/^use .*{/ {
			sub(/use /, "", $0);
			gsub(/::/, "/", $0);
			sub(/sys\//, "sys/fuse/", $0);
			prefix=$0
			tail=$0
			sub(/\/?\{.*/, "", prefix);
			sub(/^\//, "", prefix);
			sub(/^.*\{/, "", tail);
			sub(/\}.*$/, "", tail);
			gsub(/,/, "", tail);
			split(tail, modules)
			OFS="/"
			if (prefix == "")
				for (mod in modules)
					print modules[mod]
			else
				for (mod in modules)
					print prefix, modules[mod]
		}
		/^use / {
			sub(/(::\*)?;/, "", $2);
			sub(/^::/, "", $2);
			gsub(/::/, "/", $2);
			sub(/sys\//, "sys/fuse/", $2);
			print $2
		}' $f`
	for u in $uses; do
		if [ -f ${SRC}/${u}.rs ]; then
			usefile=${u}
		elif [ -f ${SRC}/${u}/mod.rs ]; then
			usefile=${u}
		elif [ -f `dirname ${SRC}/${u}.rs`.rs ]; then
			usefile=`dirname ${u}.rs`
		else
			echo "File ${u} not found" >&2
			continue
		fi
		if echo $usefile | egrep -q $BORING_FILES; then
			# Skip this boring file
		else
			echo "\"$mod\" -> \"$usefile\""
		fi
	done
done
echo "}" ; } > $TEMPFILE
dot -Tsvg -o use_graph.svg $TEMPFILE
rm $TEMPFILE
