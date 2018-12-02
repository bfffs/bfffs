#! /usr/bin/awk -f
#
# USAGE:
# 1) mount bfffs
# 2) cd to the mountpoint
# 3) prove -rv /path/to/pjdfstest/tests 2>&1 | tee /tmp/pjd.log
# 4) ./pjd.awk /tmp/pjd.log

BEGIN {
	total=0
	failed=0
}
$6 == "Failed:" {
	total=total + $5
	sub("\\)", " ", $7)
	failed=failed + $7
}
END {
	print "total: ", total, "failed ", failed
}
