# bfffs

The Black Footed Ferret File System

## Overview

BFFFS is a new copy-on-write file system written entirely in Rust.  It aims
to have all of the usual COW goodies (transactional integrity, snapshots,
clones, etc), but with better RAID, faster performance in some ways, and
native support for SMR hard drives and OpenChannel SSDs.

## Usage

BFFFS is currently implemented as a FUSE server, and currently runs on
FreeBSD.  The easiest way to start using it with a single disk pool is to do:

```
pkg install -y c-blosc isa-l fusefs-libs pkgconf
cargo build
truncate -s 1g /tmp/bfffs.img
target/debug/bfffs pool create foo /tmp/bfffs.img
sudo target/debug/bfffsd -o allow_other,default_permissions foo /mnt /tmp/bfffs.img
```

Where `OPTIONS` can be:

* `allow_other,default_permissions` - Allow users other than the one running
  bfffsd to access the mounted file system.
* `cache_size` - Set the maximum mount of cached clean data in bytes.  Higher
  values will generally give better performance.  Beware, though, that unlike
  an in-kernel file system bfffsd will never shrink the cache in response to
  memory pressure.  The kernel will simply kill bfffsd or some other process
  instead.
* `writeback_size` - Set the soft limit for the amount of cached dirty data in
  bytes.  This is completely independent of `cache_size`.  Generally it should
  be at least several seconds' worth of your disks' maximum throughput.  The
  actual amount of dirty data may exceed this limit, but not by more than a few
  MB.

# License
BFFFS is primarily distributed under the terms of both the MIT license
and the Apache License (Version 2.0).

See LICENSE-APACHE, and LICENSE-MIT for details
