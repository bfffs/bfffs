# bfffs

The Black Footed Ferret File System

## Overview

BFFFS is a new copy-on-write file system written entirely in Rust.  It aims
tohave all of the usual COW goodies (transactional integrity, snapshots,
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

# License
BFFFS is primarily distributed under the terms of both the MIT license
and the Apache License (Version 2.0).

See LICENSE-APACHE, and LICENSE-MIT for details
