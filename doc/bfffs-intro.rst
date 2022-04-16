BFFFS
=====

A high-performance SMR-native copy-on-write filesystem

:author: Alan Somers <asomers@gmail.com>

Introduction
------------

Sequential I/O has always been a design goal of file systems.  Most
legacy file systems do a decent, but not perfect, job of issueing their I/O
operations sequentially.  With conventional hard drives that was good enough,
especially after the advent of NCQ/TCQ.  However, SMR hard drives require
writes to be perfectly sequential.  Almost perfect isn't good enough.  Adapting
legacy file systems for SMR technology has proven intractable.  Seagate
attempted to adapt ext4 and BtrFS, but ultimately gave up.  I personally tried
to adapt ZFS, but discovered that only modest improvements were possible.
Microsoft's ReFS supports SMR disks, but only in a separate tier.  All-SMR ReFS
pools are not possible.  Samsung's F2FS's SMR support is the best of any
production file system, but F2FS is an overwriting file system without modern
features like snapshots and data checksums.

BFFFS is a novel file system, written from scratch under the assumption that
all writes must always be issued sequentially.  It is a copy-on-write file
system, with a feature set comparable to ZFS or BtrFS.  Surprisingly, not only
is it possible to achieve these features despite the sequential-write
constraint, but many of them work *better* than in legacy file systems.
Specifically, the sequential write constraint allows:

* Greatly simplified spacemaps, leading to a faster transaction sync procedure.
* No RAID-5 write-hole.
* Better random read IOPs on RAID, compared to ZFS.
* Faster RAID rebuilds
* Simpler and faster block-pointer rewrite capability, enabling features like
  defragmentation and RAID expansion as well as garbage collection.

In addition, BFFFS incorporates the latest research in file system technology.
It has several significant advantages over legacy file systems, beyond its
SMR-compatibility.

* Declustered RAID.  RAID arrays can be of any size up to 255 disks with an
  independently selectable stripe size and redundancy level.  Declustered RAID
  maximizes the IOPs available for user I/O during a rebuild.

* Dynamic striping.  Just like ZFS, BFFFS can dynamically stripe multiple RAID
  arrays to build pools with > 255 disks.

* Higher redundancy levels.  BFFS's RAID implementation allows for arrays with
  up to 253-way parity, compared to a maximum of 3-way parity for ZFS.

* Data integrity is guaranteed by a Merkle hash tree, like ZFS, but with a
  better checksum algorithm.

* Optional data compression.  Metadata compression is always enabled, using
  algorithms carefully tuned for each type of metadata record.

* More efficient metadata.  ZFS stores metadata in unbalanced trees of indirect
  blocks.  Key-Value data is stored in hash tables mapped over flat objects,
  which is slow and complicated.  ZFS's block pointers are also bloated,
  leading to a large metadata ratio.  BFFFS stores metadata in a balanced
  B+Tree, similar to BtrFS's strategy.  Key-Value data is stored directly in
  the B+Tree, eliminating the need for hash tables.  But BFFFS uses ZFS's
  Merkle hash tree of checksums, which is superior to BtrFS's strategy, and a
  novel spacemap.  So BFFFS has a lower metadata ratio than either ZFS or
  BtrFS.

* More efficient deduplication.  BFFFS can deduplicate data with about 1000x
  less RAM than ZFS.

* Hybrid pools.  Though BFFFS can run on SMR-disks alone, it's also designed to
  support hybrid pools that store performance-critical metadata on flash.

Current Status
--------------

BFFFS is currently implemented in userspace with FUSE.  Most of the usual POSIX
operations are supported.  Advanced features like RAID, dynamic striping,
garbage collection, checksums, and compression work, because they were designed
in from the beginning.  But much work remains to be done.  Mirroring, RAID
rebuild, deduplication, defragmentation, snapshots, and hybrid pools are not
yet done.  There's also a lot of boring details that need to be finished, like
better cache management, high-performance fsync, etc.
