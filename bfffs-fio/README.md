# BFFFS-fio

fio backend for BFFFS pools, bypassing FUSE.

# Usage

Install fio-3.12 or later.  fio-3.10 has a known bug with external engine option
processing.  Manually create your pool and update bfffs.fio with the correct
poolname and vdev name.  Then run fio.

```sh
cd /path/to/bfffs
target/release/bfffs pool create <poolname> <vdevs>
cd /path/to/bfffs
cargo build --release --all
vim bfffs-fio/bfffs.fio
fio bfffs-fio/bfffs.fio /
```

# License
`bfffs-fio` is distributed under the GPL license, version 2.
