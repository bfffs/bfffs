// vim: tw=80
mod persistence {
    use bfffs_core::vdev_block::*;
    use bfffs_core::raid;
    use bfffs_core::cluster::*;
    use bfffs_core::mirror::Mirror;
    use bfffs_core::vdev_file::*;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        num::NonZeroU64
    };
    use tempfile::{Builder, TempDir};

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_LABEL: [u8; 172] = [
        // First the VdevFile label
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        0xca, 0xf4, 0x51, 0x4b, 0x59, 0x00, 0x76, 0x4a,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64,
        0x30, 0x55, 0xe2, 0x7d, 0x68, 0xeb, 0x4c, 0x96,
        0xbd, 0x50, 0x88, 0xe4, 0x3f, 0x92, 0xe8, 0x48,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Then the mirror label
        0xb9, 0xd8, 0x56, 0x54, 0xbd, 0xe4, 0x40, 0xe5,
        0xa2, 0xfb, 0x7b, 0x8b, 0xb6, 0x17, 0xff, 0x55,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x57, 0x33, 0x3b, 0x93, 0xce, 0xea, 0x44, 0x42,
        0xa6, 0xd2, 0x47, 0x07, 0x26, 0x74, 0x76, 0xdb,
        // Then the raid label
        0x00, 0x00, 0x00, 0x00, 0x86, 0x82, 0x03, 0x1d,
        0x3a, 0x06, 0x4b, 0x4a, 0xb0, 0xb5, 0x5e, 0x85,
        0xd9, 0x0c, 0x17, 0xca, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x30, 0x55, 0xe2, 0x7d,
        0x68, 0xeb, 0x4c, 0x96, 0xbd, 0x50, 0x88, 0xe4,
        0x3f, 0x92, 0xe8, 0x48,
        // Cluster does not have a label of its own
    ];
    const GOLDEN_SPACEMAP: [u8; 48] = [
        225, 77, 241, 146, 92, 56, 181, 129,            // Checksum
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
    ];

    #[fixture]
    fn objects() -> (Cluster, TempDir, String) {
        let len = 1 << 29;  // 512 MB
        let tempdir =
            t!(Builder::new().prefix("test_cluster_persistence").tempdir());
        let fname = format!("{}/vdev", tempdir.path().display());
        let file = t!(fs::File::create(&fname));
        t!(file.set_len(len));
        let lpz = NonZeroU64::new(65536);
        let mirror = Mirror::create(&[&fname], lpz).unwrap();
        let raid = raid::create(None, 1, 0, vec![mirror]);
        let cluster = Cluster::create(raid);
        (cluster, tempdir, fname)
    }

    // No need to test dumping non-empty Clusters here.  That's handled by
    // Cluster's unit tests.
    #[rstest]
    #[tokio::test]
    async fn dump_empty(objects: (Cluster, TempDir, String)) {
        let (cluster, _tempdir, _path) = objects;
        let dumped = cluster.dump_fsm();
        assert_eq!(dumped,
"FreeSpaceMap: 2 Zones: 0 Closed, 2 Empty, 0 Open
 Zone | TXG |                              Space                               |
------|-----|------------------------------------------------------------------|
    0 |  -  |                                                                  |
");
    }

    // Test Cluster::open
    #[rstest]
    #[tokio::test]
    async fn open(objects: (Cluster, TempDir, String)) {
        {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .open(objects.2.clone()).unwrap();
            f.write_all(&GOLDEN_LABEL).unwrap();
            f.seek(SeekFrom::Start(32768)).unwrap();
            f.write_all(&GOLDEN_SPACEMAP).unwrap();
        }
        let (leaf, reader) = VdevFile::open(objects.2.clone()).await.unwrap();
        let mirror_children = vec![(VdevBlock::new(leaf), reader)];
        let raid_children = Mirror::open(None, mirror_children);
        let (vdev_raid, _reader) = raid::open(None, vec![raid_children]);
        let cluster = Cluster::open(vdev_raid).await.unwrap();
        assert_eq!(cluster.allocated(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn flush(objects: (Cluster, TempDir, String)) {
        let (old_cluster, _tempdir, path) = objects;
        old_cluster.flush(0).await.unwrap();

        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 4096];
        // Skip the whole label and just read the spacemap
        f.seek(SeekFrom::Start(32768)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master
        assert_eq!(&v[0..48], &GOLDEN_SPACEMAP[..]);
        // Rest of the buffer should be zero-filled
        assert!(v[48..].iter().all(|&x| x == 0));
    }
}
