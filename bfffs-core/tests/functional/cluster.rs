// vim: tw=80
mod persistence {
    use bfffs_core::raid;
    use bfffs_core::cluster::{Cluster, Manager};
    use bfffs_core::mirror::Mirror;
    use bfffs_core::Uuid;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        num::NonZeroU64
    };
    use tempfile::{Builder, TempDir};
    use uuid::uuid;
    
    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_LABEL: [u8; 148] = [
        // First the Root label
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        0x15, 0x86, 0x02, 0x82, 0xd1, 0xb9, 0xd9, 0x61, // checksum
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, // label length, BE
        0x14, 0xc5, 0x36, 0x80, 0x29, 0x2b, 0x4c, 0x8a, // UUID
        0x85, 0xa5, 0x30, 0x02, 0xd0, 0x6e, 0xda, 0x1f, // UUID, line 2
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // LBAs per zone
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, // LBAs
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // LBAs for spacemap
        0x00, 0x00, 0x00, 0x00,                         // TXG number
        // Then the mirror label
        0xf0, 0x90, 0xc2, 0x77, 0x81, 0xd4, 0x45, 0xb3, // UUID
        0xa8, 0x99, 0x6d, 0xad, 0xcd, 0x57, 0xee, 0xb0, // UUID, line 2
        0x01, 0x00, 0x00, 0x00,                         // child count
        0x14, 0xc5, 0x36, 0x80, 0x29, 0x2b, 0x4c, 0x8a, // child UUID
        0x85, 0xa5, 0x30, 0x02, 0xd0, 0x6e, 0xda, 0x1f, // child UUID, line 2

        // Then the raid label
        0x00, 0x00, 0x00, 0x00,                         // NullRaid discrim.
        0x03, 0x20, 0x02, 0xb4, 0xc0, 0xfa, 0x46, 0x49, // UUID
        0xbe, 0x68, 0x96, 0x30, 0x84, 0x46, 0xcf, 0x82, // UUID v2
        0xf0, 0x90, 0xc2, 0x77, 0x81, 0xd4, 0x45, 0xb3, // child UUID
        0xa8, 0x99, 0x6d, 0xad, 0xcd, 0x57, 0xee, 0xb0, // child UUID, v2

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
    const GOLDEN_UUID: Uuid = Uuid::from_uuid(uuid!("032002b4-c0fa-4649-be68-96308446cf82"));

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
"FreeSpaceMap: 2 Zones: 2 Empty, 0 Open, 0 Closed, 0 Dead
 Zone | S | TXG |                            Space                             |
------|---|-----|--------------------------------------------------------------|
    0 | E |  -  |                                                              |
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
            drop(objects.0);
        }
        let mut manager = Manager::default();
        manager.taste(objects.2).await.unwrap();
        let (cluster, _) = manager.import(GOLDEN_UUID).await.unwrap();
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
