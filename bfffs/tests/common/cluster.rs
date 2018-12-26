// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name persistence;

    use bfffs::common::vdev_block::*;
    use bfffs::common::vdev_raid::*;
    use bfffs::common::cluster::*;
    use bfffs::sys::vdev_file::*;
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        num::NonZeroU64
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_LABEL: [u8; 160] = [
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        0x14, 0x60, 0xb0, 0x0a, 0xa9, 0xaf, 0x30, 0x2a, // .`....0*
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, // .......x
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x6b, 0xc6, 0x31, 0xf8, 0xa4, 0x4d, 0x49, 0xd2, // k.1..MI.
        0x8d, 0x8d, 0xb6, 0xb2, 0x59, 0x2c, 0xb1, 0xfc, // ....Y,..
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0xde, 0x70, 0x49, 0x56, 0xe7, 0xb7, 0x4b, 0x1c, // .pIV..K.
        0x98, 0x0f, 0xf5, 0xf7, 0xc3, 0xe0, 0x94, 0x9d, // ........
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x6b, 0xc6, 0x31, 0xf8, 0xa4, 0x4d, 0x49, 0xd2, // k.1..MI.
        0x8d, 0x8d, 0xb6, 0xb2, 0x59, 0x2c, 0xb1, 0xfc, // ....Y,..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
    ];
    const GOLDEN_SPACEMAP: [u8; 48] = [
        225, 77, 241, 146, 92, 56, 181, 129,            // Checksum
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
    ];

    fixture!( objects() -> (Runtime, Cluster, TempDir, String) {
        setup(&mut self) {
            let len = 1 << 29;  // 512 MB
            let tempdir = t!(TempDir::new("test_cluster_persistence"));
            let fname = format!("{}/vdev", tempdir.path().display());
            let file = t!(fs::File::create(&fname));
            t!(file.set_len(len));
            let rt = Runtime::new().unwrap();
            let lpz = NonZeroU64::new(65536);
            let cluster = Cluster::create(None, 1, 1, lpz, 0, &[fname.clone()]);
            (rt, cluster, tempdir, fname)
        }
    });

    // Test Cluster::open
    test open(objects()) {
        {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .open(objects.val.3.clone()).unwrap();
            f.write_all(&GOLDEN_LABEL).unwrap();
            f.seek(SeekFrom::Start(32768)).unwrap();
            f.write_all(&GOLDEN_SPACEMAP).unwrap();
        }
        Runtime::new().unwrap().block_on(future::lazy(|| {
            VdevFile::open(objects.val.3.clone())
            .map(|(leaf, reader)| {
                (VdevBlock::new(leaf), reader)
            }).and_then(move |combined| {
                let (vdev_raid, _reader) = VdevRaid::open(None, vec![combined]);
                 Cluster::open(vdev_raid)
            }).map(|cluster| {
                assert_eq!(cluster.allocated(), 0);
            })
        })).unwrap();
    }

    test flush(objects()) {
        let (mut rt, old_cluster, _tempdir, path) = objects.val;
        rt.block_on(future::lazy(|| {
            old_cluster.flush(0)
        })).unwrap();

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
