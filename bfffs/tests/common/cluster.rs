// vim: tw=80
use galvanic_test::*;

test_suite! {
    name persistence;

    use bfffs::common::vdev_block::*;
    use bfffs::common::vdev_raid::*;
    use bfffs::common::cluster::*;
    use bfffs::sys::vdev_file::*;
    use futures::{Future, future};
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
    const GOLDEN_LABEL: [u8; 217] = [
        0x42, 0x46, 0x46, 0x46, 0x53, 0x20, 0x56, 0x64, // BFFFS Vd
        0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ev......
        0x24, 0xff, 0x7e, 0x7c, 0xa3, 0x77, 0xcd, 0x84, // $.~|.w..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb9, // ........
        0xa4, 0x64, 0x75, 0x75, 0x69, 0x64, 0x50, 0xfe, // .duuidP.
        0x6e, 0x0e, 0xd1, 0x2f, 0xcf, 0x46, 0x3e, 0xb7, // n../.F>.
        0x96, 0xbd, 0x5f, 0x9e, 0xeb, 0x42, 0x07, 0x6d, // .._..B.m
        0x6c, 0x62, 0x61, 0x73, 0x5f, 0x70, 0x65, 0x72, // lbas_per
        0x5f, 0x7a, 0x6f, 0x6e, 0x65, 0x1a, 0x00, 0x01, // _zone...
        0x00, 0x00, 0x64, 0x6c, 0x62, 0x61, 0x73, 0x1a, // ..dlbas.
        0x00, 0x02, 0x00, 0x00, 0x6e, 0x73, 0x70, 0x61, // ....nspa
        0x63, 0x65, 0x6d, 0x61, 0x70, 0x5f, 0x73, 0x70, // cemap_sp
        0x61, 0x63, 0x65, 0x01, 0xa6, 0x64, 0x75, 0x75, // ace..duu
        0x69, 0x64, 0x50, 0xa1, 0xe7, 0x3c, 0xc1, 0x00, // idP..<..
        0x26, 0x46, 0x9f, 0xae, 0xd7, 0x00, 0x7f, 0x29, // &F.....)
        0x23, 0x30, 0x72, 0x69, 0x63, 0x68, 0x75, 0x6e, // #0richun
        0x6b, 0x73, 0x69, 0x7a, 0x65, 0x01, 0x70, 0x64, // ksize.pd
        0x69, 0x73, 0x6b, 0x73, 0x5f, 0x70, 0x65, 0x72, // isks_per
        0x5f, 0x73, 0x74, 0x72, 0x69, 0x70, 0x65, 0x01, // _stripe.
        0x6a, 0x72, 0x65, 0x64, 0x75, 0x6e, 0x64, 0x61, // jredunda
        0x6e, 0x63, 0x79, 0x00, 0x70, 0x6c, 0x61, 0x79, // ncy.play
        0x6f, 0x75, 0x74, 0x5f, 0x61, 0x6c, 0x67, 0x6f, // out_algo
        0x72, 0x69, 0x74, 0x68, 0x6d, 0x68, 0x4e, 0x75, // rithmhNu
        0x6c, 0x6c, 0x52, 0x61, 0x69, 0x64, 0x68, 0x63, // llRaidhc
        0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x81, // hildren.
        0x50, 0xfe, 0x6e, 0x0e, 0xd1, 0x2f, 0xcf, 0x46, // P.n../.F
        0x3e, 0xb7, 0x96, 0xbd, 0x5f, 0x9e, 0xeb, 0x42, // >..._..B
        0x07,
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
            VdevFile::open(objects.val.3.clone()).map(|(leaf, reader)| {
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
