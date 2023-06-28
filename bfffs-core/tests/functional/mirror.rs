// vim: tw=80

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};
use bfffs_core::{
    label::*,
    mirror::Mirror,
    vdev_block::*,
    vdev::Vdev,
    vdev_file::*,
};
use itertools::Itertools;
use rstest::{fixture, rstest};
use tempfile::{Builder, TempDir};

type Harness = (Mirror, TempDir, Vec<String>);
#[fixture]
fn harness() -> Harness {
    let num_disks = 3;
    let len = 1 << 26;  // 64 MB
    let tempdir = t!(
        Builder::new().prefix("test_mirror_persistence").tempdir()
    );
    let paths = (0..num_disks).map(|i| {
        let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
        let file = t!(fs::File::create(&fname));
        t!(file.set_len(len));
        fname
    }).collect::<Vec<_>>();
    let mirror = Mirror::create(&paths, None).unwrap();
    (mirror, tempdir, paths)
}

mod open {
    use super::*;

    /// Regardless of the order in which the devices are given to
    /// Mirror::open, it will construct itself in the correct order.
    #[rstest]
    #[tokio::test]
    async fn ordering(harness: Harness) {
        let (old_mirror, _tempdir, paths) = harness;
        let uuid = old_mirror.uuid();
        let label_writer = LabelWriter::new(0);
        old_mirror.write_label(label_writer).await.unwrap();
        drop(old_mirror);

        for perm in paths.iter().permutations(paths.len()) {
            let mut combined = Vec::new();
            for path in perm.iter() {
                let (leaf, reader) = VdevFile::open(path).await.unwrap();
                combined.push((VdevBlock::new(leaf), reader));
            }
            let (mirror, _) = Mirror::open(Some(uuid), combined);
            let status = mirror.status();
            assert_eq!(status.leaves[0].0, Path::new(&paths[0]));
            assert_eq!(status.leaves[1].0, Path::new(&paths[1]));
            assert_eq!(status.leaves[2].0, Path::new(&paths[2]));
        }
    }

}

mod persistence {
    use super::*;
    use pretty_assertions::assert_eq;

    const GOLDEN_MIRROR_LABEL: [u8; 72] = [
        // Past the VdevFile::Label, we have a mirror::Label
        // First come's the mirror's UUID
        0x75, 0x9a, 0x2b, 0x9e, 0x56, 0x11, 0x41, 0x7c,
        0x80, 0xe1, 0x32, 0x13, 0xd9, 0xef, 0x88, 0x3b,
        // Then the vector of children's UUIDs.  A 64-bit count of children,
        // then each UUID is 64-bits long
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x9c, 0xaf, 0x4c, 0xf8, 0xba, 0x40, 0xc6, 0x64,
        0x2f, 0x88, 0x5a, 0x01, 0x62, 0xbf, 0xbd, 0x54,
        0x9f, 0xa3, 0x41, 0x65, 0x8e, 0x75, 0xfa, 0x7e,
        0xcb, 0x52, 0x45, 0x2e, 0xd3, 0x14, 0x96, 0x91,
        0x17, 0x18, 0x4a, 0xc5, 0xbd, 0x06, 0x24, 0xd1,
        0xd2, 0xa9, 0x6d, 0x67, 0x24, 0x31, 0xb8, 0x32,
    ];

    #[rstest]
    #[tokio::test]
    async fn open_after_write(harness: Harness) {
        let (old_vdev, _tempdir, paths) = harness;
        let uuid = old_vdev.uuid();
        let label_writer = LabelWriter::new(0);
        old_vdev.write_label(label_writer).await.unwrap();
        let mut children = Vec::new();
        for path in paths {
            let (leaf, reader) = VdevFile::open(path).await.unwrap();
            children.push((VdevBlock::new(leaf), reader));
        }
        let (mirror, _) = Mirror::open(Some(uuid), children);
        assert_eq!(uuid, mirror.uuid());
    }

    #[rstest]
    #[tokio::test]
    async fn write_label(harness: Harness) {
        let label_writer = LabelWriter::new(0);
        harness.0.write_label(label_writer).await.unwrap();

        for path in harness.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(72)).unwrap();   // Skip the VdevLeaf label
            f.read_exact(&mut v).unwrap();
            // Uncomment this block to save the binary label for inspection
            /* {
                use std::fs::File;
                use std::io::Write;
                let mut df = File::create("/tmp/label.bin").unwrap();
                df.write_all(&v[..]).unwrap();
                println!("UUID is {}", harness.0.uuid());
            } */
            // Compare against the golden master, skipping the checksum and UUID
            // fields
            assert_eq!(&v[16..24], &GOLDEN_MIRROR_LABEL[16..24]);
            // Rest of the buffer should be zero-filled
            assert!(v[72..].iter().all(|&x| x == 0));
        }
    }
}

