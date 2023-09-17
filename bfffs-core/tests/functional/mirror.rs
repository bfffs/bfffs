// vim: tw=80

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};
use bfffs_core::{
    label::*,
    mirror::{Manager, Mirror},
    vdev::{Health, Vdev},
};
use nonzero_ext::nonzero;
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

    /// It should be possible to import a mirror when some children are missing
    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[tokio::test]
    async fn missing_children(harness: Harness, #[case] missing: usize) {
        let (old_vdev, _tempdir, paths) = harness;
        let uuid = old_vdev.uuid();
        let label_writer = LabelWriter::new(0);
        old_vdev.write_label(label_writer).await.unwrap();
        let old_status = old_vdev.status();
        drop(old_vdev);

        fs::remove_file(paths[missing].clone()).unwrap();
        let mut manager = Manager::default();
        for path in paths.iter() {
            let _ = manager.taste(path).await;
        }
        let (mirror, _) = manager.import(uuid).await.unwrap();
        assert_eq!(uuid, mirror.uuid());
        let status = mirror.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        for i in 0..paths.len() {
            assert_eq!(old_status.leaves[i].uuid, status.leaves[i].uuid);
            if i != missing {
                // BFFFS doesn't yet remember the paths of disks, whether
                // they're imported or not, missing or present.
                assert_eq!(old_status.leaves[i].path, status.leaves[i].path);
            }
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
        drop(old_vdev);

        let mut manager = Manager::default();
        for path in paths.iter() {
            manager.taste(path).await.unwrap();
        }
        let (mirror, _) = manager.import(uuid).await.unwrap();
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

