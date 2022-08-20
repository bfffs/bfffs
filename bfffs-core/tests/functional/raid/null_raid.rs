// vim: tw=80

mod persistence {
    use bfffs_core::{
        label::*,
        mirror::Mirror,
        vdev_block::*,
        vdev::Vdev,
        vdev_file::*,
        raid::{self, NullRaid, VdevRaidApi},
    };
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
    };
    use super::super::super::*;
    use tempfile::{Builder, TempDir};

    const GOLDEN_VDEV_NULLRAID_LABEL: [u8; 36] = [
        // Past the mirror::Label, we have a raid::Label
        // First comes the NullRaid discriminant
        0x00, 0x00, 0x00, 0x00,
        // Then the NullRaid label, beginning with a UUID
                                0x2f, 0x27, 0x51, 0xe5,
        0xe8, 0x58, 0x45, 0x1b, 0x92, 0xb5, 0x24, 0x0f,
        0x23, 0x7b, 0xc9, 0xbe,
        // Then the child's UUID
                                0xe7, 0x4c, 0xba, 0x28,
        0xbb, 0xf1, 0x4c, 0x1a, 0xad, 0x90, 0xbf, 0x48,
        0xf4, 0x26, 0x1f, 0x7a,
    ];

    #[fixture]
    fn harness() -> (NullRaid, TempDir, String) {
        let len = 1 << 26;  // 64 MB
        let tempdir = t!(
            Builder::new().prefix("test_vdev_null_raid_persistence").tempdir()
        );
        let path = format!("{}/vdev", tempdir.path().display());
        let file = t!(fs::File::create(&path));
        t!(file.set_len(len));
        let mirror = Mirror::create(&[&path], None).unwrap();
        let vdev = NullRaid::create(mirror);
        (vdev, tempdir, path)
    }

    #[rstest]
    #[tokio::test]
    async fn open_after_write(harness: (NullRaid, TempDir, String)) {
        let (old_vdev, _tempdir, path) = harness;
        let uuid = old_vdev.uuid();
        let label_writer = LabelWriter::new(0);
        old_vdev.write_label(label_writer).await.unwrap();
        let (leaf, reader) = VdevFile::open(path).await.unwrap();
        let mirror_children = vec![(VdevBlock::new(leaf), reader)];
        let (mirror, reader) = Mirror::open(None, mirror_children);
        let (vdev, _) = raid::open(Some(uuid), vec![(mirror, reader)]);
        assert_eq!(uuid, vdev.uuid());
    }

    #[rstest]
    fn write_label(harness: (NullRaid, TempDir, String)) {
        basic_runtime().block_on(async {
            let label_writer = LabelWriter::new(0);
            harness.0.write_label(label_writer).await
        }).unwrap();
        let mut f = fs::File::open(harness.2).unwrap();
        let mut v = vec![0; 8192];
        f.seek(SeekFrom::Start(112)).unwrap();   // Skip the leaf, mirror labels
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
            println!("UUID is {}", harness.0.uuid());
        } */
        // Compare against the golden master, skipping the UUID fields
        assert_eq!(&v[0..4], &GOLDEN_VDEV_NULLRAID_LABEL[0..4]);
        // Rest of the buffer should be zero-filled
        assert!(v[36..].iter().all(|&x| x == 0));
    }
}
