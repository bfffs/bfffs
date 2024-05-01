// vim: tw=80

use bfffs_core::{
    label::*,
    mirror::Mirror,
    vdev::Health,
    raid::{Manager, NullRaid, VdevRaidApi},
    Uuid,
    Error
};
use nonzero_ext::nonzero;
use rstest::rstest;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::PathBuf
};
use tempfile::{Builder, TempDir};

struct Harness {
    vdev: NullRaid,
    _tempdir: TempDir,
    paths: Vec<PathBuf>
}

fn harness(mirror_children: usize) -> Harness {
    let len = 1 << 26;  // 64 MB
    let tempdir = Builder::new()
        .prefix("test_vdev_null_raid")
        .tempdir()
        .unwrap();
    let mut paths = Vec::new();
    for i in 0..mirror_children {
        let s = format!("{}/vdev.{}", tempdir.path().display(), i);
        let path = PathBuf::from(s);
        let file = fs::File::create(&path).unwrap();
        file.set_len(len).unwrap();
        paths.push(path);   
    }
    let mirror = Mirror::create(&paths[..], None).unwrap();
    let vdev = NullRaid::create(mirror);
    Harness{vdev, _tempdir: tempdir, paths}
}

mod fault {
    use super::*;

    #[rstest(h, case(harness(2)))]
    #[tokio::test]
    async fn disk_enoent(mut h: Harness) {
        let r = h.vdev.fault(Uuid::new_v4());
        assert_eq!(Err(Error::ENOENT), r);
    }

    #[rstest(h, case(harness(2)))]
    #[tokio::test]
    async fn disk_missing(mut h: Harness) {
        let duuid = h.vdev.status().mirrors[0].leaves[0].uuid;

        h.vdev.fault(duuid).unwrap();
        h.vdev.fault(duuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].leaves[0].health, Health::Faulted);
    }

    #[rstest(h, case(harness(2)))]
    #[tokio::test]
    async fn disk_present(mut h: Harness) {
        let duuid = h.vdev.status().mirrors[0].leaves[0].uuid;
        h.vdev.fault(duuid).unwrap();

        let status = h.vdev.status();
        assert_eq!(status.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(status.mirrors[0].leaves[0].health, Health::Faulted);
    }

    // Attempt to fault the entire RAID device
    #[rstest(h, case(harness(1)))]
    #[tokio::test]
    async fn raid(mut h: Harness) {
        let ruuid = h.vdev.uuid();
        let r = h.vdev.fault(ruuid);
        assert_eq!(Err(Error::EINVAL), r);
    }
}

mod persistence {
    use super::*;

    use pretty_assertions::assert_eq;

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

    #[rstest]
    #[case(harness(1))]
    #[tokio::test]
    async fn open_after_write(#[case] h: Harness) {
        let uuid = h.vdev.uuid();
        let label_writer = LabelWriter::new(0);
        h.vdev.write_label(label_writer).await.unwrap();
        drop(h.vdev);

        let mut manager = Manager::default();
        manager.taste(&h.paths[0]).await.unwrap();
        let (vdev, _) = manager.import(uuid).await.unwrap();
        assert_eq!(uuid, vdev.uuid());
    }

    #[rstest]
    #[case(harness(1))]
    #[tokio::test]
    async fn write_label(#[case] h: Harness) {
        let label_writer = LabelWriter::new(0);
        h.vdev.write_label(label_writer).await.unwrap();
        let mut f = fs::File::open(&h.paths[0]).unwrap();
        let mut v = vec![0; 8192];
        f.seek(SeekFrom::Start(112)).unwrap();   // Skip the leaf, mirror labels
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
            println!("UUID is {}", h.vdev.uuid());
        } */
        // Compare against the golden master, skipping the UUID fields
        assert_eq!(&v[0..4], &GOLDEN_VDEV_NULLRAID_LABEL[0..4]);
        // Rest of the buffer should be zero-filled
        assert!(v[36..].iter().all(|&x| x == 0));
    }
}
