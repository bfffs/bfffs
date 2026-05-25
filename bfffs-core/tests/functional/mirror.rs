// vim: tw=80

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};
use bfffs_core::{
    label::*,
    mirror::{Manager, Mirror},
    vdev::{FaultedReason, Health, Vdev},
    Error,
    LbaT,
    TxgT,
    Uuid,
    BYTES_PER_LBA,
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
    let mirror = Mirror::create(&paths, Some(nonzero!(32u64))).unwrap();
    (mirror, tempdir, paths)
}

mod attach {
    use super::*;

    #[tokio::test]
    async fn basic() {
        let len = 1 << 26;  // 64 MB
        let tempdir = Builder::new()
            .prefix("test_mirror_attach")
            .tempdir()
            .unwrap();
        let path0 = format!("{}/vdev.0", tempdir.path().display());
        let path1 = format!("{}/vdev.1", tempdir.path().display());
        let file0 = fs::File::create(&path0).unwrap();
        let file1 = fs::File::create(&path1).unwrap();
        file0.set_len(len).unwrap();
        file1.set_len(len).unwrap();

        let mut vdev = Mirror::create(&[path0][..], Some(nonzero!(32u64))).unwrap();
        vdev.attach(path1).unwrap();

        let stat = vdev.status();
        assert_eq!(stat.leaves.len(), 2);
        assert_eq!(stat.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(stat.leaves[1].health, Health::Degraded(nonzero!(1u8)));
    }

    #[tokio::test]
    async fn ebusy() {
        let len = 1 << 26;  // 64 MB
        let tempdir = Builder::new()
            .prefix("test_mirror_attach_ebusy")
            .tempdir()
            .unwrap();
        let path0 = format!("{}/vdev.0", tempdir.path().display());
        let path1 = format!("{}/vdev.1", tempdir.path().display());
        let path2 = format!("{}/vdev.2", tempdir.path().display());
        let file0 = fs::File::create(&path0).unwrap();
        let file1 = fs::File::create(&path1).unwrap();
        let file2 = fs::File::create(&path2).unwrap();
        file0.set_len(len).unwrap();
        file1.set_len(len).unwrap();
        file2.set_len(len).unwrap();

        let mut vdev = Mirror::create(&[path0][..], Some(nonzero!(32u64))).unwrap();
        vdev.attach(path1).unwrap();
        assert_eq!(vdev.attach(path2), Err(Error::EBUSY));
    }
}

mod fault {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn enoent(mut harness: Harness) {
        assert_eq!(Err(Error::ENOENT), harness.0.fault(Uuid::new_v4()));
    }

    #[rstest]
    #[tokio::test]
    async fn missing(mut harness: Harness) {
        let uuid = harness.0.status().leaves[2].uuid;
        harness.0.fault(uuid).unwrap();
        harness.0.fault(uuid).unwrap();
        let stat = harness.0.status();
        assert_eq!(stat.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(stat.leaves[2].health,
                   Health::Faulted(FaultedReason::User));
    }

    #[tokio::test]
    async fn only_child() {
    let len = 1 << 26;  // 64 MB
        let tempdir = Builder::new()
            .prefix("test_mirror_fault")
            .tempdir()
            .unwrap();
        let path = format!("{}/vdev", tempdir.path().display());
        let file = fs::File::create(&path).unwrap();
        file.set_len(len).unwrap();
        let mut vdev = Mirror::create(&[path][..], None).unwrap();
        let uuid = vdev.status().leaves[0].uuid;

        vdev.fault(uuid).unwrap();
        let stat = vdev.status();
        assert_eq!(stat.health,
                   Health::Faulted(FaultedReason::InsufficientRedundancy));
        assert_eq!(stat.leaves[0].health,
                   Health::Faulted(FaultedReason::User));
    }

    #[rstest]
    #[tokio::test]
    async fn present(mut harness: Harness) {
        let uuid = harness.0.status().leaves[2].uuid;
        harness.0.fault(uuid).unwrap();
        let stat = harness.0.status();
        assert_eq!(stat.health, Health::Degraded(nonzero!(1u8)));
        assert_eq!(stat.leaves[2].health,
                   Health::Faulted(FaultedReason::User));
    }
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
        let txg = TxgT::from(1);
        old_vdev.write_label(label_writer, txg, false).await.unwrap();
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

    const GOLDEN_MIRROR_LABEL: [u8; 68] = [
        // Past the VdevFile::Label, we have a mirror::Label
        // First come's the mirror's UUID
        0x75, 0x9a, 0x2b, 0x9e, 0x56, 0x11, 0x41, 0x7c,
        0x80, 0xe1, 0x32, 0x13, 0xd9, 0xef, 0x88, 0x3b,
        // Then the vector of children's UUIDs.  A 64-bit count of children,
        // then each UUID is 64-bits long
        0x03, 0x00, 0x00, 0x00, 0x9c, 0xaf, 0x4c, 0xf8,
        0xba, 0x40, 0xc6, 0x64, 0x2f, 0x88, 0x5a, 0x01,
        0x62, 0xbf, 0xbd, 0x54, 0x9f, 0xa3, 0x41, 0x65,
        0x8e, 0x75, 0xfa, 0x7e, 0xcb, 0x52, 0x45, 0x2e,
        0xd3, 0x14, 0x96, 0x91, 0x17, 0x18, 0x4a, 0xc5,
        0xbd, 0x06, 0x24, 0xd1, 0xd2, 0xa9, 0x6d, 0x67,
        0x24, 0x31, 0xb8, 0x32,
    ];

    #[rstest]
    #[tokio::test]
    async fn open_after_write(harness: Harness) {
        let (old_vdev, _tempdir, paths) = harness;
        let uuid = old_vdev.uuid();
        let label_writer = LabelWriter::new(0);
        old_vdev.write_label(label_writer, TxgT::from(1), false).await.unwrap();
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
        harness.0.write_label(label_writer, TxgT::from(1), false).await.unwrap();

        for path in harness.2 {
            let mut f = fs::File::open(path).unwrap();
            let mut v = vec![0; 8192];
            f.seek(SeekFrom::Start(76)).unwrap();   // Skip the VdevBlock label
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
            assert_eq!(&v[16..20], &GOLDEN_MIRROR_LABEL[16..20]);
            // Rest of the buffer should be zero-filled
            assert!(v[68..].iter().all(|&x| x == 0));
        }
    }
}

mod read_long {
    use super::*;

    use std::io::Write;

    /// Mirror::read_long should return every possible reconstruction of the
    /// data.
    #[rstest]
    #[tokio::test]
    async fn reconstructions(harness: Harness) {
        const OFS: LbaT = 3;

        // Write patterns directly to each disk
        for (i, path) in harness.2.iter().enumerate() {
            let v = vec![110u8 + i as u8; BYTES_PER_LBA];
            let mut f = fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap();
            f.seek(SeekFrom::Start(OFS * BYTES_PER_LBA as LbaT)).unwrap();
            f.write_all(&v[..]).unwrap();
        }

        let mut reconstructor = harness.0.read_long(1, 3).await.unwrap();
        let mut reconstructions = [
            reconstructor.next().unwrap().try_const().unwrap(),
            reconstructor.next().unwrap().try_const().unwrap(),
            reconstructor.next().unwrap().try_const().unwrap()
        ];
        (reconstructions[..]).sort_by_key(|db| db[0]);
        assert_eq!(&vec![110; BYTES_PER_LBA][..], &reconstructions[0][..]);
        assert_eq!(&vec![111; BYTES_PER_LBA][..], &reconstructions[1][..]);
        assert_eq!(&vec![112; BYTES_PER_LBA][..], &reconstructions[2][..]);
        assert!(reconstructor.next().is_none());
    }
}

mod repair_zone {
    use super::*;
    use divbuf::DivBufShared;

    #[rstest]
    #[tokio::test]
    async fn basic(harness: Harness) {
        let (mut mirror, _tempdir, _paths) = harness;
        let stat = mirror.status();
        let uuid0 = stat.leaves[0].uuid;
        let uuid1 = stat.leaves[1].uuid;
        let uuid2 = stat.leaves[2].uuid;
        let zone = 1;
        let (start, _end) = mirror.zone_limits(zone);

        // Fault one disk
        mirror.fault(uuid0).unwrap();

        // Write some data to the mirror.
        let dbs = DivBufShared::from(vec![42u8; BYTES_PER_LBA]);
        mirror.write_at(dbs.try_const().unwrap(), start).await.unwrap();

        // Mark the faulted disk as rebuilding
        mirror.rebuild(uuid0).unwrap();

        // Repair the zone
        mirror.repair_zone(zone, None).await.unwrap();

        // Restore the mirror
        mirror.restore();

        // Verify by faulting the other disks and reading from the repaired one.
        mirror.fault(uuid1).unwrap();
        mirror.fault(uuid2).unwrap();
        let mut dbm = dbs.try_mut().unwrap();
        dbm.fill(0);
        mirror.read_at(dbm, start).await.unwrap();
        assert_eq!(&dbs.try_const().unwrap()[..], &vec![42u8; BYTES_PER_LBA][..]);
    }
}
