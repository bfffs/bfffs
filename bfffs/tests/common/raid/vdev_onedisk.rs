// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name persistence;

    use bfffs::{
        common::label::*,
        common::vdev_block::*,
        common::vdev::Vdev,
        common::vdev_file::*,
        common::raid::{self, VdevOneDisk, VdevRaidApi},
    };
    use futures::{Future, future};
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread;

    const GOLDEN_VDEV_ONEDISK_LABEL: [u8; 36] = [
        // Past the VdevFile::Label, we have a raid::Label
        // First comes the VdevOneDisk discriminant
        0x00, 0x00, 0x00, 0x00,
        // Then the VdevOneDisk label, beginning with a UUID
                                0x2f, 0x27, 0x51, 0xe5,
        0xe8, 0x58, 0x45, 0x1b, 0x92, 0xb5, 0x24, 0x0f,
        0x23, 0x7b, 0xc9, 0xbe,
        // Then the child's UUID
                                0xe7, 0x4c, 0xba, 0x28,
        0xbb, 0xf1, 0x4c, 0x1a, 0xad, 0x90, 0xbf, 0x48,
        0xf4, 0x26, 0x1f, 0x7a,
    ];

    fixture!( mocks() -> (VdevOneDisk, TempDir, String) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_vdev_onedisk_persistence"));
            let path = format!("{}/vdev", tempdir.path().display());
            let file = t!(fs::File::create(&path));
            t!(file.set_len(len));
            let vdev = VdevOneDisk::create(None, &path);
            (vdev, tempdir, path)
        }
    });

    test open_after_write(mocks()) {
        let (old_vdev, _tempdir, path) = mocks.val;
        let uuid = old_vdev.uuid();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(move || {
            let label_writer = LabelWriter::new(0);
            old_vdev.write_label(label_writer).and_then(move |_| {
                VdevFile::open(path)
                .map(|(leaf, reader)| {
                    (VdevBlock::new(leaf), reader)
                })
            }).map(move |vb| {
                let (vdev, _) = raid::open(Some(uuid), vec![vb]);
                assert_eq!(uuid, vdev.uuid());
            })
        })).unwrap();
    }

    test write_label(mocks()) {
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let label_writer = LabelWriter::new(0);
            mocks.val.0.write_label(label_writer)
        })).unwrap();
        let mut f = fs::File::open(mocks.val.2).unwrap();
        let mut v = vec![0; 8192];
        f.seek(SeekFrom::Start(72)).unwrap();   // Skip the VdevLeaf label
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
            println!("UUID is {}", mocks.val.0.uuid());
        } */
        // Compare against the golden master, skipping the UUID fields
        assert_eq!(&v[0..4], &GOLDEN_VDEV_ONEDISK_LABEL[0..4]);
        // Rest of the buffer should be zero-filled
        assert!(v[36..].iter().all(|&x| x == 0));
    }
}
