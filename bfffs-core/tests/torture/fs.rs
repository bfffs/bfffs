use bfffs_core::{
    *,
    cache::*,
    database::*,
    ddml::*,
    fs::*,
    idml::*,
};
use futures::{future, StreamExt};
use tracing::*;
use pretty_assertions::assert_eq;
use rand::{
    Rng,
    RngCore,
    SeedableRng,
    distributions::{Distribution, WeightedIndex},
    thread_rng
};
use rand_xorshift::XorShiftRng;
use rstest::rstest;
use std::{
    ffi::OsString,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub enum Op {
    Clean,
    /// Should be `Sync`, but that word is reserved
    SyncAll,
    RmEnoent,
    Ls,
    Rmdir,
    Mkdir,
    Rm,
    Touch,
    Write,
    Read
}

struct TortureTest {
    db: Option<Arc<Database>>,
    dirs: Vec<(u64, FileDataMut)>,
    fs: Fs,
    files: Vec<(u64, FileDataMut)>,
    rng: XorShiftRng,
    root: FileDataMut,
    w: Vec<(Op, f64)>,
    wi: WeightedIndex<f64>
}

impl TortureTest {
    async fn check(&mut self) {
        let db = self.db.as_ref().unwrap();
        assert!(db.check().await.unwrap());
    }

    async fn clean(&mut self) {
        info!("clean");
        let db = self.db.as_ref().unwrap();
        db.clean().await.unwrap();
    }

    async fn mkdir(&mut self) {
        let num: u64 = self.rng.gen();
        let fname = format!("{num:x}");
        info!("mkdir {}", fname);
        let fd = self.fs.mkdir(&self.root.handle(), &OsString::from(&fname),
                0o755, 0, 0).await.unwrap();
        self.dirs.push((num, fd));
    }

    async fn ls(&mut self) {
        let idx = self.rng.gen_range(0..self.dirs.len() + 1);
        let (fname, fd) = if idx == self.dirs.len() {
            ("/".to_owned(), &self.root)
        } else {
            let spec = &self.dirs[idx];
            (format!("{:x}", spec.0), &spec.1)
        };
        let mut c = 0;
        self.fs.readdir(&fd.handle(), 0)
            .for_each(|_| {
                c += 1;
                future::ready(())
            }).await;
        info!("ls {}: {} entries", fname, c);
    }

    fn new(db: Arc<Database>, fs: Fs, rng: XorShiftRng,
           w: Option<Vec<(Op, f64)>>) -> Self
    {
        let w = w.unwrap_or_else(|| vec![
            (Op::Clean, 0.001),
            (Op::SyncAll, 0.003),
            (Op::RmEnoent, 1.0),
            (Op::Ls, 5.0),
            (Op::Rmdir, 5.0),
            (Op::Mkdir, 6.0),
            (Op::Rm, 15.0),
            (Op::Touch, 18.0),
            (Op::Write, 25.0),
            (Op::Read, 25.0)
        ]);
        let wi = WeightedIndex::new(w.iter().map(|item| item.1)).unwrap();
        let root = fs.root();
        TortureTest{db: Some(db), dirs: Vec::new(), files: Vec::new(), fs,
                    rng, root, w, wi}
    }

    async fn read(&mut self) {
        if !self.files.is_empty() {
            // Pick a random file to read from
            let idx = self.rng.gen_range(0..self.files.len());
            let fd = &self.files[idx].1;
            // Pick a random offset within the first 8KB
            let ofs = 2048 * self.rng.gen_range(0..4);
            info!("read {:x} at offset {}", self.files[idx].0, ofs);
            let r = self.fs.read(&fd.handle(), ofs, 2048).await;
            // TODO: check buffer contents
            assert!(r.is_ok());
        }
    }

    async fn rm_enoent(&mut self) {
        // Generate a random name that corresponds to no real file, but
        // could be sorted anywhere amongst them.
        let num: u64 = self.rng.gen();
        let fname = format!("{num:x}_x");
        let fd = FileDataMut::new_for_tests(Some(1), num);
        let fdh = fd.handle();
        info!("rm {}", fname);
        let r = self.fs.unlink(&self.root.handle(), Some(&fdh),
            &OsString::from(&fname))
            .await;
        assert_eq!(r, Err(Error::ENOENT.into()));
    }

    async fn rm(&mut self) {
        if !self.files.is_empty() {
            let idx = self.rng.gen_range(0..self.files.len());
            let (basename, fd) = self.files.remove(idx);
            let fname = format!("{basename:x}");
            info!("rm {}", fname);
            self.fs.unlink(&self.root.handle(), Some(&fd.handle()),
                &OsString::from(&fname)).await.unwrap();
        }
    }

    async fn rmdir(&mut self) {
        if !self.dirs.is_empty() {
            let idx = self.rng.gen_range(0..self.dirs.len());
            let fname = format!("{:x}", self.dirs.remove(idx).0);
            info!("rmdir {}", fname);
            self.fs.rmdir(&self.root.handle(),
                &OsString::from(&fname)).await.unwrap();
        }
    }

    async fn shutdown(mut self) {
        self.fs.inactive(self.root).await;
        drop(self.fs);
        let db = Arc::try_unwrap(self.db.take().unwrap())
            .ok().expect("Arc::try_unwrap");
        db.shutdown().await;
    }

    async fn step(&mut self) {
        match self.w[self.wi.sample(&mut self.rng)].0 {
            Op::Clean => self.clean().await,
            Op::Ls => self.ls().await,
            Op::Mkdir => self.mkdir().await,
            Op::Read => self.read().await,
            Op::Rm => self.rm().await,
            Op::Rmdir => self.rmdir().await,
            Op::RmEnoent => self.rm_enoent().await,
            Op::SyncAll => self.sync().await,
            Op::Touch => self.touch().await,
            Op::Write => self.write().await,
        };
        self.check().await
    }

    async fn sync(&mut self) {
        info!("sync");
        self.fs.sync().await;
    }

    async fn touch(&mut self) {
        // The BTree is basically a flat namespace, so there's little test
        // coverage to be gained by testing a hierarchical directory
        // structure.  Instead, we'll stick all files in the root directory,
        let num: u64 = self.rng.gen();
        let fname = format!("{num:x}");
        info!("Touch {}", fname);
        let fd = self.fs.create(&self.root.handle(),
            &OsString::from(&fname), 0o644, 0, 0).await.unwrap();
        self.files.push((num, fd));
    }

    /// Write to a file.
    ///
    /// Writes just 2KB.  This may create inline or on-disk extents.  It may
    /// RMW on-disk extents.  The purpose is to exercise the tree, not large
    /// I/O.
    async fn write(&mut self) {
        if !self.files.is_empty() {
            // Pick a random file to write to
            let idx = self.rng.gen_range(0..self.files.len());
            let fd = &self.files[idx].1;
            // Pick a random offset within the first 8KB
            let piece: u64 = self.rng.gen_range(0..4);
            let ofs = 2048 * piece;
            // Use a predictable fill value
            let fill = (fd.ino().wrapping_mul(piece) % u64::from(u8::MAX))
                as u8;
            let buf = [fill; 2048];
            info!("write {:x} at offset {}", self.files[idx].0, ofs);
            let r = self.fs.write(&fd.handle(), ofs, &buf[..], 0).await;
            assert!(r.is_ok());
        }
    }
}

async fn torture_test(seed: Option<[u8; 16]>, freqs: Option<Vec<(Op, f64)>>,
                zone_size: u64) -> TortureTest
{
    let ph = crate::PoolBuilder::new()
        .zone_size(zone_size)
        .build();
    let cache = Arc::new(
        Mutex::new(
            Cache::with_capacity(32_000_000)
        )
    );
    let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
    let idml = IDML::create(ddml, cache);
    let db = Arc::new(Database::create(Arc::new(idml)));
    let tree_id = db.create_fs(None, "").await.unwrap();
    let fs = Fs::new(db.clone(), tree_id).await;
    let seed = seed.unwrap_or_else(|| {
        let mut seed = [0u8; 16];
        let mut seeder = thread_rng();
        seeder.fill_bytes(&mut seed);
        seed
    });
    println!("Using seed {:?}", &seed);
    // Use XorShiftRng because it's deterministic and seedable
    let rng = XorShiftRng::from_seed(seed);

    TortureTest::new(db, fs, rng, freqs)
}

async fn do_test(mut torture_test: TortureTest, duration: Duration)
{
    // Random torture test.  At each step check the trees and also do one of:
    // *) Clean zones
    // *) Sync
    // *) Remove a nonexisting regular file
    // *) Remove an existing file
    // *) Create a new regular file
    // *) Remove an empty directory
    // *) Create a directory
    // *) List a directory
    // *) Write to a regular file
    // *) Read from a regular file
    let start = Instant::now();
    while start.elapsed() < duration {
        torture_test.step().await
    }
    torture_test.shutdown().await;
}

/// Randomly execute a long series of filesystem operations.
#[rstest]
#[case(None, None, 512)]
#[test_log::test(tokio::test)]
async fn random(
    #[case] seed: Option<[u8; 16]>,
    #[case] freqs: Option<Vec<(Op, f64)>>,
    #[case] zone_size: u64)
{
    let t = 5.0 * crate::test_scale();
    let duration = Duration::from_secs_f64(t);
    let torture = torture_test(seed, freqs, zone_size).await;
    do_test(torture, duration).await;
}

/// Randomly execute a series of filesystem operations, designed expecially
/// to stress the cleaner.
#[rstest]
#[case(None,
    Some(vec![
        (Op::Clean, 0.01),
        (Op::SyncAll, 0.03),
        (Op::Mkdir, 10.0),
        (Op::Touch, 10.0),
    ]),
    512
)]
#[test_log::test(tokio::test)]
async fn random_clean_zone(
    #[case] seed: Option<[u8; 16]>,
    #[case] freqs: Option<Vec<(Op, f64)>>,
    #[case] zone_size: u64)
{
    let t = 1.0 * crate::test_scale();
    let torture = torture_test(seed, freqs, zone_size).await;
    do_test(torture, Duration::from_secs_f64(t)).await;
}
