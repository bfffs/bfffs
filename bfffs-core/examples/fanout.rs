// vim: tw=80
//! Measures BTree metadata efficiency
//!
//! This program constructs a filesystem BTree and fills it with records
//! simulating both sequential and random insertion into a large file.  It
//! computes the Tree's padding fraction (lower is better) and the overall
//! metadata fraction of the file system.
use bfffs_core::{
    *,
    ddml::DRP,
    dml::*,
    idml::RidtEntry,
    fs_tree::*,
    tree::*,
    writeback::{Credit, WriteBack}
};
use clap::Clap;
use divbuf::DivBufShared;
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    StreamExt,
    TryStreamExt,
    future,
    stream
};
use rand::{Rng, RngCore, SeedableRng, thread_rng};
use rand_xorshift::XorShiftRng;
use std::{
    collections::BTreeMap,
    io::{ErrorKind, Write},
    mem,
    num::NonZeroU8,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
        Mutex
    }
};
use tokio::runtime::Builder;

const RECSIZE: u32 = 131_072;

#[derive(Default)]
struct Stats {
    put_counts: Mutex<BTreeMap<u32, u64>>,
}

impl Stats {
    fn padding_fraction(&self) -> f64 {
        let guard = self.put_counts.lock().unwrap();
        let (padding, total) = guard.iter().map(|(lsize, count)| {
            let lsize = *lsize as usize;
            let padding = {
                if lsize % BYTES_PER_LBA != 0{
                    BYTES_PER_LBA - lsize % BYTES_PER_LBA
                } else {
                    0
                }
            };
            let asize = div_roundup(lsize, BYTES_PER_LBA) * BYTES_PER_LBA;
            (padding * *count as usize, asize * *count as usize)
        }).fold((0, 0), |accum, (padding, total)| {
            (accum.0 + padding, accum.1 + total)
        });
        padding as f64 / total as f64
    }

    fn put(&self, lsize: u32) {
        let mut guard = self.put_counts.lock().unwrap();
        let count = *guard.get(&lsize).unwrap_or(&0) + 1;
        guard.insert(lsize, count);
    }

    fn metadata_size(&self) -> u64 {
        let guard = self.put_counts.lock().unwrap();
        guard.iter().fold(0, |total, (lsize, count)| {
            let lsize = *lsize as usize;
            let asize = div_roundup(lsize, BYTES_PER_LBA) * BYTES_PER_LBA;
            total + asize as u64 * *count
        })
    }
}

struct FakeDDML {
    name: &'static str,
    next_lba: Arc<AtomicU64>,
    save: bool,
    stats: Stats
}

impl FakeDDML {
    fn next_drp(&self, z: Compression, lsize: u32, csize: u32) -> DRP {
        let lbas = div_roundup(lsize as usize, BYTES_PER_LBA) as u64;
        let lba = self.next_lba.fetch_add(lbas, Ordering::Relaxed);
        let pba = PBA::new(0, lba);
        let checksum = thread_rng().gen::<u64>();
        DRP::new(pba, z, lsize, csize, checksum)
    }

    fn new(name: &'static str, next_lba: Arc<AtomicU64>, save: bool) -> Self {
        FakeDDML {
            name,
            next_lba,
            save,
            stats: Stats::default()
        }
    }

    /// Just like `put`, but separate for record-keeping purposes
    fn put_data(&self, cacheref: Box<dyn CacheRef>, compression: Compression,
                             _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<DRP, Error>> + Send>>
    {
        let db = cacheref.serialize();
        let lsize = db.len() as u32;
        let (zdb, compression) = compression.compress(db);
        let csize = zdb.len() as u32;
        let drp = self.next_drp(compression, lsize, csize);
        future::ok(drp).boxed()
    }
}

impl DML for FakeDDML {
    type Addr = DRP;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>, Error>> + Send>>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>, Error>> + Send>>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self::Addr, Error>> + Send>>
    {
        let db = cacheable.make_ref().serialize();
        if self.save {
            // We don't know which nodes are Int and which are Leaf; all we know
            // are their shuffle settings.  So write those in their filenames.
            let shuf = compression.shuffle().map(NonZeroU8::get).unwrap_or(0);
            let fname = format!("/tmp/fanout/{}.{}.{}.bin", self.name, shuf,
                                self.next_lba.load(Ordering::Relaxed));
            let mut f = std::fs::File::create(fname).unwrap();
            f.write_all(&db[..]).unwrap();
        }
        let lsize = db.len() as u32;
        let (zdb, compression) = compression.compress(db);
        let csize = zdb.len() as u32;
        self.stats.put(csize);
        let drp = self.next_drp(compression, lsize, csize);
        future::ok(drp).boxed()
    }

    fn repay(&self, credit: Credit) {
        debug_assert!(credit.is_null());
        mem::forget(credit);
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
    {
        unimplemented!()
    }
}

struct FakeIDML {
    alloct: Arc<Tree<DRP, FakeDDML, PBA, RID>>,
    data_size: AtomicU64,
    data_ddml: Arc<FakeDDML>,
    name: &'static str,
    next_rid: AtomicU64,
    ridt: Arc<Tree<DRP, FakeDDML, RID, RidtEntry>>,
    save: bool,
    stats: Stats,
    writeback: WriteBack
}

impl FakeIDML {
    fn borrow_credit(&self, want: usize) -> impl Future<Output=Credit> + Send {
        self.writeback.borrow(want)
    }

    fn data_size(&self) -> u64 {
        self.data_size.load(Ordering::Relaxed)
    }

    fn new(name: &'static str, alloct_ddml: Arc<FakeDDML>,
           data_ddml: Arc<FakeDDML>,
           ridt_ddml: Arc<FakeDDML>, save: bool) -> Self
    {
        let alloct = Arc::new(Tree::create(alloct_ddml, true, 16.5,
            2.809));
        let ridt = Arc::new(Tree::create(ridt_ddml, true, 4.22, 3.73));
        let writeback = WriteBack::limitless();
        FakeIDML {
            alloct,
            data_size: AtomicU64::default(),
            data_ddml,
            name,
            next_rid: AtomicU64::default(),
            ridt,
            save,
            stats: Stats::default(),
            writeback
        }
    }

    /// Just like `put`, but separate for record-keeping purposes
    fn put_data<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<RID, Error>> + Send>>
    {
        self.data_size.fetch_add(cacheable.cache_space() as u64,
            Ordering::Relaxed);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let cacheref = cacheable.make_ref();
        let fut = self.data_ddml.put_data(cacheref, compression, txg)
        .and_then(move |drp| {
            let pba = drp.pba();
            let ridt_entry = RidtEntry::new(drp);
            future::try_join(ridt2.insert(rid, ridt_entry, txg, Credit::null()),
                             alloct2.insert(pba, rid, txg, Credit::null()))
            .map_ok(move |_| rid)
        });
        fut.boxed()
    }
}

impl DML for FakeIDML {
    type Addr = RID;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>, Error>> + Send>>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>, Error>> + Send>>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self::Addr, Error>> + Send>>
    {
        let db = cacheable.make_ref().serialize();
        if self.save {
            // We don't know which nodes are Int and which are Leaf; all we know
            // are their shuffle settings.  So write those in their filenames.
            let shuf = compression.shuffle().map(NonZeroU8::get).unwrap_or(0);
            let fname = format!("/tmp/fanout/{}.{}.{}.bin", self.name, shuf,
                                self.next_rid.load(Ordering::Relaxed));
            let mut f = std::fs::File::create(fname).unwrap();
            f.write_all(&db[..]).unwrap();
        }
        let (zdb, _compression) = compression.compress(db);
        let csize = zdb.len() as u32;
        self.stats.put(csize);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let fut = self.data_ddml.put_data(Box::new(zdb), Compression::None, txg)
        .and_then(move |drp| {
            let pba = drp.pba();
            let ridt_entry = RidtEntry::new(drp);
            future::try_join(ridt2.insert(rid, ridt_entry, txg, Credit::null()),
                             alloct2.insert(pba, rid, txg, Credit::null()))
            .map_ok(move |_| rid)
        });
        fut.boxed()
    }

    fn repay(&self, credit: Credit) {
        self.writeback.repay(credit);
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
    {
        unimplemented!()
    }
}

fn experiment<F>(nelems: u64, save: bool, mut f: F)
    where F: FnMut(u64) -> u64 + Send + 'static
{
    const INODE: u64 = 2;

    let rt = Builder::new_current_thread()
        .build()
        .unwrap();
    let next_lba = Arc::new(AtomicU64::default());
    let alloct_ddml = Arc::new(FakeDDML::new("alloct", next_lba.clone(),
                                             save));
    let ridt_ddml = Arc::new(FakeDDML::new("ridt", next_lba.clone(), save));
    let data_ddml = Arc::new(FakeDDML::new("data", next_lba, false));
    let idml = Arc::new(FakeIDML::new("fs", alloct_ddml.clone(), data_ddml,
                                      ridt_ddml.clone(), save));
    let idml2 = idml.clone();
    let idml3 = idml.clone();
    let idml4 = idml.clone();
    let tree = Arc::new(
        Tree::<RID, FakeIDML, FSKey, FSValue<RID>>::create(idml2, false, 9.00,
                                                           1.61)
    );
    let tree2 = tree.clone();
    let txg = TxgT::from(0);
    let data = vec![0u8; RECSIZE as usize];
    let cr = tree.credit_requirements();

    let (alloct_entries, ridt_entries) = rt.block_on(
        stream::iter(0..nelems)
        .map(Ok)
        .try_for_each(move |i| {
            let dbs = DivBufShared::from(data.clone());
            let offset = f(i);
            let tree3 = tree.clone();
            future::try_join(
                idml3.put_data(dbs, Compression::None, txg),
                idml3.borrow_credit(cr.insert).map(Ok)
            ).and_then(move |(rid, credit)| {
                let key = FSKey::new(INODE, ObjKey::Extent(offset));
                let be = BlobExtent { lsize: RECSIZE, rid};
                let value = FSValue::BlobExtent(be);
                tree3.insert(key, value, txg, credit)
            }).map_ok(drop)
        }).and_then(move |_| {
            tree2.clone().flush(txg)
        }).and_then(move |_| {
            let ridt_fut = idml4.ridt.range(..)
            .try_fold(0, |count, _| future::ok::<_, Error>(count + 1));
            let alloct_fut = idml4.alloct.range(..)
            .try_fold(0, |count, _| future::ok::<_, Error>(count + 1));
            future::try_join(ridt_fut, alloct_fut)
            .and_then(move |entries| {
                future::try_join(idml4.ridt.clone().flush(txg),
                                 idml4.alloct.clone().flush(txg))
                .map_ok(move |_| entries)
            })
        })
    ).unwrap();

    let fs_metadata_size = idml.stats.metadata_size();
    println!("FS Metadata size:      {} bytes", fs_metadata_size);
    println!("FS Padding fraction:   {:#.3}%",
             100.0 * idml.stats.padding_fraction());
    let fs_guard = idml.stats.put_counts.lock().unwrap();
    println!("FS Tree put counts: {:?}", *fs_guard);

    println!();
    let alloct_metadata_size = alloct_ddml.stats.metadata_size();
    println!("AllocT entries:          {:?}", alloct_entries);
    println!("AllocT Metadata size:    {} bytes", alloct_metadata_size);
    println!("AllocT Padding fraction: {:#.3}%",
             100.0 * alloct_ddml.stats.padding_fraction());
    let alloct_guard = alloct_ddml.stats.put_counts.lock().unwrap();
    println!("AllocT put counts: {:?}", *alloct_guard);

    println!();
    let ridt_metadata_size = ridt_ddml.stats.metadata_size();
    println!("RIDT entries:          {:?}", ridt_entries);
    println!("RIDT Metadata size:    {} bytes", ridt_metadata_size);
    println!("RIDT Padding fraction: {:#.3}%",
             100.0 * ridt_ddml.stats.padding_fraction());
    let ridt_guard = ridt_ddml.stats.put_counts.lock().unwrap();
    println!("RIDT put counts: {:?}", *ridt_guard);

    println!();
    let metadata_size = fs_metadata_size + alloct_metadata_size +
        ridt_metadata_size;
    let data_size = idml.data_size();
    let mf = metadata_size as f64 / (data_size + metadata_size) as f64;
    println!("Overall Metadata fraction: {:#.3}%", 100.0 * mf);
}

#[derive(Clap, Clone, Copy, Debug)]
struct Cli {
    /// simulate sequential insertion
    #[clap(short = 's')]
    sequential: bool,
    /// simulate random insertion
    #[clap(short = 'r')]
    random: bool,
    /// save metadata records as /tmp/fanout/$table.$i.bin
    #[clap(long)]
    save: bool,
    /// Number of records to simulate
    records: Option<u64>
}

fn main() {
    let cli = Cli::parse();
    let nrecs = cli.records.unwrap_or(100_000);
    if cli.save {
        let r = std::fs::create_dir("/tmp/fanout");
        match r {
            Ok(_) => (),
            Err(ref e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
    }
    if cli.sequential {
        println!("=== Sequential insertion ===");
        experiment(nrecs, cli.save, |i| u64::from(RECSIZE) * i);
        println!();
    }
    if cli.random {
        println!("=== Random insertion ===");
        let mut rng = XorShiftRng::seed_from_u64(0);
        experiment(nrecs, cli.save, move |_| {
            u64::from(rng.next_u32()) * u64::from(RECSIZE)
        });
    }
}
