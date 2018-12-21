// vim: tw=80
//! Measures BTree metadata efficiency
//!
//! This program constructs a filesystem BTree and fills it with records
//! simulating both sequential and random insertion into a large file.  It
//! computes the Tree's padding fraction (lower is better) and the overall
//! metadata fraction of the file system.
use atomic::{Atomic, Ordering};
use bfffs::{
    boxfut,
    common::{
        *,
        ddml::DRP,
        dml::*,
        idml::RidtEntry,
        fs_tree::*,
        tree::*
    }
};
use divbuf::DivBufShared;
use futures::{Future, Stream, future, stream};
use rand::{Rng, RngCore, SeedableRng, thread_rng};
use rand_xorshift::XorShiftRng;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex}
};
use tokio_io_pool::Runtime;

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
    next_lba: Arc<Atomic<u64>>,
    stats: Stats
}

impl FakeDDML {
    fn next_drp(&self, lsize: u32) -> DRP {
        let lbas = div_roundup(lsize as usize, BYTES_PER_LBA) as u64;
        let lba = self.next_lba.fetch_add(lbas, Ordering::Relaxed);
        let pba = PBA::new(0, lba);
        let checksum = thread_rng().gen::<u64>();
        DRP::new(pba, Compression::None, lsize, lsize, checksum)
    }

    fn new(next_lba: Arc<Atomic<u64>>) -> Self {
        FakeDDML {
            next_lba,
            stats: Stats::default()
        }
    }

    /// Just like `put`, but separate for record-keeping purposes
    fn put_data<T: Cacheable>(&self, cacheable: T, _compression: Compression,
                             _txg: TxgT)
        -> Box<dyn Future<Item=DRP, Error=Error> + Send>
    {
        let db = cacheable.make_ref().serialize();
        let lsize = db.len() as u32;
        let drp = self.next_drp(lsize);
        boxfut!(future::ok(drp))
    }
}

impl DML for FakeDDML {
    type Addr = DRP;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Box<dyn Future<Item=Box<R>, Error=Error> + Send>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, _compression: Compression,
                             _txg: TxgT)
        -> Box<dyn Future<Item=Self::Addr, Error=Error> + Send>
    {
        let db = cacheable.make_ref().serialize();
        let lsize = db.len() as u32;
        self.stats.put(lsize);
        let drp = self.next_drp(lsize);
        boxfut!(future::ok(drp))
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        unimplemented!()
    }
}

struct FakeIDML {
    alloct: Arc<Tree<DRP, FakeDDML, PBA, RID>>,
    data_size: Atomic<u64>,
    data_ddml: Arc<FakeDDML>,
    next_rid: Atomic<u64>,
    ridt: Arc<Tree<DRP, FakeDDML, RID, RidtEntry>>,
    stats: Stats
}

impl FakeIDML {
    fn data_size(&self) -> u64 {
        self.data_size.load(Ordering::Relaxed)
    }

    fn new(alloct_ddml: Arc<FakeDDML>, data_ddml: Arc<FakeDDML>,
           ridt_ddml: Arc<FakeDDML>) -> Self
    {
        let alloct = Arc::new(Tree::create(alloct_ddml.clone(), true));
        let ridt = Arc::new(Tree::create(ridt_ddml.clone(), true));
        FakeIDML {
            alloct,
            data_size: Atomic::<u64>::default(),
            data_ddml: data_ddml,
            next_rid: Atomic::<u64>::default(),
            ridt,
            stats: Stats::default()
        }
    }

    /// Just like `put`, but separate for record-keeping purposes
    fn put_data<T: Cacheable>(&self, cacheable: T, _compression: Compression,
                             txg: TxgT)
        -> Box<dyn Future<Item=RID, Error=Error> + Send>
    {
        self.data_size.fetch_add(cacheable.len() as u64, Ordering::Relaxed);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let fut = self.data_ddml.put_data(cacheable, Compression::None, txg)
        .and_then(move |drp| {
            let pba = drp.pba();
            let ridt_entry = RidtEntry::new(drp);
            ridt2.insert(rid, ridt_entry, txg)
            .join(alloct2.insert(pba, rid, txg))
            .map(move |_| rid)
        });
        boxfut!(fut)
    }
}

impl DML for FakeIDML {
    type Addr = RID;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Box<dyn Future<Item=Box<R>, Error=Error> + Send>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, _compression: Compression,
                             txg: TxgT)
        -> Box<dyn Future<Item=Self::Addr, Error=Error> + Send>
    {
        let db = cacheable.make_ref().serialize();
        let lsize = db.len() as u32;
        self.stats.put(lsize);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let fut = self.data_ddml.put_data(cacheable, Compression::None, txg)
        .and_then(move |drp| {
            let pba = drp.pba();
            let ridt_entry = RidtEntry::new(drp);
            ridt2.insert(rid, ridt_entry, txg)
            .join(alloct2.insert(pba, rid, txg))
            .map(move |_| rid)
        });
        boxfut!(fut)
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        unimplemented!()
    }
}

fn experiment<F>(mut f: F)
    where F: FnMut(u64) -> u64 + Send + 'static
{
    const INODE: u64 = 2;
    const NELEMS: u64 = 100_000;

    let mut rt = Runtime::new();
    let next_lba = Arc::new(Atomic::default());
    let alloct_ddml = Arc::new(FakeDDML::new(next_lba.clone()));
    let ridt_ddml = Arc::new(FakeDDML::new(next_lba.clone()));
    let data_ddml = Arc::new(FakeDDML::new(next_lba));
    let idml = Arc::new(FakeIDML::new(alloct_ddml.clone(), data_ddml,
                                      ridt_ddml.clone()));
    let idml2 = idml.clone();
    let idml3 = idml.clone();
    let idml4 = idml.clone();
    let tree = Arc::new(
        Tree::<RID, FakeIDML, FSKey, FSValue<RID>>::create(idml2, false)
    );
    let tree2 = tree.clone();
    let txg = TxgT::from(0);
    let data = vec![0u8; RECSIZE as usize];

    let (alloct_entries, ridt_entries) = rt.block_on(
        stream::iter_ok(0..NELEMS)
        .for_each(move |i| {
            let dbs = DivBufShared::from(data.clone());
            let offset = f(i);
            let tree3 = tree.clone();
            idml3.put_data(dbs, Compression::None, txg)
            .and_then(move |rid| {
                let key = FSKey::new(INODE, ObjKey::Extent(offset));
                let be = BlobExtent { lsize: RECSIZE, rid};
                let value = FSValue::BlobExtent(be);
                tree3.insert(key, value, txg)
            }).map(drop)
        }).and_then(move |_| {
            tree2.flush(txg)
        }).and_then(move |_| {
            let ridt_fut = idml4.ridt.range(..)
            .fold(0, |count, _| future::ok::<_, Error>(count + 1));
            let alloct_fut = idml4.alloct.range(..)
            .fold(0, |count, _| future::ok::<_, Error>(count + 1));
            ridt_fut.join(alloct_fut)
            .and_then(move |entries| {
                idml4.ridt.flush(txg)
                .join(idml4.alloct.flush(txg))
                .map(move |_| entries)
            })
        })
    ).unwrap();

    let fs_metadata_size = idml.stats.metadata_size();
    println!("FS Metadata size:      {} bytes", fs_metadata_size);
    println!("FS Padding fraction:   {:#.3}%",
             100.0 * idml.stats.padding_fraction());
    let fs_guard = idml.stats.put_counts.lock().unwrap();
    println!("FS Tree put counts: {:?}", *fs_guard);

    println!("");
    let alloct_metadata_size = alloct_ddml.stats.metadata_size();
    println!("AllocT entries:          {:?}", alloct_entries);
    println!("AllocT Metadata size:    {} bytes", alloct_metadata_size);
    println!("AllocT Padding fraction: {:#.3}%",
             100.0 * alloct_ddml.stats.padding_fraction());
    let alloct_guard = alloct_ddml.stats.put_counts.lock().unwrap();
    println!("AllocT put counts: {:?}", *alloct_guard);

    println!("");
    let ridt_metadata_size = ridt_ddml.stats.metadata_size();
    println!("RIDT entries:          {:?}", ridt_entries);
    println!("RIDT Metadata size:    {} bytes", ridt_metadata_size);
    println!("RIDT Padding fraction: {:#.3}%",
             100.0 * ridt_ddml.stats.padding_fraction());
    let ridt_guard = ridt_ddml.stats.put_counts.lock().unwrap();
    println!("RIDT put counts: {:?}", *ridt_guard);

    println!("");
    let metadata_size = fs_metadata_size + alloct_metadata_size +
        ridt_metadata_size;
    let data_size = idml.data_size();
    let mf = metadata_size as f64 / (data_size + metadata_size) as f64;
    println!("Overall Metadata fraction: {:#.3}%", 100.0 * mf);
}

fn main() {
    println!("=== Sequential insertion ===");
    experiment(|i| u64::from(RECSIZE) * i);
    println!("");
    println!("=== Random insertion ===");
    let mut rng = XorShiftRng::seed_from_u64(0);
    experiment(move |_| {
        rng.next_u32() as u64 * RECSIZE as u64
    });
}
