// vim: tw=80
//! Measures BTree metadata efficiency
//!
//! This program constructs a filesystem BTree and fills it with records
//! simulating both sequential and random insertion into a large file.  It
//! computes the Tree's padding fraction (lower is better) and the overall
//! metadata fraction of the file system.
use bfffs::{
    boxfut,
    common::{
        *,
        dml::*,
        fs_tree::*,
        tree::*
    }
};
use futures::{Future, Stream, future, stream};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex}
};
use tokio_io_pool::Runtime;

const RECSIZE: u32 = 131_072;

#[derive(Default)]
struct FakeDML {
    put_counts: Mutex<BTreeMap<u32, u64>>
}

impl FakeDML {
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

    fn total(&self) -> u64 {
        let guard = self.put_counts.lock().unwrap();
        guard.iter().fold(0, |total, (lsize, count)| {
            let lsize = *lsize as usize;
            let asize = div_roundup(lsize, BYTES_PER_LBA) * BYTES_PER_LBA;
            total + asize as u64 * *count
        })
    }
}

impl DML for FakeDML {
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
                             _txg: TxgT)
        -> Box<dyn Future<Item=Self::Addr, Error=Error> + Send>
    {
        let db = cacheable.make_ref().serialize();
        let mut guard = self.put_counts.lock().unwrap();
        let lsize = db.len() as u32;
        let count = *guard.get(&lsize).unwrap_or(&0) + 1;
        guard.insert(lsize, count);
        boxfut!(future::ok(RID(0)))
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
    let fake_dml = Arc::new(FakeDML::default());
    let fake_dml2 = fake_dml.clone();
    let tree = Arc::new(
        Tree::<RID, FakeDML, FSKey, FSValue<RID>>::create(fake_dml)
    );
    let tree2 = tree.clone();
    let txg = TxgT::from(0);

    rt.block_on(
        stream::iter_ok(0..NELEMS)
        .for_each(move |i| {
            let offset = f(i);
            let key = FSKey::new(INODE, ObjKey::Extent(offset));
            let be = BlobExtent { lsize: RECSIZE, rid: RID(i)};
            let value = FSValue::BlobExtent(be);
            tree.insert(key, value, txg)
            .map(drop)
        }).and_then(move |_| {
            tree2.flush(txg)
        })
    ).unwrap();

    println!("Padding fraction:  {:#.3}%",
             100.0 * fake_dml2.padding_fraction());
    let metadata_size = fake_dml2.total();
    let data_size = NELEMS * u64::from(RECSIZE);
    let mf = metadata_size as f64 / (data_size + metadata_size) as f64;
    println!("Metadata fraction: {:#.3}%", 100.0 * mf);
    let guard = fake_dml2.put_counts.lock().unwrap();
    println!("put counts: {:?}", *guard);
}

fn main() {
    println!("=== Sequential insertion ===");
    experiment(|i| u64::from(RECSIZE) * i);
    println!("");
    println!("=== Random insertion ===");
    let mut rng = XorShiftRng::seed_from_u64(0);
    experiment(move |_| {
        rng.next_u32() as u64 * BYTES_PER_LBA as u64
    });
}
