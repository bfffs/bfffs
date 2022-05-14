use std::{
    pin::Pin,
    sync::Arc
};
use bfffs_core::{
    cache::{CacheRef, Cacheable},
    ddml::DRP,
    dml::{Compression, DML},
    fs_tree::{BlobExtent, FSKey, FSValue, ObjKey},
    idml::RidtEntry,
    tree::{LeafData, Node, NodeData},
    writeback::{Credit, WriteBack},
    PBA,
    Result,
    RID,
    TxgT
};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use divbuf::DivBufShared;
use rand::{Rng, thread_rng};
use futures::{Future, FutureExt};

struct FakeDDML {}
impl DML for FakeDDML {
    type Addr = DRP;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, _: T, _: Compression, _: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self::Addr>> + Send>>
    {
        unimplemented!()
    }

    fn repay(&self, _: Credit) {
        unimplemented!()
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }
}

struct FakeIDML {}
impl DML for FakeIDML {
    type Addr = RID;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }

    /// If the given record is present in the cache, evict it.
    fn evict(&self, _addr: &Self::Addr) {
        unimplemented!()
    }

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        unimplemented!()
    }

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        unimplemented!()
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, _: T, _: Compression, _: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self::Addr>> + Send>>
    {
        unimplemented!()
    }

    fn repay(&self, _: Credit) {
        unimplemented!()
    }

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }
}

fn fs_leaf(c: &mut Criterion) {
    // 937 is a typical fanout for sequential insertion, according to
    // examples/fanout.rs
    const NELEMS: u64 = 937;

    let mut g = c.benchmark_group("fs-leaf");
    g.throughput(Throughput::Elements(NELEMS));

    let wb = WriteBack::limitless();
    let mut ld = LeafData::default();
    let txg = TxgT::from(42);
    let dml = FakeDDML{};
    for i in 0..NELEMS {
        let key = FSKey::new(1, ObjKey::Extent(i << 17));
        let value = FSValue::BlobExtent(BlobExtent{lsize: 131072, rid: RID(i)});
        let credit = wb.borrow(99999999).now_or_never().unwrap();
        let (r, excess) = ld.insert(key, value, txg, &dml, credit)
            .now_or_never().unwrap();
        r.unwrap();
        wb.repay(excess);
    }
    let nd = NodeData::Leaf(ld);
    let node = Arc::new(Node::<RID, _, _>::new(nd));

    g.bench_function("serialize", |b| b.iter(|| {
        node.serialize();
    }));

    let db = node.serialize();
    // Deconstruct the arc to get that credit back
    wb.repay(Arc::try_unwrap(node)
             .unwrap()
             .try_unwrap()
             .unwrap()
             .into_leaf()
             .forget()
    );

    g.bench_function("deserialize", |b| b.iter(|| {
        let dbs = DivBufShared::from(&db[..]);
        let _: Arc<Node<RID, FSKey, FSValue>> = Cacheable::deserialize(dbs);
    }));
}

fn alloct_leaf(c: &mut Criterion) {
    // 3634 is a typical fanout, according to examples/fanout.rs
    const NELEMS: u64 = 3634;

    let mut g = c.benchmark_group("alloct-leaf");
    g.throughput(Throughput::Elements(NELEMS));

    let mut ld = LeafData::default();
    let txg = TxgT::from(42);
    let dml = FakeIDML{};
    for i in 0..NELEMS {
        let key = PBA::new(1, i * 128);
        let value = RID(i);
        let credit = Credit::null();
        ld.insert(key, value, txg, &dml, credit)
            .now_or_never().unwrap()
            .0.unwrap();
    }
    let nd = NodeData::<DRP, _, _>::Leaf(ld);
    let node = Arc::new(Node::new(nd));

    g.bench_function("serialize", |b| b.iter(|| {
        node.serialize();
    }));

    let db = node.serialize();
    g.bench_function("deserialize", |b| b.iter(|| {
        let dbs = DivBufShared::from(&db[..]);
        let _: Arc<Node<DRP, PBA, RID>> = Cacheable::deserialize(dbs);
    }));
    g.finish();
}

fn ridt_leaf(c: &mut Criterion) {
    // 417 is a typical fanout, according to examples/fanout.rs
    const NELEMS: u64 = 417;

    let mut g = c.benchmark_group("ridt-leaf");
    g.throughput(Throughput::Elements(NELEMS));

    let mut ld = LeafData::default();
    let txg = TxgT::from(42);
    let dml = FakeIDML{};
    let mut rng = thread_rng();
    for i in 0..NELEMS {
        let key = RID(i);
        let pba = PBA::new(1, i * 128);
        let drp = DRP::new(pba, Compression::None, 131072, 131072, rng.gen());
        let value = RidtEntry::new(drp);
        let credit = Credit::null();
        ld.insert(key, value, txg, &dml, credit)
            .now_or_never().unwrap()
            .0.unwrap();
    }
    let nd = NodeData::<DRP, _, _>::Leaf(ld);
    let node = Arc::new(Node::new(nd));

    g.bench_function("serialize", |b| b.iter(|| {
        node.serialize();
    }));

    let db = node.serialize();
    g.bench_function("deserialize", |b| b.iter(|| {
        let dbs = DivBufShared::from(&db[..]);
        let _: Arc<Node<DRP, RID, RidtEntry>> = Cacheable::deserialize(dbs);
    }));
}

criterion_group!(
    benches,
    alloct_leaf,
    fs_leaf,
    ridt_leaf,
);
criterion_main!(benches);
