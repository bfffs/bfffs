//! Measures the actual memory consumption of Cacheable implementors
//!
//! Can't use the standard test harness because we need to run single-threaded.

use bfffs_core::{
    cache::{Cacheable, CacheRef},
    ddml::DRP,
    dml::{Compression, DML},
    fs_tree::*,
    idml::RidtEntry,
    property::Property,
    tree::*,
    LbaT,
    PBA,
    Result,
    RID,
    TxgT,
    writeback::{Credit, WriteBack}
};
use divbuf::DivBufShared;
use futures::{Future, FutureExt};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    ffi::OsString,
    mem,
    pin::Pin,
    sync::Arc
};

struct Counter;

thread_local! {
    pub static ALLOCATED: Cell<isize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.set(ALLOCATED.get() + isize::try_from(layout.size()).unwrap());
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.set(ALLOCATED.get() - isize::try_from(layout.size()).unwrap());
    }
}

#[global_allocator]
static A: Counter = Counter;

/// This program will never do disk I/O, but it needs a stub DML to satisfy the
/// compiler.
struct StubDML {}
impl DML for StubDML {
    type Addr = RID;

    fn delete(&self, _addr: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }

    fn evict(&self, _addr: &Self::Addr)
    {
        unimplemented!()
    }

    fn get<T: Cacheable, R: CacheRef>(&self, _addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        unimplemented!()
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, _rid: &Self::Addr, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        unimplemented!()
    }

    fn put<T: Cacheable>(&self, _cacheable: T, _compression: Compression,
                             _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<<Self as DML>::Addr>> + Send>>
    {
        unimplemented!()
    }

    fn repay(&self, _credit: Credit)
    {
        unimplemented!()
    }

    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        unimplemented!()
    }
}


/// Borrow enough credit for an insertion.
///
/// This test program doesn't really care about credit.  Borrow enough to
/// satisfy Node's assertions.
fn borrow_credit<V: Value>(wb: &WriteBack, v: &V) -> Credit {
    let want = mem::size_of::<(FSKey, V)>() + v.allocated_space();
    wb.borrow(want).now_or_never().unwrap()
}

trait CacheableForgetable: Cacheable {
    fn forget(self: Box<Self>) -> Credit;
}

impl<K: Key, V: Value> CacheableForgetable for Arc<Node<DRP, K, V>> {
    fn forget(self: Box<Self>) -> Credit {
        Credit::null()
    }
}

impl CacheableForgetable for Arc<Node<RID, FSKey, FSValue>> {
    fn forget(self: Box<Self>) -> Credit {
        let nd = Arc::try_unwrap(*self).unwrap().try_unwrap().unwrap();
        if nd.is_leaf() {
            nd.into_leaf().forget()
        } else {
            Credit::null()
        }
    }
}

fn logrange(min: usize, max: usize) -> impl Iterator<Item=usize> {
    let minf = min as f64;
    let grange = (max as f64) / minf;
    let mult = grange.powf(1.0/8f64);
    (1..=8).map(move |exp| ((minf * mult.powf(exp as f64)).round()) as usize)
}

fn measure(name: &str, pos: &str, n: usize,
    f: fn(&WriteBack, usize) -> Box<dyn CacheableForgetable>) -> bool
{
    let wb = WriteBack::limitless();
    let before = ALLOCATED.get();
    let c = f(&wb, n);
    let after = ALLOCATED.get();
    let actual = after - before;
    let calc = c.cache_space();
    wb.repay(c.forget());
    let err = 100.0 * (calc as f64) / (actual as f64) - 100.0;
    println!("{name:>8}{pos:>22}{n:>8}{actual:>12}{calc:>12}{err:>12.2}%");
    err.abs() <= 5.0
}

macro_rules! test_measure {
    ($tc: ident, $name:expr, $pos: expr, $min:expr, $max:expr, $fn:expr) => {
        #[test]
        fn $tc() {
            println!("\n{:>8}{:>22}{:>8}{:>12}{:>12}{:>12}", "Table", "Position",
                     "N", "Actual size", "Calculated", "Error");

            let mut pass = true;
            for n in logrange($min, $max) {
                pass &= measure($name, $pos, n, $fn)
            }
            assert!(pass, "Calculated size out of tolerance in at least one case");
        }
    }
}

test_measure!(alloct_leaf, "AllocT", "Leaf", 134, 535, |_wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = PBA::new(1, i as LbaT);
        let v = RID(i as u64);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, Credit::null())
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<DRP, PBA, RID>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(alloct_int, "AllocT", "Int", 109, 433, |_wb, n| {
    let txgs = TxgT::from(0)..TxgT::from(1);
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let addr = PBA::new(0, i as LbaT);
        let k = PBA::new(1, i as LbaT);
        let drp = DRP::new(addr, Compression::None, 40000, 40000, 0);
        let child = IntElem::new(k, txgs.clone(), TreePtr::Addr(drp));
        children.push(child);
    }
    let node_data = NodeData::<DRP, PBA, RID>::Int(IntData::new(children));
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(ridt_int, "RIDT", "Int", 98, 389, |_wb, n| {
    let txgs = TxgT::from(0)..TxgT::from(1);
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let addr = PBA::new(0, i as LbaT);
        let k = RID(i as u64);
        let drp = DRP::new(addr, Compression::None, 40000, 40000, 0);
        let child = IntElem::new(k, txgs.clone(), TreePtr::Addr(drp));
        children.push(child);
    }
    let nd = NodeData::<DRP, RID, RidtEntry>::Int(IntData::new(children));
    Box::new(Arc::new(Node::new(nd)))
});

test_measure!(ridt_leaf, "RIDT", "Leaf", 114, 454, |_wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = RID(i as u64);
        let addr = PBA::new(0, i as LbaT);
        let drp = DRP::new(addr, Compression::None, 40000, 40000, 0);
        let v = RidtEntry::new(drp);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, Credit::null())
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<DRP, RID, RidtEntry>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_int, "FS", "Int", 91, 364, |_wb, n| {
    let txgs = TxgT::from(0)..TxgT::from(1);
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let addr = RID(i as u64);
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let child = IntElem::new(k, txgs.clone(), TreePtr::Addr(addr));
        children.push(child);
    }
    let nd = NodeData::<RID, FSKey, FSValue>::Int(IntData::new(children));
    Box::new(Arc::new(Node::new(nd)))
});

test_measure!(fs_leaf_blob_extent, "FS", "Leaf (Blob Extent)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let extent = BlobExtent {
            lsize: 4096,
            rid: RID(i as u64)
        };
        let v = FSValue::BlobExtent(extent);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_direntry, "FS", "Leaf (DirEntry)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dirent = Dirent {
            ino: 0,
            dtype: libc::DT_REG,
            name: OsString::from("something_moderately_long_but_not_too_long")
        };
        let v = FSValue::DirEntry(dirent);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_direntries, "FS", "Leaf (DirEntries)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dirent0 = Dirent {
            ino: i as u64,
            dtype: libc::DT_REG,
            name: OsString::from("something_moderately_long_but_not_too_long")
        };
        let dirent1 = Dirent {
            ino: 10000 + i as u64,
            dtype: libc::DT_REG,
            name: OsString::from("something_also_pretty_long_string")
        };
        let v = FSValue::DirEntries(vec![dirent0, dirent1]);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_dyinginode, "FS", "Leaf (Dying Inode)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let v = FSValue::DyingInode(DyingInode::from(0));
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_extattr_blob, "FS", "Leaf (Blob Extattr)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let extent = BlobExtent {
            lsize: 4096,
            rid: RID(i as u64)
        };
        let blob_ext_attr = BlobExtAttr {
            namespace: ExtAttrNamespace::User,
            name: OsString::from("Some extended attribute stored as a blob"),
            extent,
        };
        let extattr = ExtAttr::Blob(blob_ext_attr);
        let v = FSValue::ExtAttr(extattr);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_extattr_inline, "FS", "Leaf (Inline Extattr)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        let extent = InlineExtent::new(Arc::new(dbs));
        let inline_ext_attr = InlineExtAttr {
            namespace: ExtAttrNamespace::User,
            name: OsString::from("Some extended attribute stored as a blob"),
            extent,
        };
        let extattr = ExtAttr::Inline(inline_ext_attr);
        let v = FSValue::ExtAttr(extattr);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_extattrs, "FS", "Leaf (Extattrs)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let extent0 = BlobExtent {
            lsize: 4096,
            rid: RID(i as u64)
        };
        let blob_ext_attr0 = BlobExtAttr {
            namespace: ExtAttrNamespace::User,
            name: OsString::from("The first extended attribute"),
            extent: extent0,
        };
        let extattr0 = ExtAttr::Blob(blob_ext_attr0);
        let extent1 = BlobExtent {
            lsize: 4096,
            rid: RID((10000 + i) as u64)
        };
        let blob_ext_attr1 = BlobExtAttr {
            namespace: ExtAttrNamespace::User,
            name: OsString::from("The second extended attribute"),
            extent: extent1,
        };
        let extattr1 = ExtAttr::Blob(blob_ext_attr1);
        let v = FSValue::ExtAttrs(vec![extattr0, extattr1]);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_inline_extent, "FS", "Leaf (Inline Extent)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dbs = DivBufShared::from(vec![42u8; 2048]);
        let extent = InlineExtent::new(Arc::new(dbs));
        let v = FSValue::InlineExtent(extent);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_inode, "FS", "Leaf (Inode)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let inode = Inode {
            size: 0,
            bytes: 0,
            nlink: 1,
            flags: 0,
            atime: Timespec{sec: 0, nsec: 0},
            mtime: Timespec{sec: 0, nsec: 0},
            ctime: Timespec{sec: 0, nsec: 0},
            birthtime: Timespec{sec: 0, nsec: 0},
            uid: 0,
            gid: 0,
            perm: 0o644,
            file_type: FileType::Reg(17)
        };
        let v = FSValue::inode(inode);
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});

test_measure!(fs_leaf_property, "FS", "Leaf (Property)", 576, 2302, |wb, n| {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let v = FSValue::Property(Property::RecordSize(17));
        let credit = borrow_credit(wb, &v);
        ld.insert(k, v, TxgT::from(0), &StubDML{}, credit)
            .now_or_never()
            .unwrap()
            .0.unwrap();
    }
    let node_data = NodeData::<RID, FSKey, FSValue>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
});
