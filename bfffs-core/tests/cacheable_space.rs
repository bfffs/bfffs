//! Measures the actual memory consumption of Cacheable implementors
//!
//! Can't use the standard test harness because we need to run single-threaded.
use bfffs_core::{
    cache::Cacheable,
    ddml::DRP,
    dml::Compression,
    fs_tree::*,
    idml::RidtEntry,
    property::Property,
    tree::*,
    LbaT,
    PBA,
    RID,
    TxgT,
    writeback::{Credit, WriteBack}
};
use divbuf::DivBufShared;
use futures::FutureExt;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    ffi::OsString,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    }
};
use time::Timespec;

struct Counter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), SeqCst);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size(), SeqCst);
    }
}

#[global_allocator]
static A: Counter = Counter;

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

impl CacheableForgetable for Arc<Node<RID, FSKey, FSValue<RID>>> {
    fn forget(self: Box<Self>) -> Credit {
        let nd = Arc::try_unwrap(*self).unwrap().try_unwrap().unwrap();
        if nd.is_leaf() {
            nd.into_leaf().forget()
        } else {
            Credit::null()
        }
    }
}

fn alloct_leaf(_wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = PBA::new(1, i as LbaT);
        let v = RID(i as u64);
        ld.insert(k, v, Credit::null());
    }
    let node_data = NodeData::<DRP, PBA, RID>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn alloct_int(_wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
}

fn ridt_int(_wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
}

fn ridt_leaf(_wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = RID(i as u64);
        let addr = PBA::new(0, i as LbaT);
        let drp = DRP::new(addr, Compression::None, 40000, 40000, 0);
        let v = RidtEntry::new(drp);
        ld.insert(k, v, Credit::null());
    }
    let node_data = NodeData::<DRP, RID, RidtEntry>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_int(_wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let txgs = TxgT::from(0)..TxgT::from(1);
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let addr = RID(i as u64);
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let child = IntElem::new(k, txgs.clone(), TreePtr::Addr(addr));
        children.push(child);
    }
    let nd = NodeData::<RID, FSKey, FSValue<RID>>::Int(IntData::new(children));
    Box::new(Arc::new(Node::new(nd)))
}

fn fs_leaf_blob_extent(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable>
{
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let extent = BlobExtent {
            lsize: 4096,
            rid: RID(i as u64)
        };
        let v = FSValue::BlobExtent(extent);
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_direntry(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dirent = Dirent {
            ino: 0,
            dtype: libc::DT_REG,
            name: OsString::from("something_moderately_long_but_not_too_long")
        };
        let v = FSValue::DirEntry(dirent);
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_direntries(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_dyinginode(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let v = FSValue::DyingInode(DyingInode::from(0));
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_extattr_blob(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_extattr_inline(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_extattrs(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
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
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_inline_extent(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let dbs = DivBufShared::from(vec![42u8; 2048]);
        let extent = InlineExtent::new(Arc::new(dbs));
        let v = FSValue::InlineExtent(extent);
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_inode(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let inode = Inode {
            size: 0,
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
        let v = FSValue::Inode(inode);
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn fs_leaf_property(wb: &WriteBack, n: usize) -> Box<dyn CacheableForgetable> {
    let mut ld = LeafData::default();
    for i in 0..n {
        let k = FSKey::new(i as u64, ObjKey::Inode);
        let v = FSValue::Property(Property::RecordSize(17));
        let credit = borrow_credit(&wb, &v);
        ld.insert(k, v, credit);
    }
    let node_data = NodeData::<RID, FSKey, FSValue<RID>>::Leaf(ld);
    Box::new(Arc::new(Node::new(node_data)))
}

fn logrange(min: usize, max: usize) -> impl Iterator<Item=usize> {
    let minf = min as f64;
    let grange = (max as f64) / minf;
    let mult = grange.powf(1.0/8f64);
    (1..=8).map(move |exp| ((minf * mult.powf(exp as f64)).round()) as usize)
}

fn measure(name: &str, pos: &str, n: usize, verbose: bool,
    f: fn(&WriteBack, usize) -> Box<dyn CacheableForgetable>) -> bool
{
    let wb = WriteBack::limitless();
    let before = ALLOCATED.load(SeqCst);
    let c = f(&wb, n);
    let after = ALLOCATED.load(SeqCst);
    let actual = after - before;
    let calc = c.cache_space();
    wb.repay(c.forget());
    let err = 100.0 * (calc as f64) / (actual as f64) - 100.0;
    if verbose {
        println!("{:>8}{:>22}{:>8}{:>12}{:>12}{:>12.2}%", name, pos, n, actual,
                 calc, err);
    }
    err.abs() <= 5.0
}

fn main() {
    let app = clap::App::new("cacheable_len")
        .arg(clap::Arg::with_name("nocapture")
             .long("nocapture"));
    let matches = app.get_matches();
    let verbose = matches.is_present("nocapture");
    let mut pass = true;

    if verbose {
        println!("{:>8}{:>22}{:>8}{:>12}{:>12}{:>12}", "Table", "Position", "N",
                 "Actual size", "Calculated", "Error");
    }
    for n in logrange(109, 433) {
        pass &= measure("AllocT", "Int", n, verbose, alloct_int);
    }
    for n in logrange(134, 535) {
        pass &= measure("AllocT", "Leaf", n, verbose, alloct_leaf);
    }
    for n in logrange(98, 389) {
        pass &= measure("RIDT", "Int", n, verbose, ridt_int);
    }
    for n in logrange(114, 454) {
        pass &= measure("RIDT", "Leaf", n, verbose, ridt_leaf);
    }
    for n in logrange(91, 364) {
        pass &= measure("FS", "Int", n, verbose, fs_int);
    }
    for n in logrange(576, 2302) {
        measure("FS", "Leaf (Blob Extent)", n, verbose, fs_leaf_blob_extent);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (DirEntry)", n, verbose, fs_leaf_direntry);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (DirEntries)", n, verbose,
            fs_leaf_direntries);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Dying Inode)", n, verbose,
            fs_leaf_dyinginode);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Blob Extattr)", n, verbose,
            fs_leaf_extattr_blob);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Inline Extattr)", n, verbose,
            fs_leaf_extattr_inline);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Extattrs)", n, verbose, fs_leaf_extattrs);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Inline Extent)", n, verbose,
            fs_leaf_inline_extent);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Inode)", n, verbose, fs_leaf_inode);
    }
    for n in logrange(576, 2302) {
        pass &= measure("FS", "Leaf (Property)", n, verbose, fs_leaf_property);
    }
    if !pass {
        panic!("Calculated size out of tolerance in at least one case");
    }
}
