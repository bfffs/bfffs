//! Tests regarding disk I/O for Trees
// LCOV_EXCL_START

use common::tree::*;
use common::ddml_mock::*;
use common::ddml::DRP;
use futures::future;
use simulacrum::*;
use tokio::prelude::task::current;
use tokio::runtime::current_thread;

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_below_root() {
    let mut mock = DDMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::new())));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    mock.expect_pop::<Arc<Node<DRP, u32, u32>>, Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(*const DRP, TxgT)|
                     unsafe {*args.0 == drpl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 41
              end: 42
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 36
                csize: 36
                checksum: 0
          - key: 256
            txgs:
              start: 41
              end: 42
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 1234567
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0, TxgT::from(42)));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0
          - key: 256
            txgs:
              start: 41
              end: 42
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 1234567"#);
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_root() {
    let mut mock = DDMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::new())));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    mock.expect_pop::<Arc<Node<DRP, u32, u32>>, Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(*const DRP, TxgT)|
                     unsafe {*args.0 == drpl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0, TxgT::from(42)));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 42
    end: 43
  ptr:
    Mem:
      Leaf:
        items:
          0: 0"#);
}

#[test]
fn open() {
    let v = vec![
        1u8, 0, 0, 0, 0, 0, 0, 0,               // Height = 1
        2, 0, 0, 0, 0, 0, 0, 0,                 // min_fanout = 2
        5, 0, 0, 0, 0, 0, 0, 0,                 // max_fanout = 5
        0, 0, 0x40, 0, 0, 0, 0, 0,              // max_size = 4MB
        0, 0, 0, 0,                             // root.key = 0
        0, 0, 0, 0,                             // root.txgs.start = 0
        42, 0, 0, 0,                            // root.txgs.end = 42
        1, 0, 0, 0,                             // root.ptr is a TreePtr::Addr
        2, 0,                                   // pba.cluster = 2
        8, 7, 6, 5, 4, 3, 2, 1,                 // pba.lba = 0x0102030405060708
        1, 0, 0, 0,                             // compression = ZstdL9NoShuffle
        78, 0, 0, 0,                            // lsize = 78
        36, 0, 0, 0,                            // csize = 36
        1, 2, 3, 4, 5, 6, 7, 8,                 // checksum = 0x0807060504030201
    ];
    let expected_drp = DRP::new(PBA::new(2, 0x0102030405060708),
        Compression::ZstdL9NoShuffle,
        78,     // lsize
        36,     // csize
        0x0807060504030201
    );
    let on_disk = TreeOnDisk(v);
    let mock = DDMLMock::new();
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, u32>::open(ddml, on_disk).unwrap();
    assert_eq!(tree.i.height.load(Ordering::Relaxed), 1);
    assert_eq!(tree.i.min_fanout, 2);
    assert_eq!(tree.i.max_fanout, 5);
    assert_eq!(tree.i._max_size, 4194304);
    let root_elem_guard = tree.i.root.try_read().unwrap();
    assert_eq!(root_elem_guard.key, 0);
    assert_eq!(root_elem_guard.txgs, TxgT::from(0)..TxgT::from(42));
    let drp = root_elem_guard.ptr.as_addr();
    assert_eq!(*drp, expected_drp);
}

#[test]
fn range_leaf() {
    struct FutureMock {
        e: Expectations
    }
    impl FutureMock {
        pub fn new() -> Self {
            Self {
                e: Expectations::new()
            }
        }

        pub fn expect_poll(&mut self)
            -> Method<(), Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>
        {
            self.e.expect::<(), Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>("poll")
        }

        pub fn then(&mut self) -> &mut Self {
            self.e.then();
            self
        }
    }

    impl Future for FutureMock {
        type Item = Box<Arc<Node<DRP, u32, f32>>>;
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.e.was_called_returning::<(),
                Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>("poll", ())
        }
    }

    // XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.
    // So we have to cheat.  This works as long as FutureMock is only used in
    // single-threaded unit tests.
    unsafe impl Send for FutureMock {}

    let mut mock = DDMLMock::new();
    let mut ld1 = LeafData::new();
    ld1.insert(0, 0.0);
    ld1.insert(1, 1.0);
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let node1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    mock.expect_get::<Arc<Node<DRP, u32, f32>>>()
        .called_once()
        .returning(move |_| {
            let mut fut = FutureMock::new();
            let node2 = node1.clone();
            fut.expect_poll()
                .called_once()
                .returning(|_| {
                    current().notify();
                    Ok(Async::NotReady)
                });
            FutureMock::then(&mut fut).expect_poll()
                .called_once()
                .returning(move |_| {
                    // XXX simulacrum can't return a uniquely owned object in an
                    // expectation, so we must clone db here.
                    // https://github.com/pcsm/simulacrum/issues/52
                    let res = Box::new(node2.clone());
                    Ok(Async::Ready(res))
                });
            Box::new(fut)
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..3).collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (2, 2.0)]));
}

#[test]
/// Read an IntNode.  The public API doesn't provide any way to read an IntNode
/// without also reading its children, so we'll test this through the private
/// IntElem::rlock API.
fn read_int() {
    let drp0 = DRP::random(Compression::None, 40000);
    let drp1 = DRP::random(Compression::ZstdL9NoShuffle, 16000);
    let children = vec![
        IntElem::new(0u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drp0)),
        IntElem::new(256u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drp1)),
    ];
    let node = Arc::new(Node::new(NodeData::Int(IntData::new(children))));
    let drpl = DRP::new(PBA{cluster: 1, lba: 2}, Compression::None, 36, 36, 0);
    let mut mock = DDMLMock::new();
    mock.expect_get::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml.clone(), r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 1
        lba: 2
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        let root_guard = tree.i.root.try_read().unwrap();
        root_guard.rlock(ddml).map(|node| {
            let int_data = (*node).as_int();
            assert_eq!(int_data.nchildren(), 2);
            // Validate DRPs as well as possible using their public API
            assert_eq!(int_data.children[0].key, 0);
            assert!(!int_data.children[0].ptr.is_mem());
            assert_eq!(int_data.children[1].key, 256);
            assert!(!int_data.children[1].ptr.is_mem());
        })
    }));
    assert!(r.is_ok());
}

#[test]
fn read_leaf() {
    let mut mock = DDMLMock::new();
    let mut ld = LeafData::new();
    ld.insert(0, 100);
    ld.insert(1, 200);
    ld.insert(99, 50_000);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_get::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(1));
    assert_eq!(Ok(Some(200)), r);
}

// Tree::flush should serialize the Tree::Inner object
#[test]
fn serialize_inner() {
    let expected = vec![
        1u8, 0, 0, 0, 0, 0, 0, 0,               // Height = 1
        2, 0, 0, 0, 0, 0, 0, 0,                 // min_fanout = 2
        5, 0, 0, 0, 0, 0, 0, 0,                 // max_fanout = 5
        0, 0, 0x40, 0, 0, 0, 0, 0,              // max_size = 4MB
        0, 0, 0, 0,                             // root.key = 0
        0, 0, 0, 0,                             // root.txgs.start = 0
        42, 0, 0, 0,                            // root.txgs.end = 42
        1, 0, 0, 0,                             // root.ptr is a TreePtr::Addr
        2, 0,                                   // pba.cluster = 2
        8, 7, 6, 5, 4, 3, 2, 1,                 // pba.lba = 0x0102030405060708
        1, 0, 0, 0,                             // compression = ZstdL9NoShuffle
        78, 0, 0, 0,                            // lsize = 78
        36, 0, 0, 0,                            // csize = 36
        1, 2, 3, 4, 5, 6, 7, 8,                 // checksum = 0x0807060504030201
    ];
    let mock = DDMLMock::new();
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 2
        lba: 0x0102030405060708
      compression: ZstdL9NoShuffle
      lsize: 78
      csize: 36
      checksum: 0x0807060504030201
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert_eq!(&expected[..], &r.unwrap().0[..])
}

// If the tree isn't dirty, then there's nothing to do
#[test]
fn write_clean() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
}

/// Sync a Tree with both dirty Int nodes and dirty Leaf nodes
#[test]
fn write_deep() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, _)| {
            let node_data = (args.0).0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200) &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(drp).into_future()));
    mock.then().expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, _)| {
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].ptr.is_addr() &&
            int_data.children[1].key == 256 &&
            int_data.children[1].ptr.is_addr() &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(drp).into_future()));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 100
                    1: 200
          - key: 256
            txgs:
              start: 41
              end: 42
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 0x1a7ebabe
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}

#[test]
fn write_int() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, _)|{
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            !int_data.children[0].ptr.is_mem() &&
            int_data.children[1].key == 256 &&
            !int_data.children[1].ptr.is_mem() &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(drp).into_future()));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 5
    end: 25
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 5
              end: 15
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 40000
                csize: 40000
                checksum: 0xdeadbeef
          - key: 256
            txgs:
              start: 18
              end: 25
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 0x1a7ebabe
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}

#[test]
fn write_leaf() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, _)|{
            let node_data = (args.0).0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200) &&
            args.2 == TxgT::from(42)
        })).returning(move |_| Box::new(Ok(drp).into_future()));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 1
  ptr:
    Mem:
      Leaf:
        items:
          0: 100
          1: 200
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}
// LCOV_EXCL_STOP
