// vim: tw=80
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
        0, 0, 0, 0,                             // root.ptr is a TreePtr::Addr
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

// A Tree with 3 IntNodes, each with 3-4 children.  The range_delete will totally
// delete the middle IntNode and partially delete the other two.
#[test]
fn range_delete() {
    let drpl0 = DRP::new(PBA{cluster: 0, lba: 10}, Compression::None, 36, 36, 0);
    let drpl1 = DRP::new(PBA{cluster: 0, lba: 11}, Compression::None, 36, 36, 0);
    let drpl2 = DRP::new(PBA{cluster: 0, lba: 12}, Compression::None, 36, 36, 0);
    let drpl3 = DRP::new(PBA{cluster: 0, lba: 13}, Compression::None, 36, 36, 0);
    let children0 = vec![
        IntElem::new(0u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl0)),
        IntElem::new(1u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl1)),
        IntElem::new(3u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl2)),
        IntElem::new(6u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl3)),
    ];
    let intnode0 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode0 = Some(intnode0);
    let drpi0 = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);

    let drpl4 = DRP::new(PBA{cluster: 0, lba: 20}, Compression::None, 36, 36, 0);
    let drpl5 = DRP::new(PBA{cluster: 0, lba: 21}, Compression::None, 36, 36, 0);
    let drpl6 = DRP::new(PBA{cluster: 0, lba: 22}, Compression::None, 36, 36, 0);
    let children0 = vec![
        IntElem::new(10u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl4)),
        IntElem::new(13u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl5)),
        IntElem::new(16u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl6)),
    ];
    let intnode1 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode1 = Some(intnode1);
    let drpi1 = DRP::new(PBA{cluster: 0, lba: 1}, Compression::None, 36, 36, 0);

    let drpl7 = DRP::new(PBA{cluster: 0, lba: 30}, Compression::None, 36, 36, 0);
    let drpl8 = DRP::new(PBA{cluster: 0, lba: 31}, Compression::None, 36, 36, 0);
    let drpl9 = DRP::new(PBA{cluster: 0, lba: 32}, Compression::None, 36, 36, 0);
    let drpl10 = DRP::new(PBA{cluster: 0, lba: 33}, Compression::None, 36, 36, 0);
    let children0 = vec![
        IntElem::new(20u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl7)),
        IntElem::new(26u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl8)),
        IntElem::new(29u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl9)),
        IntElem::new(30u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(drpl10)),
    ];
    let intnode2 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode2 = Some(intnode2);
    let drpi2 = DRP::new(PBA{cluster: 0, lba: 2}, Compression::None, 36, 36, 0);

    let mut ld2 = LeafData::new();
    ld2.insert(3, 3.0);
    ld2.insert(4, 4.0);
    ld2.insert(5, 5.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));
    let opt_leafnode2 = Some(leafnode2);

    let mut ld7 = LeafData::new();
    ld7.insert(20, 20.0);
    ld7.insert(21, 21.0);
    ld7.insert(22, 22.0);
    let leafnode7 = Arc::new(Node::new(NodeData::Leaf(ld7)));
    let opt_leafnode7 = Some(leafnode7);

    let mut ld8 = LeafData::new();
    ld8.insert(26, 26.0);
    ld8.insert(27, 27.0);
    ld8.insert(28, 28.0);
    let leafnode8 = Arc::new(Node::new(NodeData::Leaf(ld8)));
    let opt_leafnode8 = Some(leafnode8);

    let mut mock = DDMLMock::new();

    fn expect_delete(mock: &mut DDMLMock, drp: DRP) {
        mock.then().expect_delete()
            .called_once()
            .with(passes(move |(arg, _): &(*const DRP, TxgT)| {
                unsafe {**arg == drp}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
    }

    fn expect_pop(mock: &mut DDMLMock, drp: DRP,
                  mut node: Option<Arc<Node<DRP, u32, f32>>>)
    {
        mock.then().expect_pop::<Arc<Node<DRP, u32, f32>>,
                                 Arc<Node<DRP, u32, f32>>>()
            .called_once()
            .with(passes(move |(arg, _): &(*const DRP, TxgT)| {
                unsafe {**arg == drp}
            })).returning(move |_| {
                let res = Box::new(node.take().unwrap());
                Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
            });
    }

    // Simulacrum requires us to define the expectations in their exact call
    // order, even though we don't really care about it.
    // These nodes are popped or deleted in pass1
    expect_pop(&mut mock, drpi0.clone(), opt_intnode0);
    expect_pop(&mut mock, drpl2.clone(), opt_leafnode2);
    expect_delete(&mut mock, drpl3.clone());
    expect_pop(&mut mock, drpi2.clone(), opt_intnode2);
    expect_pop(&mut mock, drpl7.clone(), opt_leafnode7);
    expect_pop(&mut mock, drpi1.clone(), opt_intnode1);
    expect_delete(&mut mock, drpl4.clone());
    expect_delete(&mut mock, drpl5.clone());
    expect_delete(&mut mock, drpl6.clone());
    // leafnode8 is popped as part of the fixup in pass 2
    expect_pop(&mut mock, drpl8.clone(), opt_leafnode8);

    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 8
              end: 9
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 36
                csize: 36
                checksum: 0
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 1
                compression: None
                lsize: 36
                csize: 36
                checksum: 0
          - key: 20
            txgs:
              start: 8
              end: 24
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 2
                compression: None
                lsize: 36
                csize: 36
                checksum: 0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on(
        tree.range_delete(5..25, TxgT::from(42))
    ).unwrap();
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 10
                          compression: None
                          lsize: 36
                          csize: 36
                          checksum: 0
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 11
                          compression: None
                          lsize: 36
                          csize: 36
                          checksum: 0
                    - key: 3
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
          - key: 20
            txgs:
              start: 0
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              26: 26.0
                              27: 27.0
                              28: 28.0
                    - key: 29
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 32
                          compression: None
                          lsize: 36
                          csize: 36
                          checksum: 0
                    - key: 30
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 33
                          compression: None
                          lsize: 36
                          csize: 36
                          checksum: 0"#);
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

#[test]
fn remove_and_merge_down() {
    let mut mock = DDMLMock::new();

    let mut ld = LeafData::new();
    ld.insert(3, 3.0);
    ld.insert(4, 4.0);
    ld.insert(5, 5.0);
    let leafnode = Arc::new(Node::new(NodeData::Leaf(ld)));
    let mut opt_leafnode = Some(leafnode);
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const DRP, TxgT)| unsafe {*args.0 == drpl}))
        .returning(move |_| {
            let res = Box::new(opt_leafnode.take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });

    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(1, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 1
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
      Leaf:
        items:
          3: 3.0
          4: 4.0
          5: 5.0"#);
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
        0, 0, 0, 0,                             // root.ptr is a TreePtr::Addr
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
    let root_drp = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_drp, drp);
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
    let root_drp = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_drp, drp);
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
    let root_drp = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_drp, drp);
}
// LCOV_EXCL_STOP
