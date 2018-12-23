// vim: tw=80
//! Tests regarding disk I/O for Trees
// LCOV_EXCL_START

use crate::common::{
    ddml_mock::*,
    ddml::DRP,
    dml_mock::*,
    fs_tree::{FSKey, FSValue},
    idml_mock::*,
    tree::*,
};
use futures::future;
use simulacrum::*;
use pretty_assertions::assert_eq;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use tokio::prelude::task::current;
use tokio::runtime::current_thread;

#[test]
fn addresses() {
    type NodeT = Arc<Node<u32, u32, f32>>;
    fn expect_get(mock: &mut DMLMock, addr: u32,
                  mut node: Option<NodeT>)
    {
        mock.then().expect_get::<NodeT>()
            .called_once()
            .with(passes(move |arg: &*const u32| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                let res = Box::new(node.take().unwrap());
                Box::new(future::ok::<Box<NodeT>, Error>(res))
            });
    }

    let mut mock = DMLMock::new();
    let addrl0 = 0;
    let addrl1 = 1;
    let addrl2 = 2;
    let addri0 = 3;

    let children0 = vec![
        IntElem::new(0u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl0)),
        IntElem::new(1u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl1)),
    ];
    let intnode0 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode0 = Some(intnode0);
    
    expect_get(&mut mock, addri0, opt_intnode0);

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
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
              Addr: 3
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Addr: 2
                    - key: 15
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
          # This  Node is not returned due to its TXG range
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 4
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let addrs = rt.block_on(future::lazy(|| {
        tree.addresses(TxgT::from(5)..).collect()
    })).unwrap();
    assert_eq!(vec![addri0, addrl0, addrl1, addrl2], addrs);
}

/// Tree::addresses on a Tree with a single leaf node
#[test]
fn addresses_leaf() {
    let mock = DMLMock::new();
    let addrl = 0;
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Addr: 0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let addrs = rt.block_on(future::lazy(|| {
        tree.addresses(..).collect()
    })).unwrap();
    assert_eq!(vec![addrl], addrs);
}

#[test]
fn dump() {
    type NodeT = Arc<Node<u32, u32, f32>>;
    fn expect_get(mock: &mut DMLMock, addr: u32,
                  mut node: Option<NodeT>)
    {
        mock.then().expect_get::<NodeT>()
            .called_once()
            .with(passes(move |arg: &*const u32| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                let res = Box::new(node.take().unwrap());
                Box::new(future::ok::<Box<NodeT>, Error>(res))
            });
    }

    let mut mock = DMLMock::new();
    let addrl0 = 0;
    let addrl1 = 1;
    let addrl2 = 2;
    let addri0 = 3;
    let addri2 = 4;
    let addrl4 = 5;
    let addrl5 = 6;

    let children0 = vec![
        IntElem::new(0u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl0)),
        IntElem::new(2u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl1)),
    ];
    let intnode0 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode0 = Some(intnode0);
    let opt_intnode0c = opt_intnode0.clone();

    let mut ld0 = LeafData::default();
    ld0.insert(0, 0.0);
    ld0.insert(1, 1.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));
    let opt_leafnode0 = Some(leafnode0);

    let mut ld1 = LeafData::default();
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    let leafnode1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    let opt_leafnode1 = Some(leafnode1);

    let mut ld2 = LeafData::default();
    ld2.insert(10, 10.0);
    ld2.insert(11, 11.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));
    let opt_leafnode2 = Some(leafnode2);

    let children2 = vec![
        IntElem::new(20u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl4)),
        IntElem::new(25u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl5)),
    ];
    let intnode2 = Arc::new(Node::new(NodeData::Int(IntData::new(children2))));
    let opt_intnode2 = Some(intnode2);

    let mut ld4 = LeafData::default();
    ld4.insert(20, 20.0);
    ld4.insert(21, 21.0);
    let leafnode4 = Arc::new(Node::new(NodeData::Leaf(ld4)));
    let opt_leafnode4 = Some(leafnode4);

    let mut ld5 = LeafData::default();
    ld5.insert(25, 25.0);
    ld5.insert(26, 26.0);
    let leafnode5 = Arc::new(Node::new(NodeData::Leaf(ld5)));
    let opt_leafnode5 = Some(leafnode5);

    expect_get(&mut mock, addri0, opt_intnode0c);    // lba 3
    expect_get(&mut mock, addri2, opt_intnode2);     // lba 4
    expect_get(&mut mock, addrl0, opt_leafnode0);    // lba 0
    expect_get(&mut mock, addrl1, opt_leafnode1);    // lba 1
    expect_get(&mut mock, addrl2, opt_leafnode2);    // lba 2
    expect_get(&mut mock, addrl4, opt_leafnode4);    // lba 5
    expect_get(&mut mock, addrl5, opt_leafnode5);    // lba 6

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
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
              Addr: 3
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Addr: 2
                    - key: 15
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
          # This  Node is not returned due to its TXG range
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 4
"#);
let expected =
r#"---
height: 3
limits:
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
              Addr: 3
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Addr: 2
                    - key: 15
                      txgs:
                        start: 9
                        end: 10
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 4
---
0:
  Leaf:
    items:
      0: 0.0
      1: 1.0
1:
  Leaf:
    items:
      2: 2.0
      3: 3.0
---
2:
  Leaf:
    items:
      10: 10.0
      11: 11.0
---
5:
  Leaf:
    items:
      20: 20.0
      21: 21.0
6:
  Leaf:
    items:
      25: 25.0
      26: 26.0
---
3:
  Int:
    children:
      - key: 0
        txgs:
          start: 8
          end: 9
        ptr:
          Addr: 0
      - key: 2
        txgs:
          start: 8
          end: 9
        ptr:
          Addr: 1
4:
  Int:
    children:
      - key: 20
        txgs:
          start: 8
          end: 9
        ptr:
          Addr: 5
      - key: 25
        txgs:
          start: 8
          end: 9
        ptr:
          Addr: 6
"#;
    let mut out = Vec::new();
    tree.dump(&mut out).unwrap();
    println!("{:?}", OsStr::from_bytes(&out[..]));
    assert_eq!(expected, OsStr::from_bytes(&out[..]));
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_below_root() {
    let mut mock = DMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::default())));
    let node_holder = RefCell::new(Some(node));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)|
                     unsafe {*args.0 == addrl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, u32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
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
              Addr: 0
          - key: 256
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 256
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0, TxgT::from(42)));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 2
limits:
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
              Addr: 256"#);
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_root() {
    let mut mock = DMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::default())));
    let node_holder = RefCell::new(Some(node));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)|
                     unsafe {*args.0 == addrl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, u32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Addr: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0, TxgT::from(42)));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
limits:
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
fn is_dirty() {
    let mut mock = DMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::default())));
    let node_holder = RefCell::new(Some(node));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)|
                     unsafe {*args.0 == addrl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, u32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 41
    end: 42
  ptr:
    Addr: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!tree.is_dirty());
    rt.block_on(tree.insert(0, 0, TxgT::from(42))).unwrap();
    assert!(tree.is_dirty());
}

#[test]
fn open() {
    let root_drp = DRP::new(PBA::new(2, 0x0102_0304_0506_0708),
        Compression::Zstd(None),
        78,     // lsize
        36,     // csize
        0x0807_0605_0403_0201
    );
    let tod = TreeOnDisk(
        InnerOnDisk {
            height: 1,
            limits: Limits::new(2, 5),
            root: root_drp,
            txgs: TxgT(0)..TxgT(42),
        }
    );
    let mock = DDMLMock::default();
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, u32>::open(ddml, false, tod);
    assert_eq!(tree.i.height.load(Ordering::Relaxed), 1);
    assert_eq!(tree.i.limits.min_fanout(), 2);
    assert_eq!(tree.i.limits.max_fanout(), 5);
    let root_elem_guard = tree.i.root.try_read().unwrap();
    assert_eq!(root_elem_guard.key, 0);
    assert_eq!(root_elem_guard.txgs, TxgT::from(0)..TxgT::from(42));
    let drp = root_elem_guard.ptr.as_addr();
    assert_eq!(*drp, root_drp);
}

// A Tree with 3 IntNodes, each with 3-4 children.  The range_delete will totally
// delete the middle IntNode and partially delete the other two.
#[test]
fn range_delete() {
    let addrl0 = 10;
    let addrl1 = 11;
    let addrl2 = 12;
    let addrl3 = 13;
    let children0 = vec![
        IntElem::new(0u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl0)),
        IntElem::new(1u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl1)),
        IntElem::new(3u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl2)),
        IntElem::new(6u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl3)),
    ];
    let intnode0 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    let opt_intnode0 = Some(intnode0);
    let addri0 = 0;

    let addrl4 = 20;
    let addrl5 = 21;
    let addrl6 = 22;
    let children1 = vec![
        IntElem::new(10u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl4)),
        IntElem::new(13u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl5)),
        IntElem::new(16u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl6)),
    ];
    let intnode1 = Arc::new(Node::new(NodeData::Int(IntData::new(children1))));
    let opt_intnode1 = Some(intnode1);
    let addri1 = 1;

    let addrl7 = 30;
    let addrl8 = 31;
    let addrl9 = 32;
    let addrl10 = 33;
    let children2 = vec![
        IntElem::new(20u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl7)),
        IntElem::new(26u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl8)),
        IntElem::new(29u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl9)),
        IntElem::new(30u32, TxgT::from(0)..TxgT::from(1), TreePtr::Addr(addrl10)),
    ];
    let intnode2 = Arc::new(Node::new(NodeData::Int(IntData::new(children2))));
    let opt_intnode2 = Some(intnode2);
    let addri2 = 2;

    let mut ld2 = LeafData::default();
    ld2.insert(3, 3.0);
    ld2.insert(4, 4.0);
    ld2.insert(5, 5.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));
    let opt_leafnode2 = Some(leafnode2);

    let mut ld7 = LeafData::default();
    ld7.insert(20, 20.0);
    ld7.insert(21, 21.0);
    ld7.insert(22, 22.0);
    let leafnode7 = Arc::new(Node::new(NodeData::Leaf(ld7)));
    let opt_leafnode7 = Some(leafnode7);

    let mut mock = DMLMock::new();

    fn expect_delete(mock: &mut DMLMock, addr: u32) {
        mock.then().expect_delete()
            .called_once()
            .with(passes(move |(arg, _): &(*const u32, TxgT)| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
    }

    fn expect_pop(mock: &mut DMLMock, addr: u32,
                  mut node: Option<Arc<Node<u32, u32, f32>>>)
    {
        mock.then().expect_pop::<Arc<Node<u32, u32, f32>>,
                                 Arc<Node<u32, u32, f32>>>()
            .called_once()
            .with(passes(move |(arg, _): &(*const u32, TxgT)| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                let res = Box::new(node.take().unwrap());
                Box::new(future::ok::<Box<Arc<Node<u32, u32, f32>>>, Error>(res))
            });
    }

    // Simulacrum requires us to define the expectations in their exact call
    // order, even though we don't really care about it.
    // These nodes are popped or deleted in pass1
    expect_pop(&mut mock, addri0, opt_intnode0);
    expect_pop(&mut mock, addri2, opt_intnode2);
    expect_pop(&mut mock, addri1, opt_intnode1);
    expect_pop(&mut mock, addrl2, opt_leafnode2);
    expect_delete(&mut mock, addrl3);
    expect_delete(&mut mock, addrl4);
    expect_delete(&mut mock, addrl5);
    expect_delete(&mut mock, addrl6);
    expect_pop(&mut mock, addrl7, opt_leafnode7);

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
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
              Addr: 0
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr:
              Addr: 1
          - key: 20
            txgs:
              start: 8
              end: 24
            ptr:
              Addr: 2
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on(
        tree.range_delete(5..25, TxgT::from(42))
    ).unwrap();
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
limits:
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
                        Addr: 10
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 11
          - key: 3
            txgs:
              start: 0
              end: 43
            ptr:
              Mem:
                Int:
                  children:
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
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 31
                    - key: 29
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 32
                    - key: 30
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 33"#);
}

/// Regression test for bug 2d045899e991a7cf977303abb565c09cf8c34b2f
/// If range_delete removes all keys from a node on the left side of the cut, it
/// should remove the entire node.
#[test]
fn range_delete_left_in_cut_full() {
    fn expect_delete(mock: &mut DMLMock, addr: u32)
    {
        mock.then().expect_delete()
            .called_once()
            .with(passes(move |(arg, _): &(*const u32, TxgT)| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
    }

    fn expect_pop(mock: &mut DMLMock, addr: u32,
                  mut node: Option<Arc<Node<u32, u32, f32>>>)
    {
        mock.then().expect_pop::<Arc<Node<u32, u32, f32>>,
                                 Arc<Node<u32, u32, f32>>>()
            .called_once()
            .with(passes(move |(arg, _): &(*const u32, TxgT)| {
                unsafe {**arg == addr}
            })).returning(move |_| {
                let res = Box::new(node.take().unwrap());
                Box::new(future::ok::<Box<Arc<Node<u32, u32, f32>>>, Error>(res))
            });
    }

    let mut mock = DMLMock::new();

    let addrl0 = 81;
    let mut ld0 = LeafData::default();
    ld0.insert(19, 15.0);
    ld0.insert(20, 16.0);
    ld0.insert(21, 17.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));
    let opt_leafnode0 = Some(leafnode0);

    let addrl1 = 94;

    expect_pop(&mut mock, addrl0, opt_leafnode0);
    expect_delete(&mut mock, addrl1);

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 3
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 81
                    - key: 22
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Addr: 94
                    - key: 25
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              25: 21
                              31: 27
                              32: 16
                    - key: 33
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              33: 17
                              34: 18
                              35: 19
                    - key: 37
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 17
                              38: 18
                              39: 19
          - key: 46
            txgs:
              start: 1
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 46
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              46: 20
                              47: 21
                              48: 27
                              77: 33
                              78: 34
                    - key: 69
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 84
                    - key: 72
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Addr: 172
                    - key: 75
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Addr: 175
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on(
        tree.range_delete(4..32, TxgT::from(42))
    ).unwrap();
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
limits:
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
              start: 2
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 25
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              32: 16.0
                              33: 17.0
                              34: 18.0
                              35: 19.0
                    - key: 37
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 17.0
                              38: 18.0
                              39: 19.0
                    - key: 46
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              46: 20.0
                              47: 21.0
                              48: 27.0
                              77: 33.0
                              78: 34.0
          - key: 69
            txgs:
              start: 1
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 69
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 84
                    - key: 72
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Addr: 172
                    - key: 75
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Addr: 175"#);
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
            -> Method<(), Poll<Box<Arc<Node<u32, u32, f32>>>, Error>>
        {
            self.e.expect::<(), Poll<Box<Arc<Node<u32, u32, f32>>>, Error>>("poll")
        }

        pub fn then(&mut self) -> &mut Self {
            self.e.then();
            self
        }
    }

    impl Future for FutureMock {
        type Item = Box<Arc<Node<u32, u32, f32>>>;
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.e.was_called_returning::<(),
                Poll<Box<Arc<Node<u32, u32, f32>>>, Error>>("poll", ())
        }
    }

    // XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.
    // So we have to cheat.  This works as long as FutureMock is only used in
    // single-threaded unit tests.
    unsafe impl Send for FutureMock {}

    let mut mock = DMLMock::new();
    let mut ld1 = LeafData::default();
    ld1.insert(0, 0.0);
    ld1.insert(1, 1.0);
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let node1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    mock.expect_get::<Arc<Node<u32, u32, f32>>>()
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
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr: 0
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
    let addr0 = 8888;
    let addr1 = 9999;
    let children = vec![
        IntElem::new(0u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(addr0)),
        IntElem::new(256u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(addr1)),
    ];
    let node = Arc::new(Node::new(NodeData::Int(IntData::new(children))));
    let addrl = 102;
    let mut mock = DMLMock::new();
    mock.expect_get::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const u32| unsafe {**arg == addrl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, u32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> =
        Tree::from_str(dml.clone(), false, r#"
---
height: 2
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr: 102
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        let root_guard = tree.i.root.try_read().unwrap();
        root_guard.rlock(&dml).map(|node| {
            let int_data = (*node).as_int();
            assert_eq!(int_data.nchildren(), 2);
            // Validate IntElems as well as possible using their public API
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
    let mut mock = DMLMock::new();
    let mut ld = LeafData::default();
    ld.insert(0, 100);
    ld.insert(1, 200);
    ld.insert(99, 50_000);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_get::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, u32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(1));
    assert_eq!(Ok(Some(200)), r);
}

#[test]
fn remove_and_merge_down() {
    let mut mock = DMLMock::new();

    let mut ld = LeafData::default();
    ld.insert(3, 3.0);
    ld.insert(4, 4.0);
    ld.insert(5, 5.0);
    let leafnode = Arc::new(Node::new(NodeData::Leaf(ld)));
    let mut opt_leafnode = Some(leafnode);
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)| unsafe {*args.0 == addrl}))
        .returning(move |_| {
            let res = Box::new(opt_leafnode.take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, f32>>>, Error>(res))
        });

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
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
              Addr: 0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(1, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 1
limits:
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

// This test mimics what the IDML does with the alloc_t
#[test]
fn serialize_alloc_t() {
    let mock = DDMLMock::default();
    let idml = Arc::new(mock);
    let typical_tree: Tree<DRP, DDMLMock, PBA, RID> =
        Tree::from_str(idml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key:
    cluster: 0
    lba: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 2
        lba: 0x0102030405060708
      compressed: true
      lsize: 78
      csize: 36
      checksum: 0x0807060504030201
"#);
    let typical_tod = typical_tree.serialize().unwrap();
    assert_eq!(TreeOnDisk::<DRP>::TYPICAL_SIZE,
               bincode::serialized_size(&typical_tod).unwrap() as usize);
}

// This test mimics what Database does with its forest object
#[test]
fn serialize_forest() {
    let mock = IDMLMock::default();
    let idml = Arc::new(mock);
    let typical_tree: Tree<RID, IDMLMock, FSKey, FSValue<RID>> =
        Tree::from_str(idml, false, r#"
---
height: 1
limits:
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
      1
"#);

    let typical_tod = typical_tree.serialize().unwrap();
    assert_eq!(TreeOnDisk::<RID>::TYPICAL_SIZE,
               bincode::serialized_size(&typical_tod).unwrap() as usize);
}

// Tree::serialize should serialize the Tree::Inner object
#[test]
fn serialize_inner() {
    let root_pba = PBA::new(2, 0x0102_0304_0506_0708);
    let root_drp = DRP::new(root_pba, Compression::Zstd(None), 78, 36,
                            0x0807_0605_0403_0201);
    let expected = TreeOnDisk(
        InnerOnDisk {
            height: 1,
            limits: Limits::new(2, 5),
            root: root_drp,
            txgs: TxgT(0)..TxgT(42),
        }
    );
    let mock = DDMLMock::default();
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, false, r#"
---
height: 1
limits:
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
      compressed: true
      lsize: 78
      csize: 36
      checksum: 0x0807060504030201
"#);

    assert_eq!(expected, tree.serialize().unwrap())
}

// If the tree isn't dirty, then there's nothing to do
#[test]
fn write_clean() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_fanout: 2
  max_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 42
  ptr:
    Addr: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
}

/// Sync a Tree with both dirty Int nodes and dirty Leaf nodes
#[test]
fn write_deep() {
    let mut mock = DMLMock::new();
    let addr = 42;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, _)| {
            let node_data = (args.0).0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200) &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(addr).into_future()));
    mock.then().expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, _)| {
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].ptr.is_addr() &&
            int_data.children[1].key == 256 &&
            int_data.children[1].ptr.is_addr() &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(addr).into_future()));
    let dml = Arc::new(mock);
    let mut tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
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
              Addr: 256
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
    let root_addr = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_addr, addr);
}

#[test]
fn write_int() {
    let mut mock = DMLMock::new();
    let addr = 9999;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, _)|{
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            !int_data.children[0].ptr.is_mem() &&
            int_data.children[1].key == 256 &&
            !int_data.children[1].ptr.is_mem() &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(addr).into_future()));
    let dml = Arc::new(mock);
    let mut tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
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
              Addr: 0
          - key: 256
            txgs:
              start: 18
              end: 25
            ptr:
              Addr: 256
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush(TxgT::from(42)));
    assert!(r.is_ok());
    let root_addr = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_addr, addr);
}

#[test]
fn write_leaf() {
    let mut mock = DMLMock::new();
    let addr = 9999;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, _)|{
            let node_data = (args.0).0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200) &&
            args.2 == TxgT::from(42)
        })).returning(move |_| Box::new(Ok(addr).into_future()));
    let dml = Arc::new(mock);
    let mut tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
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
    let root_addr = *Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap()
        .ptr.as_addr();
    assert_eq!(root_addr, addr);
}
// LCOV_EXCL_STOP
