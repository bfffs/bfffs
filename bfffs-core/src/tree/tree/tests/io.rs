// vim: tw=80
//! Tests regarding disk I/O for Trees
// LCOV_EXCL_START

use crate::{
    ddml::*,
    idml::IDML,
    fs_tree::{FSKey, FSValue},
};
use mockall::{
    Sequence,
    predicate::{always, eq}
};
use pretty_assertions::assert_eq;
use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    sync::RwLock
};
use super::*;

type NodeT = Arc<Node<u32, u32, f32>>;

/// Helper method for setting MockDML::get expectations
fn expect_get(mock: &mut MockDML, addr: u32, node: NodeT)
{
    mock.expect_get::<NodeT, NodeT>()
        .once()
        .with(eq(addr))
        .return_once(move |_| future::ok(Box::new(node)).boxed());
}

/// Helper method for setting MockDML::delete expectations
fn expect_delete(mock: &mut MockDML, addr: u32) {
    mock.expect_delete()
        .once()
        .with(eq(addr), always())
        .returning(move |_, _| future::ok(()).boxed());
}

/// Helper method for setting MockDML::pop expectations
fn expect_pop(mock: &mut MockDML, addr: u32, node: NodeT)
{
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .with(eq(addr), always())
        .return_once(move |_, _| future::ok(Box::new(node)).boxed());
}

#[tokio::test]
async fn addresses() {
    let mut mock = mock_dml();
    let addrl0 = 0;
    let addrl1 = 1;
    let addrl2 = 2;
    let addri0 = 3;

    let children0 = vec![
        IntElem::new(0u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl0)),
        IntElem::new(1u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl1)),
    ];
    let intnode0 = Arc::new(Node::new(NodeData::Int(IntData::new(children0))));
    expect_get(&mut mock, addri0, intnode0);

    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, f32> = Tree::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 8
            end: 9
          ptr: !Addr 3
        - key: 10
          txgs:
            start: 20
            end: 32
          ptr: !Mem
            Int:
              children:
              - key: 10
                txgs:
                  start: 9
                  end: 10
                ptr: !Addr 2
              - key: 15
                txgs:
                  start: 9
                  end: 10
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      15: 15.0
                      16: 16.0
                      17: 17.0
        # This  Node is not returned due to its TXG range
        - key: 20
          txgs:
            start: 0
            end: 1
          ptr: !Addr 4
"#);
    let addrs = tree.addresses(TxgT::from(5)..)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![addri0, addrl0, addrl1, addrl2], addrs);
}

/// Tree::addresses on a Tree with a single leaf node
#[tokio::test]
async fn addresses_leaf() {
    let mock = MockDML::new();
    let addrl = 0;
    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, u32> = Tree::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Addr 0"#);
    let addrs = tree.addresses(..)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![addrl], addrs);
}

#[test]
fn dump() {
    let mut mock = mock_dml();
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

    let mut ld0 = LeafData::default();
    ld0.items.insert(0, 0.0);
    ld0.items.insert(1, 1.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));

    let mut ld1 = LeafData::default();
    ld1.items.insert(2, 2.0);
    ld1.items.insert(3, 3.0);
    let leafnode1 = Arc::new(Node::new(NodeData::Leaf(ld1)));

    let mut ld2 = LeafData::default();
    ld2.items.insert(10, 10.0);
    ld2.items.insert(11, 11.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));

    let children2 = vec![
        IntElem::new(20u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl4)),
        IntElem::new(25u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(addrl5)),
    ];
    let intnode2 = Arc::new(Node::new(NodeData::Int(IntData::new(children2))));

    let mut ld4 = LeafData::default();
    ld4.items.insert(20, 20.0);
    ld4.items.insert(21, 21.0);
    let leafnode4 = Arc::new(Node::new(NodeData::Leaf(ld4)));

    let mut ld5 = LeafData::default();
    ld5.items.insert(25, 25.0);
    ld5.items.insert(26, 26.0);
    let leafnode5 = Arc::new(Node::new(NodeData::Leaf(ld5)));

    expect_get(&mut mock, addri0, intnode0);     // lba 3
    expect_get(&mut mock, addri2, intnode2);     // lba 4
    expect_get(&mut mock, addrl0, leafnode0);    // lba 0
    expect_get(&mut mock, addrl1, leafnode1);    // lba 1
    expect_get(&mut mock, addrl2, leafnode2);    // lba 2
    expect_get(&mut mock, addrl4, leafnode4);    // lba 5
    expect_get(&mut mock, addrl5, leafnode5);    // lba 6

    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, f32> = Tree::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
          - key: 0
            txgs:
              start: 8
              end: 9
            ptr: !Addr 3
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr: !Mem
              Int:
                children:
                  - key: 10
                    txgs:
                      start: 9
                      end: 10
                    ptr: !Addr 2
                  - key: 15
                    txgs:
                      start: 9
                      end: 10
                    ptr: !Mem
                      Leaf:
                        credit: 48
                        items:
                          15: 15.0
                          16: 16.0
                          17: 17.0
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr: !Addr 4
  "#);
let expected =
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 8
            end: 9
          ptr: !Addr 3
        - key: 10
          txgs:
            start: 20
            end: 32
          ptr: !Mem
            Int:
              children:
              - key: 10
                txgs:
                  start: 9
                  end: 10
                ptr: !Addr 2
              - key: 15
                txgs:
                  start: 9
                  end: 10
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      15: 15.0
                      16: 16.0
                      17: 17.0
        - key: 20
          txgs:
            start: 0
            end: 1
          ptr: !Addr 4
---
0: !Leaf
  credit: 0
  items:
    0: 0.0
    1: 1.0
1: !Leaf
  credit: 0
  items:
    2: 2.0
    3: 3.0
---
2: !Leaf
  credit: 0
  items:
    10: 10.0
    11: 11.0
---
5: !Leaf
  credit: 0
  items:
    20: 20.0
    21: 21.0
6: !Leaf
  credit: 0
  items:
    25: 25.0
    26: 26.0
---
3: !Int
  children:
  - key: 0
    txgs:
      start: 8
      end: 9
    ptr: !Addr 0
  - key: 2
    txgs:
      start: 8
      end: 9
    ptr: !Addr 1
4: !Int
  children:
  - key: 20
    txgs:
      start: 8
      end: 9
    ptr: !Addr 5
  - key: 25
    txgs:
      start: 8
      end: 9
    ptr: !Addr 6
"#;
    let mut out = Vec::new();
    tree.dump(&mut out).now_or_never().unwrap().unwrap();
    println!("{:?}", OsStr::from_bytes(&out[..]));
    assert_eq!(expected, OsStr::from_bytes(&out[..]));
}

/// Dump a Tree, but get an I/O error reading part of it.
#[test]
fn dump_error() {
    let mut mock = mock_dml();
    let addrl0 = 3;
    let addrl1 = 4;
    let addrl2 = 5;

    let mut ld0 = LeafData::default();
    ld0.items.insert(0, 0.0);
    ld0.items.insert(1, 1.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));

    let mut ld2 = LeafData::default();
    ld2.items.insert(20, 20.0);
    ld2.items.insert(21, 21.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));

    expect_get(&mut mock, addrl0, leafnode0);
    mock.expect_get::<NodeT, NodeT>()
        .once()
        .with(eq(addrl1))
        .return_once(move |_| future::err(Error::EIO).boxed());
    expect_get(&mut mock, addrl2, leafnode2);

    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, f32> = Tree::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
          - key: 0
            txgs:
              start: 8
              end: 9
            ptr: !Addr 3
          - key: 10
            txgs:
              start: 20
              end: 32
            ptr: !Addr 4
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr: !Addr 5
  "#);
let expected =
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 8
            end: 9
          ptr: !Addr 3
        - key: 10
          txgs:
            start: 20
            end: 32
          ptr: !Addr 4
        - key: 20
          txgs:
            start: 0
            end: 1
          ptr: !Addr 5
--- 'Error reading 4: EIO'
---
3: !Leaf
  credit: 0
  items:
    0: 0.0
    1: 1.0
5: !Leaf
  credit: 0
  items:
    20: 20.0
    21: 21.0
"#;
    let mut out = Vec::new();
    tree.dump(&mut out).now_or_never().unwrap().unwrap();
    assert_eq!(expected, OsStr::from_bytes(&out[..]));
}

/// Insert an item for a key that already has a value, and needs dclone
#[test]
fn insert_dclone() {
    let txg = TxgT::from(42);
    let mut mock = MockDML::new();
    mock.expect_pop::<DivBufShared, DivBuf>()
        .with(eq(4), eq(txg))
        .times(1)
        .returning(|_, _| future::ok(Box::new(DivBufShared::from(vec![])))
                   .boxed()
        );

    let mut ld = LeafData::default();
    ld.items.insert(3, NeedsDcloneV(3));
    ld.items.insert(4, NeedsDcloneV(4));
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_pop::<NeedsDcloneNode, NeedsDcloneNode>()
        .once()
        .with(eq(0), always())
        .return_once(move |_, _| future::ok(Box::new(node)).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 0usize)
        .return_const(());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, NeedsDcloneV>::from_str(
        dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 0
  "#));

    let r = tree.clone().insert(4, NeedsDcloneV(400), txg, Credit::forge(16))
        .now_or_never().unwrap();
    assert_eq!(r, Ok(Some(NeedsDcloneV(4))));
    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 42
      end: 43
    ptr: !Mem
      Leaf:
        credit: 32
        items:
          3: 3
          4: 400
"#);
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_below_root() {
    let mut mock = mock_dml();
    let mut ld = LeafData::default();
    ld.items.insert(3, 3);
    ld.items.insert(4, 4);
    ld.items.insert(5, 5);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| {
            future::ok(Box::new(node)).boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 256
          txgs:
            start: 41
            end: 42
          ptr: !Addr 256
  "#));

    let r = tree.clone().insert(0, 0, TxgT::from(42), Credit::forge(80))
        .now_or_never().unwrap();
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 64
              items:
                0: 0
                3: 3
                4: 4
                5: 5
        - key: 256
          txgs:
            start: 41
            end: 42
          ptr: !Addr 256
"#);
}

/// The insert operation has insufficient credit to xlock the leaf node.  This
/// should work, but log a warning.
#[test]
fn insert_insufficient_credit() {
    let mut mock = mock_dml();
    let mut ld = LeafData::default();
    ld.items.insert(3, 3);
    ld.items.insert(4, 4);
    ld.items.insert(5, 5);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| {
            future::ok(Box::new(node)).boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 256
          txgs:
            start: 41
            end: 42
          ptr: !Addr 256
  "#));

    tree.insert(0, 0, TxgT::from(42), Credit::forge(8))
        .now_or_never().unwrap()
        .unwrap();
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_root() {
    let mut mock = mock_dml();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::default())));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| {
            future::ok(Box::new(node)).boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Addr 0
  "#));

    let r = tree.clone().insert(0, 0, TxgT::from(42), Credit::forge(8))
        .now_or_never().unwrap();
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 42
      end: 43
    ptr: !Mem
      Leaf:
        credit: 16
        items:
          0: 0
"#);
}

/// Insert a key that splits the root leaf node
#[test]
fn insert_split_root_leaf() {
    let mut mock = mock_dml();
    let mut ld = LeafData::default();
    ld.items.insert(0, 0.0);
    ld.items.insert(1, 1.0);
    ld.items.insert(2, 2.0);
    ld.items.insert(3, 3.0);
    ld.items.insert(4, 4.0);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| {
            future::ok(Box::new(node)).boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Addr 0
  "#));
    let r2 = tree.clone().insert(5, 5.0, TxgT::from(42), Credit::forge(80))
        .now_or_never().unwrap();
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 48
              items:
                0: 0.0
                1: 1.0
                2: 2.0
        - key: 3
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 48
              items:
                3: 3.0
                4: 4.0
                5: 5.0
"#);
}

#[test]
fn is_dirty() {
    let mut mock = mock_dml();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::default())));
    let addrl = 0;
    mock.expect_pop::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| {
            future::ok(Box::new(node)).boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Addr 0
  "#));

    assert!(!tree.is_dirty());
    tree.clone().insert(0, 0, TxgT::from(42), Credit::forge(80))
        .now_or_never().unwrap().unwrap();
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
    let limits = Limits::new(2, 5, 2, 5);
    let tod = TreeOnDisk(
        InnerOnDisk {
            height: 1,
            _reserved: Default::default(),
            limits,
            root: root_drp,
            txgs: TxgT(0)..TxgT(42),
        }
    );
    let mock = DDML::default();
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDML, u32, u32>::open(ddml, false, tod);
    assert_eq!(tree.limits, limits);
    let root_guard = tree.root.try_read().unwrap();
    assert_eq!(root_guard.height, 1);
    assert_eq!(root_guard.elem.key, 0);
    assert_eq!(root_guard.elem.txgs, TxgT::from(0)..TxgT::from(42));
    let drp = root_guard.elem.ptr.as_addr();
    assert_eq!(*drp, root_drp);
}

// A Tree with 3 IntNodes, each with 3-4 children.  The range_delete will
// totally delete the middle IntNode and partially delete the other two.
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
    let addri2 = 2;

    let mut ld2 = LeafData::default();
    ld2.items.insert(3, 3.0);
    ld2.items.insert(4, 4.0);
    ld2.items.insert(5, 5.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));

    let mut ld7 = LeafData::default();
    ld7.items.insert(20, 20.0);
    ld7.items.insert(21, 21.0);
    ld7.items.insert(22, 22.0);
    let leafnode7 = Arc::new(Node::new(NodeData::Leaf(ld7)));

    let mut mock = mock_dml();

    // These nodes are popped or deleted in pass1
    expect_delete(&mut mock, addrl3);
    expect_delete(&mut mock, addrl4);
    expect_delete(&mut mock, addrl5);
    expect_delete(&mut mock, addrl6);
    expect_pop(&mut mock, addri0, intnode0);
    expect_pop(&mut mock, addri1, intnode1);
    expect_pop(&mut mock, addri2, intnode2);
    expect_pop(&mut mock, addrl2, leafnode2);
    expect_pop(&mut mock, addrl7, leafnode7);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 8
            end: 9
          ptr: !Addr 0
        - key: 10
          txgs:
            start: 20
            end: 32
          ptr: !Addr 1
        - key: 20
          txgs:
            start: 8
            end: 24
          ptr: !Addr 2
  "#));
    tree.clone().range_delete(5..25, TxgT::from(42), Credit::forge(120))
        .now_or_never().unwrap().unwrap();
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 0
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 0
                txgs:
                  start: 0
                  end: 1
                ptr: !Addr 10
              - key: 1
                txgs:
                  start: 0
                  end: 1
                ptr: !Addr 11
        - key: 3
          txgs:
            start: 0
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 3
                txgs:
                  start: 42
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      3: 3.0
                      4: 4.0
              - key: 26
                txgs:
                  start: 0
                  end: 1
                ptr: !Addr 31
              - key: 29
                txgs:
                  start: 0
                  end: 1
                ptr: !Addr 32
              - key: 30
                txgs:
                  start: 0
                  end: 1
                ptr: !Addr 33
"#);
}

/// range_delete with a value type that needs dclone
#[test]
fn range_delete_dclone() {
    let mut mock = mock_dml();
    let txg = TxgT::from(42);

    let addr = 1u32;
    let mut ld = LeafData::default();
    ld.items.insert(11, NeedsDcloneV(11));
    ld.items.insert(12, NeedsDcloneV(12));
    ld.items.insert(13, NeedsDcloneV(13));
    let node1 = Arc::new(Node::new(NodeData::Leaf(ld)));

    for v in [11, 12, 13]{
        mock.expect_delete()
            .with(eq(v), eq(txg))
            .times(1)
            .returning(|_, _| future::ok(()).boxed());
    }
    mock.expect_pop::<NeedsDcloneNode, NeedsDcloneNode>()
        .once()
        .with(eq(addr), always())
        .return_once(move |_, _| future::ok(Box::new(node1)).boxed());
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, NeedsDcloneV>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 1
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 11
          txgs:
            start: 41
            end: 42
          ptr: !Addr 1
        - key: 21
          txgs:
            start: 41
            end: 42
          ptr: !Addr 2
  "#));
    let r = tree.range_delete(11..21, txg, Credit::forge(64))
        .now_or_never().unwrap();
    assert!(r.is_ok());
}

/// Regression test for bug 2d045899e991a7cf977303abb565c09cf8c34b2f
/// If range_delete removes all keys from a node on the left side of the cut, it
/// should remove the entire node.
#[test]
fn range_delete_left_in_cut_full() {
    let mut mock = mock_dml();

    let addrl0 = 81;
    let mut ld0 = LeafData::default();
    ld0.items.insert(19, 15.0);
    ld0.items.insert(20, 16.0);
    ld0.items.insert(21, 17.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));

    let addrl1 = 94;

    expect_pop(&mut mock, addrl0, leafnode0);
    expect_delete(&mut mock, addrl1);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 3
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 0
            end: 3
          ptr: !Mem
            Int:
              children:
              - key: 0
                txgs:
                  start: 1
                  end: 2
                ptr: !Addr 81
              - key: 22
                txgs:
                  start: 2
                  end: 3
                ptr: !Addr 94
              - key: 25
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      25: 21
                      31: 27
                      32: 16
              - key: 33
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      33: 17
                      34: 18
                      35: 19
              - key: 37
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      37: 17
                      38: 18
                      39: 19
        - key: 46
          txgs:
            start: 1
            end: 3
          ptr: !Mem
            Int:
              children:
              - key: 46
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 80
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
                ptr: !Addr 84
              - key: 72
                txgs:
                  start: 2
                  end: 3
                ptr: !Addr 172
              - key: 75
                txgs:
                  start: 2
                  end: 3
                ptr: !Addr 175
  "#));
    tree.clone().range_delete(4..32, TxgT::from(42), Credit::forge(80))
        .now_or_never().unwrap().unwrap();
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 2
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 25
                txgs:
                  start: 42
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 64
                    items:
                      32: 16.0
                      33: 17.0
                      34: 18.0
                      35: 19.0
              - key: 37
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      37: 17.0
                      38: 18.0
                      39: 19.0
              - key: 46
                txgs:
                  start: 2
                  end: 3
                ptr: !Mem
                  Leaf:
                    credit: 80
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
          ptr: !Mem
            Int:
              children:
              - key: 69
                txgs:
                  start: 1
                  end: 2
                ptr: !Addr 84
              - key: 72
                txgs:
                  start: 2
                  end: 3
                ptr: !Addr 172
              - key: 75
                txgs:
                  start: 2
                  end: 3
                ptr: !Addr 175
"#);
}

#[tokio::test]
async fn range_leaf() {
    let mut mock = mock_dml();
    let mut ld1 = LeafData::default();
    ld1.items.insert(0, 0.0);
    ld1.items.insert(1, 1.0);
    ld1.items.insert(2, 2.0);
    ld1.items.insert(3, 3.0);
    ld1.items.insert(4, 4.0);
    let node1 = Arc::new(Node::<u32, u32, f32>::new(NodeData::Leaf(ld1)));
    mock.expect_get::<NodeT, NodeT>()
        .once()
        .returning(move |_| {
            let mut seq = Sequence::new();
            let mut fut = MockFuture::new();
            let node2 = node1.clone();
            fut.expect_poll()
                .once()
                .in_sequence(&mut seq)
                .returning(|cx| {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                });
            fut.expect_poll()
                .once()
                .in_sequence(&mut seq)
                .return_once(move |_| {
                    Poll::Ready(Ok(Box::new(node2)))});
            fut.boxed()
        });
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 0
  "#));
    let r = tree.range(1..3)
        .try_collect()
        .await;
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
    let mut mock = mock_dml();
    mock.expect_get::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .with(eq(addrl))
        .return_once(move |_| future::ok(Box::new(node)).boxed());
    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, u32> =
        Tree::from_str(dml.clone(), false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 102
  "#);

    let root_guard = tree.root.try_read().unwrap();
    let r = root_guard.elem.rlock(&dml).map_ok(|node| {
        let int_data = (*node).as_int();
        assert_eq!(int_data.nchildren(), 2);
        // Validate IntElems as well as possible using their public API
        assert_eq!(int_data.children[0].key, 0);
        assert!(!int_data.children[0].ptr.is_mem());
        assert_eq!(int_data.children[1].key, 256);
        assert!(!int_data.children[1].ptr.is_mem());
    }).now_or_never();
    assert!(r.is_some());
}

#[test]
fn read_leaf() {
    let mut mock = mock_dml();
    let mut ld = LeafData::default();
    ld.items.insert(0, 100);
    ld.items.insert(1, 200);
    ld.items.insert(99, 50_000);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_get::<Arc<Node<u32, u32, u32>>, Arc<Node<u32, u32, u32>>>()
        .once()
        .return_once(move |_| future::ok(Box::new(node)).boxed());
    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, u32> = Tree::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 0
  "#);

    let r = tree.get(1).now_or_never().unwrap();
    assert_eq!(Ok(Some(200)), r);
}

#[test]
fn remove() {
    let mut mock = MockDML::new();

    let mut ld = LeafData::default();
    ld.items.insert(3, 3.0);
    ld.items.insert(4, 4.0);
    ld.items.insert(5, 5.0);
    let leafnode = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl: u32 = 0;
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .with(eq(addrl), always())
        .return_once(move |_, _| future::ok(Box::new(leafnode)).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 32usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 10
          txgs:
            start: 41
            end: 42
          ptr: !Addr 1
  "#));
    let r2 = tree.clone().remove(4, TxgT::from(42), Credit::forge(48))
        .now_or_never()
        .unwrap();
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 32
              items:
                3: 3.0
                5: 5.0
        - key: 10
          txgs:
            start: 41
            end: 42
          ptr: !Addr 1
"#);
}

/// Remove an on-disk Value that requires dclone/ddrop
#[test]
fn remove_dclone() {
    let txg = TxgT::from(42);
    let mut mock = MockDML::new();
    mock.expect_pop::<DivBufShared, DivBuf>()
        .with(eq(4), eq(txg))
        .times(1)
        .returning(|_, _| future::ok(Box::new(DivBufShared::from(vec![])))
                   .boxed()
        );

    let mut ld = LeafData::default();
    ld.items.insert(3, NeedsDcloneV(3));
    ld.items.insert(4, NeedsDcloneV(4));
    ld.items.insert(5, NeedsDcloneV(5));
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_pop::<NeedsDcloneNode, NeedsDcloneNode>()
        .once()
        .with(eq(0), always())
        .return_once(move |_, _| future::ok(Box::new(node)).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 32usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, NeedsDcloneV>::from_str(
        dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 0
  "#));

    let r = tree.clone().remove(4, txg, Credit::forge(48))
        .now_or_never()
        .unwrap();
    assert_eq!(Ok(Some(NeedsDcloneV(4))), r);
    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 43
    ptr: !Mem
      Leaf:
        credit: 32
        items:
          3: 3
          5: 5
"#);
}

/// Allow merging the root down even when it's clean
#[test]
fn remove_and_merge_down() {
    let mut mock = MockDML::new();

    let mut ld = LeafData::default();
    ld.items.insert(3, 3.0);
    ld.items.insert(4, 4.0);
    ld.items.insert(5, 5.0);
    let leafnode = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl: u32 = 0;
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .with(eq(addrl), always())
        .return_once(move |_, _| future::ok(Box::new(leafnode)).boxed());
    // First repay is excess credit from the xlock
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 56usize)
        .returning(mem::forget);
    // Second repay is when dropping the tree
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 24usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
  "#));
    let r2 = tree.clone().remove(1, TxgT::from(42), Credit::forge(80))
        .now_or_never()
        .unwrap();
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Mem
      Leaf:
        credit: 48
        items:
          3: 3.0
          4: 4.0
          5: 5.0
"#);
}

#[test]
fn remove_and_steal_leaf_left() {
    let mut seq = Sequence::new();
    let mut mock = MockDML::new();//mock_dml();

    let mut ld2 = LeafData::default();
    ld2.items.insert(8, 8.0);
    ld2.items.insert(9, 9.0);
    let leafnode2 = Arc::new(Node::new(NodeData::Leaf(ld2)));
    let addr2: u32 = 2;
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .in_sequence(&mut seq)
        .with(eq(addr2), always())
        .return_once(move |_, _| future::ok(Box::new(leafnode2)).boxed());

    let mut ld1 = LeafData::default();
    ld1.items.insert(2, 2.0);
    ld1.items.insert(3, 3.0);
    ld1.items.insert(4, 4.0);
    ld1.items.insert(5, 5.0);
    ld1.items.insert(6, 6.0);
    let leafnode1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    let addr1: u32 = 1;
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .with(eq(addr1), always())
        .return_once(move |_, _| future::ok(Box::new(leafnode1)).boxed());
    // the first repay is excess credit from the remove operation
    // The second is when dropping the tree
    mock.expect_repay()
        .times(2)
        .withf(|credit| *credit == 32usize)
        .returning(mem::forget);

    // The third is also when dropping the tree
    mock.expect_repay()
        .times(1)
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 2
          txgs:
            start: 41
            end: 42
          ptr: !Addr 1
        - key: 8
          txgs:
            start: 41
            end: 42
          ptr: !Addr 2
  "#));
    let r2 = tree.clone().remove(8, TxgT::from(42), Credit::forge(80))
        .now_or_never().unwrap();
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 2
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 64
              items:
                2: 2.0
                3: 3.0
                4: 4.0
                5: 5.0
        - key: 6
          txgs:
            start: 42
            end: 43
          ptr: !Mem
            Leaf:
              credit: 32
              items:
                6: 6.0
                9: 9.0
"#);
}

/// The remove operation has insufficient credit to xlock the leaf node.  This
/// should work, but log a warning.
#[test]
fn remove_insufficient_credit() {
    let mut mock = MockDML::new();

    let mut ld = LeafData::default();
    ld.items.insert(3, 3.0);
    ld.items.insert(4, 4.0);
    ld.items.insert(5, 5.0);
    let leafnode = Arc::new(Node::new(NodeData::Leaf(ld)));
    let addrl: u32 = 0;
    mock.expect_pop::<NodeT, NodeT>()
        .once()
        .with(eq(addrl), always())
        .return_once(move |_, _| future::ok(Box::new(leafnode)).boxed());
    // The first repayment is for excess credit during remove
    mock.expect_repay()
        .times(1)
        .withf(|credit| *credit == -1isize)
        .returning(mem::forget);
    // The second is when dropping the tree
    mock.expect_repay()
        .times(1)
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 42
          ptr: !Addr 0
        - key: 10
          txgs:
            start: 41
            end: 42
          ptr: !Addr 1
  "#));
    tree.remove(4, TxgT::from(42), Credit::forge(15))
        .now_or_never()
        .unwrap()
        .unwrap();
}

// This test mimics what the IDML does with the alloc_t
#[test]
fn serialize_alloc_t() {
    let mock = DDML::default();
    let idml = Arc::new(mock);
    let typical_tree: Tree<DRP, DDML, PBA, RID> =
        Tree::from_str(idml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key:
      cluster: 0
      lba: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr
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
    let mock = IDML::default();
    let idml = Arc::new(mock);
    let typical_tree: Tree<RID, IDML, FSKey, FSValue> =
        Tree::from_str(idml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr
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
            _reserved: Default::default(),
            limits: Limits::new(2, 5, 2, 5),
            root: root_drp,
            txgs: TxgT(0)..TxgT(42),
        }
    );
    let mock = DDML::default();
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDML, u32, u32> = Tree::from_str(ddml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr
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
    let dml = Arc::new(MockDML::new());
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Addr 0
  "#));

    let r = tree.flush(TxgT::from(42)).now_or_never().unwrap();
    assert!(r.is_ok());
}

/// Sync a Tree with both dirty Int nodes and dirty Leaf nodes
#[test]
fn write_height2() {
    let mut seq = Sequence::new();
    let mut mock = MockDML::new();
    let addrl = 100;
    let addri = 101;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .once()
        .in_sequence(&mut seq)
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&0) == Some(100) &&
                    leaf_data.get(&1) == Some(200) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| future::ok(addrl).boxed());
    mock.expect_repay()
        .once()
        .in_sequence(&mut seq)
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .once()
        .in_sequence(&mut seq)
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].ptr == TreePtr::Addr(addrl) &&
            int_data.children[0].txgs == (TxgT::from(42) .. TxgT::from(43)) &&
            int_data.children[1].key == 256 &&
            int_data.children[1].ptr == TreePtr::Addr(256) &&
            int_data.children[1].txgs == (TxgT::from(41) .. TxgT::from(42)) &&
            *txg == TxgT::from(42)
        }).return_once(move |_, _, _| future::ok(addri).boxed());
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 30
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Leaf:
              credit: 32
              items:
                0: 100
                1: 200
        - key: 256
          txgs:
            start: 41
            end: 42
          ptr: !Addr 256
  "#));

    let r = tree.clone().flush(TxgT::from(42)).now_or_never().unwrap();
    assert!(r.is_ok());

    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 41
      end: 43
    ptr: !Addr 101
"#);
}

/// Sync a Tree with dirty nodes at all levels, with a height of 3.
#[test]
fn write_height3() {
    let mut mock = MockDML::new();
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&15) == Some(15.0) &&
                    leaf_data.get(&16) == Some(16.0) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| future::ok(7).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let nd = cacheable.0.try_read().unwrap();
            !nd.is_leaf() &&
            nd.as_int().children[0].key == 10 &&
            nd.as_int().children[0].ptr.is_addr() &&
            nd.as_int().children[0].txgs == (TxgT::from(9)..TxgT::from(10)) &&
            nd.as_int().children[1].key == 15 &&
            nd.as_int().children[1].ptr.is_addr() &&
            nd.as_int().children[1].txgs == (TxgT::from(42)..TxgT::from(43)) &&
            nd.as_int().children[2].key == 20 &&
            nd.as_int().children[2].ptr.is_addr() &&
            nd.as_int().children[2].txgs == (TxgT::from(5)..TxgT::from(7)) &&
            *txg == TxgT::from(42)
        }).return_once(move |_, _, _| future::ok(8).boxed());
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&50) == Some(50.0) &&
                    leaf_data.get(&51) == Some(51.0) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| future::ok(9).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let nd = cacheable.0.try_read().unwrap();
            !nd.is_leaf() &&
            nd.as_int().children[0].key == 40 &&
            nd.as_int().children[0].ptr.is_addr() &&
            nd.as_int().children[0].txgs == (TxgT::from(9)..TxgT::from(10)) &&
            nd.as_int().children[1].key == 50 &&
            nd.as_int().children[1].ptr.is_addr() &&
            nd.as_int().children[1].txgs == (TxgT::from(42)..TxgT::from(43)) &&
            nd.as_int().children[2].key == 60 &&
            nd.as_int().children[2].ptr.is_addr() &&
            nd.as_int().children[2].txgs == (TxgT::from(7)..TxgT::from(8)) &&
            *txg == TxgT::from(42)
        }).return_once(move |_, _, _| future::ok(10).boxed());
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let nd = cacheable.0.try_read().unwrap();
            !nd.is_leaf() &&
            nd.as_int().children[0].key == 0 &&
            nd.as_int().children[0].ptr.is_addr() &&
            nd.as_int().children[0].txgs == (TxgT::from(5)..TxgT::from(43)) &&
            nd.as_int().children[1].key == 30 &&
            nd.as_int().children[1].ptr.is_addr() &&
            nd.as_int().children[1].txgs == (TxgT::from(20)..TxgT::from(32)) &&
            nd.as_int().children[2].key == 40 &&
            nd.as_int().children[2].ptr.is_addr() &&
            nd.as_int().children[2].txgs == (TxgT::from(7)..TxgT::from(43)) &&
            *txg == TxgT::from(42)
        }).return_once(move |_, _, _| future::ok(11).boxed());
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 0
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 5
            end: 42
          ptr: !Mem
            Int:
              children:
              - key: 10
                txgs:
                  start: 9
                  end: 10
                ptr: !Addr 2
              - key: 15
                txgs:
                  start: 9
                  end: 42
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      15: 15.0
                      16: 16.0
              - key: 20
                txgs:
                  start: 5
                  end: 7
                ptr: !Addr 3
        - key: 30
          txgs:
            start: 20
            end: 32
          ptr: !Addr 4
        - key: 40
          txgs:
            start: 7
            end: 42
          ptr: !Mem
            Int:
              children:
              - key: 40
                txgs:
                  start: 9
                  end: 10
                ptr: !Addr 5
              - key: 50
                txgs:
                  start: 11
                  end: 42
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      50: 50.0
                      51: 51.0
              - key: 60
                txgs:
                  start: 7
                  end: 8
                ptr: !Addr 6
  "#));

    let r = tree.clone().flush(TxgT::from(42)).now_or_never().unwrap();
    assert!(r.is_ok());
    assert_eq!(format!("{tree}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 5
      end: 43
    ptr: !Addr 11
"#);
}

#[test]
fn write_int() {
    let mut mock = MockDML::new();
    let addr = 9999;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            !int_data.children[0].ptr.is_mem() &&
            int_data.children[1].key == 256 &&
            !int_data.children[1].ptr.is_mem() &&
            *txg == TxgT::from(42)
        }).returning(move |_, _, _| future::ok(addr).boxed());
    let dml = Arc::new(mock);
    let mut tree = Arc::new(
        Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 2
  elem:
    key: 0
    txgs:
      start: 5
      end: 25
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 5
            end: 15
          ptr: !Addr 0
        - key: 256
          txgs:
            start: 18
            end: 25
          ptr: !Addr 256
  "#));

    let r = tree.clone().flush(TxgT::from(42)).now_or_never().unwrap();
    assert!(r.is_ok());
    assert!(r.is_ok());
    let tref = Arc::get_mut(&mut tree).unwrap();
    let root = tref.root.get_mut().unwrap();
    assert_eq!(*root.elem.ptr.as_addr(), addr);
    assert_eq!(root.elem.txgs.start, TxgT::from(5));
    assert_eq!(root.elem.txgs.end, TxgT::from(43));
}

#[test]
fn write_leaf() {
    let mut mock = MockDML::new();
    let addr = 9999;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .once()
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200) &&
            *txg == TxgT::from(42)
        }).returning(move |_, _, _| future::ok(addr).boxed());
    mock.expect_repay()
        .once()
        .withf(|credit| *credit == 16usize)
        .returning(mem::forget);
    let dml = Arc::new(mock);
    let mut tree = Arc::new(
        Tree::<u32, MockDML, u32, u32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0
    txgs:
      start: 0
      end: 1
    ptr: !Mem
      Leaf:
        credit: 32
        items:
          0: 100
          1: 200
  "#));

    let r = tree.clone().flush(TxgT::from(42)).now_or_never().unwrap();
    assert!(r.is_ok());
    let tref = Arc::get_mut(&mut tree).unwrap();
    let root = tref.root.get_mut().unwrap();
    assert_eq!(*root.elem.ptr.as_addr(), addr);
    assert_eq!(root.elem.txgs.start, TxgT::from(42));
    assert_eq!(root.elem.txgs.end, TxgT::from(43));
}

/// While flushing a Tree, another task dirties a previously-flushed node.
///
/// This is ok!  `Tree::flush_once` should proceed, and _not_ re-flush the
/// dirtied node.
#[test]
fn write_race() {
    let mut seq = Sequence::new();
    let otree: Arc<RwLock<Option<Arc<Tree<u32, MockDML, u32, f32>>>>> =
        Arc::new(RwLock::new(None));
    let otree2 = otree.clone();
    let mut mock = mock_dml();
    let addr0 = 69;
    let addr10 = 70;
    let addr20 = 71;
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .in_sequence(&mut seq)
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&0) == Some(0.0) &&
                    leaf_data.get(&1) == Some(1.0) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| future::ok(addr0).boxed());
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .in_sequence(&mut seq)
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&10) == Some(10.0) &&
                    leaf_data.get(&11) == Some(11.0) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| {
            // Now dirty the leaf node just flushed
            otree2.read()
                .unwrap()
                .as_ref()
                .unwrap()
                .clone()
                .insert(2, 2.0, TxgT::from(42), Credit::forge(80))
                .now_or_never()
                .unwrap()
                .unwrap();
            future::ok(addr10).boxed()
        });
    let mut ld0 = LeafData::default();
    ld0.items.insert(0, 0.0);
    ld0.items.insert(1, 1.0);
    let leafnode0 = Arc::new(Node::new(NodeData::Leaf(ld0)));
    expect_pop(&mut mock, addr0, leafnode0);
    mock.expect_put::<Arc<Node<u32, u32, f32>>>()
        .once()
        .in_sequence(&mut seq)
        .withf(move |cacheable, _compression, txg| {
            let node_data = cacheable.0.try_read().unwrap();
            match node_data.deref() {
                NodeData::Leaf(leaf_data) => {
                    leaf_data.get(&20) == Some(20.0) &&
                    leaf_data.get(&21) == Some(21.0) &&
                    *txg == TxgT::from(42)
                },
                _ => false
            }
        }).return_once(move |_, _, _| future::ok(addr20).boxed());

    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 30
      end: 42
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 0
                txgs:
                  start: 42
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      0: 0.0
                      1: 1.0
              - key: 5
                txgs:
                  start: 42
                  end: 43
                ptr: !Addr 0
        - key: 10
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 10
                txgs:
                  start: 42
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      10: 10.0
                      11: 11.0
              - key: 15
                txgs:
                  start: 41
                  end: 42
                ptr: !Addr 1
        - key: 20
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 20
                txgs:
                  start: 41
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 32
                    items:
                      20: 20.0
                      21: 21.0
              - key: 25
                txgs:
                  start: 40
                  end: 41
                ptr: !Addr 2
  "#));
    *otree.write().unwrap() = Some(tree);
    let guard = otree.read().unwrap();
    let tref = guard.as_ref().unwrap();
    let r = tref.clone().flush_once(TxgT::from(42))
        .now_or_never().unwrap();
    assert_eq!(r, Ok(true));

    assert_eq!(format!("{tref}"),
r#"limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  height: 3
  elem:
    key: 0
    txgs:
      start: 30
      end: 43
    ptr: !Mem
      Int:
        children:
        - key: 0
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 0
                txgs:
                  start: 42
                  end: 43
                ptr: !Mem
                  Leaf:
                    credit: 48
                    items:
                      0: 0.0
                      1: 1.0
                      2: 2.0
              - key: 5
                txgs:
                  start: 42
                  end: 43
                ptr: !Addr 0
        - key: 10
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 10
                txgs:
                  start: 42
                  end: 43
                ptr: !Addr 70
              - key: 15
                txgs:
                  start: 41
                  end: 42
                ptr: !Addr 1
        - key: 20
          txgs:
            start: 41
            end: 43
          ptr: !Mem
            Int:
              children:
              - key: 20
                txgs:
                  start: 42
                  end: 43
                ptr: !Addr 71
              - key: 25
                txgs:
                  start: 40
                  end: 41
                ptr: !Addr 2
"#);
}

// LCOV_EXCL_STOP
