//! Tests regarding transaction transaction membership of nodes
// LCOV_EXCL_START

use crate::common::dml_mock::*;
use futures::future;
use pretty_assertions::assert_eq;
use simulacrum::*;
use super::*;
use tokio::runtime::current_thread;

#[test]
fn check_bad_root_txgs() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
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
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 1.0
                    1: 2.0
          - key: 256
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    256: 256.0
                    257: 257.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_bad_int_txgs() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 20
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 30
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 30
                        end: 31
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 1.0
                              1: 2.0
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 3.0
                              3: 4.0
          - key: 9
            txgs:
              start: 21
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_bad_key() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
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
          - key: 11
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
          - key: 13
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    13: 13.0
                    14: 14.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_ok() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 20
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 30
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 30
                        end: 31
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
                              4: 4.0
                              5: 5.0
                              6: 6.0
          - key: 9
            txgs:
              start: 20
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_empty() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::create(dml, false, 1.0, 1.0);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_leaf_underflow() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
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
          - key: 9
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
          - key: 13
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    13: 13.0
                    14: 14.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

// The root Node is always allowed to underflow
#[test]
fn check_root_int_underflow() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 3
  max_int_fanout: 7
  min_leaf_fanout: 3
  max_leaf_fanout: 7
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
          - key: 9
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 19
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0
                    23: 23.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
}

// The root node is allowed to underflow if it's a leaf
#[test]
fn check_root_leaf_ok() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
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
          0: 0.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_root_leaf_overflow() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 1
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
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
          0: 0.0
          1: 1.0
          2: 2.0
          3: 3.0
          4: 4.0
          5: 5.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

// The tree is unsorted overall, even though each Node is correctly sorted
#[test]
fn check_unsorted() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 1
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    10: 10.0
          - key: 5
            txgs:
              start: 0
              end: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    11: 11.0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

/// Recompute start TXGs on Tree flush
#[test]
fn flush() {
    let mut mock = DMLMock::new();
    let addr = 9999;
    mock.expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, TxgT)|{
            let node_data = (args.0).0.try_read().unwrap();
            node_data.is_leaf() || args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(addr).into_future()));
    mock.then().expect_put::<Arc<Node<u32, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<u32, u32, u32>>, _, TxgT)|{
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].txgs == (TxgT::from(42)..TxgT::from(43)) &&
            int_data.children[1].key == 256 &&
            int_data.children[1].txgs == (TxgT::from(41)..TxgT::from(42)) &&
            args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(addr).into_future()));
    let dml = Arc::new(mock);
    let mut tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 30
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 40
              end: 43
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
    let root_elem = Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap();
    assert_eq!(root_elem.txgs, TxgT::from(41)..TxgT::from(43));
}

/// Remove a key that merges two int nodes
#[test]
fn merge() {
    let mut mock = DMLMock::new();
    let mut ld1 = LeafData::default();
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let addrl1 = 2;
    let ln1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    let holder1 = RefCell::new(Some(ln1));
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)|
                     unsafe {*args.0 == addrl1} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(holder1.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, f32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 20
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 30
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 30
                        end: 31
                      ptr:
                        Addr: 1
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Addr: 2
          - key: 9
            txgs:
              start: 20
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 3
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr: 4
                    - key: 15
                      txgs:
                        start: 24
                        end: 25
                      ptr:
                        Addr: 5
          - key: 18
            txgs:
              start: 15
              end: 16
            ptr:
              Addr: 6"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 20
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 20
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 30
                        end: 31
                      ptr:
                        Addr: 1
                    - key: 2
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 3
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr: 4
                    - key: 15
                      txgs:
                        start: 24
                        end: 25
                      ptr:
                        Addr: 5
          - key: 18
            txgs:
              start: 15
              end: 16
            ptr:
              Addr: 6"#);
}

/// Insert a key that splits the root IntNode
#[test]
fn split() {
    let mut mock = DMLMock::new();
    let mut ld = LeafData::default();
    ld.insert(12, 12.0);
    ld.insert(13, 13.0);
    ld.insert(14, 14.0);
    let addrl = 1280;
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    let node_holder = RefCell::new(Some(node));
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const u32, TxgT)|
                     unsafe {*args.0 == addrl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32, f32>>>, Error>(res))
        });
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, false, r#"
---
height: 2
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 3
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 4
              end: 10
            ptr:
              Addr: 256
          - key: 3
            txgs:
              start: 5
              end: 11
            ptr:
              Addr: 512
          - key: 6
            txgs:
              start: 3
              end: 12
            ptr:
              Addr: 768
          - key: 9
            txgs:
              start: 6
              end: 22
            ptr:
              Addr: 1024
          - key: 12
            txgs:
              start: 7
              end: 34
            ptr:
              Addr: 1280
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(15, 15.0, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 3
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 3
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 4
                        end: 10
                      ptr:
                        Addr: 256
                    - key: 3
                      txgs:
                        start: 5
                        end: 11
                      ptr:
                        Addr: 512
                    - key: 6
                      txgs:
                        start: 3
                        end: 12
                      ptr:
                        Addr: 768
          - key: 9
            txgs:
              start: 6
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 6
                        end: 22
                      ptr:
                        Addr: 1024
                    - key: 12
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                              15: 15.0"#);
}

/// Recompute TXG ranges after stealing keys
#[test]
fn steal() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, false, r#"
---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 10
    end: 42
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 10
              end: 11
            ptr:
              Addr: 0
          - key: 9
            txgs:
              start: 21
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 9
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 12
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 15
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 17
                    - key: 19
                      txgs:
                        start: 21
                        end: 22
                      ptr:
                        Addr: 19
          - key: 21
            txgs:
              start: 39
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 21
                      txgs:
                        start: 39
                        end: 40
                      ptr:
                        Addr: 21
                    - key: 24
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              25: 25.0
                              26: 26.0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(26, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
limits:
  min_int_fanout: 2
  max_int_fanout: 5
  min_leaf_fanout: 2
  max_leaf_fanout: 5
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 10
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 10
              end: 11
            ptr:
              Addr: 0
          - key: 9
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 9
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 12
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 15
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 17
          - key: 19
            txgs:
              start: 21
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 19
                      txgs:
                        start: 21
                        end: 22
                      ptr:
                        Addr: 19
                    - key: 21
                      txgs:
                        start: 39
                        end: 40
                      ptr:
                        Addr: 21
                    - key: 24
                      txgs:
                        start: 41
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              25: 25.0"#);
}

// LCOV_EXCL_STOP
