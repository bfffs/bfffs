//! Tests regarding transaction transaction membership of nodes
// LCOV_EXCL_START

use crate::dml::MockDML;
use futures::future;
use mockall::predicate::eq;
use pretty_assertions::assert_eq;
use super::*;

#[test]
fn check_bad_root_txgs() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_bad_int_txgs() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_bad_key() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_ok() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_empty() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(
        Tree::<u32, MockDML, u32, f32>::create(dml, false, 1.0, 1.0)
    );

    assert!(tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_leaf_underflow() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

// The root Node is always allowed to underflow
#[test]
fn check_root_int_underflow() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

// The root node is allowed to underflow if it's a leaf
#[test]
fn check_root_leaf_ok() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

#[test]
fn check_root_leaf_overflow() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

// The tree is unsorted overall, even though each Node is correctly sorted
#[test]
fn check_unsorted() {
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));

    assert!(!tree.check()
            .now_or_never().unwrap()
            .unwrap());
}

/// Remove a key that merges two int nodes
#[test]
fn merge() {
    let mut mock = MockDML::new();
    let mut ld1 = LeafData::default();
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let addrl1 = 2;
    let ln1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .once()
        .with(eq(addrl1), eq(TxgT::from(42)))
        .return_once(move |_, _| Box::pin(future::ok(Box::new(ln1))));
    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, f32> = Tree::from_str(dml, false, r#"
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
    let r2 = tree.remove(4, TxgT::from(42))
        .now_or_never().unwrap();
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
    let mut mock = MockDML::new();
    let mut ld = LeafData::default();
    ld.insert(12, 12.0);
    ld.insert(13, 13.0);
    ld.insert(14, 14.0);
    let addrl = 1280;
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_pop::<Arc<Node<u32, u32, f32>>, Arc<Node<u32, u32, f32>>>()
        .once()
        .with(eq(addrl), eq(TxgT::from(42)))
        .return_once(move |_, _| Box::pin(future::ok(Box::new(node))));
    let dml = Arc::new(mock);
    let tree = Arc::new(Tree::<u32, MockDML, u32, f32>::from_str(dml, false, r#"
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
"#));
    let r2 = tree.clone().insert(15, 15.0, TxgT::from(42))
        .now_or_never().unwrap();
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
    let mock = MockDML::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, MockDML, u32, f32> = Tree::from_str(dml, false, r#"
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
    let r2 = tree.remove(26, TxgT::from(42))
        .now_or_never().unwrap();
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
