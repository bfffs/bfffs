//! Tests regarding in-memory manipulation of Trees
// LCOV_EXCL_START

use common::tree::*;
use common::dml_mock::*;
use futures::future;
use tokio::runtime::current_thread;

#[test]
fn insert() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0.0, TxgT::from(42)));
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
          0: 0.0"#);
}

// When inserting into an int node's first child and the key is lower than the
// int node's own key, the int node's own key must be lowered to match.
#[test]
fn insert_lower_than_parents_key() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
          - key: 67
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    67: 67.0
                    68: 68.0
                    69: 69.0
          - key: 70
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    70: 70.0
                    71: 71.0
                    72: 72.0
                    73: 73.0
                    74: 74.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(36, 36.0, TxgT::from(42)));
    assert!(r2.is_ok());
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
          - key: 36
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    36: 36.0
                    67: 67.0
                    68: 68.0
                    69: 69.0
          - key: 70
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    70: 70.0
                    71: 71.0
                    72: 72.0
                    73: 73.0
                    74: 74.0"#);
}

#[test]
fn insert_dup() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 100.0, TxgT::from(42)));
    assert_eq!(r, Ok(Some(0.0)));
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
          0: 100.0"#);
}

/// Insert a key that splits a non-root interior node
#[test]
fn insert_split_int() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
---
height: 3
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
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
          - key: 9
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
                              9: 9.0
                              10: 10.0
                              11: 11.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                              20: 20.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(24, 24.0, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                              11: 11.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
          - key: 18
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                              20: 20.0
                    - key: 21
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0
                              24: 24.0"#);
}

/// Insert a key that splits a non-root leaf node
#[test]
fn insert_split_leaf() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(8, 8.0, TxgT::from(42)));
    assert!(r2.is_ok());
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
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
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
                    5: 5.0
          - key: 6
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0"#);
}

/// Insert a key that splits the root IntNode
#[test]
fn insert_split_root_int() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0
          - key: 6
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0
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
                    11: 11.0
          - key: 12
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    12: 12.0
                    13: 13.0
                    14: 14.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(15, 15.0, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
              start: 41
              end: 43
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
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                              11: 11.0
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

/// Insert a key that splits the root leaf node
#[test]
fn insert_split_root_leaf() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
          3: 3.0
          4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(5, 5.0, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
                    0: 0.0
                    1: 1.0
                    2: 2.0
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
                    5: 5.0"#);
}

#[test]
fn get() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        tree.insert(0, 0.0, TxgT::from(42))
            .and_then(|_| tree.get(0))
    }));
    assert_eq!(r, Ok(Some(0.0)));
}

#[test]
fn get_deep() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(3));
    assert_eq!(r, Ok(Some(3.0)))
}

#[test]
fn get_nonexistent() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(0));
    assert_eq!(r, Ok(None))
}

#[test]
fn last_key_empty() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.last_key());
    assert_eq!(r, Ok(None))
}

#[test]
fn last_key() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.last_key());
    assert_eq!(r, Ok(Some(4)))
}

// The range delete example from Figures 13-14 of B-Trees, Shadowing, and
// Range-operations.  Our result is slightly different than in the paper,
// however, because in the second pass:
// a) We fix nodes [10, -] and [-, 32] by merging them with their outer
//    neighbors rather than themselves (this is legal).
// b) When removing key 31 from [31, 32], we don't adjust the key in its parent
//    IntElem.  This is legal because it still obeys the minimum-key-rule.  It's
//    not generally possible to fix a parent's IntElem's key when removing a key
//    from a child, because it may require holding the locks on more than 2
//    nodes.
#[test]
fn range_delete() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 5
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              5: 5.0
                              6: 6.0
                              7: 7.0
                    - key: 10
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
          - key: 15
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 20
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              25: 25.0
          - key: 30
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 31
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              31: 31.0
                              32: 32.0
                    - key: 37
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 37.0
                              40: 40.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(11..=31, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 5
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    10: 10.0
          - key: 31
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    32: 32.0
                    37: 37.0
                    40: 40.0"#);
}

// After removing keys, one node is in danger because it has too many children
// in the cut
#[test]
fn range_delete_danger() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 5
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              5: 5.0
                              6: 6.0
                              7: 7.0
                              8: 8.0
                              9: 9.0
                    - key: 10
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Addr: 110
          - key: 15
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 20
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              25: 25.0
          - key: 30
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 31
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              31: 31.0
                              32: 32.0
                    - key: 37
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 37.0
                              40: 40.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5..6, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 5
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
                              9: 9.0
                    - key: 10
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Addr: 110
          - key: 15
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 20
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              25: 25.0
          - key: 30
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 31
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              31: 31.0
                              32: 32.0
                    - key: 37
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 37.0
                              40: 40.0"#);
}

// Delete a range that's exclusive on the left and right
#[test]
fn range_delete_exc_exc() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 10
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
          - key: 20
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete((Bound::Excluded(4), Bound::Excluded(10)),
                          TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}

// Delete a range that's exclusive on the left and inclusive on the right
#[test]
fn range_delete_exc_inc() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 10
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
          - key: 20
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete((Bound::Excluded(4), Bound::Included(10)),
                          TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}

/// Do a range delete that causes two Nodes to merge and yet still underflow.
/// Regression test for bug c2bd706
#[test]
fn range_delete_merge_and_underflow() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 2
min_fanout: 3
max_fanout: 7
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
                    3: 3.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
          - key: 7
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    7: 7.0
                    8: 8.0
                    9: 9.0
          - key: 10
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
                    15: 15.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(2..6, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert!(rt.block_on(tree.check()).unwrap());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 3
max_fanout: 7
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
          - key: 1
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
                    9: 9.0
          - key: 10
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
                    15: 15.0"#);
}

/// Do a range delete that causes Nodes to merge during pass2, causing an
/// underflow in the parent.
#[test]
fn range_delete_merge_and_parent_underflow() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 3
max_fanout: 7
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                              3: 3.0
                    - key: 4
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                    - key: 7
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7: 7.0
                              8: 8.0
                              9: 9.0
                    - key: 10
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10010
          - key: 20
            txgs:
              start: 0
              end: 1
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
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
                    - key: 23
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10023
                    - key: 26
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10026
          - key: 30
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 300
          - key: 40
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 10040
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(2..6, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 3
max_fanout: 7
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
          - key: 1
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              6: 6.0
                              7: 7.0
                              8: 8.0
                              9: 9.0
                    - key: 10
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10010
                    - key: 20
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
                    - key: 23
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10023
                    - key: 26
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 10026
          - key: 30
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 300
          - key: 40
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 10040"#);
}

// Regression test for bug 9bdac7a, where Tree::range_delete caused the
// allocation table to become unsorted.  During pass2, one node must be merged
// while descending and again while ascending.
#[test]
fn range_delete_merge_descending_and_ascending() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 3
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
          - key: 452
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 452
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              462: 458
                              494: 490
                    - key: 1021
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1025: 835
                              1027: 837
          - key: 1245
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1309
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1313: 1123
                              1316: 1126
                    - key: 1341
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1368: 1178
                              1662: 518
          - key: 1663
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1663
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1663: 523
                              1666: 547
                    - key: 1687
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1687: 776
                              1690: 779
          - key: 1727
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1727
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1727: 828
                              1730: 832
                    - key: 1759
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1759: 861
                              1762: 864
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
    let r = rt.block_on(
        tree.range_delete(1024..1536, TxgT::from(42))
    );
    assert!(r.is_ok());
    println!("{}", &tree);
    assert!(rt.block_on(tree.check()).unwrap());
}

/// Regression test for bug e6fd45d.  After range_delete_pass1, Node 2,7148 has
/// two children that both underflow.  During range_delete_pass2, the right
/// child (key 7268) got merged during descent.  Then the left child (key 7148)
/// got merged during ascent.  Then the code hit a "subtract with overflow"
/// error when trying to merge the right child again.
#[test]
fn range_delete_merge_right_child_first() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 4
max_fanout: 9
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 4
    end: 15
  ptr:
    Mem:
      Int:
        children:
          - key: 7148
            txgs:
              start: 13
              end: 15
            ptr:
              Mem:
                Int:
                  children:
                    - key: 7148
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7153: 6699
                              7156: 6702
                              7164: 6710
                              7173: 6719
                    - key: 7252
                      txgs:
                        start: 13
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7252: 6798
                              7253: 6799
                              7254: 6800
                              7255: 6801
                    - key: 7260
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7260: 6806
                              7265: 6811
                              7266: 6812
                              7267: 6813
                    - key: 7268
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7281: 6827
                              7282: 6828
                              7287: 6833
                              7735: 5525
          - key: 7736
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Int:
                  children:
                    - key: 7736
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7736: 5527
                              7737: 5538
                              7738: 5540
                              7739: 5543
                    - key: 7744
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007744
                    - key: 7752
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007752
                    - key: 7760
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007760
          - key: 7840
            txgs:
              start: 14
              end: 15
            ptr:
              Addr: 7693
          - key: 8000
            txgs:
              start: 14
              end: 15
            ptr:
              Addr: 7692
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on(
        tree.range_delete(7168..7680, TxgT::from(42))
    ).unwrap();
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 4
max_fanout: 9
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 4
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 7148
            txgs:
              start: 14
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 7148
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7153: 6699
                              7156: 6702
                              7164: 6710
                              7735: 5525
                    - key: 7736
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7736: 5527
                              7737: 5538
                              7738: 5540
                              7739: 5543
                    - key: 7744
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007744
                    - key: 7752
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007752
                    - key: 7760
                      txgs:
                        start: 14
                        end: 15
                      ptr:
                        Addr: 1007760
          - key: 7840
            txgs:
              start: 14
              end: 15
            ptr:
              Addr: 7693
          - key: 8000
            txgs:
              start: 14
              end: 15
            ptr:
              Addr: 7692"#);
}

/// Delete a range that causes the root node to be merged down
#[test]
fn range_delete_merge_root() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
                    3: 3.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(0..4, TxgT::from(42))
    );
    assert!(r.is_ok());
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
          4: 4.0
          5: 5.0
          6: 6.0
          7: 7.0
          8: 8.0"#);
}


/// Delete a range that causes the root node to be merged two steps down
#[test]
fn range_delete_merge_root_twice() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                              3: 3.0
                    - key: 4
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                              7: 7.0
                              8: 8.0
          - key: 10
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10
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
                              15: 15.0
                              16: 16.0
                              17: 17.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(0..13, TxgT::from(42))
    );
    assert!(r.is_ok());
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
          13: 13.0
          14: 14.0
          15: 15.0
          16: 16.0
          17: 17.0"#);
}

// After pass1, there are two sibling nodes that can't be fixed so that each of
// them satisfies the min_fanout + 2 rule.
// Regression test for bug b959707
#[test]
fn range_delete_cant_fix_for_minfanout_plus_two() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4160
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004160
                    - key: 4194
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4193: 4193.0
                              4195: 4195.0
          - key: 4599
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4600
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4601: 4601.0
                              4608: 4608.0
                    - key: 4609
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4610: 4610.0
                              4612: 4612.0
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617
                    - key: 4627
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004627
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(4480..4600, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
              start: 41
              end: 42
            ptr:
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4160
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004160
                    - key: 4194
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4193: 4193.0
                              4195: 4195.0
          - key: 4599
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4600
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4601: 4601.0
                              4608: 4608.0
                    - key: 4609
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4610: 4610.0
                              4612: 4612.0
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617
                    - key: 4627
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004627"#);
}

// Delete a range that's contained within a single LeafNode
#[test]
fn range_delete_single_node() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 10
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5..7, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    7: 7.0
                    8: 8.0
          - key: 10
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0"#);
}

/// At 769:376dcac1d512, this test will cause a node to underflow during pass1,
/// be stolen left during pass2, and never get fixed.
/// Regression test for 4a99d7f
#[test]
fn range_delete_underflow_and_steal_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4193
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4193: 4193.0
                              4195: 4195.0
                    - key: 4457
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4565: 4565.0
                              4567: 4567.0
          - key: 4599
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4600
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4601: 4601.0
                              4605: 4605.0
                              4606: 4606.0
                              4607: 4607.0
                              4608: 4608.0
                    - key: 4609
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4610: 4610.0
                              4612: 4612.0
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617
                    - key: 4627
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004627
                    - key: 4637
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004637
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(4480..4605, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
              start: 41
              end: 42
            ptr:
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4193
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4193: 4193.0
                              4195: 4195.0
                    - key: 4600
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4605: 4605.0
                              4606: 4606.0
                              4607: 4607.0
                              4608: 4608.0
                    - key: 4609
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4610: 4610.0
                              4612: 4612.0
          - key: 4617
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617
                    - key: 4627
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004627
                    - key: 4637
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004637"#);
}

// Delete a range that includes a whole Node at the end of the Tree
#[test]
fn range_delete_to_end() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
          - key: 10
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(7..20, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0"#);
}

// Delete a range that passes the right side of an int node, with another
// int node to its right
#[test]
fn range_delete_to_end_of_int_node() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 4
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                    - key: 7
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7: 7.0
                              8: 8.0
                    - key: 10
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
          - key: 15
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
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
                    - key: 24
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0
          - key: 30
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 30
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              30: 30.0
                              31: 31.0
                    - key: 40
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              40: 40.0
                              41: 41.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(10..16, TxgT::from(42))
    );
    assert!(r.is_ok());
    // Ideally the leaf node with key 7 wouldn't have its end txg changed.
    // However, range_delete is a 2-pass operation, and by the time that the 1st
    // pass is done, the 2nd pass can't tell that that node wasn't modified.
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
          - key: 1
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 4
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                    - key: 7
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7: 7.0
                              8: 8.0
          - key: 15
            txgs:
              start: 41
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
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 24
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0
          - key: 30
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 30
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              30: 30.0
                              31: 31.0
                    - key: 40
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              40: 40.0
                              41: 41.0"#);
}

// Delete a range that includes only whole nodes
#[test]
fn range_delete_whole_nodes() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
                    3: 3.0
          - key: 4
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
          - key: 10
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
          - key: 20
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(4..20, TxgT::from(42))
    );
    assert!(r.is_ok());
    // Ideally the first leaf node wouldn't have its end txg changed.  However,
    // range_delete is a 2-pass operation, and by the time that the 1st pass is
    // done, the 2nd pass can't tell that that node wasn't modified.
    assert_eq!(format!("{}", &tree),
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
          - key: 1
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
                    3: 3.0
          - key: 20
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}

// Delete a range of just a single whole node whose parent key isn't normalized
#[test]
fn range_delete_whole_node_denormalized() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4193
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004193
                    - key: 4457
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004457
          - key: 4599
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4600
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004600
                    - key: 4609
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4610: 4610.0
                              4612: 4612.0
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(4610..4613, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
              start: 41
              end: 42
            ptr:
              Addr: 1003623
          - key: 3889
            txgs:
              start: 41
              end: 42
            ptr:
              Addr: 1003889
          - key: 4145
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4193
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004193
                    - key: 4457
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004457
          - key: 4599
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4600
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004600
                    - key: 4617
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004617"#);
}

// Range delete pass2 steals a leaf node to the left
#[test]
fn range_delete_pass2_steal_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 11
                    - key: 3
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
          - key: 20
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 126
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
                        Addr: 33
                    - key: 40
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 34
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(3..21, TxgT::from(2))
    );
    assert!(r.is_ok());
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
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 11
                    - key: 20
                      txgs:
                        start: 2
                        end: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 126
          - key: 29
            txgs:
              start: 0
              end: 3
            ptr:
              Mem:
                Int:
                  children:
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
                        Addr: 33
                    - key: 40
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 34"#);
}

// range_delete of a small range at the end of the tree
#[test]
fn range_delete_at_end() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on( {
        let insert_futs = (0..23).map(|k| {
            tree.insert(k, k as f32, TxgT::from(2))
        }).collect::<Vec<_>>();
        future::join_all(insert_futs)
    }).unwrap();
    let r = rt.block_on({
        tree.range_delete(22..23, TxgT::from(2))
    });
    assert!(r.is_ok());
}

// range_delete with a RangeFrom (x..) argument
#[test]
fn range_delete_range_from() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 10010
                    - key: 3
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
          - key: 20
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              26: 26.0
                              27: 27.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5.., TxgT::from(2))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
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
          - key: 1
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 10010
          - key: 3
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0"#);
}

// range_delete with a RangeFull (..) argument
#[test]
fn range_delete_range_full() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
          - key: 20
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              26: 26.0
                              27: 27.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(.., TxgT::from(2))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 1
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
      Leaf:
        items: {}"#);
}

// range_delete with a RangeTo (..x) argument
#[test]
fn range_delete_range_to() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
          - key: 20
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Addr: 10026
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(..21, TxgT::from(2))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
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
          - key: 20
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    21: 21.0
                    22: 22.0
          - key: 26
            txgs:
              start: 0
              end: 1
            ptr:
              Addr: 10026"#);
}

// range_delete with a range that includes a whole IntNode at the end of the
// tree
#[test]
fn range_delete_to_end_deep() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    rt.block_on( {
        let insert_futs = (0..23).map(|k| {
            tree.insert(k, k as f32, TxgT::from(2))
        }).collect::<Vec<_>>();
        future::join_all(insert_futs)
    }).unwrap();
    let r = rt.block_on({
        tree.range_delete(5..24, TxgT::from(2))
    });
    assert!(r.is_ok());
    // NB: in this case, it would be acceptable for the final Tree to consist of
    // either a single IntNode with two Leaves as shown below, or simply as a
    // single Leaf.
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 3
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            txgs:
              start: 2
              end: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0"#);

}

#[test]
fn range_delete_underflow_in_parent_and_child() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 3
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
          - key: 20
            txgs:
              start: 0
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 26
                      txgs:
                        start: 0
                        end: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              26: 26.0
                              27: 27.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(2..30, TxgT::from(2))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 3
  ptr:
    Mem:
      Leaf:
        items:
          1: 1.0"#);
}

// Empty tree
#[test]
fn range_empty_tree() {
    let dml = Arc::new(DMLMock::new());
    let tree = Tree::<u32, DMLMock, u32, f32>::create(dml);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(..).collect()
    );
    assert_eq!(r, Ok(Vec::new()));
}

// Unbounded range lookup
#[test]
fn range_full() {
    let dml = Arc::new(DMLMock::new());
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(..).collect()
    );
    assert_eq!(r, Ok(vec![(0, 0.0), (1, 1.0), (3, 3.0), (4, 4.0)]));
}

#[test]
fn range_exclusive_start() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    // A query that starts on a leaf
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range((Bound::Excluded(0), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));

    // A query that starts between leaves
    let r = rt.block_on(
        tree.range((Bound::Excluded(1), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_leaf() {
    let dml = Arc::new(DMLMock::new());
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
          3: 3.0
          4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..3)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (2, 2.0)]));
}

#[test]
fn range_leaf_inclusive_end() {
    let dml = Arc::new(DMLMock::new());
    let tree = Tree::<u32, DMLMock, u32, f32>::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
          3: 3.0
          4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(3..=4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0), (4, 4.0)]));
}

#[test]
fn range_nonexistent_between_two_leaves() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![]));
}

#[test]
fn range_two_ints() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
              start: 0
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      txgs:
                        start: 0
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
          - key: 9
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      txgs:
                        start: 0
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..10)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (9, 9.0)]));
}

#[test]
fn range_ends_between_two_leaves() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 4
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(0..3)
            .collect()
    );
    assert_eq!(r, Ok(vec![(0, 0.0), (1, 1.0)]));
}

#[test]
fn range_ends_before_node_but_after_parent_pointer() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 4
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(0..5)
            .collect()
    );
    assert_eq!(r, Ok(vec![(0, 0.0), (1, 1.0)]));
}

#[test]
fn range_starts_between_two_leaves() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(2..6)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0), (4, 4.0), (5, 5.0)]));
}

#[test]
fn range_two_leaves() {
    let dml = Arc::new(DMLMock::new());
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 0
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));
}

#[test]
fn remove_last_key() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(0, TxgT::from(42)));
    assert_eq!(r, Ok(Some(0.0)));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
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
      Leaf:
        items: {}"#);
}

#[test]
fn remove_from_leaf() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(1, TxgT::from(42)));
    assert_eq!(r, Ok(Some(1.0)));
    assert_eq!(format!("{}", tree),
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
          0: 0.0
          2: 2.0"#);
}

#[test]
fn remove_and_merge_down() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
                    2: 2.0
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
          0: 0.0
          2: 2.0"#);
}

#[test]
fn remove_and_merge_int_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 3
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                    - key: 6
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
          - key: 9
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
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(23, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 3
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                    - key: 6
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
}

#[test]
fn remove_and_merge_int_right() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
                              4: 4.0
          - key: 9
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
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
              start: 41
              end: 43
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
}

#[test]
fn remove_and_merge_leaf_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    7: 7.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(7, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
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
                    5: 5.0"#);
}

#[test]
fn remove_and_merge_leaf_right() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    5: 5.0
                    6: 6.0
                    7: 7.0"#);
}

#[test]
fn remove_and_steal_int_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
                        end: 42
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
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
          - key: 21
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
          - key: 19
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 19
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
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

#[test]
fn remove_and_steal_int_right() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
                        end: 42
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
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
          - key: 15
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Int:
                  children:
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(14, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
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
                              0: 0.0
                              1: 1.0
                    - key: 2
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
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
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      txgs:
                        start: 41
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 17
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0"#);
}

#[test]
fn remove_and_steal_leaf_left() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
          - key: 2
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    2: 2.0
                    3: 3.0
                    4: 4.0
                    5: 5.0
                    6: 6.0
          - key: 8
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    8: 8.0
                    9: 9.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(8, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
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
                    4: 4.0
                    5: 5.0
          - key: 6
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    9: 9.0"#);
}

#[test]
fn remove_and_steal_leaf_right() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
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
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            txgs:
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
                    9: 9.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4, TxgT::from(42)));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
              start: 41
              end: 42
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    5: 5.0
          - key: 6
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0
                    9: 9.0"#);
}

#[test]
fn remove_nonexistent() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::new(dml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(3, TxgT::from(42)));
    assert_eq!(r, Ok(None));
}
// LCOV_EXCL_STOP
