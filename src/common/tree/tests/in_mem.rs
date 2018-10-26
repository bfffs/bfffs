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

// During range_delete_pass2, one node needs to be fixed three times.  The node
// starts with one child.  The first fix merges two nodes to produce a single
// one with 3 children.  The second steals a node, so the target has 4 children.
// But the min fanout is 3 and the target is now a common ancestor, so it needs
// a third merge to get 5 children.
#[test]
fn range_delete_fix_three_times() {
    let mut mock = DMLMock::new();
    mock.expect_delete()
        .called_any()
        .returning(move |_| {
            Box::new(future::ok::<(), Error>(()))
        });

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 4
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 8
  ptr:
    Mem:
      Int:
        children:
          - key: 664
            txgs:
              start: 1
              end: 5
            ptr:
              Addr: 3977
          - key: 728
            txgs:
              start: 1
              end: 8
            ptr:
              Addr: 100728
          - key: 832
            txgs:
              start: 4
              end: 8
            ptr:
              Mem:
                Int:
                  children:
                    - key: 832
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 832
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 4627
                              - key: 836
                                txgs:
                                  start: 6
                                  end: 7
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        838: 646
                                        839: 647
                                        1027: 1814
                              - key: 1028
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 1001028
                              - key: 1054
                                txgs:
                                  start: 6
                                  end: 7
                                ptr:
                                  Addr: 8892
                    - key: 1090
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 1090
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 101090
                              - key: 1130
                                txgs:
                                  start: 6
                                  end: 7
                                ptr:
                                  Addr: 8894
                              - key: 1170
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 108894
                              - key: 1297
                                txgs:
                                  start: 6
                                  end: 7
                                ptr:
                                  Addr: 8897
                              - key: 1314
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4054
                    - key: 1318
                      txgs:
                        start: 4
                        end: 7
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 1318
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4055
                              - key: 1322
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4056
                              - key: 1326
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4057
                              - key: 1330
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4058
                              - key: 1334
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4059
          - key: 1466
            txgs:
              start: 4
              end: 8
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1498
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 1498
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4100
                              - key: 1502
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4101
                              - key: 1506
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4102
                              - key: 1510
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4103
                    - key: 1514
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 1514
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4104
                              - key: 1530
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        1530: 2317
                                        1535: 2322
                                        1536: 984
                              - key: 1537
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4109
                    - key: 1540
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 1540
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4110
                              - key: 1544
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4111
                              - key: 1548
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4112
          - key: 1552
            txgs:
              start: 4
              end: 8
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1552
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10020
                    - key: 1568
                      txgs:
                        start: 4
                        end: 7
                      ptr:
                        Addr: 9592
                    - key: 1624
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10022
                    - key: 1640
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10024
                    - key: 1656
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10025
"#);
  let mut rt = current_thread::Runtime::new().unwrap();
  let r = rt.block_on(
  tree.range_delete(1024..1536, TxgT::from(42))
  );
  assert!(r.is_ok());
  assert_eq!(format!("{}", &tree),
r#"---
height: 4
min_fanout: 3
max_fanout: 7
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
          - key: 664
            txgs:
              start: 1
              end: 5
            ptr:
              Addr: 3977
          - key: 728
            txgs:
              start: 1
              end: 8
            ptr:
              Addr: 100728
          - key: 832
            txgs:
              start: 4
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 832
                      txgs:
                        start: 4
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 832
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 4627
                              - key: 836
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        838: 646
                                        839: 647
                                        1536: 984
                              - key: 1537
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4109
                              - key: 1540
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4110
                              - key: 1544
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4111
                              - key: 1548
                                txgs:
                                  start: 4
                                  end: 5
                                ptr:
                                  Addr: 4112
                    - key: 1552
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10020
                    - key: 1568
                      txgs:
                        start: 4
                        end: 7
                      ptr:
                        Addr: 9592
          - key: 1624
            txgs:
              start: 4
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1624
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10022
                    - key: 1640
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10024
                    - key: 1656
                      txgs:
                        start: 4
                        end: 8
                      ptr:
                        Addr: 10025"#);
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
    assert!(rt.block_on(tree.check()).unwrap());
}

// A scenario in which the child on the left side of the cut is merged twice
// during ascent.
#[test]
fn range_delete_merge_left_child_twice() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, f32> = Tree::from_str(dml, r#"
---
height: 2
min_fanout: 5
max_fanout: 11
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 15
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
                    3: 3.0
                    4: 4.0
          - key: 10
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
                    14: 14.0
          - key: 20
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0
                    23: 23.0
                    24: 24.0
          - key: 30
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    30: 30.0
                    31: 31.0
                    32: 32.0
                    33: 33.0
                    34: 34.0
          - key: 40
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    40: 40.0
                    41: 41.0
                    42: 42.0
                    43: 43.0
                    44: 44.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(12..23, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 5
max_fanout: 11
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
                    3: 3.0
                    4: 4.0
          - key: 10
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    23: 23.0
                    24: 24.0
                    30: 30.0
                    31: 31.0
                    32: 32.0
                    33: 33.0
                    34: 34.0
          - key: 40
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    40: 40.0
                    41: 41.0
                    42: 42.0
                    43: 43.0
                    44: 44.0"#);
}

/// range_delete_pass1 results in two adjacent leaves with different parents
/// having one item apiece.  range_delete_pass2 must merge them, then merge the
/// left one again.
#[test]
fn range_delete_merge_to_lca_twice() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 4
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 10
  ptr:
    Mem:
      Int:
        children:
          - key: 10422
            txgs:
              start: 7
              end: 10
            ptr:
              Addr: 1010422
          - key: 10694
            txgs:
              start: 7
              end: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10694
                      txgs:
                        start: 7
                        end: 8
                      ptr:
                        Addr: 11632
                    - key: 10710
                      txgs:
                        start: 7
                        end: 9
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 10711
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 11432
                              - key: 10714
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        10714: 5916
                                        11722: 3874
                                        11725: 3877
                              - key: 11730
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        10726: 5916
                                        11727: 3874
                                        11729: 3877
                    - key: 11734
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 11734
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        10738: 5916
                                        11739: 3874
                                        11741: 3877
                              - key: 11742
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11742: 3927
                                        11744: 3938
                                        11781: 4342
                              - key: 11782
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11782: 4343
                                        11784: 4345
                                        11785: 4346
                    - key: 11786
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 11786
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7170
                              - key: 11790
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7171
                              - key: 11794
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7172
                              - key: 11798
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7173
          - key: 11802
            txgs:
              start: 8
              end: 10
            ptr:
              Mem:
                Int:
                  children:
                    - key: 11802
                      txgs:
                        start: 8
                        end: 10
                      ptr:
                        Addr: 111802
                    - key: 11865
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7605
                    - key: 11881
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7606
                    - key: 11897
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7607
                    - key: 12018
                      txgs:
                        start: 8
                        end: 10
                      ptr:
                        Addr: 112018
          - key: 12050
            txgs:
              start: 8
              end: 9
            ptr:
              Addr: 7713
          - key: 12114
            txgs:
              start: 8
              end: 10
            ptr:
              Addr: 112144
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(11264..11776, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 4
min_fanout: 3
max_fanout: 7
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
          - key: 10422
            txgs:
              start: 7
              end: 10
            ptr:
              Addr: 1010422
          - key: 10694
            txgs:
              start: 7
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10694
                      txgs:
                        start: 7
                        end: 8
                      ptr:
                        Addr: 11632
                    - key: 10710
                      txgs:
                        start: 7
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 10711
                                txgs:
                                  start: 7
                                  end: 8
                                ptr:
                                  Addr: 11432
                              - key: 10714
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        10714: 5916
                                        11781: 4342
                                        11782: 4343
                                        11784: 4345
                                        11785: 4346
                              - key: 11786
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7170
                    - key: 11790
                      txgs:
                        start: 8
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 11790
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7171
                              - key: 11794
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7172
                              - key: 11798
                                txgs:
                                  start: 8
                                  end: 9
                                ptr:
                                  Addr: 7173
                    - key: 11802
                      txgs:
                        start: 8
                        end: 10
                      ptr:
                        Addr: 111802
          - key: 11865
            txgs:
              start: 8
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 11865
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7605
                    - key: 11881
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7606
                    - key: 11897
                      txgs:
                        start: 8
                        end: 9
                      ptr:
                        Addr: 7607
                    - key: 12018
                      txgs:
                        start: 8
                        end: 10
                      ptr:
                        Addr: 112018
          - key: 12050
            txgs:
              start: 8
              end: 9
            ptr:
              Addr: 7713
          - key: 12114
            txgs:
              start: 8
              end: 10
            ptr:
              Addr: 112144"#);
}

/// Regression test for another bug similar to e6fd45d.  After pass1, Node
/// 1,4898 has two children that both underflow.  During pass2, the right child
/// (key 5626) got merged during descent.  Then the left child got merged during
/// ascent, causing a "subtract with overflow" error.
#[test]
fn range_delete_merge_right_child_descending() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 2
min_fanout: 4
max_fanout: 9
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 15
  ptr:
    Mem:
      Int:
        children:
          - key: 4898
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    4903: 3431
                    4993: 3521
                    5045: 3573
                    5583: 2277
          - key: 5618
            txgs:
              start: 13
              end: 14
            ptr:
              Mem:
                Leaf:
                  items:
                    5618: 5459
                    5623: 5464
                    5624: 5465
                    5625: 5466
          - key: 5626
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    5629: 5470
                    5630: 5471
                    5631: 5472
                    5632: 7554
          - key: 5634
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    5635: 7557
                    5637: 7559
                    5638: 7560
                    5642: 7564
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5120..5632, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 4
max_fanout: 9
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 4898
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    4903: 3431
                    4993: 3521
                    5045: 3573
                    5632: 7554
          - key: 5634
            txgs:
              start: 14
              end: 15
            ptr:
              Mem:
                Leaf:
                  items:
                    5635: 7557
                    5637: 7559
                    5638: 7560
                    5642: 7564"#);
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

/// Regression test for bug 40b01eb: Can't fix a node during range_delete_pass2
/// ascent because its parent has only 1 child.  The correct solution is to
/// wait.  The node will be fixed at one higher level up the stack.  In this
/// case, that means by merging the root node down.
#[test]
fn range_delete_merge_root_during_ascent() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 3
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 14
  ptr:
    Mem:
      Int:
        children:
          - key: 2762
            txgs:
              start: 12
              end: 14
            ptr:
              Mem:
                Int:
                  children:
                    - key: 2778
                      txgs:
                        start: 12
                        end: 13
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2778: 2859
                              2781: 2868
                              2782: 2869
                    - key: 2788
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2965: 4758
                              3027: 4820
                              3074: 6186
                    - key: 3077
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3081: 6193
                              3085: 6197
                              3088: 6200
          - key: 3109
            txgs:
              start: 12
              end: 14
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3109
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3112: 6224
                              3113: 6225
                              3122: 6234
                    - key: 3149
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3149: 6261
                              3153: 6265
                              3154: 6266
                    - key: 3157
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3157: 6269
                              3166: 6278
                              3167: 6279
          - key: 3365
            txgs:
              start: 13
              end: 14
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3509
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3509: 6621
                              3531: 6643
                              3532: 6644
                    - key: 3541
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3546: 6658
                              3547: 6659
                              3572: 6684
                    - key: 3573
                      txgs:
                        start: 13
                        end: 14
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3581: 6693
                              3584: 3091
                              3585: 3092
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(3072..3584, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 12
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 2778
            txgs:
              start: 12
              end: 13
            ptr:
              Mem:
                Leaf:
                  items:
                    2778: 2859
                    2781: 2868
                    2782: 2869
          - key: 2788
            txgs:
              start: 42
              end: 43
            ptr:
              Mem:
                Leaf:
                  items:
                    2965: 4758
                    3027: 4820
                    3584: 3091
                    3585: 3092"#);
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

// After the descent phase of range_delete_pass2_r, there is an underflowing
// node who is its parent's only child.
// Bug #3a1c768
#[test]
fn range_delete_parent_and_child_underflow_after_descent() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 4
min_fanout: 4
max_fanout: 16
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 11
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            txgs:
              start: 0
              end: 11
            ptr:
              Addr: 1000000
          - key: 2218
            txgs:
              start: 5
              end: 11
            ptr:
              Addr: 1002218
          - key: 3563
            txgs:
              start: 8
              end: 11
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3563
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 3563
                                txgs:
                                  start: 9
                                  end: 10
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3563: 2604
                                        3564: 2605
                                        3565: 2606
                                        3566: 2607
                              - key: 3611
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3613: 3005
                                        3621: 3013
                                        3624: 3016
                                        3625: 3017
                              - key: 3627
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3627: 3019
                                        3640: 3032
                                        3641: 3033
                                        3642: 3034
                              - key: 3643
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3643: 3035
                                        3664: 3056
                                        3665: 3057
                                        3666: 3058
                    - key: 4051
                      txgs:
                        start: 8
                        end: 11
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 4075
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        4075: 3467
                                        4076: 3468
                                        4077: 3469
                                        4078: 3470
                              - key: 4083
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        4083: 3475
                                        4084: 3476
                                        4085: 3477
                                        4086: 3478
                              - key: 4091
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        4091: 3483
                                        4092: 3484
                                        4093: 3485
                                        4094: 3486
                                        4095: 3487
                              - key: 4604
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        4609: 3745
                                        4610: 3746
                                        4614: 3750
                                        4615: 3751
          - key: 4620
            txgs:
              start: 9
              end: 11
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4620
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 4620
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004620
                              - key: 4628
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004628
                              - key: 4636
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004636
                              - key: 4644
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004644
                    - key: 4684
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1004684
                    - key: 4748
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1004748
                    - key: 5119
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1005119
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(3584..4096, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 4
min_fanout: 4
max_fanout: 16
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
              end: 11
            ptr:
              Addr: 1000000
          - key: 2218
            txgs:
              start: 5
              end: 11
            ptr:
              Addr: 1002218
          - key: 3563
            txgs:
              start: 8
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3563
                      txgs:
                        start: 10
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 3563
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3563: 2604
                                        3564: 2605
                                        3565: 2606
                                        3566: 2607
                              - key: 4604
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        4609: 3745
                                        4610: 3746
                                        4614: 3750
                                        4615: 3751
                              - key: 4620
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004620
                              - key: 4628
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004628
                              - key: 4636
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004636
                              - key: 4644
                                txgs:
                                  start: 10
                                  end: 11
                                ptr:
                                  Addr: 1004644
                    - key: 4684
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1004684
                    - key: 4748
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1004748
                    - key: 5119
                      txgs:
                        start: 9
                        end: 11
                      ptr:
                        Addr: 1005119"#);
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

#[test]
fn range_delete_cant_steal_to_fix_lca() {
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
    start: 1
    end: 2
  ptr:
    Mem:
      Int:
        children:
          - key: 1
            txgs:
              start: 1
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000001
                    - key: 10
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
                              12: 12.0
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
                    - key: 30
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              30: 30.0
                              31: 31.0
                              32: 32.0
          - key: 40
            txgs:
              start: 1
              end: 2
            ptr:
              Mem:
                Int:
                  children:
                    - key: 40
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              40: 40.0
                              41: 41.0
                              42: 42.0
                    - key: 50
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000050
                    - key: 60
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000060
                    - key: 70
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000070
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(21..32, TxgT::from(42))
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
    start: 1
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 1
            txgs:
              start: 1
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000001
                    - key: 10
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
                              12: 12.0
                    - key: 20
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              32: 32.0
                              40: 40.0
                              41: 41.0
                              42: 42.0
          - key: 50
            txgs:
              start: 1
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 50
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000050
                    - key: 60
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000060
                    - key: 70
                      txgs:
                        start: 1
                        end: 2
                      ptr:
                        Addr: 1000070"#);
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
                        end: 42
                      ptr:
                        Addr: 1004617
          - key: 4627
            txgs:
              start: 41
              end: 43
            ptr:
              Mem:
                Int:
                  children:
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
                        Addr: 1004193
                    - key: 4457
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr: 1004457
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

// range_delete_pass2 needs to steal 2 nodes in order to guarantee invariants.
#[test]
fn range_delete_pass2_steal_creates_an_lca() {
    let mut mock = DMLMock::new();
    mock.expect_delete()
        .called_any()
        .returning(move |_| {
            Box::new(future::ok::<(), Error>(()))
        });

    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 5
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 1
    end: 6
  ptr:
    Mem:
      Int:
        children:
          - key: 2599
            txgs:
              start: 2
              end: 6
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3634
                      txgs:
                        start: 3
                        end: 6
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 3634
                                txgs:
                                  start: 3
                                  end: 6
                                ptr:
                                  Addr: 103634
                              - key: 3682
                                txgs:
                                  start: 3
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 3682
                                          txgs:
                                            start: 3
                                            end: 4
                                          ptr:
                                            Addr: 4494
                                        - key: 3686
                                          txgs:
                                            start: 3
                                            end: 4
                                          ptr:
                                            Addr: 4495
                                        - key: 3690
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 7382
                                        - key: 3698
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 7384
                              - key: 3706
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 3706
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  3706: 3060
                                                  3704: 3061
                                                  3711: 3065
                                        - key: 4774
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4774: 3069
                                                  4775: 3070
                                                  4776: 3071
                                        - key: 4778
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4778: 3073
                                                  4780: 3075
                                                  4781: 3076
                                                  4785: 3080
                                                  4786: 3081
                                                  4789: 3084
                              - key: 4790
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 4790
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4791: 3086
                                                  4792: 3087
                                                  4793: 3088
                                        - key: 4794
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4794: 3089
                                                  4795: 3090
                                                  4796: 3091
                                                  4801: 3096
                                        - key: 4802
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4802: 3097
                                                  4804: 3099
                                                  4805: 3100
                                        - key: 4806
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4806: 3101
                                                  4808: 3103
                                                  4812: 3107
                                                  4815: 3110
                                        - key: 4818
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4818: 3113
                                                  4820: 3115
                                                  4832: 3127
                                                  4834: 3129
                                                  4835: 3130
                                                  4836: 3131
                                        - key: 4838
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  4838: 3133
                                                  4839: 3134
                                                  4845: 3140
                                                  4846: 3141
                                                  4847: 3142
                    - key: 4850
                      txgs:
                        start: 4
                        end: 6
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 4850
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 4850
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 104850
                                        - key: 4865
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 104865
                                        - key: 4878
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 104878
                              - key: 5035
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 5035
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105035
                                        - key: 5058
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105058
                                        - key: 5062
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105062
                              - key: 5070
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 5070
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105070
                                        - key: 5090
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105090
                                        - key: 5100
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 105100
                    - key: 5106
                      txgs:
                        start: 4
                        end: 6
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 5106
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 5106
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  5107: 3402
                                                  5112: 3407
                                                  5113: 3408
                                        - key: 5114
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  5115: 3410
                                                  5119: 3414
                                                  5120: 3415
                                        - key: 5122
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Mem:
                                              Leaf:
                                                items:
                                                  5122: 3417
                                                  5123: 3418
                                                  5137: 3432
                              - key: 5138
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Mem:
                                    Int:
                                      children:
                                        - key: 5138
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 1005138
                                        - key: 5146
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 1005146
                                        - key: 5162
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 1005162
                                        - key: 5166
                                          txgs:
                                            start: 5
                                            end: 6
                                          ptr:
                                            Addr: 1005166
                              - key: 5182
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Addr: 105182
                              - key: 5202
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 105202
                              - key: 5298
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Addr: 105298
                              - key: 5330
                                txgs:
                                  start: 4
                                  end: 6
                                ptr:
                                  Addr: 105330
"#);
let mut rt = current_thread::Runtime::new().unwrap();
let r = rt.block_on(
        tree.range_delete(4608..5120, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 4
min_fanout: 3
max_fanout: 7
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 2
    end: 43
  ptr:
    Mem:
      Int:
        children:
          - key: 3634
            txgs:
              start: 3
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 3634
                      txgs:
                        start: 3
                        end: 6
                      ptr:
                        Addr: 103634
                    - key: 3682
                      txgs:
                        start: 3
                        end: 6
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 3682
                                txgs:
                                  start: 3
                                  end: 4
                                ptr:
                                  Addr: 4494
                              - key: 3686
                                txgs:
                                  start: 3
                                  end: 4
                                ptr:
                                  Addr: 4495
                              - key: 3690
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 7382
                              - key: 3698
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 7384
                    - key: 3706
                      txgs:
                        start: 5
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 3706
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        3704: 3061
                                        3706: 3060
                                        3711: 3065
                              - key: 5114
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        5120: 3415
                                        5122: 3417
                                        5123: 3418
                                        5137: 3432
                              - key: 5138
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 1005138
                              - key: 5146
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 1005146
                              - key: 5162
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 1005162
                              - key: 5166
                                txgs:
                                  start: 5
                                  end: 6
                                ptr:
                                  Addr: 1005166
          - key: 5182
            txgs:
              start: 4
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 5182
                      txgs:
                        start: 4
                        end: 6
                      ptr:
                        Addr: 105182
                    - key: 5202
                      txgs:
                        start: 5
                        end: 6
                      ptr:
                        Addr: 105202
                    - key: 5298
                      txgs:
                        start: 4
                        end: 6
                      ptr:
                        Addr: 105298
                    - key: 5330
                      txgs:
                        start: 4
                        end: 6
                      ptr:
                        Addr: 105330"#);
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
                        end: 1
                      ptr:
                        Addr: 32
          - key: 30
            txgs:
              start: 0
              end: 3
            ptr:
              Mem:
                Int:
                  children:
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

// range_delete_pass2 tries to steal a node to the right
#[test]
fn range_delete_pass2_steal_right() {
    let mock = DMLMock::new();
    let dml = Arc::new(mock);
    let tree: Tree<u32, DMLMock, u32, u32> = Tree::from_str(dml, r#"
---
height: 4
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 23
  ptr:
    Mem:
      Int:
        children:
          - key: 5409
            txgs:
              start: 14
              end: 23
            ptr:
              Mem:
                Int:
                  children:
                    - key: 5729
                      txgs:
                        start: 14
                        end: 23
                      ptr:
                        Addr: 1005729
                    - key: 6809
                      txgs:
                        start: 16
                        end: 23
                      ptr:
                        Addr: 1006809
                    - key: 9437
                      txgs:
                        start: 17
                        end: 23
                      ptr:
                        Addr: 1009437
          - key: 10049
            txgs:
              start: 18
              end: 23
            ptr:
              Mem:
                Int:
                  children:
                    - key: 10113
                      txgs:
                        start: 22
                        end: 23
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 10113
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11264: 9282
                                        11269: 9287
                                        11270: 9288
                                        11271: 9289
                              - key: 11279
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11281: 9299
                                        11283: 9301
                                        11284: 9302
                                        11285: 9303
                              - key: 11287
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11288: 9306
                                        11292: 9310
                                        11293: 9311
                                        11294: 9312
                              - key: 11295
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11295: 9313
                                        11298: 9316
                                        11300: 9318
                                        11301: 9319
                    - key: 11495
                      txgs:
                        start: 21
                        end: 23
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 11511
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11511: 9529
                                        11514: 9532
                              - key: 11519
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11616: 9634
                                        11994: 6844
                              - key: 11995
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11995: 6871
                                        12002: 7037
                              - key: 12003
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        12003: 7053
                                        12010: 7164
                    - key: 12037
                      txgs:
                        start: 22
                        end: 23
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 12037
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Addr: 1012037
                              - key: 12095
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Addr: 1012095
                    - key: 12155
                      txgs:
                        start: 22
                        end: 23
                      ptr:
                        Addr: 1012155
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(11264..11776, TxgT::from(42))
    );
    assert!(r.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 4
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
          - key: 5409
            txgs:
              start: 14
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 5729
                      txgs:
                        start: 14
                        end: 23
                      ptr:
                        Addr: 1005729
                    - key: 6809
                      txgs:
                        start: 16
                        end: 23
                      ptr:
                        Addr: 1006809
          - key: 9437
            txgs:
              start: 17
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9437
                      txgs:
                        start: 17
                        end: 23
                      ptr:
                        Addr: 1009437
                    - key: 11495
                      txgs:
                        start: 22
                        end: 43
                      ptr:
                        Mem:
                          Int:
                            children:
                              - key: 11519
                                txgs:
                                  start: 42
                                  end: 43
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        11994: 6844
                                        11995: 6871
                                        12002: 7037
                              - key: 12003
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Mem:
                                    Leaf:
                                      items:
                                        12003: 7053
                                        12010: 7164
                              - key: 12037
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Addr: 1012037
                              - key: 12095
                                txgs:
                                  start: 22
                                  end: 23
                                ptr:
                                  Addr: 1012095
                    - key: 12155
                      txgs:
                        start: 22
                        end: 23
                      ptr:
                        Addr: 1012155"#);
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
