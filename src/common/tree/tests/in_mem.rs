//! Tests regarding in-memory manipulation of Trees
// LCOV_EXCL_START

use common::tree::*;
use common::ddml_mock::*;
use common::ddml::DRP;
use futures::future;
use tokio::runtime::current_thread;

#[test]
fn insert() {
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0.0));
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

#[test]
fn insert_dup() {
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let r = rt.block_on(tree.insert(0, 100.0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let r2 = rt.block_on(tree.insert(24, 24.0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let r2 = rt.block_on(tree.insert(8, 8.0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let r2 = rt.block_on(tree.insert(15, 15.0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let r2 = rt.block_on(tree.insert(5, 5.0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.get(0))
    }));
    assert_eq!(r, Ok(Some(0.0)));
}

#[test]
fn get_deep() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mock = DDMLMock::new();
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(0));
    assert_eq!(r, Ok(None))
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(11..=31)
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(5..6)
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
                        start: 41
                        end: 43
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
                              9: 9.0
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete((Bound::Excluded(4), Bound::Excluded(10)))
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete((Bound::Excluded(4), Bound::Included(10)))
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

// Delete a range that's contained within a single LeafNode
#[test]
fn range_delete_single_node() {
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(5..7)
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

// Delete a range that includes a whole Node at the end of the Tree
#[test]
fn range_delete_to_end() {
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(7..20)
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(10..16)
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
                        end: 43
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
                        start: 41
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        tree.range_delete(4..20)
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
              end: 43
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


// Unbounded range lookup
#[test]
fn range_full() {
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let ddml = Arc::new(DDMLMock::new());
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
fn range_starts_between_two_leaves() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_two_leaves() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    Mem:
      Leaf:
        items:
          0: 0.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(0));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r = rt.block_on(tree.remove(1));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(1));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(23));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(4));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(7));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(4));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(26));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(14));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(8));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
    let r2 = rt.block_on(tree.remove(4));
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
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(3));
    assert_eq!(r, Ok(None));
}
// LCOV_EXCL_STOP
