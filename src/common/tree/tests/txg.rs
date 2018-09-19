//! Tests regarding transaction transaction membership of nodes
// LCOV_EXCL_START

use common::tree::*;
use common::ddml_mock::*;
#[cfg(test)] use common::ddml::DRP;
use futures::future;
use simulacrum::*;
use tokio::runtime::current_thread;

#[test]
fn check_bad_root_txgs() {
    let mock = DDMLMock::new();
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
    assert!(!rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_bad_int_txgs() {
    let mock = DDMLMock::new();
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 1
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 2
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 3
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 4
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(!rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_ok() {
    let mock = DDMLMock::new();
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 1
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 2
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 3
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 4
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    assert!(rt.block_on(tree.check()).unwrap());
}

#[test]
fn check_leaf() {
    let mock = DDMLMock::new();
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
    end: 1
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
    assert!(rt.block_on(tree.check()).unwrap());
}

/// Recompute start TXGs on Tree flush
#[test]
fn flush() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, TxgT)|{
            let node_data = (args.0).0.try_read().unwrap();
            node_data.is_leaf() || args.2 == TxgT::from(42)
        }))
        .returning(move |_| Box::new(Ok(drp).into_future()));
    mock.then().expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |args: &(Arc<Node<DRP, u32, u32>>, _, TxgT)|{
            let node_data = (args.0).0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].txgs == (TxgT::from(42)..TxgT::from(43)) &&
            int_data.children[1].key == 256 &&
            int_data.children[1].txgs == (TxgT::from(41)..TxgT::from(42)) &&
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
    let root_elem = Arc::get_mut(&mut tree.i).unwrap()
        .root.get_mut().unwrap();
    assert_eq!(root_elem.txgs, TxgT::from(41)..TxgT::from(43));
}

/// Remove a key that merges two int nodes
#[test]
fn merge() {
    let mut mock = DDMLMock::new();
    let mut ld1 = LeafData::new();
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let drpl1 = DRP::new(PBA{cluster: 0, lba: 2}, Compression::None,
                         16000, 8000, 1);
    let ln1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    let holder1 = RefCell::new(Some(ln1));
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const DRP, TxgT)|
                     unsafe {*args.0 == drpl1} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(holder1.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 1
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 2
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 2
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 3
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 4
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 15
                      txgs:
                        start: 24
                        end: 25
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 5
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
          - key: 18
            txgs:
              start: 15
              end: 16
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 6
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 1"#);
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 1
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 3
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 12
                      txgs:
                        start: 20
                        end: 21
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 4
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 15
                      txgs:
                        start: 24
                        end: 25
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 5
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
          - key: 18
            txgs:
              start: 15
              end: 16
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 6
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 1"#);
}

/// Insert a key that splits the root IntNode
#[test]
fn split() {
    let mut mock = DDMLMock::new();
    let mut ld = LeafData::new();
    ld.insert(12, 12.0);
    ld.insert(13, 13.0);
    ld.insert(14, 14.0);
    let drpl = DRP::new(PBA{cluster: 0, lba: 1280}, Compression::None,
                        16000, 8000, 5);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    let node_holder = RefCell::new(Some(node));
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_once()
        .with(passes(move |args: &(*const DRP, TxgT)|
                     unsafe {*args.0 == drpl} && args.1 == TxgT::from(42))
        ).returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
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
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 1
          - key: 3
            txgs:
              start: 5
              end: 11
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 512
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 2
          - key: 6
            txgs:
              start: 3
              end: 12
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 768
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 3
          - key: 9
            txgs:
              start: 6
              end: 22
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 1024
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 4
          - key: 12
            txgs:
              start: 7
              end: 34
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 1280
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 5
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 256
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 1
                    - key: 3
                      txgs:
                        start: 5
                        end: 11
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 512
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 2
                    - key: 6
                      txgs:
                        start: 3
                        end: 12
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 768
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 3
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 1024
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
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
    let mock = DDMLMock::new();
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
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 4
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 9
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 12
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 15
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 17
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 19
                      txgs:
                        start: 21
                        end: 22
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 19
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 21
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
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
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 16000
                csize: 8000
                checksum: 4
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 9
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 12
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 15
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 15
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 17
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 17
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
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
                        Addr:
                          pba:
                            cluster: 0
                            lba: 19
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
                    - key: 21
                      txgs:
                        start: 39
                        end: 40
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 21
                          compression: None
                          lsize: 16000
                          csize: 8000
                          checksum: 4
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
