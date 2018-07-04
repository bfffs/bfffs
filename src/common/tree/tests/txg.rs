//! Tests regarding transaction transaction membership of nodes
// LCOV_EXCL_START

use common::tree::*;
use common::ddml_mock::*;
#[cfg(test)] use common::ddml::DRP;
use futures::future;
use nix::Error;
use simulacrum::*;
use tokio::runtime::current_thread;

/// Recompute start TXGs on Tree flush
#[test]
fn flush() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_txg().called_any().returning(|_| 42);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            node_data.is_leaf()
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    mock.then().expect_txg().called_any().returning(|_| 42);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].txgs == (42..43) &&
            int_data.children[1].key == 256 &&
            int_data.children[1].txgs == (41..42)
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
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
    start: 40
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
    let r = rt.block_on(tree.flush());
    assert!(r.is_ok());
    let root_elem = tree.i.root.get_mut().unwrap();
    assert_eq!(root_elem.txgs, 41..43);
}

/// Insert a key that splits the root IntNode
#[test]
fn split() {
    let mut mock = DDMLMock::new();
    mock.expect_txg().called_any().returning(|_| 42);
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
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
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
// LCOV_EXCL_STOP
