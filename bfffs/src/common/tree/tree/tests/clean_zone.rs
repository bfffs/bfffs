//! Tests regarding Tree::clean_zone
// LCOV_EXCL_START

use crate::common::ddml::*;
use futures::{future::IntoFuture, future};
use mockall::{Predicate, params, predicate::*};
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicU64, Ordering};
use super::*;
use tokio::runtime::current_thread;

#[test]
fn basic() {
    // The Int Node at level 1, key 0 lies outside of the operation's TXG range,
    // so it won't be read at all.

    // On-disk internal node with children both in and outside of target zone
    let drpl2 = DRP::new(PBA{cluster: 0, lba: 3}, Compression::None, 0, 0, 0);
    let drpl3 = DRP::new(PBA{cluster: 0, lba: 100}, Compression::None, 0, 0, 0);
    // We must make two copies of in1, one for DDML::get and one for ::pop
    let children1 = vec![
        IntElem::new(4u32, TxgT::from(31)..TxgT::from(32), TreePtr::Addr(drpl2)),
        IntElem::new(6u32, TxgT::from(20)..TxgT::from(21),
                     TreePtr::Addr(drpl3)),
    ];
    let children1_c = vec![
        IntElem::new(4u32, TxgT::from(31)..TxgT::from(32), TreePtr::Addr(drpl2)),
        IntElem::new(6u32, TxgT::from(20)..TxgT::from(21),
                     TreePtr::Addr(drpl3)),
    ];
    let in1 = Arc::new(Node::new(NodeData::Int(IntData::new(children1))));
    let in1_c = Arc::new(Node::new( NodeData::Int(IntData::new(children1_c))));
    let drpi1 = DRP::new(PBA{cluster: 0, lba: 4}, Compression::None, 0, 0, 0);

    let mut ld3 = LeafData::default();
    ld3.insert(6, 6.0);
    ld3.insert(7, 7.0);
    let ln3 = Arc::new(Node::new(NodeData::Leaf(ld3)));

    // On-disk internal node in the target zone, but with children outside
    let drpl4 = DRP::new(PBA{cluster: 0, lba: 5}, Compression::None, 0, 0, 0);
    let drpl5 = DRP::new(PBA{cluster: 0, lba: 6}, Compression::None, 0, 0, 0);
    // We must make two copies of in2, one for DDML::get and one for ::pop
    let children2 = vec![
        IntElem::new(8u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(drpl4)),
        IntElem::new(10u32, TxgT::from(10)..TxgT::from(11),
                     TreePtr::Addr(drpl5)),
    ];
    let children2_c = vec![
        IntElem::new(8u32, TxgT::from(8)..TxgT::from(9), TreePtr::Addr(drpl4)),
        IntElem::new(10u32, TxgT::from(10)..TxgT::from(11),
                     TreePtr::Addr(drpl5)),
    ];
    let in2 = Arc::new(Node::new(NodeData::Int(IntData::new(children2))));
    let in2_c = Arc::new(Node::new( NodeData::Int(IntData::new(children2_c))));
    let drpi2 = DRP::new(PBA{cluster: 0, lba: 101}, Compression::None, 0, 0, 0);

    // On-disk leaf node in the target zone
    let drpl8 = DRP::new(PBA{cluster: 0, lba: 102}, Compression::None, 0, 0, 0);
    let mut ld8 = LeafData::default();
    ld8.insert(16, 16.0);
    ld8.insert(17, 17.0);
    let ln8 = Arc::new(Node::new(NodeData::Leaf(ld8)));

    let mut mock = DDML::default();
    type T = Arc<Node<DRP, u32, f32>>;
    // Safe because the test is single-threaded
    unsafe {
        mock.expect_get::<T, T>()
            .withf_unsafe(move |arg: & *const DRP| **arg == drpi1)
            .returning(move |_| Box::new(future::ok(Box::new(in1.clone()))));
        mock.expect_get::<T, T>()
            .withf_unsafe(move |arg: & *const DRP| **arg == drpi2)
            .returning(move |_| Box::new(future::ok(Box::new(in2.clone()))));
        mock.expect_pop::<T, T>()
            .once()
            .withf_unsafe(move |args: &(*const DRP, TxgT)| *args.0 == drpl3)
            .return_once(move |_| Box::new(future::ok(Box::new(ln3))));
        mock.expect_pop::<T, T>()
            .once()
            .withf_unsafe(move |args: &(*const DRP, TxgT)| *args.0 == drpi1)
            .return_once(move |_| Box::new(future::ok(Box::new(in1_c))));
        mock.expect_pop::<T, T>()
            .once()
            .withf_unsafe(move |args: &(*const DRP, TxgT)| *args.0 == drpi2)
            .return_once(move |_| Box::new(future::ok(Box::new(in2_c))));
        mock.expect_pop::<T, T>()
            .once()
            .withf_unsafe(move |args: &(*const DRP, TxgT)| *args.0 == drpl8)
            .return_once(move |_| Box::new(future::ok(Box::new(ln8))));
    }
    let next_lba = AtomicU64::new(0);
    mock.expect_put::<Arc<Node<DRP, u32, f32>>>()
        .times(3)
        .with(params!(always(), always(), eq(TxgT::from(42))))
        .returning(move |(_cacheable, compression, _txg)| {
            let lba = next_lba.fetch_add(1, Ordering::Relaxed);
            let drp = DRP::new(PBA{cluster: 1, lba}, compression, 0, 0, 0);
            Box::new(Ok(drp).into_future())
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDML, u32, f32> = Tree::from_str(ddml, false, r#"
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
    start: 8
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
                  lba: 2
                compressed: false
                lsize: 0
                csize: 0
                checksum: 0
          - key: 4
            txgs:
              start: 20
              end: 32
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 4
                compressed: false
                lsize: 0
                csize: 0
                checksum: 0
          - key: 8
            txgs:
              start: 8
              end: 24
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 101
                compressed: false
                lsize: 0
                csize: 0
                checksum: 0
          - key: 12
            txgs:
              start: 21
              end: 42
            ptr:
              Mem:  # In-memory Int node with a child in the target zone
                Int:
                  children:
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                    - key: 16
                      txgs:
                        start: 21
                        end: 22
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 102
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0
                    - key: 20   # Leaf node in TXG range but not in PBA range
                      txgs:
                        start: 29
                        end: 30
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 200
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let start = PBA::new(0, 100);
    let end = PBA::new(0, 200);
    let txgs = TxgT::from(20)..TxgT::from(30);
    rt.block_on(tree.clean_zone(start..end, txgs, TxgT::from(42))).unwrap();
    let clean_tree = format!("{}", tree);
    assert_eq!(clean_tree,
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
    start: 8
    end: 43
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
                  lba: 2
                compressed: false
                lsize: 0
                csize: 0
                checksum: 0
          - key: 4
            txgs:
              start: 20
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 4
                      txgs:
                        start: 31
                        end: 32
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 3
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0
                    - key: 6
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Addr:
                          pba:
                            cluster: 1
                            lba: 0
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0
          - key: 8
            txgs:
              start: 8
              end: 43
            ptr:
              Addr:
                pba:
                  cluster: 1
                  lba: 2
                compressed: false
                lsize: 0
                csize: 0
                checksum: 0
          - key: 12
            txgs:
              start: 21
              end: 43
            ptr:
              Mem:
                Int:
                  children:
                    - key: 12
                      txgs:
                        start: 41
                        end: 42
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                    - key: 16
                      txgs:
                        start: 42
                        end: 43
                      ptr:
                        Addr:
                          pba:
                            cluster: 1
                            lba: 1
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0
                    - key: 20
                      txgs:
                        start: 29
                        end: 30
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 200
                          compressed: false
                          lsize: 0
                          csize: 0
                          checksum: 0"#);
}

// The Root node lies in the dirty zone
#[test]
fn dirty_root() {
    // On-disk internal root node in the target zone, but with children outside
    let drpl0 = DRP::new(PBA{cluster: 0, lba: 5}, Compression::None, 0, 0, 0);
    let drpl1 = DRP::new(PBA{cluster: 0, lba: 6}, Compression::None, 0, 0, 0);
    // We must make two copies of inr, one for DDML::get and one for ::pop
    let children = vec![
        IntElem::new(8u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drpl0)),
        IntElem::new(10u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drpl1)),
    ];
    let children_c = vec![
        IntElem::new(8u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drpl0)),
        IntElem::new(10u32, TxgT::from(0)..TxgT::from(9), TreePtr::Addr(drpl1)),
    ];
    let inr = Arc::new(Node::<DRP, u32, f32>::new(
            NodeData::Int(IntData::new(children))));
    let inr_c = Arc::new(Node::<DRP, u32, f32>::new(
            NodeData::Int(IntData::new(children_c)
    )));
    let drpir = DRP::new(PBA{cluster: 0, lba: 100}, Compression::None, 0, 0, 0);

    let mut mock = DDML::default();
    type T = Arc<Node<DRP, u32, f32>>;
    // Safe because the test is single-threaded
    unsafe {
        mock.expect_get::<T, T>()
            .withf_unsafe(move |arg: & *const DRP| **arg == drpir )
            .returning(move |_arg: *const DRP| {
                Box::new(future::ok(Box::new(inr.clone())))
            });
        mock.expect_pop::<T, T>()
            .once()
            .withf_unsafe(move |args: &(*const DRP, TxgT)|
                 *args.0 == drpir && args.1 == TxgT::from(42)
            ).return_once(move |_args: (*const DRP, TxgT)| {
                Box::new(future::ok(Box::new(inr_c)))
            });
    }
    mock.expect_put::<T>()
        .once()
        .with(params!(always(), always(), eq(TxgT::from(42))))
        .returning(move |(_cacheable, _compression, _txg)| {
            let drp = DRP::random(Compression::None, 1024);
            Box::new(Ok(drp).into_future())
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDML, u32, f32> = Tree::from_str(ddml, false, r#"
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
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 100
      compressed: false
      lsize: 0
      csize: 0
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let start = PBA::new(0, 100);
    let end = PBA::new(0, 200);
    let txgs = TxgT::from(1000)..TxgT::from(1001);  // XXX placeholder
    rt.block_on(tree.clean_zone(start..end, txgs, TxgT::from(42))).unwrap();
}
// LCOV_EXCL_STOP
