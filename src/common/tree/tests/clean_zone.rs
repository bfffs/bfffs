//! Tests regarding Tree::clean_zone
// LCOV_EXCL_START

use common::tree::*;
use common::ddml_mock::*;
use common::ddml::DRP;
use futures::future;
use simulacrum::*;
use tokio::runtime::current_thread;

#[test]
fn basic() {
    // The Int Node at level 1, key 0 lies outside of the operation's TXG range,
    // so it won't be read at all.

    // On-disk internal node with children both in and outside of target zone
    let drpl2 = DRP::new(PBA{cluster: 0, lba: 3}, Compression::None, 0, 0, 0);
    let drpl3 = DRP::new(PBA{cluster: 0, lba: 100}, Compression::None, 0, 0, 0);
    // We must make two copies of in1, one for DDMLMock::get and one for ::pop
    let children1 = vec![
        IntElem::new(4u32, 8..9, TreePtr::Addr(drpl2)),
        IntElem::new(6u32, 20..21, TreePtr::Addr(drpl3)),
    ];
    let children1_c = vec![
        IntElem::new(4u32, 8..9, TreePtr::Addr(drpl2)),
        IntElem::new(6u32, 20..21, TreePtr::Addr(drpl3)),
    ];
    let in1 = Arc::new(Node::new(NodeData::Int(IntData::new(children1))));
    let mut in1_c = Some(Arc::new(Node::new(
                NodeData::Int(IntData::new(children1_c))
    )));
    let drpi1 = DRP::new(PBA{cluster: 0, lba: 4}, Compression::None, 0, 0, 0);

    let mut ld3 = LeafData::new();
    ld3.insert(6, 6.0);
    ld3.insert(7, 7.0);
    let mut ln3 = Some(Arc::new(Node::new(NodeData::Leaf(ld3))));

    // On-disk internal node in the target zone, but with children outside
    let drpl4 = DRP::new(PBA{cluster: 0, lba: 5}, Compression::None, 0, 0, 0);
    let drpl5 = DRP::new(PBA{cluster: 0, lba: 6}, Compression::None, 0, 0, 0);
    // We must make two copies of in2, one for DDMLMock::get and one for ::pop
    let children2 = vec![
        IntElem::new(8u32, 8..9, TreePtr::Addr(drpl4)),
        IntElem::new(10u32, 10..11, TreePtr::Addr(drpl5)),
    ];
    let children2_c = vec![
        IntElem::new(8u32, 8..9, TreePtr::Addr(drpl4)),
        IntElem::new(10u32, 10..11, TreePtr::Addr(drpl5)),
    ];
    let in2 = Arc::new(Node::new(NodeData::Int(IntData::new(children2))));
    let mut in2_c = Some(Arc::new(Node::new(
                NodeData::Int(IntData::new(children2_c))
    )));
    let drpi2 = DRP::new(PBA{cluster: 0, lba: 101}, Compression::None, 0, 0, 0);

    // On-disk leaf node in the target zone
    let drpl8 = DRP::new(PBA{cluster: 0, lba: 102}, Compression::None, 0, 0, 0);
    let mut ld8 = LeafData::new();
    ld8.insert(16, 16.0);
    ld8.insert(17, 17.0);
    let mut ln8 = Some(Arc::new(Node::new(NodeData::Leaf(ld8))));

    let mut mock = DDMLMock::new();
    mock.expect_get::<Arc<Node<DRP, u32, f32>>>()
        .called_any()
        .with(passes(move |arg: & *const DRP| {
            unsafe {
            **arg == drpi1 || **arg == drpi2
        }} ))
        .returning(move |arg: *const DRP| {
            let res = Box::new( unsafe {
                if *arg == drpi1 {
                    in1.clone()
                } else if *arg == drpi2 {
                    in2.clone()
                } else {
                    panic!("unexpected DDMLMock::get {:?}", *arg);
                }
            });
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_times(4)
        .with(passes(move |args: &(*const DRP, TxgT)| unsafe {
            (*args.0 == drpl3 || *args.0 == drpi2 || *args.0 == drpl8 ||
            *args.0 == drpi1) && args.1 == 42
        } ))
        .returning(move |args: (*const DRP, TxgT)| {
            let res = Box::new( unsafe {
                if *args.0 == drpl3 {
                    ln3.take().unwrap()
                } else if *args.0 == drpi1 {
                    in1_c.take().unwrap()
                } else if *args.0 == drpi2 {
                    in2_c.take().unwrap()
                } else if *args.0 == drpl8 {
                    ln8.take().unwrap()
                } else {
                    panic!("unexpected DDMLMock::pop {:?}", *args.0);
                }
            });
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_put::<Arc<Node<DRP, u32, f32>>>()
        .called_times(3)
        .with(passes(|args: &(_, _, TxgT)| args.2 == 42))
        .returning(move |(_cacheable, _compression, _txg)| {
            let drp = DRP::random(Compression::None, 1024);
            (drp, Box::new(future::ok::<(), Error>(())))
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
                compression: None
                lsize: 0
                csize: 0
                checksum: 0
          - key: 4
            txgs:
              start: 8
              end: 21
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 4
                compression: None
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
                compression: None
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
                          compression: None
                          lsize: 0
                          csize: 0
                          checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let start = PBA::new(0, 100);
    let end = PBA::new(0, 200);
    let txgs = 20..30;
    rt.block_on(tree.clean_zone(start..end, txgs, 42)).unwrap();
}

// The Root node lies in the dirty zone
#[test]
fn dirty_root() {
    // On-disk internal root node in the target zone, but with children outside
    let drpl0 = DRP::new(PBA{cluster: 0, lba: 5}, Compression::None, 0, 0, 0);
    let drpl1 = DRP::new(PBA{cluster: 0, lba: 6}, Compression::None, 0, 0, 0);
    // We must make two copies of inr, one for DDMLMock::get and one for ::pop
    let children = vec![
        IntElem::new(8u32, 0..9, TreePtr::Addr(drpl0)),
        IntElem::new(10u32, 0..9, TreePtr::Addr(drpl1)),
    ];
    let children_c = vec![
        IntElem::new(8u32, 0..9, TreePtr::Addr(drpl0)),
        IntElem::new(10u32, 0..9, TreePtr::Addr(drpl1)),
    ];
    let inr = Arc::new(Node::<DRP, u32, f32>::new(
            NodeData::Int(IntData::new(children))));
    let mut inr_c = Some(Arc::new(Node::<DRP, u32, f32>::new(
                NodeData::Int(IntData::new(children_c))
    )));
    let drpir = DRP::new(PBA{cluster: 0, lba: 100}, Compression::None, 0, 0, 0);

    let mut mock = DDMLMock::new();
    mock.expect_get::<Arc<Node<DRP, u32, f32>>>()
        .called_any()
        .with(passes(move |arg: & *const DRP| unsafe { **arg == drpir } ))
        .returning(move |_arg: *const DRP| {
            let res = Box::new(inr.clone());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_times(1)
        .with(passes(move |args: &(*const DRP, TxgT)|
                     unsafe { *args.0 == drpir } && args.1 == 42)
        ).returning(move |_args: (*const DRP, TxgT)| {
            let res = Box::new(inr_c.take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_put::<Arc<Node<DRP, u32, f32>>>()
        .called_times(1)
        .with(passes(|args: &(_, _, TxgT)| args.2 == 42))
        .returning(move |(_cacheable, _compression, _txg)| {
            let drp = DRP::random(Compression::None, 1024);
            (drp, Box::new(future::ok::<(), Error>(())))
        });
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
    start: 0
    end: 42
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 100
      compression: None
      lsize: 0
      csize: 0
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let start = PBA::new(0, 100);
    let end = PBA::new(0, 200);
    let txgs = 1000..1001;  // XXX placeholder
    rt.block_on(tree.clean_zone(start..end, txgs, 42)).unwrap();
}
// LCOV_EXCL_STOP
