#[cfg(test)] mod clean_zone;
#[cfg(test)] mod in_mem;
#[cfg(test)] mod io;
#[cfg(test)] mod txg;

use common::tree::*;

#[test]
fn ranges_overlap_test() {
    assert!(!ranges_overlap(&(0..10), &(10..20)));
    assert!(ranges_overlap(&(0..10), &(9..10)));
    assert!(ranges_overlap(&(9..10), &(0..10)));
    assert!(!ranges_overlap(&(10..20), &(0..10)));
    assert!(ranges_overlap(&(0..10), &(9..11)));
    assert!(ranges_overlap(&(9..11), &(0..10)));
}
