#[cfg(test)] mod clean_zone;
#[cfg(test)] mod in_mem;
#[cfg(test)] mod io;
#[cfg(test)] mod txg;

use crate::common::tree::*;

#[test]
fn ranges_overlap_test() {
    // x is empty
    assert!(!ranges_overlap(&(5..5), &(0..10)));
    assert!(!ranges_overlap(&(5..5), &(0..10)));
    // y is empty
    assert!(!ranges_overlap(&(0..10), &(5..5)));
    // x precedes y
    assert!(!ranges_overlap(&(0..10), &(10..20)));
    assert!(!ranges_overlap(&(..10), &(10..20)));
    // x contains y
    assert!(ranges_overlap(&(0..10), &(9..10)));
    assert!(ranges_overlap(&(..), &(9..10)));
    // y contains x
    assert!(ranges_overlap(&(9..10), &(0..10)));
    assert!(ranges_overlap(&(9..=10), &(0..10)));
    // y precedes x
    assert!(!ranges_overlap(&(10..20), &(0..10)));
    assert!(!ranges_overlap(&(10..), &(0..10)));
    // end of x overlaps start of y
    assert!(ranges_overlap(&(0..10), &(9..11)));
    assert!(ranges_overlap(&(..=10), &(10..20)));
    // end of y overlaps start of x
    assert!(ranges_overlap(&(9..11), &(0..10)));
    assert!(ranges_overlap(&(9..), &(0..10)));
}
