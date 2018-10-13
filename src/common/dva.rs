// vim: tw=80
use common::*;
use std::ops::{Add, AddAssign, Sub, SubAssign};

/// Data Virtual Address for BFFFS.  Each DVA uniquely identifies the physical
/// location of a record.
// LCOV_EXCL_START
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Dva {
    /// Cluster id.  Analogous to a ZFS top-level vdev
    // NB: This element must come first so Ord can be Derived!
    cluster : ClusterT,

    /// Logical fragment address.
	///
	///Like an LBA, but denominated in fragments instead of blocks
	lfa : u64,
}
// LCOV_EXCL_STOP

impl<T: Into<i64>> Add<T> for Dva {
    type Output = Dva;

    /// Add a byte offset to a Dva
    fn add(self, other: T) -> Dva {
        let mut x = self;
        x += other.into();
        x
    }
}

impl AddAssign<i64> for Dva {
    /// Add a byte offset to this Dva
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::suspicious_op_assign_impl))]
    fn add_assign(&mut self, other: i64) {
        assert_eq!(other % BYTES_PER_FRAGMENT as i64, 0,
                   "Cannot add fractional fragment");
        let delta = other / BYTES_PER_FRAGMENT as i64;
        debug_assert!(if delta > 0 {
                self.lfa.checked_add(delta as u64)
            } else {
                self.lfa.checked_sub(-delta as u64)
            }.is_some());
            
        self.lfa += delta as u64;
    }
}

impl<T: Into<i64>> Sub<T> for Dva {
    type Output = Dva;

    /// Subtract a byte offset to a Dva
    fn sub(self, other: T) -> Dva {
        let mut x = self;
        x -= other.into();
        x
    }
}

impl SubAssign<i64> for Dva {
    /// Subtract a byte offset from this Dva
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::suspicious_op_assign_impl))]
    fn sub_assign(&mut self, other: i64) {
        assert_eq!(other % BYTES_PER_FRAGMENT as i64, 0,
                   "Cannot add fractional fragment");
        let delta = other / BYTES_PER_FRAGMENT as i64;
        debug_assert!(if delta < 0 {
                self.lfa.checked_add(-delta as u64)
            } else {
                self.lfa.checked_sub(delta as u64)
            }.is_some());
            
        self.lfa -= delta as u64;
    }
}


// LCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_overflow() {
        let x = Dva { cluster: 0, lfa: u64::max_value() - 10 };
        let max = x + (10 * BYTES_PER_FRAGMENT) as i64;
        assert_eq!(max.lfa, u64::max_value());
    }

    #[test]
    #[should_panic]
    fn overflow() {
        let x = Dva { cluster: 0, lfa: u64::max_value() - 10 };
        let _ = x + (11 * BYTES_PER_FRAGMENT) as i64;
    }

    #[test]
    fn no_underflow() {
        let x = Dva { cluster: 0, lfa: 10 };
        let min = x - (10 * BYTES_PER_FRAGMENT) as i64;
        assert_eq!(min.lfa, u64::min_value());
    }
    #[test]
    #[should_panic]
    fn underflow() {
        let x = Dva { cluster: 0, lfa: 10 };
        let _ = x - (11 * BYTES_PER_FRAGMENT) as i64;
    }
}
// LCOV_EXCL_STOP
