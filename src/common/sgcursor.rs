// vim: tw=80

use common::*;

/// A Cursor type that can iterate over `SGList`s.
///
/// This structure can be used to iterate through an `SGList` as a series of
/// `IoVec`s, but not necessarily with the same boundaries as the `SGList` is
/// composed of.  It has two main purposes:
///
/// - Immutably iterate through several `SGList`s simultaneously.  This is
///   necessary for calculating parity when the data columns are `SGList`s
///   instead of `IoVec`s.
/// - Transform an `SGList` into another `SGList` with `IoVec` boundaries at
///   chosen locations (plus the original boundaries, too).  This is necessary
///   for splitting an `SGList` up into multiple columns.
pub struct SGCursor<'a> {
    sglist: &'a SGList,
    sglist_idx: usize,
    iovec_idx: usize
}

impl<'a> SGCursor<'a> {
    /// Return a contiguous segment from the beginning of the Cursor.
    ///
    /// It will be at most `max` bytes long, but it may be less.  If the
    /// `SGCursor` is empty, `None` will be returned
    pub fn next(&mut self, max: usize) -> Option<IoVec> {
        let ncl = self.peek_len();
        if ncl == 0 {
            None
        } else if max < ncl {
            let b = self.iovec_idx;
            let e = b + max;
            let iovec = Some(self.sglist[self.sglist_idx].slice(b, e));
            self.iovec_idx += max;
            iovec
        } else if self.iovec_idx > 0 {
            let b = self.iovec_idx;
            let iovec = Some(self.sglist[self.sglist_idx].slice_from(b));
            self.iovec_idx = 0;
            self.sglist_idx += 1;
            iovec
        } else {    // LCOV_EXCL_LINE   kcov false negative
            let r = Some(self.sglist[self.sglist_idx].clone());
            self.sglist_idx += 1;
            r
        }   // LCOV_EXCL_LINE   kcov false negative
    }

    /// Return the length of the next contiguous segment.
    ///
    /// This will be the length of the next segment returned by
    /// `next(usize::max_value())`
    pub fn peek_len(&self) -> usize {
        if self.sglist_idx < self.sglist.len() {
            self.sglist[self.sglist_idx].len() - self.iovec_idx
        } else {
            0
        }
    }
}

impl<'a> From<&'a SGList> for SGCursor<'a> {
    fn from(src: &'a SGList) -> SGCursor {
        SGCursor { sglist: src, sglist_idx: 0, iovec_idx: 0}
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    const MAX: usize = usize::max_value();

    #[test]
    pub fn test_multisegment() {
        let dbs0 = DivBufShared::from(vec![0, 1, 2, 3, 4]);
        let dbs1 = DivBufShared::from(vec![5, 6, 7, 8, 9]);
        let dbs2 = DivBufShared::from(vec![10, 11, 12, 13, 14]);
        let db0 = dbs0.try().unwrap();
        let db1 = dbs1.try().unwrap();
        let db2 = dbs2.try().unwrap();
        let sglist : SGList = vec![db0, db1, db2];
        let mut cursor = SGCursor::from(&sglist);
        assert_eq!(cursor.peek_len(), 5);
        assert_eq!(&cursor.next(MAX).unwrap()[..], &[0, 1, 2, 3, 4][..]);
        assert_eq!(cursor.peek_len(), 5);
        assert_eq!(&cursor.next(2).unwrap()[..], &[5, 6][..]);
        assert_eq!(cursor.peek_len(), 3);
        assert_eq!(&cursor.next(3).unwrap()[..], &[7, 8, 9][..]);
        assert_eq!(cursor.peek_len(), 5);
        assert_eq!(&cursor.next(MAX).unwrap()[..], &[10, 11, 12, 13, 14][..]);
        assert_eq!(cursor.peek_len(), 0);
        assert_eq!(cursor.next(MAX), None);
    }

    #[test]
    pub fn test_null() {
        let sglist = SGList::new();
        let mut cursor = SGCursor::from(&sglist);
        assert_eq!(cursor.peek_len(), 0);
        assert_eq!(cursor.next(MAX), None);
    }

    #[test]
    pub fn test_one_segment() {
        let dbs = DivBufShared::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let divbuf = dbs.try().unwrap();
        let sglist : SGList = vec![divbuf.clone()];
        {
            let mut cursor = SGCursor::from(&sglist);
            assert_eq!(cursor.peek_len(), 10);
            assert_eq!(cursor.next(MAX).unwrap(), divbuf);
            assert_eq!(cursor.next(MAX), None);
        }
        // Now try smaller accesses
        {
            let mut cursor = SGCursor::from(&sglist);
            assert_eq!(cursor.peek_len(), 10);
            assert_eq!(&cursor.next(3).unwrap()[..], &[0, 1, 2][..]);
            assert_eq!(cursor.peek_len(), 7);
            assert_eq!(&cursor.next(3).unwrap()[..], &[3, 4, 5][..]);
            assert_eq!(cursor.peek_len(), 4);
            assert_eq!(&cursor.next(4).unwrap()[..], &[6, 7, 8, 9][..]);
            assert_eq!(cursor.next(MAX), None);
        }
    }
}
// LCOV_EXCL_STOP
