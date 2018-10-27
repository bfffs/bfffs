// vim: tw=80

use crate::common::*;
use isa_l;
use fixedbitset::FixedBitSet;
use std::borrow::BorrowMut;

/// An encoder/decoder for Reed-Solomon Erasure coding in GF(2^8), oriented
/// towards RAID applications
pub struct Codec {
    /// Total number of disks (or other storage devices) in the RAID stripe
    ///
    /// GF(2^8) works with up to 255 disks.
    m : u32,

    /// Redundancy level of the RAID array.
    ///
    /// This many disks may fail before the data becomes irrecoverable.
    f : u32,

    /// Encoding coefficients, aka the distribution matrix
    enc_matrix : Box<[u8]>,

    /// Encoding tables
    enc_tables: Box<[u8]>,
}

impl Codec {
    /// Initialize a new erasure codec
    ///
    /// # Parameters
    ///
    /// - `num_disks`:  Total number of disks (or other storage devices) in the
    ///                 RAID stripe.  May be up to 255.
    /// - `redundancy`: Redundancy level of the RAID array.  This many disks may
    ///                 fail before the data becomes irrecoverable.
    pub fn new(num_disks: u32, redundancy: u32) -> Self {
        let m = num_disks;
        let f = redundancy;
        let k = m - f;
        let mut enc_matrix = vec![0u8; (m * k) as usize].into_boxed_slice();
        let mut enc_tables = vec![0u8; (32 * k * f) as usize].into_boxed_slice();
        // Use Cauchy matrices instead of RS matrices because they guarantee
        // that all square submatrices are invertible.  That means that they can
        // provide any degree of redundancy, unlike RS matrices.   However, for
        // single-parity arrays an RS matrix produces parity information that is
        // compatible with a simple XOR-based codec.  An XOR codec is much
        // faster than ISA-L's erasure coding functions.  So use RS matrices for
        // single parity arrays for compatibility with a faster future codec.
        if f == 1 {
            isa_l::gf_gen_rs_matrix(&mut enc_matrix, m, k);
        } else {
            isa_l::gf_gen_cauchy1_matrix(&mut enc_matrix, m, k);
        }
        // The encoding tables only use the encoding matrix's parity rows (e.g.
        // rows k and higher)
        isa_l::ec_init_tables(k, f, &enc_matrix[(k*k) as usize ..],
                              &mut enc_tables);
        Codec {m, f, enc_matrix, enc_tables}
    }

    /// Verify parity and identify corrupt columns
    ///
    /// # Parameters
    /// - `len`:    Size of each column, in bytes
    /// - `data`:   Data array: `k` columns of `len` bytes each
    /// - `parity`: Parity array: `f` columns of `len` bytes each
    ///
    /// # Returns
    ///
    /// A bitset identifies which columns are corrupt.  A 1 indicates a corrupt
    /// column and a 0 indicates a healthy column.  If the parity does not
    /// verify successfully but it cannot be determined which column(s) are
    /// corrupt, then all bits will be set.  All bits set indicates that the row
    /// is irrecoverable without additional information.  Note that when the
    /// number of corrupt columns equals `f` the row will be considered
    /// irrecoverable even though the original data can still be recovered via
    /// combinatorial reconstruction.
    pub fn check(&self, _len: usize, _data: &[*const u8],
                 _parity: &[*const u8]) -> FixedBitSet {
        panic!("Unimplemented");
    }

    /// Reconstruct missing data from partial surviving columns
    ///
    /// Given a `Codec` with `m` total columns composed of `k` data columns and
    /// `f` parity columns, where one or more columns is missing, reconstruct
    /// the data from the missing columns.  Takes as a parameter exactly `k`
    /// surviving columns, even if more than `k` columns survive.  These *must*
    /// be the lowest `k` surviving columns.  For example, in a 5+3 array where
    /// the columns 0 and 3 are missing, Provide columns 1, 2, 4, 5, and 6 (data
    /// columns 1, 2, and 4 and parity columns 0 and 1).
    ///
    /// This method cannot reconstruct missing parity columns.  In order to
    /// reconstruct missing parity columns, you must first use this method to
    /// regenerate all data columns, *and then* use `encode` to recreate the
    /// parity.
    ///
    /// # Parameters
    ///
    /// - `len`:            Size of each column, in bytes
    /// - `surviving`:      Exactly `k` columns of surviving data and parity,
    ///                     sorted in order of the original column index, with
    ///                     data columns preceding parity columns.
    /// - `missing`:        Reconstructed data (not parity!) columns.  The
    ///                     number should be no more than the ones count of
    ///                     `erasures`.  Upon return, they will be populated
    ///                     with the original data of the missing columns.
    /// - `erasures`:       Bitmap of the column indices of the missing columns.
    pub fn decode(&self, len: usize, surviving: &[*const u8],
                       missing: &[*mut u8], erasures: &FixedBitSet) {
        let k = self.m - self.f;
        let errs = erasures.count_ones(..k as usize) as u32;
        assert!(errs > 0, "Only a fool would reconstruct an undamaged array!");
        let dec_tables = self.mk_decode_tables(erasures);
        isa_l::ec_encode_data(len, k, errs, &dec_tables, surviving, missing);
    }

    /// Generate parity columns from a complete set of data columns
    ///
    /// # Parameters
    /// - `len`:    Size of each column, in bytes
    /// - `data`:   Input array: `k` columns of `len` bytes each
    /// - `parity`: Storage for parity columns.  `f` columns of `len` bytes
    ///             each: will be populated upon return.
    pub fn encode(&self, len: usize, data: &[*const u8], parity: &[*mut u8]) {
        let k = self.m - self.f;
        isa_l::ec_encode_data(len, k, self.f, &self.enc_tables, data, parity);
    }

    /// Encode parity, using vectored input
    ///
    /// Like `encode`, but with discontiguous the data columns.
    ///
    /// # Parameters
    /// - `len`:    Size of each column, in bytes
    /// - `data`:   Input array: `k` columns of `len` bytes each.  They may be
    ///             discontiguous, and each may have a different structure.
    /// - `parity`: Storage for parity columns.  `f` columns of `len` bytes
    ///             each: will be populated upon return.
    pub fn encodev<T>(&self, len: usize, data: &[SGList],
                      parity: &mut [T])
                      where T : BorrowMut<[u8]> {
        let mut cursors : Vec<SGCursor> =
            data.iter()
                .map(|sglist| SGCursor::from(&sglist[..]))
                .collect();
        let mut l = 0;
        while l < len {
            let ncl =
                cursors.iter()
                       .map(|c| c.peek_len())
                       .min().unwrap();
            let (refs, _iovecs) : (Vec<_>, Vec<_>) =
                cursors.iter_mut()
                       .map(|sg| {
                           let iovec = sg.next(ncl).unwrap();
                           (iovec.as_ptr(), iovec)
                       })
                       .unzip();
            let prefs : Vec<*mut u8> =
                parity.iter_mut()
                      .map(|iov| iov.borrow_mut()[l..].as_mut_ptr())
                      .collect();
            self.encode(ncl, &refs, &prefs);
            l += ncl;
        }
    }

    /// Update parity columns from a single data column.
    ///
    /// This method can be used to progressively update a set of parity columns
    /// by feeding in one data column at a time.
    ///
    /// # Parameters
    /// - `len`:        Size of each column, in bytes
    /// - `data`:       Input array: a single column of `len` bytes
    /// - `parity`:     Storage for parity columns.  `f` columns of `len` bytes
    ///                 each: will be updated upon return.
    /// - `data_idx`:   Column index of the supplied data column.  Must lie in
    ///                 the range `[0, k)`.
    pub fn encode_update(&self, len: usize, data: &[u8], parity: &[*mut u8],
                         data_idx: u32) {
        let k = self.m - self.f;
        isa_l::ec_encode_data_update(len, k, self.f, data_idx, &self.enc_tables,
                                     data, parity);
    }

    // Generate tables for RAID decoding
    // Loosely based on erasure_code_perf.c from ISA-L's internal test suite
    // NB: For reasonably small values of m and f, it should be possible to
    // cache all possible decode tables.
    // Clippy bug https://github.com/rust-lang-nursery/rust-clippy/issues/3308
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::explicit_counter_loop))]
    fn mk_decode_tables(&self, erasures: &FixedBitSet) -> Box<[u8]> {
        let k : usize = (self.m - self.f) as usize;
        // Exclude missing parity columns from the list
        let errs : usize = erasures.count_ones(..k);
        let mut dec_tables = vec![0u8; 32 * k * errs].into_boxed_slice();

        // To generate the decoding matrix, first select k healthy rows from the
        // encoding matrix.
        let mut dec_matrix_inv = vec![0u8; k * k].into_boxed_slice();
        let mut skips = 0;
        for i in 0..k {
            while erasures.contains(i + skips) {
                skips += 1;
            }
            let row = i + skips;
            for j in 0..k {
                dec_matrix_inv[k * i + j] =
                    self.enc_matrix[k * row + j];
            }
        }
        // Then invert the result
        let mut dec_matrix = vec![0u8; k * k].into_boxed_slice();
        isa_l::gf_invert_matrix(&dec_matrix_inv, &mut dec_matrix, k as u32)
            .unwrap();
        // Finally, select the rows corresponding to missing data
        let mut dec_rows = vec![0u8; k * errs].into_boxed_slice();
        for (i, r) in erasures.ones().enumerate() {
            if r >= k {
                break;  // Exclude missing parity columns
            }
            for j in 0..k {
                dec_rows[k * i + j] =
                    dec_matrix[k * r + j];
            }
        }

        // Finally generate the fast encoding tables
        isa_l::ec_init_tables(k as u32, errs as u32, &dec_rows, &mut dec_tables);
        dec_tables
    }

    /// Return the degree of redundancy
    pub fn protection(&self) -> i16 {
        self.f as i16
    }

    /// Return the total number of disks in the raid stripe
    pub fn stripesize(&self) -> i16 {
        self.m as i16
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;

    // If the encoding matrix ever changes, it will change the on-disk format.
    // Generate several different encoding matrices and compare them against
    // golden masters
    #[test]
    fn format_stability() {
        let testpairs = [
            (3, 1, vec![1,   0,
                        0,   1,
                        1,   1]),
            (5, 1, vec![1,   0,   0,   0,
                        0,   1,   0,   0,
                        0,   0,   1,   0,
                        0,   0,   0,   1,
                        1,   1,   1,   1]),
            (5, 2, vec![1,   0,   0,
                        0,   1,   0,
                        0,   0,   1,
                      244, 142,   1,
                       71, 167,  122]),
            (7, 3, vec![1,   0,   0,   0,
                        0,   1,   0,   0,
                        0,   0,   1,   0,
                        0,   0,   0,   1,
                       71, 167, 122, 186,
                      167,  71, 186, 122,
                      122, 186,  71, 167]),
            (15, 5, vec![1,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                         0,   1,   0,   0,   0,   0,   0,   0,   0,   0,
                         0,   0,   1,   0,   0,   0,   0,   0,   0,   0,
                         0,   0,   0,   1,   0,   0,   0,   0,   0,   0,
                         0,   0,   0,   0,   1,   0,   0,   0,   0,   0,
                         0,   0,   0,   0,   0,   1,   0,   0,   0,   0,
                         0,   0,   0,   0,   0,   0,   1,   0,   0,   0,
                         0,   0,   0,   0,   0,   0,   0,   1,   0,   0,
                         0,   0,   0,   0,   0,   0,   0,   0,   1,   0,
                         0,   0,   0,   0,   0,   0,   0,   0,   0,   1,
                       221, 152, 173, 157,  93, 150,  61, 170, 142, 244,
                       152, 221, 157, 173, 150,  93, 170,  61, 244, 142,
                        61, 170,  93, 150, 173, 157, 221, 152,  71, 167,
                       170,  61, 150,  93, 157, 173, 152, 221, 167,  71,
                        93, 150,  61, 170, 221, 152, 173, 157, 122, 186]),
        ];
        for triple in testpairs.iter() {
            let m = triple.0;
            let f = triple.1;
            let encmat = &triple.2;
            let codec = Codec::new(m, f);
            assert_eq!(&encmat.deref(), &codec.enc_matrix.deref());
        }
    }
}
// LCOV_EXCL_STOP
