// vim: tw=80

use isa_l;
use fixedbitset::FixedBitSet;

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
        isa_l::gf_gen_rs_matrix(&mut enc_matrix, m, k);
        // The encoding tables only use the encoding matrix's parity rows (e.g.
        // rows k and higher)
        isa_l::ec_init_tables(k, f, &enc_matrix[(k*k) as usize ..],
                              &mut enc_tables);
        Codec {m: m, f: f, enc_matrix: enc_matrix, enc_tables: enc_tables}
    }

    /// Reconstruct missing data and parity from partial surviving columns
    ///
    /// Given a `Codec` with `m` total columns composed of `k` data columns and
    /// `f` parity columns, where one or more columns is missing, reconstruct
    /// the data from the missing columns.  Takes as a parameter exactly `k`
    /// surviving columns, even if more than `k` columns survive.  These *must*
    /// be the lowest `k` surviving columns.  For example, in a 5+3 array where
    /// the columns 0 and 3 are missing, Provide columns 1, 2, 4, 5, and 6 (data
    /// columns 1, 2, and 4 and parity columns 0 and 1).
    ///
    /// # Parameters
    ///
    /// - `len`:            Size of each column, in bytes
    /// - `surviving`:      Exactly `k` columns of surviving data and parity,
    ///                     sorted in order of the original column index, with
    ///                     data columns preceding parity columns.
    /// - `missing`:        Reconstructed data and parity columns.  The number
    ///                     should be equal to the ones count of `erasures`.
    ///                     Upon return, they will be populated with the
    ///                     original data of the missing columns.
    /// - `erasures`:       Bitmap of the column indices of the missing columns.
    pub fn decode(&self, len: usize, surviving: &[&[u8]],
                       missing: &mut [&mut [u8]], erasures: FixedBitSet) {
        let k = self.m - self.f;
        let errs = erasures.count_ones(..) as u32;
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
    pub fn encode(&self, len: usize, data: &[&[u8]], parity: &mut [&mut [u8]]) {
        let k = self.m - self.f;
        isa_l::ec_encode_data(len, k, self.f, &self.enc_tables, data, parity);
    }

    // Generate tables for RAID decoding
    // Loosely based on erasure_code_perf.c from ISA-L's internal test suite
    // NB: For reasonably small values of m and f, it should be possible to cache
    // all possible decode tables.
    fn mk_decode_tables(&self, erasures: FixedBitSet) -> Box<[u8]> {
        let k : usize = (self.m - self.f) as usize;
        let errs : usize = erasures.count_ones(..);
        let mut dec_tables = vec![0u8; 32 * k * errs].into_boxed_slice();

        // To generate the decoding matrix, first select k healthy rows from the
        // encoding matrix.
        let mut dec_matrix_inv = vec![0u8; k * k].into_boxed_slice();
        let mut skips = 0;
        for i in 0..k {
            while erasures.contains(i + skips) {
                skips += 1;
            }
            for j in 0..i {
                dec_matrix_inv[k * i + j] =
                    self.enc_matrix[k * (i + skips) + j];
            }
        }
        // Then invert the result
        let mut dec_matrix = vec![0u8; k * k].into_boxed_slice();
        isa_l::gf_invert_matrix(&dec_matrix_inv, &mut dec_matrix, k as u32)
            .unwrap();
        // Finally, select the rows corresponding to missing data
        let mut dec_rows = vec![0u8; k * errs].into_boxed_slice();
        for (i, r) in erasures.ones().enumerate() {
            for j in 0..k {
                dec_rows[k * i + j] =
                    dec_matrix[k * r + j];
            }
        }

        // Finally generate the fast encoding tables
        isa_l::ec_init_tables(k as u32, errs as u32, &dec_rows, &mut dec_tables);
        dec_tables
    }
}
