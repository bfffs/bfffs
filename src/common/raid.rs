// vim: tw=80

use isa_l;

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

    /// Encoding coefficients
    coeffs : Box<[u8]>,

    /// Encoding tables
    enctables: Box<[u8]>,
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
        let mut coeffs = vec![0u8; (m * k) as usize].into_boxed_slice();
        let mut enctables = vec![0u8; (32 * k * f) as usize].into_boxed_slice();
        isa_l::gf_gen_rs_matrix(&mut coeffs, m, k);
        isa_l::ec_init_tables(k, f, &coeffs[(k*k) as usize ..], &mut enctables);
        Codec {m: m, f: f, coeffs: coeffs, enctables: enctables}
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
        isa_l::ec_encode_data(len, k, self.f, &self.enctables, data, parity);
    }
}
