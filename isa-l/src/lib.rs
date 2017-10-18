// vim: tw=80
mod ffi;

use std::io::{Error, ErrorKind};
use std::os::raw::*;
use std::result::Result;

pub fn version() -> u32 {
    ffi::ISAL_MAJOR_VERSION * 0x10000 +
    ffi::ISAL_MINOR_VERSION * 0x100 +
    ffi::ISAL_PATCH_VERSION
}

/// Generate or decode erasure codes on blocks of data.
///
/// Given a list of source data blocks, generate one or multiple blocks of
/// encoded data as specified by a matrix of GF(2^8) coefficients. When given a
/// suitable set of coefficients, this function will perform the fast generation
/// or decoding of Reed-Solomon type erasure codes.
///
/// # Parameters
///
/// - `len`:    Length of each block of data (vector) of source or dest data.
/// - `k`:      The number of vector sources or rows in the generator matrix for
///             coding.
/// - `f`:      The number of output vectors to concurrently encode/decode.
/// - `gftbls`: Pointer to array of input tables generated from coding
///             coefficients in ec_init_tables(). Must be of size `32×k×f`
/// - `data`:   Array of input vectors.  Must `k` vectors each of size `len`.
/// - `parity`: Array of output vectors for parity columns.  Must be `f` vectors
///             each of size `len`.
pub fn ec_encode_data(len: usize, k: u32, f: u32, gftbls: &[u8],
                      data: &[*const u8], parity: &[*mut u8]) {
    assert_eq!(gftbls.len(), (32 * f * k) as usize);
    assert_eq!(data.len(), k as usize);
    assert_eq!(parity.len(), f as usize);

    unsafe {
        // Note: isa-l defines gftbls and data as non-const, even though the
        // implementation doesn't modify them
        ffi::ec_encode_data(len as c_int, k as c_int, f as c_int,
                            gftbls.as_ptr() as *mut c_uchar,
                            data.as_ptr() as *mut *mut c_uchar,
                            parity.as_ptr() as *mut *mut c_uchar);
    }
}

/// Generate update for encode or decode of erasure codes from single source.
///
/// Given one source data block, update one or multiple blocks of encoded data as
/// specified by a matrix of GF(2^8) coefficients. When given a suitable set of
/// coefficients, this function will perform the fast generation or decoding of
/// Reed-Solomon type erasure codes from one input source at a time.
///
/// # Parameters
///
/// - `len`:    Length of each block of data (vector) of source or dest data.
/// - `k`:      The number of vector sources or rows in the generator matrix
///             for coding.
/// - `f`:      The number of output vectors to concurrently encode/decode.
/// - `vec_i`:  The vector index corresponding to the single input source.  Must
///             lie in the range `[0, k)`.
/// - `gftbls`: Pointer to array of input tables generated from coding
///             coefficients in ec_init_tables(). Must be of size `32×k×f`
/// - `data`:   Array of single input column used to update parity.  Must be of
///             size `len`.
/// - `parity`: Two-dimensional array of pointers to coded output buffers.  Must
///             be of size `f×len`.
///
pub fn ec_encode_data_update(len: usize,
                             k: u32,
                             f: u32,
                             vec_i: u32,
                             gftbls: &[u8],
                             data: &[u8],
                             parity: &[*mut u8]) {
    assert_eq!(gftbls.len(), (32 * f * k) as usize);
    assert_eq!(data.len(), len);
    assert_eq!(parity.len(), f as usize);
    assert!(vec_i < k);

    unsafe {
        // Note: isa-l defines gftbls and data as non-const, even though the
        // implementation doesn't modify them
        ffi::ec_encode_data_update(len as c_int, k as c_int, f as c_int,
                                   vec_i as c_int,
                                   gftbls.as_ptr() as *mut c_uchar,
                                   data.as_ptr() as *mut c_uchar,
                                   parity.as_ptr() as *mut *mut c_uchar);
    }
}

/// Initialize tables for fast Erasure Code encode and decode.
///
/// Generates the expanded tables needed for fast encode or decode for erasure
/// codes on blocks of data.  32bytes is generated for each input coefficient.
///
/// # Parameters
///
/// - `k`:      The number of vector sources or rows in the generator matrix for
///             coding.
/// - `f`:      The number of output vectors to concurrently encode/decode.
/// - `a`:      Pointer to sets of arrays of input coefficients used to encode
///             or decode data.  Must be of size `
/// - `gftbls`: Pointer to start of space for concatenated output tables
///             generated from input coefficients.  Must be of size `32×k×f`.
pub fn ec_init_tables(k: u32, f: u32, a: &[u8], gftbls: &mut [u8]) {
    assert_eq!(a.len(), (f * k) as usize);
    assert_eq!(gftbls.len(), (32 * f * k) as usize);
    unsafe {
        // Note: isa-l defines a as non-const, even though the implementation
        // doesn't modify it.
        ffi::ec_init_tables(k as c_int, f as c_int, a.as_ptr() as *mut c_uchar,
                            gftbls.as_mut_ptr() as *mut c_uchar);
    }
}

/// Generate a Cauchy matrix of coefficients to be used for encoding.
///
/// Cauchy matrix example of encoding coefficients where high portion of matrix
/// is identity matrix I and lower portion is constructed as 1/(i + j) | i != j,
/// i:{0,k-1} j:{k,m-1}.  Any sub-matrix of a Cauchy matrix should be invertable.
///
/// # Parameters
///
/// - `a`:  `[m × k]` array to hold coefficients
/// - `m`:  number of rows in matrix corresponding to srcs + parity.
/// - `k`:  number of columns in matrix corresponding to srcs.
pub fn gf_gen_cauchy1_matrix(a: &mut [u8], m: u32, k: u32) {
    assert_eq!(a.len(), (m * k) as usize);
    unsafe {
        ffi::gf_gen_cauchy1_matrix(a.as_mut_ptr() as *mut c_uchar,
                                   m as c_int, k as c_int);
    }
}

/// Generate a matrix of coefficients to be used for encoding.
///
/// Vandermonde matrix example of encoding coefficients where high portion of
/// matrix is identity matrix I and lower portion is constructed as 2^{i*(j-k+1)}
/// i:{0,k-1} j:{k,m-1}. Commonly used method for choosing coefficients in
/// erasure encoding but does not guarantee invertable for every sub matrix. For
/// large pairs of m and k it is possible to find cases where the decode matrix
/// chosen from sources and parity is not invertable. Users may want to adjust
/// for certain pairs m and k. If m and k satisfy one of the following
/// inequalities, no adjustment is required:
///
/// - `k <= 3`
/// - `k = 4, m <= 25`
/// - `k = 5, m <= 10`
/// - `k <= 21, m-k = 4`
/// - `m - k <= 3`
///
/// # Parameters
/// - `a`:  `[m × k]` array to hold coefficients
/// - `m`:  number of rows in matrix corresponding to srcs + parity.
/// - `k`:  number of columns in matrix corresponding to srcs.
pub fn gf_gen_rs_matrix(a: &mut [u8], m: u32, k: u32) {
    assert_eq!(a.len(), (m * k) as usize);
    assert!( ( k <= 3 ) ||
             ( k == 4 && m <= 25 ) ||
             ( k == 5 && m <= 10 ) ||
             ( k <= 21 && m - k == 4) ||
             ( m - k <= 3 ), "Matrix not guaranteed to be invertible!");
    unsafe {
        ffi::gf_gen_rs_matrix(a.as_mut_ptr() as *mut c_uchar,
                              m as c_int, k as c_int);
    }
}

/// Invert a matrix in GF(2^8)
///
/// # Parameters
///
/// - `input`: input matrix.  A two-dimensional square array.
/// - `output`:   output matrix such that `[input] × [output] = [I]`
/// - `n`:     size of matrix `[n × n]`
///
/// # Returns
///
/// `()` on success, or one of these errors on failure:
/// - `InvalidData`:   The input matrix was singular
pub fn gf_invert_matrix(input: &[u8], output: &mut [u8],
                        n: u32) -> Result<(), Error> {
    assert_eq!(input.len(), (n * n) as usize);
    assert_eq!(output.len(), (n * n) as usize);
    if 0 == unsafe {
        ffi::gf_invert_matrix(input.as_ptr() as *mut c_uchar,
                              output.as_mut_ptr() as *mut c_uchar,
                              n as c_int)
    } {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::InvalidData, "Singular matrix"))
    }
}
