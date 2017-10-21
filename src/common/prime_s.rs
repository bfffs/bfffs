// vim: tw=80

use common::declust::*;

///
/// Return the multiplicative inverse of a, mod n.  n must be prime.
///
/// Use the extended Euclidean algorithm.  Since n is always prime for the
/// PRIME-S algorithm, we could use Fermat's little theorem instead, but I'm not
/// sure it would be any faster.
fn invmod(a: i16, n: i16) -> i16 {
	let mut t = 0;
	let mut r = n;
	let mut newt = 1;
	let mut newr = a;

	while newr > 0 {
		let q = r / newr;
		let mut temp = newt;
		newt = t - q * newt;
		t = temp;
		temp = newr;
		newr = r - q * newr;
		r = temp;
	}

	assert_eq!(r, 1);

	if t < 0 {
		t = t + n;
    }

	t
}

/// A simple primality tester.  Optimized for size, not speed
fn is_prime(n: i16) -> bool {
    if n <= 1 {
		return false;
    } else if n <= 3 {
		return true;
    } else if n % 2 == 0 || n % 3 == 0 {
		return false;
    }
    let mut i = 5;
    while i * i <= n {
		if n % i == 0 || n % (i + 2) == 0 {
			return false;
        }
        i = i + 6;
	}
	return true;
}

/// PRIME-S: PRIME-Sequential declustering
/// 
/// This class implements a variation of the PRIME algorithm, described by
/// Alvarez, et al.  Our variation leaves PRIME's disk and check-disk functions
/// untouched, but modifies the offset and check-offset functions to guarantee
/// that the layout will be monotonic.  That is, the stripe units of each disk
/// are layed out in monotonically increasing order according to their user
/// LBAs.
/// 
/// The method for the modification was basically to sort the stripe units of
/// each iteration of each disk.  The data stripe units were already sorted, so
/// all we have to do is to sort the check stripe units, then virtually merge
/// sort the two lists.  See pages 93-97 of Alan's notebook for details.  See
/// tools/declust.rb for a prototype implementation.
/// 
/// This class internally uses the same terminology as the Alvarez paper.  Here
/// is the glossary:
/// 
/// - `z`:      iteration number
/// - `y`:      stride
/// - `n`:      Number of disks in the layout
/// - `k`:      Number of disks in each stripe
/// - `m`:      Number of data disks in each stripe
/// - `f`:      Number of parity disks in each stripe
/// - `rowsize: Number of bytes in each stripe unit
/// - `chunk:   A stripe unit
/// 
/// # References
///
/// Alvarez, Guillermo A., et al. "Declustered disk array architectures with
/// optimal and near-optimal parallelism." ACM SIGARCH Computer Architecture
/// News. Vol. 26. No. 3. IEEE Computer Society, 1998.
/// 
struct PrimeS {
    /// Total number of disks
    n:  i16,

    /// Number of disks per stripe (data & parity)
    k:  u8,

    /// Number of data disks per stripe
    m:  u8,

    /// Multiplicative inverse of m, mod n
    m_inv: i16,

    /// Protection level
    f:  u8
}

impl PrimeS {
    /// Create a new PrimeS Locator
    ///
    /// # Parameters
    ///
    /// `num_disks`:        Total number of disks in the array
    /// `disks_per_strip`:  Number of disks in each parity group  
    /// `redundancy`:       Redundancy level of the RAID array.  This many disks
    ///                     may fail before the data becomes irrecoverable.
    pub fn new(num_disks: i16, disks_per_stripe: u8, redundancy: u8) -> Self {
        assert!(is_prime(num_disks));
        let m = disks_per_stripe - redundancy;
        let m_inv = invmod(m as i16, num_disks);
        PrimeS {n: num_disks, k: disks_per_stripe, m: m, m_inv: m_inv,
                f: redundancy}
    }
}

impl Locator for PrimeS {
    fn id2loc(&self, id: ChunkId) -> Chunkloc {

    }

    fn loc2id(&self, loc: Chunkloc) -> ChunkId {
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invmod() {
        assert_eq!(invmod(2, 3), 2);
        assert_eq!(invmod(2, 5), 3);
        assert_eq!(invmod(3, 11), 4);
        assert_eq!(invmod(17, 19), 9);
        assert_eq!(invmod(128, 251), 151);
    }

    #[test]
    fn test_is_prime() {
        assert!(!is_prime(0));
        assert!(!is_prime(1));
        assert!(is_prime(2));
        assert!(is_prime(3));
        assert!(!is_prime(4));
        assert!(is_prime(5));
        // 49 is the smallest composite number not divisble by any number that
        // gets special treatment in is_prime
        assert!(!is_prime(49));
        // Greatest prime that fits in u8
        assert!(is_prime(251));
    }
}
