// vim: tw=80

use fixedbitset::FixedBitSet;
use common::declust::*;
use modulo::Mod;

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
		t += n;
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
        i += 6;
	}
	true
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
pub struct PrimeS {
    /// Total number of disks
    n:  i16,

    /// Number of disks per stripe (data & parity)
    ///
    /// Valid range for GF(2^8) encoding is [0, 256)
    k:  i16,

    /// Number of data disks per stripe
    ///
    /// Valid range for GF(2^8) encoding is [0, 256)
    m:  i16,

    /// Multiplicative inverse of m, mod n
    m_inv: i16,

    /// Protection level
    f:  i16,

    // Cache the results of some common calculations
    datachunks: u64,
    stripes: u32,
    depth: i16
}

impl PrimeS {
    /// Create a new PrimeS Locator
    ///
    /// # Parameters
    ///
    /// `num_disks`:        Total number of disks in the array
    /// `disks_per_stripe`: Number of disks in each parity group  
    /// `redundancy`:       Redundancy level of the RAID array.  This many disks
    ///                     may fail before the data becomes irrecoverable.
    pub fn new(num_disks: i16, disks_per_stripe: i16, redundancy: i16) -> Self {
        assert!(is_prime(num_disks));
        let m = disks_per_stripe - redundancy;
        let m_inv = invmod(m as i16, num_disks);
        let stripes = num_disks as u32 * (num_disks as u32 - 1);
        let datachunks = u64::from(stripes) * m as u64;
        let depth = disks_per_stripe * (num_disks - 1);
        PrimeS {n: num_disks, k: disks_per_stripe, m, m_inv,
                f: redundancy, stripes, datachunks, depth}
    }

    /// Return the repetition and iteration numbers where a given Chunk is stored
    fn id2rep_and_iter(&self, chunkid: &ChunkId) -> (u32, i16) {
        let id = match *chunkid {
            ChunkId::Data(id) | ChunkId::Parity(id, _) => id
        };
        // Good candidate for a combined division-modulo operation, if one ever
        // gets added
        // https://github.com/rust-lang/rfcs/pull/2169
        let rep = id / self.datachunks;
        let iter = id.modulo(self.datachunks) / (self.m as u64 * self.n as u64);
        assert!(rep <= u64::from(u32::max_value()));
        assert!(iter <= u64::from(i16::max_value() as u16));
        (rep as u32, iter as i16)
    }
}

impl Locator for PrimeS {
    fn clustsize(&self) -> i16 {
        self.n
    }

    fn datachunks(&self) -> u64 {
        self.datachunks
    }

    fn depth(&self) -> i16 {
        self.depth
    }

    fn id2loc(&self, chunkid: ChunkId) -> Chunkloc {
        // The repetition and iteration
        let (r, z) = self.id2rep_and_iter(&chunkid);
        // The stride
        let y = (z.modulo(self.n - 1)) + 1;
        let id = match chunkid {
            ChunkId::Data(id) | ChunkId::Parity(id, _) => id,
        };
        let a = id.modulo(self.datachunks) as i16;
        // The stripe
        let s = a / self.m;
        // Unit's position within its stripe
        let b = match chunkid {
            ChunkId::Data(_) => a - s * self.m,
            ChunkId::Parity(_, i) => self.m + i
        };
        let disk = ((s * self.m + b) * y).modulo(self.n);

        // We must loop to calculate the offset.  That's PRIME-S's disadvantage
        // vis-a-vis PRIME
        let y_inv = invmod(y, self.n);
        // Contribution to offset from data chunks in this iteration
        let o0 = (s * self.m + b) / self.n;
        // Contributions to offset from parity chunks in previous iterations
        let o1 = self.f * z;
        // Contributions to offset from parity chunks in this iteration
        let o2 = (0 .. self.f).fold(0, |acc, j| {
                let cb_stripe = ((disk * y_inv - j) * self.m_inv - 1).modulo(self.n);
                let x = if s.modulo(self.n) > cb_stripe {
                    1
                } else {
                    0
                };
                x + acc
            });
        let o3 = u64::from(r) * self.depth as u64;

        let offset = (o0 + o1 + o2) as u64 + o3;
        Chunkloc { disk, offset}
    }

    fn loc2id(&self, chunkloc: Chunkloc) -> ChunkId {
        // Algorithm:
        // Generate the set of stripes that are stored on this iteration of this
        // disk.  The offsetth one will be the stripe we want.  Then use the
        // disk formula to find b, which will determine the address, whether
        // it's a check unit, and the check index.

        // repetition
        let r = (chunkloc.offset / self.depth as u64) as u32;
        // Offset relative to start of repetition
        let offset = chunkloc.offset.modulo(self.depth as u64) as i16;
        // iteration
        let z = offset / self.k;
        // stride
        let y = z.modulo(self.n - 1) + 1;
        // inverse of the stride, mod n
        let y_inv = invmod(y, self.n);
        let disk = chunkloc.disk;

        // Stripes using this disk, within this iteration
        let mut stripes = FixedBitSet::with_capacity(self.n as usize);
        for i in 0..self.k {
            stripes.insert(((disk * y_inv - i) * self.m_inv).modulo(self.n) as usize);
        }
        let offset_in_iter = offset - self.k * z;
        // stripe
        let s = stripes.ones().nth(offset_in_iter as usize).unwrap() as i16+ self.n * z;
        // position of stripe unit within stripe
        let b = (disk * y_inv - s * self.m).modulo(self.n);
        // number of data chunks preceding this repetition
        let o = u64::from(r) * self.datachunks() as u64;
        if b >= self.m {
            ChunkId::Parity(o + (s * self.m) as u64, b - self.m)
        } else {
            ChunkId::Data(o + (s * self.m + b) as u64)
        }
    }

    fn parallel_read_count(&self, consecutive_data_chunks: usize) -> usize {
        // As given in page 5 of Alvarez et al's PRIME paper:
        // τ(ο) ≤ ⌈ο/n⌉ + 1
        (consecutive_data_chunks + self.n as usize - 1) / self.n as usize + 1
    }

    fn protection(&self) -> i16 {
        self.f
    }

    fn stripes(&self) -> u32 {
        self.stripes
    }

    fn stripesize(&self) -> i16 {
        self.k
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
