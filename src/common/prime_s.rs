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

/// Internal struture that captures some of the intermediate values used in
/// `id2loc`
struct ChunklocInt {
    a: i16,                 // Index of data chunk within its repetition
    r: u32,                 // Repetition
    s: i16,                 // Stripe within its repetition
    y: i16,                 // Stride
    z: i16,                 // Iteration
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    /// Internal helper function
    fn id2loc_int(&self, chunkid: &ChunkId) -> ChunklocInt {
        // The chunk address
        let id = match *chunkid {
            ChunkId::Data(id) | ChunkId::Parity(id, _) => id,
        };
        let a = id.modulo(self.datachunks) as i16;
        // The repetition and iteration
        let (r, z) = self.id2rep_and_iter(&chunkid);
        // The stripe
        let s = a / self.m;
        // The stride
        let y = (z.modulo(self.n - 1)) + 1;
        ChunklocInt{a, r, s, y, z}
    }

    /// Return the repetition and iteration numbers where a given Chunk is
    /// stored
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

    fn iterations_per_rep(&self) -> i16 {
        self.n - 1
    }

    /// Return the offset of a chunk relative to the first offset of its
    /// iteration
    ///
    /// # Parameters
    /// - `cli`:    Output of `id2loc_int` for this chunk
    /// - `b`:      Chunk's index within its stripe
    fn offset_within_iteration(&self, cli: &ChunklocInt, b: i16,
                               disk: i16) -> i16 {
        // We must loop to calculate the offset.  That's PRIME-S's disadvantage
        // vis-a-vis PRIME
        let y_inv = invmod(cli.y, self.n);
        // Contribution to offset from data chunks in this repetition
        let o0 = (cli.s * self.m + b) / self.n;
        // Contributions to offset from parity chunks in previous iterations
        let o1 = self.f * cli.z;
        // Contributions to offset from parity chunks in this iteration
        let o2 = (0 .. self.f).fold(0, |acc, j| {
                let cb_stripe = ((disk * y_inv - j) *
                                 self.m_inv - 1).modulo(self.n);
                let x = if cli.s.modulo(self.n) > cb_stripe {
                    1
                } else {
                    0
                };
                x + acc
            });
        o0 + o1 + o2
    }

    fn stripes_per_iteration(&self) -> i16 {
        self.n
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
        let cli = self.id2loc_int(&chunkid);
        // Unit's position within its stripe
        let b = match chunkid {
            ChunkId::Data(_) => cli.a - cli.s * self.m,
            ChunkId::Parity(_, i) => self.m + i
        };
        let disk = ((cli.s * self.m + b) * cli.y).modulo(self.n);
        let o3 = u64::from(cli.r) * self.depth as u64;
        let offset = self.offset_within_iteration(&cli, b, disk) as u64 + o3;
        Chunkloc { disk, offset}
    }

    fn iter<'a>(&'a self, start: ChunkId, end: ChunkId)
        -> Box<Iterator<Item=(ChunkId, Chunkloc)> + 'a> {
        Box::new(PrimeSIter::new(&self, start, end))
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimeSIter<'a> {
    a: i16,             // Index of a data chunk within its repetition
    end: ChunkId,       // Id of the first chunk beyond the end
    id: ChunkId,        // Id of next chunk
    layout: &'a PrimeS, // Layout to iterate over
    o: Vec<i16>,        // Offsets within an iteration for each disk
    r: u32,             // Repetition number
    stripe: i16,        // Stripe with its repetition
    stripe_iter: i16,   // Stripe within its iteration
    y: i16,             // Stride
    z: i16,             // Iteration number
}

impl<'a> PrimeSIter<'a> {
    /// Create a new iterator.  `id` is the id of the first chunk that the
    /// iterator should return.
    fn new(layout: &'a PrimeS, start: ChunkId, end: ChunkId) -> Self {
        let cli = layout.id2loc_int(&start);
        let s_z = cli.s.modulo(layout.stripes_per_iteration());
        let b = match start {
            ChunkId::Data(_) => cli.a - cli.s * layout.m,
            ChunkId::Parity(_, i) => layout.m + i
        };
        // Start with offset contributions from previous iterations
        let mut o: Vec<i16> = vec![layout.k * cli.z; layout.n as usize];
        for s in 0..(s_z + 1) {
            let end = if s == s_z { b } else { layout.k };
            // Add contributions from other chunks in this iteration
            for b in 0..end {
                let disk = ((s * layout.m + b) * cli.y).modulo(layout.n);
                o[disk as usize] += 1;
            }
        }
        PrimeSIter { a: cli.a,
                     end,
                     id: start,
                     layout,
                     o,
                     r: cli.r,
                     stripe: cli.s,
                     stripe_iter: s_z,
                     y: cli.y,
                     z: cli.z}
    }

    fn next_elem(&self) -> (i16, u64) {
        let layout = self.layout;
        let b = match self.id {
            ChunkId::Data(_) => self.a - self.stripe * layout.m,
            ChunkId::Parity(_, i) => layout.m + i
        };
        let disk = ((self.stripe * layout.m + b) * self.y).modulo(layout.n);
        let o3 = u64::from(self.r) * layout.depth as u64;
        let offset = self.o[disk as usize] as u64 + o3;
        (disk, offset)
    }

    /// Return the next element in the iterator, _without_ advancing the
    /// iterator.
    ///
    /// This differs from `std::iter::Peekable::peek` in that it actually
    /// doesn't modify the iterator's internal state
    #[cfg(test)]
    fn peek(&self) -> Option<(ChunkId, Chunkloc)> {
        if self.id == self.end {
            return None;
        }
        let (disk, offset) = self.next_elem();
        Some((self.id, Chunkloc{disk, offset}))
    }
}

impl<'a> Iterator for PrimeSIter<'a> {
    type Item = (ChunkId, Chunkloc);

    fn next(&mut self) -> Option<Self::Item> {
        if self.id == self.end {
            return None;
        }
        let layout = self.layout;
        let (disk, offset) = self.next_elem();
        let result = Some((self.id, Chunkloc{disk, offset}));

        // Now update the internal state
        self.id = match self.id {
        ChunkId::Data(i) => {
            self.o[disk as usize] += 1;
            if self.a < (self.stripe + 1) * layout.m - 1 {
                self.a += 1;
                ChunkId::Data(i + 1)
            } else {
                self.a = self.stripe * layout.m;
                ChunkId::Parity(i - (layout.m - 1) as u64, 0)
            }
        },
        ChunkId::Parity(a, i) => {
            if i < layout.f - 1 {
                self.o[disk as usize] += 1;
                ChunkId::Parity(a, i + 1)
            } else {
                // Roll over to the next stripe
                if self.stripe_iter == layout.stripes_per_iteration() - 1 {
                    // Roll over to the next iteration
                    self.stripe_iter = 0;
                    if self.z == layout.iterations_per_rep() - 1 {
                        // Roll over to the next repetition
                        for mut o in self.o.iter_mut() {
                            *o = 0;
                        }
                        self.a = 0;
                        self.r += 1;
                        self.stripe = 0;
                        self.y = 1;
                        self.z = 0;
                    } else {
                        self.a += layout.m;
                        self.o[disk as usize] += 1;
                        self.z += 1;
                        self.stripe += 1;
                        self.y = (self.z.modulo(layout.n - 1)) + 1;
                    }
                } else {
                    self.a += layout.m;
                    self.o[disk as usize] += 1;
                    self.stripe_iter += 1;
                    self.stripe += 1;
                }
                ChunkId::Data(a + layout.m as u64)
            }
        }
        };
        result
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

    /// Test basic info about a 5-4-2 layout
    #[test]
    fn basic_5_4_2() {
        let n = 5;
        let k = 4;
        let f = 2;

        let locator = PrimeS::new(n, k, f);
        assert_eq!(locator.depth(), 16);
        assert_eq!(locator.datachunks(), 40);
        assert_eq!(locator.stripes(), 20);
        assert_eq!(locator.stripes_per_iteration(), 5);
        assert_eq!(locator.iterations_per_rep(), 4);
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

    // Test creating iterators from any starting point in a 5-4-2 PRIME-S layout
    #[test]
    fn iter_5_4_2_any_start() {
        let n = 5;
        let k = 4;
        let f = 2;

        let locator = PrimeS::new(n, k, f);
        let id = Some(ChunkId::Data(0));
        // Go for two repetitions
        let end = ChunkId::Data(locator.datachunks() * 2);
        // Create the PrimeSIter directly instead of through Locator::iter so we
        // can get the real return type, not just the Trait object.
        let mut iter = PrimeSIter::new(&locator, id.unwrap(), end);
        loop {
            // Check that the internal state is identical
            let next_id = iter.peek().map(|(i, _)| i);
            if next_id.is_none() {
                break;
            }
            let iter2 = PrimeSIter::new(&locator, next_id.unwrap(), end);
            assert_eq!(&iter, &iter2);
            // Now advance the iterator
            let _ = iter.next();
        }
    }
}
