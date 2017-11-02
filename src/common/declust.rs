// vim: tw=80

/// ID of a chunk.  A chunk is the fundamental unit of declustering.  One chunk
/// (typically several KB) is the largest amount of data that can be written to
/// a single disk before the Locator switches to a new disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkId {
    /// a Data chunk is identified by its id.  It's a 0-index of all the chunks
    /// in the vdev cluster
    Data(u64),

    /// A Parity chunk is identified by the id of the first data chunk in its
    /// stripe, and by the parity id, which is 0-indexed starting with the first
    /// parity chunk in the stripe.
    Parity(u64, i16)
}

/// Describes the location of a Chunk within the declustering layout
#[derive(Debug, PartialEq, Eq)]
pub struct Chunkloc {
    /// Which disk (0-indexed) this chunk is mapped to
    pub disk: i16,

    /// The chunk's chunk offset from the start of its disk, in units of chunks
    pub offset: u64
}

impl Chunkloc {
    pub fn new(disk: i16, offset: u64) -> Self {
        Chunkloc {disk: disk, offset: offset}
    }
}

/// Declustering locator
///
/// This trait defines a declustering transform.  Given a set of `n` disks and a
/// RAID configuration of `k` data columns and `f` parity columns (where `k + f
/// <= n`), these objects calculate the physical location of each data and
/// parity chunk.  Declustered RAID not only offers more flexibility of array
/// design, but it has performance benefits as well.  Chiefly, during a rebuild
/// no healthy disk will be saturated, so user I/O suffers less than in a
/// traditional RAID array.
///
/// # References
///
/// Muntz, Richard R., and John CS Lui. Performance analysis of disk arrays
/// under failure. Computer Science Department, University of California, 1990.
///
/// Holland, Mark, and Garth A. Gibson. Parity declustering for continuous
/// operation in redundant disk arrays. Vol. 27. No. 9.  ACM, 1992.
///
/// Alvarez, Guillermo A., et al. "Declustered disk array architectures with
/// optimal and near-optimal parallelism." ACM SIGARCH Computer Architecture
/// News. Vol. 26. No. 3. IEEE Computer Society, 1998.
pub trait Locator {
    /// Return the total number of disks in the layout
    fn clustsize(&self) -> i16;

    /// Return the number of data chunks in a single repetition of the layout
    fn datachunks(&self) -> u64;

    /// Number of rows in a single repetition of the layout
    fn depth(&self) -> i16;

    /// Inverse of `id2loc`.  Returns the chunk id of the chunk at this location.
    ///
    /// # Parameters
    /// 
    /// - `loc`:    A chunk location
    fn loc2id(&self, loc: Chunkloc) -> ChunkId;

    /// Return the location of a data chunk, given its ID
	///
    /// # Parameters
    ///
	/// - `a`:	ID of the data chunk
    fn id2loc(&self, id: ChunkId) -> Chunkloc;

    /// Return the degree of redundancy
    fn protection(&self) -> i16;

    /// Return the number of stripes in a single repetition of the layout
    fn stripes(&self) -> u32;

    /// Return the total number of disks in each RAID stripe
    fn stripesize(&self) -> i16;
}
