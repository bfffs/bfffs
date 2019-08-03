// vim: tw=80

//! Common interface to declustering RAID transforms.
//!
//! Traditional RAID is *fully clustered*.  That is, each parity group, or
//! stripe, has the same number of disks as the entire array.  *Declustered*
//! RAID arrays, by contrast, can have more disks in the array that there are in
//! each stripe.  Declustering improves the performance of user reads during a
//! rebuild, and decreases the CPU and I/O usage of rebuilds too.  For a given
//! array size, declustered RAID has worse space efficiency than fully clustered
//! RAID.  However, it has the same space efficiency as fully clustered RAID for
//! a given stripe size.  Declustered RAID makes large array sizes practical,
//! where a fully clustered RAID system would resort to a stripe of multiple
//! RAID arrays instead.
//!
//! # References
//!
//! Muntz, Richard R., and John CS Lui. Performance analysis of disk arrays
//! under failure. Computer Science Department, University of California, 1990.
//!
//! Holland, Mark, and Garth A. Gibson. Parity declustering for continuous
//! operation in redundant disk arrays. Vol. 27. No. 9.  ACM, 1992.
//!
//! Alvarez, Guillermo A., et al. "Declustered disk array architectures with
//! optimal and near-optimal parallelism." ACM SIGARCH Computer Architecture
//! News. Vol. 26. No. 3. IEEE Computer Society, 1998.

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

impl ChunkId {
    /// Return the chunk's chunk address.
    ///
    /// For `Data` chunks, this is the zero-based index of all `Data` chunks in
    /// the layout.  For `Parity` chunks, it's the zero-based index of the first
    /// `Data` chunk in the same stripe.
    pub fn address(&self) -> u64 {
        match *self {
            ChunkId::Data(id) | ChunkId::Parity(id, _) => id,
        }
    }

    /// Is this a Data chunk?
    pub fn is_data(&self) -> bool {
        if let ChunkId::Data(_) = *self { true } else { false }
    }
}

/// Describes the location of a Chunk within the declustering layout
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunkloc {
    /// Which disk (0-indexed) this chunk is mapped to
    pub disk: i16,

    /// The chunk's chunk offset from the start of its disk, in units of chunks
    pub offset: u64
}

impl Chunkloc {
    pub fn new(disk: i16, offset: u64) -> Self {
        Chunkloc {disk, offset}
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
pub trait Locator : Send + Sync {
    /// Return the number of data chunks in a single repetition of the layout
    fn datachunks(&self) -> u64;

    /// Number of rows in a single repetition of the layout
    fn depth(&self) -> u32;

    /// Get an iterator that will iterate through many `ChunkId`s.
    ///
    /// This is faster than calling `id2loc` repeatedly.
    ///
    /// # Parameters
    ///
    /// - `start`:  The first `ChunkId` whose location will be returned
    /// - `end`:    The first `ChunkId` beyond the end of the iterator
    fn iter(&self, start: ChunkId, end: ChunkId)
        -> Box<dyn Iterator<Item=(ChunkId, Chunkloc)>>;

    /// Like [`iter`], but only iterates through Data chunks, skipping Parity
    ///
    /// This is faster than calling `id2loc` repeatedly.
    ///
    /// # Parameters
    ///
    /// - `start`:  The first Data `ChunkId` whose location will be returned
    /// - `end`:    The first Data `ChunkId` beyond the end of the iterator.
    ///
    /// [`iter`]: #method.iter
    fn iter_data(&self, start: ChunkId, end: ChunkId)
        -> Box<dyn Iterator<Item=(ChunkId, Chunkloc)>>;

    /// Inverse of `id2loc`.
    ///
    /// Returns the chunk id of the chunk at this location.
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

    /// Parallel Read Count, as defined by Alvarez et al.[^RELPR_]
    ///
    /// It's the maximum number of data chunks that any disk must supply when
    /// reading a given number of consecutive data chunks.  This value is purely
    /// advisory, and a valid implementation could return anything at all.
    ///
    /// [^RELPR_]: Alvarez, Guillermo A., et al. "Declustered disk array architectures with optimal and near-optimal parallelism." ACM SIGARCH Computer Architecture News. Vol. 26. No. 3. IEEE Computer Society, 1998.
    fn parallel_read_count(&self, consecutive_data_chunks: usize) -> usize;

    /// Return the degree of redundancy
    fn protection(&self) -> i16;

    /// Return the number of stripes in a single repetition of the layout
    fn stripes(&self) -> u32;

    /// Return the total number of disks in each RAID stripe
    fn stripesize(&self) -> i16;
}
