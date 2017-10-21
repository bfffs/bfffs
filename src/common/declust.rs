// vim: tw=80

/// ID of a chunk.  A chunk is the fundamental unit of declustering.  One chunk
/// (typically several KB) is the largest amount of data that can be written to
/// a single disk before the Locator switches to a new disk.
pub enum ChunkId {
    /// a Data chunk is identified by its id.  It's a 0-index of all the chunks
    /// in the vdev cluster
    Data(u64),

    /// A Parity chunk is identified by the id of the first data chunk in its
    /// stripe, and by the parity id, which is 0-indexed starting with the first
    /// parity chunk in the stripe.
    Parity((u64, u8))
}

/// Describes the location of a Chunk within the declustering layout
pub struct Chunkloc {
    /// Which repetition (0-indexed) of the layout this chunk lies in.
    ///
    /// 32-bits is enough for a 17TB disk using 512B chunks and a 1-row layout.
    /// Larger disks will require increasing the size of this type, which
    /// resides in-memory only, not on disk.
    repetition: u32,

    /// Which disk (0-indexed) this chunk is mapped to
    disk: u16,

    /// Which iteration (0-indexed) of the layout this chunk lies in.
    ///
    /// Not all declustering algorithms have iterations.
    iteration: u16,

    /// The chunk's chunk offset within its iteration, in units of chunks.
    offset: u16
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
trait Locator {
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
}
