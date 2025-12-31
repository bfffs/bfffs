// vim: tw=80

pub use crate::cache::{Cacheable, CacheRef};
use crate::{
    Error,
    writeback::Credit,
    types::*,
    util::*
};
use bincode::Options;
use divbuf::DivBufShared;
use futures::Future;
#[cfg(test)] use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::{
    num::NonZeroU8,
    pin::Pin
};

/// Compression mode in use
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Compression {
    #[default]
    None,
    /// LZ4 is very fast with decent compression.  From experiment, it's the
    /// best algorithm for metadata
    LZ4(Option<NonZeroU8>),
    /// ZStandard usually gives a very good compression ratio with moderate
    /// speed.  `typesize` is the size of each individual element.  Use
    /// `typesize=None` for an unstructured buffer.
    Zstd(Option<NonZeroU8>),
}

/// The on-disk identification of a Compression
// We have to represent it like this because bincode refuses to use a fixed-size
// format for Compression.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CompressionTag {
    LZ4(u8),
    Zstd(u8)
}

impl Compression {
    const TAGSIZE: usize = 5;

    /// Optionally compress the output.  If any compression was applied, tag the
    /// end of the compressed block with the compression mode.
    // Put the compression tag at the end, rather than at the beginning, because
    // the Zstd crate doesn't have a good API to use a caller-allocated buffer.
    pub fn compress(self, input: IoVec) -> (IoVec, Compression) {
        let lsize = input.len();
        if self == Compression::None || lsize <= BYTES_PER_LBA {
            (input, Compression::None)
        } else {
            let mut buffer = match self {
                Compression::None  => {
                    unreachable!()  // LCOV_EXCL_LINE
                },
                Compression::LZ4(typesize) => {
                    if let Some(ts) = typesize {
                        let shuffled = byteshuffle::shuffle(ts.get().into(),
                            &input[..]);
                        lz4_flex::block::compress(&shuffled[..])
                    } else {
                        lz4_flex::block::compress(&input[..])
                    }
                },
                Compression::Zstd(typesize) => {
                    if let Some(ts) = typesize {
                        let shuffled = byteshuffle::shuffle(ts.get().into(),
                            &input[..]);
                        zstd::bulk::compress(&shuffled[..], 0).unwrap()
                    } else {
                        zstd::bulk::compress(&input[..], 0).unwrap()
                    }
                }
            };
            Self::encoder().serialize_into(&mut buffer, &self.tag()).unwrap();
            let dbs = DivBufShared::from(buffer);
            let compressed_lbas = dbs.len().div_ceil(BYTES_PER_LBA);
            let uncompressed_lbas = lsize.div_ceil(BYTES_PER_LBA);
            if compressed_lbas < uncompressed_lbas {
                (dbs.try_const().unwrap(), self)
            } else {
                (input, Compression::None)
            }
        }
    }

    // TODO: accept a byte slice instead of a DivBuf
    pub fn decompress(input: &[u8], lsize: u32) -> Result<DivBufShared> {
        let lsize = lsize as usize;
        let tag = &input[(input.len() - Self::TAGSIZE)..input.len()];
        let compressed = &input[0..(input.len() - Self::TAGSIZE)];
        let tag: CompressionTag = Self::encoder()
            .deserialize(tag)
            .map_err(|_| Error::EINTEGRITY)?;
        let compression = Compression::from(tag);
        let v = match compression {
            Compression::None => { return Err(Error::EINTEGRITY); }
            Compression::LZ4(typesize) => {
                if let Some(ts) = typesize {
                    let unx = lz4_flex::block::decompress(compressed, lsize)
                        .map_err(|_| Error::EINTEGRITY)?;
                    byteshuffle::unshuffle(ts.get().into(), &unx[..])
                } else {
                    lz4_flex::block::decompress(compressed, lsize)
                        .map_err(|_| Error::EINTEGRITY)?
                }
            },
            Compression::Zstd(typesize) => {
                if let Some(ts) = typesize {
                    let unx = zstd::bulk::decompress(compressed, lsize)
                        .map_err(|_| Error::EINTEGRITY)?;
                    byteshuffle::unshuffle(ts.get().into(), &unx[..])
                } else {
                    zstd::bulk::decompress(compressed, lsize)
                        .map_err(|_| Error::EINTEGRITY)?
                }
            }
        };
        Ok(DivBufShared::from(v))
    }

    fn encoder() -> impl bincode::Options {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian()
            .allow_trailing_bytes()
    }

    /// Does this compression algorithm compress the data at all?
    pub fn is_compressed(self) -> bool {
        self != Compression::None
    }

    /// Get the shuffle setting
    pub fn shuffle(self) -> Option<NonZeroU8> {
        match self {
            Compression::None => None,
            Compression::LZ4(s) | Compression::Zstd(s) => s
        }
    }

    fn tag(&self) -> CompressionTag {
        match self {
            Compression::None => unimplemented!("Should never serialize None"),
            Compression::LZ4(ts) =>
                CompressionTag::LZ4(ts.map(NonZeroU8::get).unwrap_or(0)),
            Compression::Zstd(ts) =>
                CompressionTag::Zstd(ts.map(NonZeroU8::get).unwrap_or(0)),
        }
    }
}

impl From<CompressionTag> for Compression {
    fn from(tag: CompressionTag) -> Self {
        match tag {
            CompressionTag::LZ4(ts) => Compression::LZ4(NonZeroU8::new(ts)),
            CompressionTag::Zstd(ts) => Compression::Zstd(NonZeroU8::new(ts)),
        }
    }
}

/// DML: Data Management Layer
///
/// A DML handles reading and writing records with cacheing.  It also handles
/// compression and checksumming.
#[cfg_attr(test, automock(type Addr=u32;))]
pub trait DML: Send + Sync {
    type Addr: Copy;

    /// Delete the record from the cache, and free its storage space.
    fn delete(&self, addr: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;

    /// If the given record is present in the cache, evict it.
    fn evict(&self, addr: &Self::Addr);

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>;

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, rid: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<<Self as DML>::Addr>> + Send>>;

    /// Repay [`Credit`] to [`WriteBack`](crate::writeback::WriteBack)
    fn repay(&self, credit: Credit);

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
    use std::mem;

    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use rstest::rstest;
    use super::*;

    /// Compressible data should not be compressed, if doing so would save < 1
    /// LBA of space.
    #[test]
    fn compress_barely_compressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; lsize];
        rng.fill_bytes(&mut v[0..lsize - 1024]);
        let dbs = DivBufShared::from(v);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    /// Compressible data should be compressed
    #[test]
    fn compress_compressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert!(zdb.len() < lsize);
        assert_eq!(compression, Compression::Zstd(None));
    }

    /// Compression should not be attempted when it is disabled.
    #[test]
    fn compress_compression_disabled() {
        let lsize = 2 * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::None.compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    /// Compressible data won't be compressed if it's already <= 1 LBA.
    #[test]
    fn compress_compressible_but_short() {
        let lsize = BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    #[test]
    fn compression_taglen() {
        let tag = Compression::LZ4(None).tag();
        let tagsize = Compression::encoder().serialized_size(&tag).unwrap();
        assert_eq!(tagsize, Compression::TAGSIZE as u64);
    }

    /// Incompressible data should not be compressed, even when compression is
    /// enabled.
    #[test]
    fn compress_incompressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; lsize];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    mod decompress {
        use super::*;

        /// If the lsize recorded in the DRP is wrong, the decompression should
        /// fail gracefully
        #[rstest]
        fn corrupt_lsize(
            #[values(
                Compression::LZ4(None),
                Compression::LZ4(NonZeroU8::new(4)),
                Compression::Zstd(None),
                Compression::Zstd(NonZeroU8::new(8))
            )]
            compression: Compression
        ) {
            let (_, zdb, lsize) = mk_compressed_buffer(compression);

            let bad_lsize = lsize / 2;
            let e = Compression::decompress(&zdb, bad_lsize).unwrap_err();
            assert_eq!(Error::EINTEGRITY, e);
        }

        /// Attempt to decompress a buffer with a corrupt compression tag should
        /// fail gracefully.
        #[test]
        fn corrupt_tag() {
            let csize = 100;
            let lsize = 200;
            let mut v = vec![0u8; csize];
            v[csize - Compression::TAGSIZE..csize].copy_from_slice(
                &[0xde, 0xad, 0xbe, 0xef, 0x42][..]);
            let e = Compression::decompress(&v[..], lsize).unwrap_err();
            assert_eq!(Error::EINTEGRITY, e);
        }

        fn mk_compressed_buffer(compression: Compression) -> (IoVec, IoVec, u32)
        {
            // Generate random data that is compressible because it doesn't use
            // the MSBs of each word.
            let lsize = 4 * BYTES_PER_LBA;
            let mut rng = XorShiftRng::seed_from_u64(23456);
            let mut v = Vec::with_capacity(lsize);
            for _ in (0..lsize).step_by(mem::size_of::<u32>()) {
                v.extend_from_slice(&(rng.next_u32() & 0xFF).to_le_bytes());
            }
            let dbs = DivBufShared::from(v);
            let db = dbs.try_const().unwrap();
            let (zdb, actual_compression) = compression.compress(db.clone());
            assert_eq!(compression, actual_compression);
            (db, zdb, lsize as u32)
        }

        /// Data can be decompressed for all compression settings
        #[rstest]
        fn round_trip(
            #[values(
                Compression::LZ4(None),
                Compression::LZ4(NonZeroU8::new(4)),
                Compression::Zstd(None),
                Compression::Zstd(NonZeroU8::new(8))
            )]
            compression: Compression
        ){
            let (db, zdb, lsize) = mk_compressed_buffer(compression);

            // Decompress it.  Make sure it round-trips.
            let decompressed_dbs = Compression::decompress(&zdb, lsize)
                .unwrap();
            let decompressed_db = decompressed_dbs.try_const().unwrap();
            assert_eq!(decompressed_db, db);
        }

        /// Attempting to decompress a buffer that's marked as compressed but
        /// isn't should fail gracefully.
        #[rstest]
        fn undecompressible(
            #[values(
                Compression::LZ4(None),
                Compression::LZ4(NonZeroU8::new(4)),
                Compression::Zstd(None),
                Compression::Zstd(NonZeroU8::new(8))
            )]
            compression: Compression
        ) {
            let csize = 100;
            let lsize = 200;
            let tag = compression.tag();
            let mut v = vec![0u8; csize];
            let tagslice = &mut v[csize - Compression::TAGSIZE..csize];
            Compression::encoder().serialize_into(tagslice, &tag).unwrap();
            let e = Compression::decompress(&v[..], lsize).unwrap_err();
            assert_eq!(Error::EINTEGRITY, e);
        }
    }

    #[test]
    fn shuffle() {
        assert_eq!(Compression::None.shuffle(), None);
        assert_eq!(Compression::LZ4(None).shuffle(), None);
        assert_eq!(Compression::Zstd(None).shuffle(), None);
        assert_eq!(Compression::LZ4(NonZeroU8::new(32)).shuffle(),
            NonZeroU8::new(32));
        assert_eq!(Compression::Zstd(NonZeroU8::new(35)).shuffle(),
            NonZeroU8::new(35));
    }
}
// LCOV_EXCL_STOP
