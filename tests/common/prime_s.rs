// vim: tw=80

use arkfs::common::declust::*;
use arkfs::common::prime_s::*;

// PrimeS should panic if you try to use a composite number of disks
#[test]
#[should_panic]
pub fn composite_disks_panic() {
    PrimeS::new(9, 5, 1);
}

// Exhaustive placement test for a small array
// declust.rb's output:
// D0.0  D0.1  C0.0  C0.1  C1.0 
// C1.1  C2.0  D1.0  D1.1  D2.0 
// D2.1  D3.0  C2.1  C3.0  C3.1 
// C4.0  C4.1  D3.1  D4.0  D4.1 
// D5.0  C5.1  D5.1  C6.0  C5.0 
// C6.1  D6.1  C7.0  D7.0  D6.0 
// D7.1  C8.0  D8.0  C8.1  C7.1 
// C9.0  D9.0  C9.1  D9.1  D8.1 
// D10.0 C10.0 C11.0 D10.1 C10.1
// C11.1 D11.0 D12.0 C12.0 D11.1
// D12.1 C12.1 C13.1 D13.0 C13.0
// C14.0 D13.1 D14.1 C14.1 D14.0
// D15.0 C16.0 C15.1 C15.0 D15.1
// C16.1 D17.0 D16.1 D16.0 C17.0
// D17.1 C18.1 C18.0 C17.1 D18.0
// C19.0 D19.1 D19.0 D18.1 C19.1
#[test]
pub fn exhaustive_5_4_2() {
    let n = 5;
    let k = 4;
    let f = 2;

    let locator = PrimeS::new(n, k, f);
    assert_eq!(locator.depth(), 16);
    assert_eq!(locator.datachunks(), 40);
    assert_eq!(locator.stripes(), 20);

    // Check forward mappings against a precomputed list
    assert_eq!(locator.id2loc(ChunkId::Data(0)), Chunkloc::new(0, 0, 0, 0));
    assert_eq!(locator.id2loc(ChunkId::Data(1)), Chunkloc::new(0, 0, 1, 0));
    assert_eq!(locator.id2loc(ChunkId::Parity(0, 0)), Chunkloc::new(0, 0, 2, 0));
    assert_eq!(locator.id2loc(ChunkId::Parity(0, 1)), Chunkloc::new(0, 0, 3, 0));
    assert_eq!(locator.id2loc(ChunkId::Data(2)), Chunkloc::new(0, 0, 2, 1));
    assert_eq!(locator.id2loc(ChunkId::Data(3)), Chunkloc::new(0, 0, 3, 1));
    assert_eq!(locator.id2loc(ChunkId::Parity(2, 0)), Chunkloc::new(0, 0, 4, 0));
    assert_eq!(locator.id2loc(ChunkId::Parity(2, 1)), Chunkloc::new(0, 0, 0, 1));
    assert_eq!(locator.id2loc(ChunkId::Data(4)), Chunkloc::new(0, 0, 4, 1));
    assert_eq!(locator.id2loc(ChunkId::Data(5)), Chunkloc::new(0, 0, 0, 2));
    assert_eq!(locator.id2loc(ChunkId::Parity(4, 0)), Chunkloc::new(0, 0, 1, 1));
    assert_eq!(locator.id2loc(ChunkId::Parity(4, 1)), Chunkloc::new(0, 0, 2, 2));
    assert_eq!(locator.id2loc(ChunkId::Data(6)), Chunkloc::new(0, 0, 1, 2));
    assert_eq!(locator.id2loc(ChunkId::Data(7)), Chunkloc::new(0, 0, 2, 3));
    assert_eq!(locator.id2loc(ChunkId::Parity(6, 0)), Chunkloc::new(0, 0, 3, 2));
    assert_eq!(locator.id2loc(ChunkId::Parity(6, 1)), Chunkloc::new(0, 0, 4, 2));
    assert_eq!(locator.id2loc(ChunkId::Data(8)), Chunkloc::new(0, 0, 3, 3));
    assert_eq!(locator.id2loc(ChunkId::Data(9)), Chunkloc::new(0, 0, 4, 3));
    assert_eq!(locator.id2loc(ChunkId::Parity(8, 0)), Chunkloc::new(0, 0, 0, 3));
    assert_eq!(locator.id2loc(ChunkId::Parity(8, 1)), Chunkloc::new(0, 0, 1, 3));
    assert_eq!(locator.id2loc(ChunkId::Data(10)), Chunkloc::new(0, 1, 0, 4));
    assert_eq!(locator.id2loc(ChunkId::Data(11)), Chunkloc::new(0, 1, 2, 4));
    assert_eq!(locator.id2loc(ChunkId::Parity(10, 0)), Chunkloc::new(0, 1, 4, 4));
    assert_eq!(locator.id2loc(ChunkId::Parity(10, 1)), Chunkloc::new(0, 1, 1, 4));
    assert_eq!(locator.id2loc(ChunkId::Data(12)), Chunkloc::new(0, 1, 4, 5));
    assert_eq!(locator.id2loc(ChunkId::Data(13)), Chunkloc::new(0, 1, 1, 5));
    assert_eq!(locator.id2loc(ChunkId::Parity(12, 0)), Chunkloc::new(0, 1, 3, 4));
    assert_eq!(locator.id2loc(ChunkId::Parity(12, 1)), Chunkloc::new(0, 1, 0, 5));
    assert_eq!(locator.id2loc(ChunkId::Data(14)), Chunkloc::new(0, 1, 3, 5));
    assert_eq!(locator.id2loc(ChunkId::Data(15)), Chunkloc::new(0, 1, 0, 6));
    assert_eq!(locator.id2loc(ChunkId::Parity(14, 0)), Chunkloc::new(0, 1, 2, 5));
    assert_eq!(locator.id2loc(ChunkId::Parity(14, 1)), Chunkloc::new(0, 1, 4, 6));
    assert_eq!(locator.id2loc(ChunkId::Data(16)), Chunkloc::new(0, 1, 2, 6));
    assert_eq!(locator.id2loc(ChunkId::Data(17)), Chunkloc::new(0, 1, 4, 7));
    assert_eq!(locator.id2loc(ChunkId::Parity(16, 0)), Chunkloc::new(0, 1, 1, 6));
    assert_eq!(locator.id2loc(ChunkId::Parity(16, 1)), Chunkloc::new(0, 1, 3, 6));
    assert_eq!(locator.id2loc(ChunkId::Data(18)), Chunkloc::new(0, 1, 1, 7));
    assert_eq!(locator.id2loc(ChunkId::Data(19)), Chunkloc::new(0, 1, 3, 7));
    assert_eq!(locator.id2loc(ChunkId::Parity(18, 0)), Chunkloc::new(0, 1, 0, 7));
    assert_eq!(locator.id2loc(ChunkId::Parity(18, 1)), Chunkloc::new(0, 1, 2, 7));
    assert_eq!(locator.id2loc(ChunkId::Data(20)), Chunkloc::new(0, 2, 0, 8));
    assert_eq!(locator.id2loc(ChunkId::Data(21)), Chunkloc::new(0, 2, 3, 8));
    assert_eq!(locator.id2loc(ChunkId::Parity(20, 0)), Chunkloc::new(0, 2, 1, 8));
    assert_eq!(locator.id2loc(ChunkId::Parity(20, 1)), Chunkloc::new(0, 2, 4, 8));
    assert_eq!(locator.id2loc(ChunkId::Data(22)), Chunkloc::new(0, 2, 1, 9));
    assert_eq!(locator.id2loc(ChunkId::Data(23)), Chunkloc::new(0, 2, 4, 9));
    assert_eq!(locator.id2loc(ChunkId::Parity(22, 0)), Chunkloc::new(0, 2, 2, 8));
    assert_eq!(locator.id2loc(ChunkId::Parity(22, 1)), Chunkloc::new(0, 2, 0, 9));
    assert_eq!(locator.id2loc(ChunkId::Data(24)), Chunkloc::new(0, 2, 2, 9));
    assert_eq!(locator.id2loc(ChunkId::Data(25)), Chunkloc::new(0, 2, 0, 10));
    assert_eq!(locator.id2loc(ChunkId::Parity(24, 0)), Chunkloc::new(0, 2, 3, 9));
    assert_eq!(locator.id2loc(ChunkId::Parity(24, 1)), Chunkloc::new(0, 2, 1, 10));
    assert_eq!(locator.id2loc(ChunkId::Data(26)), Chunkloc::new(0, 2, 3, 10));
    assert_eq!(locator.id2loc(ChunkId::Data(27)), Chunkloc::new(0, 2, 1, 11));
    assert_eq!(locator.id2loc(ChunkId::Parity(26, 0)), Chunkloc::new(0, 2, 4, 10));
    assert_eq!(locator.id2loc(ChunkId::Parity(26, 1)), Chunkloc::new(0, 2, 2, 10));
    assert_eq!(locator.id2loc(ChunkId::Data(28)), Chunkloc::new(0, 2, 4, 11));
    assert_eq!(locator.id2loc(ChunkId::Data(29)), Chunkloc::new(0, 2, 2, 11));
    assert_eq!(locator.id2loc(ChunkId::Parity(28, 0)), Chunkloc::new(0, 2, 0, 11));
    assert_eq!(locator.id2loc(ChunkId::Parity(28, 1)), Chunkloc::new(0, 2, 3, 11));
    assert_eq!(locator.id2loc(ChunkId::Data(30)), Chunkloc::new(0, 3, 0, 12));
    assert_eq!(locator.id2loc(ChunkId::Data(31)), Chunkloc::new(0, 3, 4, 12));
    assert_eq!(locator.id2loc(ChunkId::Parity(30, 0)), Chunkloc::new(0, 3, 3, 12));
    assert_eq!(locator.id2loc(ChunkId::Parity(30, 1)), Chunkloc::new(0, 3, 2, 12));
    assert_eq!(locator.id2loc(ChunkId::Data(32)), Chunkloc::new(0, 3, 3, 13));
    assert_eq!(locator.id2loc(ChunkId::Data(33)), Chunkloc::new(0, 3, 2, 13));
    assert_eq!(locator.id2loc(ChunkId::Parity(32, 0)), Chunkloc::new(0, 3, 1, 12));
    assert_eq!(locator.id2loc(ChunkId::Parity(32, 1)), Chunkloc::new(0, 3, 0, 13));
    assert_eq!(locator.id2loc(ChunkId::Data(34)), Chunkloc::new(0, 3, 1, 13));
    assert_eq!(locator.id2loc(ChunkId::Data(35)), Chunkloc::new(0, 3, 0, 14));
    assert_eq!(locator.id2loc(ChunkId::Parity(34, 0)), Chunkloc::new(0, 3, 4, 13));
    assert_eq!(locator.id2loc(ChunkId::Parity(34, 1)), Chunkloc::new(0, 3, 3, 14));
    assert_eq!(locator.id2loc(ChunkId::Data(36)), Chunkloc::new(0, 3, 4, 14));
    assert_eq!(locator.id2loc(ChunkId::Data(37)), Chunkloc::new(0, 3, 3, 15));
    assert_eq!(locator.id2loc(ChunkId::Parity(36, 0)), Chunkloc::new(0, 3, 2, 14));
    assert_eq!(locator.id2loc(ChunkId::Parity(36, 1)), Chunkloc::new(0, 3, 1, 14));
    assert_eq!(locator.id2loc(ChunkId::Data(38)), Chunkloc::new(0, 3, 2, 15));
    assert_eq!(locator.id2loc(ChunkId::Data(39)), Chunkloc::new(0, 3, 1, 15));
    assert_eq!(locator.id2loc(ChunkId::Parity(38, 0)), Chunkloc::new(0, 3, 0, 15));
    assert_eq!(locator.id2loc(ChunkId::Parity(38, 1)), Chunkloc::new(0, 3, 4, 15));
    
    // TODO! Implement reverse mappings
    // Now check reverse mappings
    //for stripe in 0 .. locator.stripes() {
        //// First check data chunks
        //for u in 0 .. k {
            //let chunkid = ChunkId::Data(stripe as u64 * k as u64 + u as u64);
            //assert_eq!(locator.loc2id(locator.id2loc(chunkid)), chunkid);
        //}

        //// Now check parity chunks
        //for u in 0 .. f {
            //let chunkid = ChunkId::Parity(stripe as u64 * k as u64, u);
            //assert_eq!(locator.loc2id(locator.id2loc(chunkid)), chunkid);
        //}
    //}
}
