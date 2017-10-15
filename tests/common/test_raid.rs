use arkfs::common::raid::*;

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

#[test]
pub fn test_new_5plus1() {
    Codec::new(6, 1);
}
