// vim: tw=80

//! BFFFS RAID layer
//!
//! This provides vdevs which slot between `cluster` and `vdev_block` and
//! provide RAID-like functionality.

mod codec;
mod declust;
mod null_locator;
mod prime_s;
mod sgcursor;
mod vdev_raid;
mod vdev_raid_api;

pub use self::vdev_raid::Label;
pub use self::vdev_raid::VdevRaid;
pub use self::vdev_raid_api::VdevRaidApi;
