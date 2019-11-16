//vim: tw=80
//! Dataset Properties
use crate::common::Error;
use serde_derive::*;
use std::convert::TryFrom;

/// All dataset properties are associated with this fake inode number.
pub const PROPERTY_OBJECT: u64 = 0;

/// Dataset Properties.
///
/// Properties can be set on individual datasets to affect its behavior in some
/// way.  They all have default values, and if unset they'll inherit from their
/// parent dataset.  They're basically just like ZFS properties.
///
/// This enum is not used for User Properties.  User Properties are stored as
/// extended attributes on inode 0.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Property {
    /// Access time.
    ///
    /// When on, then POSIX atime of a file will be updated on every access.
    /// When off, atime will be treated like ctime.
    Atime(bool),

    /// Suggested block size for newly written data.
    ///
    /// Units are in bytes, log base 2.  So `RecordSize(16)` means 16KB records.
    /// BFFFS will usually divide files into blocks of this many bytes.  But the
    /// record size is only advisory.  The default is 4KB.
    RecordSize(u8),
}

impl Property {
    pub fn default_value(name: PropertyName) -> Self {
        match name {
            PropertyName::Atime => Property::Atime(true),
            PropertyName::RecordSize => Property::RecordSize(12), // 4KB
            PropertyName::Invalid => panic!("Invalid props have no values")
        }
    }

    pub fn name(&self) -> PropertyName {
        match self {
            Property::Atime(_) => PropertyName::Atime,
            Property::RecordSize(_) => PropertyName::RecordSize,
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Property::Atime(atime) => *atime,
            _ => panic!(format!("{:?} is not a boolean Property", self))
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Property::RecordSize(rs) => *rs,
            _ => panic!(format!("{:?} is not a u8 Property", self))
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Error> {
        let mut words = s.splitn(2, '=');
        let propname = words.next().ok_or(Error::EINVAL)?;
        let propval = words.next();
        if let Some(v) = propval {
            match propname {
                "atime" => {
                    match v {
                        "true" | "on" => Ok(Property::Atime(true)),
                        "false" | "off" => Ok(Property::Atime(false)),
                        _ => Err(Error::EINVAL)
                    }
                },
                "record_size" => {
                    if let Ok(rs) = v.parse::<usize>() {
                        // We need the log base 2 of rs.  We could calculate it
                        // numerically, but there are so few valid values that
                        // it's easier to use a LUT.
                        match rs {
                            4_096 => Ok(Property::RecordSize(12)),
                            8_192 => Ok(Property::RecordSize(13)),
                            16_384 => Ok(Property::RecordSize(14)),
                            32_768 => Ok(Property::RecordSize(15)),
                            65_536 => Ok(Property::RecordSize(16)),
                            131_072 => Ok(Property::RecordSize(17)),
                            262_144 => Ok(Property::RecordSize(18)),
                            524_388 => Ok(Property::RecordSize(19)),
                            1_048_776 => Ok(Property::RecordSize(20)),
                            _ => Err(Error::EINVAL)
                        }
                    } else {
                        Err(Error::EINVAL)
                    }
                },
                _ => Err(Error::EINVAL)
            }
        } else {
            // Value may be omitted only for boolean options
            match propname {
                "atime" => Ok(Property::Atime(true)),
                _ => Err(Error::EINVAL)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum PropertyName {
    Atime,
    RecordSize,
    Invalid,    // Must be last!
}

impl PropertyName {
    pub fn next(self) -> PropertyName {
        match self {
            PropertyName::Atime => PropertyName::RecordSize,
            PropertyName::RecordSize => PropertyName::Invalid,
            PropertyName::Invalid => PropertyName::Invalid,
        }
    }
}

/// Where did the property come from?
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PropertySource {
    /// No value has been set for this property on this dataset or any of its
    /// parents.
    Default = 0,
    /// The property's value is inherited from a parent dataset.
    Inherited = 1,
    /// The property was explicitly set on this dataset
    Local = 2,
}

#[cfg(test)]
mod t {

use pretty_assertions::assert_eq;
use super::*;

#[test]
fn property_try_from() {
    assert_eq!(Ok(Property::Atime(true)), Property::try_from("atime=true"));
    assert_eq!(Ok(Property::Atime(true)), Property::try_from("atime=on"));
    assert_eq!(Ok(Property::Atime(true)), Property::try_from("atime"));
    assert_eq!(Ok(Property::Atime(false)),
               Property::try_from("atime=false"));
    assert_eq!(Ok(Property::Atime(false)), Property::try_from("atime=off"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("atime=xyz"));
    assert_eq!(Ok(Property::RecordSize(12)),
               Property::try_from("record_size=4096"));
    assert_eq!(Ok(Property::RecordSize(13)),
               Property::try_from("record_size=8192"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size=12"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size=true"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size"));
}

}
