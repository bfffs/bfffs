//vim: tw=80
//! Dataset Properties
use std::{
    fmt,
    str::FromStr
};
use crate::{Error, Result};
use serde_derive::*;

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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Property {
    /// Access time.
    ///
    /// When on, then POSIX atime of a file will be updated on every access.
    /// When off, atime will be treated like ctime.
    Atime(bool),

    /// An explicitly set mountpoint of the file system or its parent.
    ///
    /// If `Mountpoint` is set explicitly on a file system, then it will be
    /// equal to `BaseMountpoint`.  If not, then `BaseMountpoint` will be equal
    /// to the `Mountpoint` of the first parent that has it explicitly set.
    // In other words, `BaseMountpoint` is inheritable just like other
    // properties.
    BaseMountpoint(String),

    /// Mountpoint of the file system.  The default is based on concatenating
    /// "/", the pool name, and the file system name.
    Mountpoint(String),

    /// The dataset's name
    Name(String),

    /// Suggested block size for newly written data.
    ///
    /// Units are in bytes, log base 2.  So `RecordSize(16)` means 64KB records.
    /// BFFFS will usually divide files into blocks of this many bytes.  But the
    /// record size is only advisory.  The default is 128KB.
    RecordSize(u8),
}

impl Property {
    pub fn default_value(name: PropertyName) -> Self {
        match name {
            PropertyName::Atime => Property::Atime(true),
            PropertyName::BaseMountpoint =>
                Property::BaseMountpoint("".to_string()),
            PropertyName::Mountpoint =>
                unimplemented!("Does not have a static default value"),
            PropertyName::Name =>
                unimplemented!("Does not have a static default value"),
            PropertyName::RecordSize => Property::RecordSize(17), // 128KB
        }
    }

    pub(crate) fn inheritable(self) -> Self {
        if let Property::Mountpoint(mp) = self {
            Property::BaseMountpoint(mp)
        } else {
            self
        }
    }

    /// Helper to construct a Mountpoint property
    pub fn mountpoint<S: Into<String>>(s: S) -> Self {
        Property::Mountpoint(s.into())
    }

    pub fn name(&self) -> PropertyName {
        match self {
            Property::Atime(_) => PropertyName::Atime,
            Property::BaseMountpoint(_) => PropertyName::BaseMountpoint,
            Property::Mountpoint(_) => PropertyName::Mountpoint,
            Property::Name(_) => PropertyName::Name,
            Property::RecordSize(_) => PropertyName::RecordSize,
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Property::Atime(atime) => *atime,
            _ => panic!("{:?} is not a boolean Property", self)
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Property::BaseMountpoint(mp) => mp,
            Property::Mountpoint(mp) => mp,
            Property::Name(s) => s,
            _ => panic!("{:?} is not a str Property", self)
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Property::RecordSize(rs) => *rs,
            _ => panic!("{:?} is not a u8 Property", self)
        }
    }
}

impl fmt::Display for Property {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Property::Atime(b) => match b {
                true => "on".fmt(f),
                false => "off".fmt(f),
            },
            Property::BaseMountpoint(s) => s.fmt(f),
            Property::Mountpoint(s) => s.fmt(f),
            Property::Name(s) => s.fmt(f),
            Property::RecordSize(i) => (1 << i).fmt(f),
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
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
                "base_mountpoint" =>
                    Ok(Property::BaseMountpoint(v.to_string())),
                "mountpoint" =>
                    Ok(Property::Mountpoint(v.to_string())),
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
                            524_288 => Ok(Property::RecordSize(19)),
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
    BaseMountpoint,
    Mountpoint,
    Name,
    RecordSize,
}

impl PropertyName {
    pub(crate) fn inheritable(self) -> Self {
        match self {
            PropertyName::Mountpoint => PropertyName::BaseMountpoint,
            PropertyName::Name => unimplemented!(),
            x => x
        }
    }
}

impl fmt::Display for PropertyName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Atime => "atime".fmt(f),
            Self::BaseMountpoint => "basemountpoint".fmt(f),
            Self::Mountpoint => "mountpoint".fmt(f),
            Self::Name => "name".fmt(f),
            Self::RecordSize => "recordsize".fmt(f),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsePropertyNameError{}
impl fmt::Display for ParsePropertyNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not a valid property name")
    }
}
impl std::error::Error for ParsePropertyNameError {}

impl FromStr for PropertyName {
    type Err = ParsePropertyNameError;

    fn from_str(s: &str) -> std::result::Result<Self, ParsePropertyNameError> {
        match s {
            "atime" => Ok(PropertyName::Atime),
            "basemountpoint" => Ok(PropertyName::BaseMountpoint),
            "mountpoint" => Ok(PropertyName::Mountpoint),
            "name" => Ok(PropertyName::Name),
            "recordsize" => Ok(PropertyName::RecordSize),
            "recsize" => Ok(PropertyName::RecordSize),
            _ => Err(ParsePropertyNameError{})
        }
    }
}

/// Where did the property come from?
// The inner value is the number of levels upward from which the property was
// inherited:
// Some(0)  -   Locally set property
// Some(1)  -   Inherited from parent
// Some(2)  -   Inherited from grandparent
// ...
// None     -   Default value
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum PropertySource {
    /// This is a pseudoproperty that has no source.
    None,
    /// No value has been set for this property on this dataset or any of its
    /// parents.
    Default,
    /// The property was explicitly set on this dataset or a on an ancestor.
    Set(u8)
}

impl PropertySource {
    /// The property was explicitly set on this dataset
    pub const LOCAL: PropertySource = PropertySource::Set(0);
    /// The property's value is inherited from the grandparent dataset.
    pub const FROM_PARENT: PropertySource = PropertySource::Set(1);
    /// The property's value is inherited from the parent dataset.
    pub const FROM_GRANDPARENT: PropertySource = PropertySource::Set(2);

    pub fn is_inherited(self) -> bool {
        matches!(self, PropertySource::Set(i) if i > 0)
    }
}

impl fmt::Display for PropertySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Default => "default".fmt(f),
            Self::LOCAL => "local".fmt(f),
            _ => "inherited".fmt(f)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsePropertySourceError{}
impl fmt::Display for ParsePropertySourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not a valid property source")
    }
}
impl std::error::Error for ParsePropertySourceError {}

impl FromStr for PropertySource {
    type Err = ParsePropertySourceError;

    fn from_str(s: &str) -> std::result::Result<Self, ParsePropertySourceError> {
        match s {
            "default" => Ok(PropertySource::Default),
            "local" => Ok(PropertySource::LOCAL),
            "none" => Ok(PropertySource::None),
            "inherited" => Ok(PropertySource::Set(1)),
            _ => Err(ParsePropertySourceError{})
        }
    }
}


// LCOV_EXCL_START
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
    assert_eq!(Ok(Property::BaseMountpoint("/mnt".to_string())),
        Property::try_from("base_mountpoint=/mnt"));
    assert_eq!(Err(Error::EINVAL),
        Property::try_from("base_mountpoint"));
    assert_eq!(Ok(Property::Mountpoint("/mnt".to_string())),
        Property::try_from("mountpoint=/mnt"));
    assert_eq!(Err(Error::EINVAL),
        Property::try_from("mountpoint"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("name"));
    assert_eq!(Ok(Property::RecordSize(12)),
               Property::try_from("record_size=4096"));
    assert_eq!(Ok(Property::RecordSize(13)),
        Property::try_from("record_size=8192"));
    assert_eq!(Ok(Property::RecordSize(14)),
        Property::try_from("record_size=16384"));
    assert_eq!(Ok(Property::RecordSize(15)),
        Property::try_from("record_size=32768"));
    assert_eq!(Ok(Property::RecordSize(16)),
        Property::try_from("record_size=65536"));
    assert_eq!(Ok(Property::RecordSize(17)),
        Property::try_from("record_size=131072"));
    assert_eq!(Ok(Property::RecordSize(18)),
        Property::try_from("record_size=262144"));
    assert_eq!(Ok(Property::RecordSize(19)),
        Property::try_from("record_size=524288"));
    assert_eq!(Ok(Property::RecordSize(20)),
        Property::try_from("record_size=1048776"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size=12"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size=true"));
    assert_eq!(Err(Error::EINVAL), Property::try_from("record_size"));
}

}
// LCOV_EXCL_STOP
