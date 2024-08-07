//vim: tw=80
//! Dataset Properties
use std::{
    fmt,
    str::FromStr
};
use serde_derive::*;
use thiserror::Error;

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
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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
            _ => panic!("{self:?} is not a boolean Property")
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Property::BaseMountpoint(mp) => mp,
            Property::Mountpoint(mp) => mp,
            Property::Name(s) => s,
            _ => panic!("{self:?} is not a str Property")
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Property::RecordSize(rs) => *rs,
            _ => panic!("{self:?} is not a u8 Property")
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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ParsePropertyError {
    #[error("Does not contain an '=' character")]
    NoEquals,
    #[error("{0}")]
    Name(ParsePropertyNameError),
    #[error("This property is read-only")]
    ReadOnly,
    #[error("Not a valid value for this property")]
    Value(String)
}

impl FromStr for Property {
    type Err = ParsePropertyError;

    fn from_str(s: &str) -> std::result::Result<Self, ParsePropertyError> {
        let mut words = s.splitn(2, '=');
        let propnamestr = words.next().unwrap();
        let propname = PropertyName::from_str(propnamestr)
            .map_err(ParsePropertyError::Name)?;
        // Value may be omitted only for boolean options
        let propval = if propname.boolean() {
            words.next().unwrap_or("on")
        } else {
            words.next().ok_or(ParsePropertyError::NoEquals)?
        };
        match propname {
            PropertyName::Atime => {
                match propval {
                    "true" | "on" => Ok(Property::Atime(true)),
                    "false" | "off" => Ok(Property::Atime(false)),
                    _ => Err(ParsePropertyError::Value(propval.to_string()))
                }
            },
            PropertyName::BaseMountpoint => Err(ParsePropertyError::ReadOnly),
            PropertyName::Mountpoint =>
                Ok(Property::Mountpoint(propval.to_string())),
            PropertyName::Name => Err(ParsePropertyError::ReadOnly),
            PropertyName::RecordSize => {
                if let Ok(rs) = propval.parse::<usize>() {
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
                        _ => Err(ParsePropertyError::Value(propval.to_string()))
                    }
                } else {
                    Err(ParsePropertyError::Value(propval.to_string()))
                }
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
    /// Does this property take boolean values?
    fn boolean(self) -> bool {
        matches!(self, Self::Atime)
    }

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

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("Not a valid property name")]
pub struct ParsePropertyNameError;

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

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("Not a valid property source")]
pub struct ParsePropertySourceError;

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
fn property_from_str() {
    assert!(matches!(
        Property::from_str(""),
        Err(ParsePropertyError::Name(_))
    ));
    assert_eq!(Ok(Property::Atime(true)), Property::from_str("atime=true"));
    assert_eq!(Ok(Property::Atime(true)), Property::from_str("atime=on"));
    assert_eq!(Ok(Property::Atime(true)), Property::from_str("atime"));
    assert_eq!(Ok(Property::Atime(false)), Property::from_str("atime=false"));
    assert_eq!(Ok(Property::Atime(false)), Property::from_str("atime=off"));
    assert!(matches!(
        Property::from_str("atime=xyz"),
        Err(ParsePropertyError::Value(_))
    ));
    assert!(matches!(
        Property::from_str("basemountpoint=/mnt"),
        Err(ParsePropertyError::ReadOnly)
    ));
    assert!(matches!(
        Property::from_str("basemountpoint"),
        Err(ParsePropertyError::NoEquals)
    ));
    assert_eq!(Ok(Property::Mountpoint("/mnt".to_string())),
        Property::from_str("mountpoint=/mnt"));
    assert!(matches!(
        Property::from_str("mountpoint"),
        Err(ParsePropertyError::NoEquals)
    ));
    assert!(matches!(
        Property::from_str("name=foo"),
        Err(ParsePropertyError::ReadOnly)
    ));
    assert_eq!(Ok(Property::RecordSize(12)),
               Property::from_str("recordsize=4096"));
    assert_eq!(Ok(Property::RecordSize(13)),
        Property::from_str("recordsize=8192"));
    assert_eq!(Ok(Property::RecordSize(14)),
        Property::from_str("recordsize=16384"));
    assert_eq!(Ok(Property::RecordSize(15)),
        Property::from_str("recordsize=32768"));
    assert_eq!(Ok(Property::RecordSize(16)),
        Property::from_str("recordsize=65536"));
    assert_eq!(Ok(Property::RecordSize(17)),
        Property::from_str("recordsize=131072"));
    assert_eq!(Ok(Property::RecordSize(18)),
        Property::from_str("recordsize=262144"));
    assert_eq!(Ok(Property::RecordSize(19)),
        Property::from_str("recordsize=524288"));
    assert_eq!(Ok(Property::RecordSize(20)),
        Property::from_str("recordsize=1048776"));
    assert!(matches!(
        Property::from_str("recordsize=12"),
        Err(ParsePropertyError::Value(_))
    ));
    assert!(matches!(
        Property::from_str("recordsize=true"),
        Err(ParsePropertyError::Value(_))
    ));
    assert_eq!(Err(ParsePropertyError::NoEquals),
        Property::from_str("recordsize"));
}

}
// LCOV_EXCL_STOP
