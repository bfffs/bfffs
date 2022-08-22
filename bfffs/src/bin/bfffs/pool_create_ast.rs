// vim: tw=80
//! Abstract Syntax Tree for the vdev specification in the "bfffs pool create"
//! command.

#[derive(Clone, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Disk<'a>(pub &'a str);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mirror<'a>(pub Vec<&'a str>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaidChild<'a> {
    Disk(Disk<'a>),
    Mirror(Mirror<'a>),
}

impl<'a> RaidChild<'a> {
    pub fn as_disk(&'a self) -> Option<&'a Disk<'a>> {
        match self {
            RaidChild::Disk(d) => Some(d),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Raid<'a> {
    pub k:     i16,
    pub f:     i16,
    pub vdevs: Vec<RaidChild<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Tlv<'a> {
    Raid(Raid<'a>),
    Mirror(Mirror<'a>),
    Disk(&'a str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pool<'a>(pub Vec<Tlv<'a>>);
