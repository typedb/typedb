/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Variable {
    id: VariableId,
}

impl Variable {
    pub fn new(id: u16) -> Self {
        Self { id: VariableId { id } }
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.id)
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct VariableId {
    id: u16,
    // TODO: retain line/character from original query at which point this Variable was declared
}

impl VariableId {
    const MAX: usize = u16::MAX as usize;
}

impl Display for VariableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VariableCategory {
    Type,
    Thing,

    Object,
    // TODO: introduce entity and relation, since these could be useful to make type inference easier
    Attribute,
    RoleImpl,
    Value,

    ObjectList,
    AttributeList,
    ValueList,
    RoleImplList,
}

impl VariableCategory {
    pub(crate) fn narrowest(&self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Type, Self::Type) => Some(Self::Type),
            (_, Self::Type) | (Self::Type, _) => None,

            (Self::Thing, Self::Thing) => Some(Self::Thing),
            (Self::Thing, Self::Object) | (Self::Object, Self::Thing) => Some(Self::Object),
            (Self::Thing, Self::Attribute) | (Self::Attribute, Self::Thing) => Some(Self::Attribute),
            (Self::Thing, Self::RoleImpl) | (Self::RoleImpl, Self::Thing) => Some(Self::RoleImpl),
            (_, Self::Thing) | (Self::Thing, _) => None,

            (Self::Object, Self::Object) => Some(Self::Object),
            (Self::Object, Self::RoleImpl) | (Self::RoleImpl, Self::Object) => Some(Self::RoleImpl),
            (_, Self::Object) | (Self::Object, _) => None,

            (Self::Attribute, Self::Attribute) => Some(Self::Attribute),
            (_, Self::Attribute) | (Self::Attribute, _) => None,

            (Self::RoleImpl, Self::RoleImpl) => Some(Self::RoleImpl),
            (_, Self::RoleImpl) | (Self::RoleImpl, _) => None,

            (Self::Value, Self::Value) => Some(Self::Value),
            (_, Self::Value) | (Self::Value, _) => None,

            (Self::ObjectList, Self::ObjectList) => Some(Self::ObjectList),
            (Self::ObjectList, Self::RoleImplList) | (Self::RoleImplList, Self::ObjectList) => Some(Self::RoleImplList),
            (_, Self::ObjectList) | (Self::ObjectList, _) => None,

            (Self::AttributeList, Self::AttributeList) => Some(Self::AttributeList),
            (_, Self::AttributeList) | (Self::AttributeList, _) => None,

            (Self::ValueList, Self::ValueList) => Some(Self::ValueList),
            (_, Self::ValueList) | (Self::ValueList, _) => None,

            (Self::RoleImplList, Self::RoleImplList) => Some(Self::RoleImplList),
            (_, Self::RoleImplList) | (Self::RoleImplList, _) => None,
        }
    }
}

impl Display for VariableCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VariableOptionality {
    Required,
    Optional,
}

impl Display for VariableOptionality {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VariableOptionality::Required => {
                write!(f, "req")
            }
            VariableOptionality::Optional => {
                write!(f, "opt")
            }
        }
    }
}

