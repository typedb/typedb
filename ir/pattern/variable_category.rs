/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VariableCategory {
    Type,
    ThingType,
    AttributeType,
    RoleType,

    Thing,
    Object,

    Attribute,
    Value,

    ThingList,
    ObjectList,

    AttributeList,
    ValueList,
}

impl VariableCategory {
    pub(crate) fn narrowest(&self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::RoleType, Self::RoleType) => Some(Self::RoleType),
            (Self::RoleType, Self::Type) | (Self::Type, Self::RoleType) => Some(Self::RoleType),
            (Self::RoleType, _) | (_, Self::RoleType) => None,

            (Self::Type, Self::AttributeType) | (Self::AttributeType, Self::Type) => Some(Self::AttributeType),
            (Self::ThingType, Self::AttributeType) | (Self::AttributeType, Self::ThingType) => {
                Some(Self::AttributeType)
            }
            (Self::AttributeType, Self::AttributeType) => Some(Self::AttributeType),
            (Self::AttributeType, _) | (_, Self::AttributeType) => None,

            (Self::ThingType, Self::ThingType) => Some(Self::ThingType),
            (Self::ThingType, Self::Type) | (Self::Type, Self::ThingType) => Some(Self::ThingType),
            (Self::ThingType, _) | (_, Self::ThingType) => None,

            (Self::Type, Self::Type) => Some(Self::Type),
            (_, Self::Type) | (Self::Type, _) => None,

            (Self::Thing, Self::Thing) => Some(Self::Thing),
            (Self::Thing, Self::Object) | (Self::Object, Self::Thing) => Some(Self::Object),
            (Self::Thing, Self::Attribute) | (Self::Attribute, Self::Thing) => Some(Self::Attribute),
            (Self::Thing, Self::Value) | (Self::Value, Self::Thing) => Some(Self::Attribute),
            (_, Self::Thing) | (Self::Thing, _) => None,

            (Self::Object, Self::Object) => Some(Self::Object),
            (_, Self::Object) | (Self::Object, _) => None,

            (Self::Attribute, Self::Attribute) => Some(Self::Attribute),
            (Self::Value, Self::Attribute) | (Self::Attribute, Self::Value) => Some(Self::Attribute),
            (_, Self::Attribute) | (Self::Attribute, _) => None,

            (Self::Value, Self::Value) => Some(Self::Value),
            (_, Self::Value) | (Self::Value, _) => None,

            (Self::ObjectList, Self::ObjectList) => Some(Self::ObjectList),
            (Self::ThingList, Self::ObjectList) | (Self::ObjectList, Self::ThingList) => Some(Self::ObjectList),
            (_, Self::ObjectList) | (Self::ObjectList, _) => None,

            (Self::AttributeList, Self::AttributeList) => Some(Self::AttributeList),
            (Self::ThingList, Self::AttributeList) | (Self::AttributeList, Self::ThingList) => {
                Some(Self::AttributeList)
            }
            (_, Self::AttributeList) | (Self::AttributeList, _) => None,

            (Self::ThingList, Self::ThingList) => Some(Self::ThingList),
            (Self::ThingList, _) | (_, Self::ThingList) => None,

            (Self::ValueList, Self::ValueList) => Some(Self::ValueList),
            (_, Self::ValueList) | (Self::ValueList, _) => None,
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
