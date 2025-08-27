/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, fmt, sync::Arc};

use encoding::value::value::Value;
use lending_iterator::higher_order::Hkt;

use crate::{Thing, Type};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum VariableValue<'a> {
    None,
    Type(Type),
    Thing(Thing),
    Value(Value<'a>),
    ThingList(Arc<[Thing]>),
    ValueList(Arc<[Value<'static>]>),
}

impl<'a> VariableValue<'a> {
    pub fn as_type(&self) -> Type {
        self.get_type().unwrap_or_else(|| panic!("VariableValue is not a Type: {:?}", self))
    }

    pub fn get_type(&self) -> Option<Type> {
        match self {
            &VariableValue::Type(type_) => Some(type_),
            _ => None,
        }
    }

    pub fn as_thing(&self) -> &Thing {
        self.get_thing().unwrap_or_else(|| panic!("VariableValue is not a Thing: {:?}", self))
    }

    pub fn get_thing(&self) -> Option<&Thing> {
        match self {
            VariableValue::Thing(thing) => Some(thing),
            _ => None,
        }
    }

    pub fn as_value(&self) -> &Value<'a> {
        match self {
            VariableValue::Value(value) => value,
            _ => panic!("VariableValue is not a value: {:?}", self),
        }
    }

    pub fn to_owned(&self) -> VariableValue<'static> {
        match self {
            VariableValue::None => VariableValue::None,
            &VariableValue::Type(type_) => VariableValue::Type(type_),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.to_owned()),
            VariableValue::Value(value) => VariableValue::Value(value.clone().into_owned()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list.clone()),
            VariableValue::ValueList(list) => VariableValue::ValueList(list.clone()),
        }
    }

    pub fn as_reference(&self) -> VariableValue<'_> {
        match self {
            VariableValue::None => VariableValue::None,
            &VariableValue::Type(type_) => VariableValue::Type(type_),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.clone()),
            VariableValue::Value(value) => VariableValue::Value(value.as_reference()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list.clone()),
            VariableValue::ValueList(list) => VariableValue::ValueList(list.clone()),
        }
    }

    pub fn into_owned(self) -> VariableValue<'static> {
        match self {
            VariableValue::None => VariableValue::None,
            VariableValue::Type(type_) => VariableValue::Type(type_),
            VariableValue::Thing(thing) => VariableValue::Thing(thing),
            VariableValue::Value(value) => VariableValue::Value(value.into_owned()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list),
            VariableValue::ValueList(list) => VariableValue::ValueList(list),
        }
    }

    pub fn next_possible(&self) -> VariableValue<'static> {
        match self {
            VariableValue::None => unreachable!("No next value for an None value."),
            VariableValue::Type(type_) => VariableValue::Type(type_.next_possible()),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.next_possible()),
            VariableValue::Value(_) => unreachable!("Value instances don't have a well defined order."),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => {
                unreachable!("Lists have no well defined order.")
            }
        }
    }

    /// Returns `true` if the variable value is [`None`].
    ///
    /// [`None`]: VariableValue::None
    #[must_use]
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn variant_name(&self) -> &'static str {
        match self {
            VariableValue::None => "none",
            VariableValue::Type(_) => "type",
            VariableValue::Thing(_) => "thing",
            VariableValue::Value(_) => "value",
            VariableValue::ThingList(_) => "thing list",
            VariableValue::ValueList(_) => "value list",
        }
    }
}

impl Hkt for VariableValue<'static> {
    type HktSelf<'a> = VariableValue<'a>;
}

impl PartialOrd for VariableValue<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // special case: None is less than everything, except also equal to None
            (Self::None, Self::None) => Some(Ordering::Equal),
            (Self::None, _) => Some(Ordering::Less),
            (_, Self::None) => Some(Ordering::Greater),
            (Self::Type(self_type), Self::Type(other_type)) => self_type.partial_cmp(other_type),
            (Self::Thing(self_thing), Self::Thing(other_thing)) => self_thing.partial_cmp(other_thing),
            (Self::Value(self_value), Self::Value(other_value)) => self_value.partial_cmp(other_value),
            _ => None,
        }
    }
}

impl fmt::Display for VariableValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VariableValue::None => write!(f, "[None]"),
            VariableValue::Type(type_) => write!(f, "{}", type_),
            VariableValue::Thing(thing) => write!(f, "{}", thing),
            VariableValue::Value(value) => write!(f, "{}", value),
            VariableValue::ThingList(thing_list) => {
                write!(f, "[")?;
                for thing in thing_list.as_ref() {
                    write!(f, "{}, ", thing)?;
                }
                write!(f, "]")
            }
            VariableValue::ValueList(value_list) => {
                write!(f, "[")?;
                for value in value_list.as_ref() {
                    write!(f, "{}, ", value)?;
                }
                write!(f, "]")
            }
        }
    }
}

impl VariableValue<'_> {
    pub const NONE: VariableValue<'static> = VariableValue::None;
}

pub enum FunctionValue<'a> {
    Thing(Thing),
    ThingOptional(Option<Thing>),
    Value(Value<'a>),
    ValueOptional(Option<Value<'a>>),
    ThingList(Vec<Thing>),
    ThingListOptional(Option<Vec<Thing>>),
    ValueList(Vec<Value<'a>>),
    ValueListOptional(Option<Vec<Value<'a>>>),
}
