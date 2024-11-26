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
    Empty,
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
    ThingList(Arc<[Thing<'static>]>),
    ValueList(Arc<[Value<'static>]>),
}

impl<'a> VariableValue<'a> {
    pub fn as_type(&self) -> &Type {
        match self {
            Self::Type(type_) => type_,
            _ => panic!("VariableValue is not a Type: {self:?}"),
        }
    }

    pub fn as_thing(&self) -> &Thing<'a> {
        match self {
            VariableValue::Thing(thing) => thing,
            _ => panic!("VariableValue is not a Thing: {self:?}"),
        }
    }

    pub fn as_value(&self) -> &Value<'a> {
        match self {
            VariableValue::Value(value) => value,
            // TODO: Do we want to implicit cast from attributes?
            _ => panic!("VariableValue is not a value"),
        }
    }

    pub fn to_owned(&self) -> VariableValue<'static> {
        match self {
            VariableValue::Empty => VariableValue::Empty,
            VariableValue::Type(type_) => VariableValue::Type(type_.clone()),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.to_owned()),
            VariableValue::Value(value) => VariableValue::Value(value.clone().into_owned()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list.clone()),
            VariableValue::ValueList(list) => VariableValue::ValueList(list.clone()),
        }
    }

    pub fn as_reference(&self) -> VariableValue<'_> {
        match self {
            VariableValue::Empty => VariableValue::Empty,
            VariableValue::Type(type_) => VariableValue::Type(type_.clone()),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.as_reference()),
            VariableValue::Value(value) => VariableValue::Value(value.as_reference()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list.clone()),
            VariableValue::ValueList(list) => VariableValue::ValueList(list.clone()),
        }
    }

    pub fn into_owned(self) -> VariableValue<'static> {
        match self {
            VariableValue::Empty => VariableValue::Empty,
            VariableValue::Type(type_) => VariableValue::Type(type_),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.into_owned()),
            VariableValue::Value(value) => VariableValue::Value(value.into_owned()),
            VariableValue::ThingList(list) => VariableValue::ThingList(list),
            VariableValue::ValueList(list) => VariableValue::ValueList(list),
        }
    }

    pub fn next_possible(&self) -> VariableValue<'static> {
        match self {
            VariableValue::Empty => unreachable!("No next value for an Empty value."),
            VariableValue::Type(type_) => VariableValue::Type(type_.next_possible()),
            VariableValue::Thing(thing) => VariableValue::Thing(thing.next_possible()),
            VariableValue::Value(_) => unreachable!("Value instances don't have a well defined order."),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => {
                unreachable!("Lists have no well defined order.")
            }
        }
    }

    /// Returns `true` if the variable value is [`Empty`].
    ///
    /// [`Empty`]: VariableValue::Empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

impl Hkt for VariableValue<'static> {
    type HktSelf<'a> = VariableValue<'a>;
}

impl PartialOrd for VariableValue<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
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
            VariableValue::Empty => write!(f, "[None]"),
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

impl<'a> VariableValue<'a> {
    pub const EMPTY: VariableValue<'static> = VariableValue::Empty;
}

pub enum FunctionValue<'a> {
    Thing(Thing<'a>),
    ThingOptional(Option<Thing<'a>>),
    Value(Value<'a>),
    ValueOptional(Option<Value<'a>>),
    ThingList(Vec<Thing<'a>>),
    ThingListOptional(Option<Vec<Thing<'a>>>),
    ValueList(Vec<Value<'a>>),
    ValueListOptional(Option<Vec<Value<'a>>>),
}
