/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use encoding::value::value::Value;

use crate::{Thing, Type};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VariableValue<'a> {
    Empty,
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
    ThingList(Arc<[Thing<'a>]>),
    ValueList(Arc<[Value<'a>]>),
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
