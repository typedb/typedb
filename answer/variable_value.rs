/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::thing::value::Value;
use encoding::value::value_type::ValueType;
use crate::{Thing, Type};

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
