/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::borrow::Cow;
use encoding::value::value_type::ValueType;

// TODO: how do we handle user-created compound structs?

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Boolean(bool),
    Long(i64),
    Double(f64),
    String(Cow<'a, Box<str>>),
}

impl<'a> Value<'a> {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Boolean(_) => ValueType::Boolean,
            Value::Long(_) => ValueType::Long,
            Value::Double(_) => ValueType::Double,
            Value::String(_) => ValueType::String,
        }
    }

    pub fn as_reference(&self) -> Value<'_> {
        match self {
            Value::Boolean(boolean) => Value::Boolean(*boolean),
            Value::Long(long) => Value::Long(*long),
            Value::Double(double) => Value::Double(*double),
            Value::String(string) => Value::String(Cow::Borrowed(string.as_ref())),
        }
    }
}
