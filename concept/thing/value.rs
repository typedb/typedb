/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;
use encoding::value::long_bytes::LongBytes;
use encoding::value::string_bytes::StringBytes;
use encoding::value::value_type::ValueType;
use encoding::value::ValueEncodable;

// TODO: how do we handle user-created compound structs?

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Boolean(bool),
    Long(i64),
    Double(f64),
    String(Cow<'a, str>),
}

impl<'a> Value<'a> {
    pub fn as_reference(&self) -> Value<'_> {
        match self {
            Value::Boolean(boolean) => Value::Boolean(*boolean),
            Value::Long(long) => Value::Long(*long),
            Value::Double(double) => Value::Double(*double),
            Value::String(string) => Value::String(Cow::Borrowed(string.as_ref())),
        }
    }

    pub fn unwrap_string(self) -> Cow<'a, str> {
        match self {
            Value::String(string) => string,
            _ => panic!("Cannot unwrap String if not a string value.")
        }
    }

    pub fn unwrap_long(self) -> i64 {
        match self {
            Value::Long(long) => long,
            _ => panic!("Cannot unwrap Long if not a long value.")
        }
    }
}

impl<'a> ValueEncodable for Value<'a> {
    fn value_type(&self) -> ValueType {
        match self {
            Value::Boolean(_) => ValueType::Boolean,
            Value::Long(_) => ValueType::Long,
            Value::Double(_) => ValueType::Double,
            Value::String(_) => ValueType::String,
        }
    }

    fn encode_long(&self) -> LongBytes {
        match self {
            Self::Long(long) => LongBytes::build(*long),
            _ => panic!("Cannot encoded non-long as LongBytes"),
        }
    }

    fn encode_string<const INLINE_LENGTH: usize>(&self) -> StringBytes<'_, INLINE_LENGTH> {
        match self {
            Value::String(str) => {
                StringBytes::build_ref(str)
            },
            _ => panic!("Cannot encoded non-String as StringBytes"),
        }
    }
}
