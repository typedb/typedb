/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::{value::Value, value_type::ValueTypeCategory};

pub trait ValueTypeTrait: Sized {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory;
    const MOCK_VALUE: Value<'static>;

    fn from_value(value: Value<'static>) -> Result<Self, ()>;

    fn into_value(self) -> Value<'static>;
}

impl ValueTypeTrait for f64 {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Double;
    const MOCK_VALUE: Value<'static> = Value::Double(0f64);

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Double(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Double(self)
    }
}

impl ValueTypeTrait for i64 {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Long;
    const MOCK_VALUE: Value<'static> = Value::Long(0);

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Long(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Long(self)
    }
}
