/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::{Cow, ToOwned},
    ops::Deref,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use encoding::value::{decimal_value::Decimal, duration_value::Duration, value::Value, value_type::ValueTypeCategory};

pub trait ValueTypeTrait: Sized {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory;

    fn from_value(value: Value<'static>) -> Result<Self, ()>;

    fn into_value(self) -> Value<'static>;
}

impl ValueTypeTrait for bool {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Boolean;

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Boolean(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Boolean(self)
    }
}

impl ValueTypeTrait for f64 {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Double;

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

impl ValueTypeTrait for String {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::String;
    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::String(value) => Ok(value.deref().to_owned()),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::String(Cow::Owned(self))
    }
}

impl ValueTypeTrait for Decimal {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Decimal;
    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Decimal(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Decimal(self)
    }
}

impl ValueTypeTrait for NaiveDate {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Date;

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Date(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Date(self)
    }
}

impl ValueTypeTrait for NaiveDateTime {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::DateTime;

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::DateTime(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::DateTime(self)
    }
}

impl ValueTypeTrait for DateTime<Tz> {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::DateTimeTZ;

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::DateTimeTZ(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::DateTimeTZ(self)
    }
}

impl ValueTypeTrait for Duration {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Duration;

    fn from_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Duration(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn into_value(self) -> Value<'static> {
        Value::Duration(self)
    }
}
