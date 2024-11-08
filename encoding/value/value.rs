/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

use bytes::byte_array::ByteArray;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

use crate::{
    value::{
        boolean_bytes::BooleanBytes,
        date_bytes::DateBytes,
        date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes,
        decimal_bytes::DecimalBytes,
        decimal_value::{Decimal, FRACTIONAL_PART_DENOMINATOR_LOG10},
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        duration_value::Duration,
        long_bytes::LongBytes,
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        timezone::TimeZone,
        value_struct::StructValue,
        value_type::{ValueType, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Boolean(bool),
    Long(i64),
    Double(f64),
    Decimal(Decimal),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    DateTimeTZ(DateTime<TimeZone>),
    Duration(Duration),
    String(Cow<'a, str>),
    Struct(Cow<'a, StructValue<'static>>),
}

// TODO: should we implement our own Equality, which takes into account floating point EPSILON? Otherwise, we'll transmit rounding errors throughout the language
impl<'a> Eq for Value<'a> {}

impl<'a> PartialOrd for Value<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Boolean(self_bool), Self::Boolean(other_bool)) => self_bool.partial_cmp(other_bool),
            (Self::Long(self_long), Self::Long(other_long)) => self_long.partial_cmp(other_long),
            (Self::Double(self_double), Self::Double(other_double)) => self_double.partial_cmp(other_double),
            (Self::Decimal(self_decimal), Self::Decimal(other_decimal)) => self_decimal.partial_cmp(other_decimal),
            (Self::Date(self_date), Self::Date(other_date)) => self_date.partial_cmp(other_date),
            (Self::DateTime(self_date_time), Self::DateTime(other_date_time)) => {
                self_date_time.partial_cmp(other_date_time)
            }
            (Self::DateTimeTZ(self_date_time_tz), Self::DateTimeTZ(other_date_time_tz)) => {
                self_date_time_tz.partial_cmp(other_date_time_tz)
            }
            (Self::String(self_string), Self::String(other_string)) => self_string.partial_cmp(other_string),
            _ => None,
        }
    }
}

impl<'a> Hash for Value<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Boolean(value) => Hash::hash(value, state),
            Value::Long(value) => Hash::hash(value, state),
            Value::Double(_value) => Hash::hash(&self.encode_double(), state), // same bitwise representation as storage of values
            Value::Decimal(value) => Hash::hash(value, state),
            Value::Date(value) => Hash::hash(value, state),
            Value::DateTime(value) => Hash::hash(value, state),
            Value::DateTimeTZ(value) => Hash::hash(value, state),
            Value::Duration(value) => Hash::hash(value, state),
            Value::String(value) => Hash::hash(value, state),
            Value::Struct(value) => Hash::hash(value, state),
        }
    }
}

impl<'a> Value<'a> {
    pub fn as_reference(&self) -> Value<'_> {
        match *self {
            Value::Boolean(boolean) => Value::Boolean(boolean),
            Value::Long(long) => Value::Long(long),
            Value::Double(double) => Value::Double(double),
            Value::Decimal(decimal) => Value::Decimal(decimal),
            Value::Date(date) => Value::Date(date),
            Value::DateTime(date_time) => Value::DateTime(date_time),
            Value::DateTimeTZ(date_time_tz) => Value::DateTimeTZ(date_time_tz),
            Value::Duration(duration) => Value::Duration(duration),
            Value::String(ref string) => Value::String(Cow::Borrowed(string.as_ref())),
            Value::Struct(ref struct_) => Value::Struct(Cow::Borrowed(struct_.as_ref())),
        }
    }

    pub fn unwrap_boolean(self) -> bool {
        match self {
            Self::Boolean(boolean) => boolean,
            _ => panic!("Cannot unwrap Long if not a long value."),
        }
    }

    pub fn unwrap_long(self) -> i64 {
        match self {
            Self::Long(long) => long,
            _ => panic!("Cannot unwrap Long if not a long value."),
        }
    }

    pub fn unwrap_double(self) -> f64 {
        match self {
            Self::Double(double) => double,
            _ => panic!("Cannot unwrap Double if not a double value."),
        }
    }

    pub fn unwrap_decimal(self) -> Decimal {
        match self {
            Self::Decimal(decimal) => decimal,
            _ => panic!("Cannot unwrap Double if not a double value."),
        }
    }

    pub fn unwrap_date(self) -> NaiveDate {
        match self {
            Self::Date(date) => date,
            _ => panic!("Cannot unwrap Date if not a date value."),
        }
    }

    pub fn unwrap_date_time(self) -> NaiveDateTime {
        match self {
            Self::DateTime(date_time) => date_time,
            _ => panic!("Cannot unwrap DateTime if not a datetime value."),
        }
    }

    pub fn unwrap_date_time_tz(self) -> DateTime<TimeZone> {
        match self {
            Self::DateTimeTZ(date_time_tz) => date_time_tz,
            _ => panic!("Cannot unwrap DateTimeTZ if not a datetime-tz value."),
        }
    }

    pub fn unwrap_duration(self) -> Duration {
        match self {
            Self::Duration(duration) => duration,
            _ => panic!("Cannot unwrap DateTimeTZ if not a datetime-tz value."),
        }
    }

    pub fn unwrap_string(self) -> Cow<'a, str> {
        match self {
            Self::String(string) => string,
            _ => panic!("Cannot unwrap String if not a string value."),
        }
    }

    pub fn unwrap_struct(self) -> Cow<'a, StructValue<'static>> {
        match self {
            Value::Struct(struct_) => struct_,
            _ => panic!("Cannot unwrap Struct if not a struct value."),
        }
    }

    pub fn into_owned(self) -> Value<'static> {
        match self {
            Self::Boolean(bool) => Value::Boolean(bool),
            Self::Long(long) => Value::Long(long),
            Self::Double(double) => Value::Double(double),
            Self::Decimal(decimal) => Value::Decimal(decimal),
            Self::Date(date) => Value::Date(date),
            Self::DateTime(date_time) => Value::DateTime(date_time),
            Self::DateTimeTZ(date_time_tz) => Value::DateTimeTZ(date_time_tz),
            Self::Duration(duration) => Value::Duration(duration),
            Self::String(string) => Value::String(Cow::Owned(string.into_owned())),
            Self::Struct(struct_) => Value::Struct(Cow::Owned(struct_.into_owned())),
        }
    }

    pub fn cast(self, value_type: &ValueType) -> Option<Value<'static>> {
        if &self.value_type() == value_type {
            return Some(self.into_owned());
        } else if !self.value_type().is_trivially_castable_to(value_type) {
            return None;
        }

        match self {
            Value::Long(long) => {
                debug_assert!(matches!(value_type, &ValueType::Double | &ValueType::Decimal));
                match value_type {
                    ValueType::Double => Some(Value::Double(long as f64)),
                    ValueType::Decimal => Some(Value::Decimal(Decimal::new(long, 0))),
                    _ => unreachable!(),
                }
            }
            Value::Decimal(decimal) => {
                debug_assert_eq!(value_type, &ValueType::Double);
                Some(Value::Double(decimal.to_f64()))
            }
            Value::Date(date) => {
                debug_assert!(matches!(value_type, &ValueType::DateTime));
                Some(Value::DateTime(date.and_time(NaiveTime::default())))
            }
            _ => unreachable!(),
        }
    }

    pub fn approximate_cast_lower_bound(self, value_type: &ValueType) -> Option<Value<'static>> {
        if &self.value_type() == value_type {
            return Some(self.into_owned());
        } else if !self.value_type().is_approximately_castable_to(value_type) {
            return None;
        } else if self.value_type().is_trivially_castable_to(value_type) {
            return self.cast(value_type);
        }

        match self {
            Value::Long(_) => unreachable!("Handled by trivial cast"),
            Value::Decimal(decimal) => {
                debug_assert!(matches!(value_type, &ValueType::Double | &ValueType::Long));
                match value_type {
                    ValueType::Long => Some(Value::Long(decimal.integer_part())),
                    ValueType::Double => Some(Value::Double(decimal.to_f64())),
                    _ => unreachable!(),
                }
            }
            Value::Double(double) => {
                debug_assert!(matches!(value_type, &ValueType::Decimal | &ValueType::Long));
                match value_type {
                    ValueType::Decimal => {
                        let integer_part =
                            if double.floor() > i64::MAX as f64 { i64::MAX } else { double.floor() as i64 };
                        let fractional_part = (double - (integer_part as f64)).abs();
                        Some(Value::Decimal(Decimal::new_lower_bound_from(integer_part, fractional_part)))
                    }
                    ValueType::Long => {
                        if double.floor() > i64::MAX as f64 {
                            Some(Value::Long(i64::MAX))
                        } else {
                            Some(Value::Long(double.floor() as i64))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Value::Date(_) => unreachable!("Handled by trivial cast"),
            _ => unreachable!(),
        }
    }

    pub fn approximate_cast_upper_bound(self, value_type: &ValueType) -> Option<Value<'static>> {
        if &self.value_type() == value_type {
            return Some(self.into_owned());
        } else if !self.value_type().is_approximately_castable_to(value_type) {
            return None;
        } else if self.value_type().is_trivially_castable_to(value_type) {
            return self.cast(value_type);
        }

        match self {
            Value::Long(_) => unreachable!("Handled by trivial cast"),
            Value::Decimal(decimal) => {
                debug_assert!(matches!(value_type, &ValueType::Double | &ValueType::Long));
                match value_type {
                    ValueType::Long => {
                        let integer_part = decimal.integer_part();
                        if decimal.fractional_part() == 0 {
                            Some(Value::Long(integer_part))
                        } else {
                            Some(Value::Long(integer_part + 1))
                        }
                    }
                    ValueType::Double => {
                        // TODO: we might be able to have a tighter bound?
                        Some(Value::Double(decimal.to_f64() + 1.0 / FRACTIONAL_PART_DENOMINATOR_LOG10 as f64))
                    }
                    _ => unreachable!(),
                }
            }
            Value::Double(double) => {
                debug_assert!(matches!(value_type, &ValueType::Decimal | &ValueType::Long));
                match value_type {
                    ValueType::Decimal => {
                        let integer_part = if double.floor() > i64::MAX as f64 {
                            panic!("Cannot create an upper-bounding decimal from double {}", double);
                        } else {
                            double.floor() as i64
                        };
                        let fractional_part = (double - (integer_part as f64)).abs();
                        Some(Value::Decimal(Decimal::new_upper_bound_from(integer_part, fractional_part)))
                    }
                    ValueType::Long => {
                        if double.floor() > i64::MAX as f64 {
                            panic!("Cannot create an upper-bounding long from double {}", double);
                        } else {
                            Some(Value::Long(double.ceil() as i64))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Value::Date(_) => unreachable!("Handled by trivial cast"),
            _ => unreachable!(),
        }
    }
}

impl<'a> ValueEncodable for Value<'a> {
    fn value_type(&self) -> ValueType {
        match self {
            Value::Boolean(_) => ValueType::Boolean,
            Value::Long(_) => ValueType::Long,
            Value::Double(_) => ValueType::Double,
            Value::Decimal(_) => ValueType::Decimal,
            Value::Date(_) => ValueType::Date,
            Value::DateTime(_) => ValueType::DateTime,
            Value::DateTimeTZ(_) => ValueType::DateTimeTZ,
            Value::Duration(_) => ValueType::Duration,
            Value::String(_) => ValueType::String,
            Value::Struct(struct_value) => ValueType::Struct(struct_value.definition_key().clone().into_owned()),
        }
    }

    fn encode_boolean(&self) -> BooleanBytes {
        match self {
            Self::Boolean(boolean) => BooleanBytes::build(*boolean),
            _ => panic!("Cannot encode non-boolean as BooleanBytes"),
        }
    }

    fn encode_long(&self) -> LongBytes {
        match self {
            Self::Long(long) => LongBytes::build(*long),
            _ => panic!("Cannot encode non-long as LongBytes"),
        }
    }

    fn encode_double(&self) -> DoubleBytes {
        match self {
            Self::Double(double) => DoubleBytes::build(*double),
            _ => panic!("Cannot encode non-double as DoubleBytes"),
        }
    }

    fn encode_decimal(&self) -> DecimalBytes {
        match self {
            Self::Decimal(decimal) => DecimalBytes::build(*decimal),
            _ => panic!("Cannot encode non-decimal as DecimalBytes"),
        }
    }

    fn encode_date(&self) -> DateBytes {
        match self {
            Self::Date(date) => DateBytes::build(*date),
            _ => panic!("Cannot encode non-date as DateBytes"),
        }
    }

    fn encode_date_time(&self) -> DateTimeBytes {
        match self {
            Self::DateTime(date_time) => DateTimeBytes::build(*date_time),
            _ => panic!("Cannot encode non-datetime as DateTimeBytes"),
        }
    }

    fn encode_date_time_tz(&self) -> DateTimeTZBytes {
        match self {
            &Self::DateTimeTZ(date_time_tz) => DateTimeTZBytes::build(date_time_tz),
            _ => panic!("Cannot encoded non-datetime as DateTimeBytes"),
        }
    }

    fn encode_duration(&self) -> DurationBytes {
        match self {
            Self::Duration(duration) => DurationBytes::build(*duration),
            _ => panic!("Cannot encoded non-duration as DurationBytes"),
        }
    }

    fn encode_string<const INLINE_LENGTH: usize>(&self) -> StringBytes<'_, INLINE_LENGTH> {
        match self {
            Value::String(str) => StringBytes::build_ref(str),
            _ => panic!("Cannot encode non-String as StringBytes"),
        }
    }

    fn encode_struct<const INLINE_LENGTH: usize>(&self) -> StructBytes<'static, INLINE_LENGTH> {
        match self {
            Value::Struct(struct_) => StructBytes::build(struct_),
            _ => panic!("Cannot encode non-Struct as StructBytes"),
        }
    }

    fn encode_bytes<const INLINE_LENGTH: usize>(&self) -> ByteArray<INLINE_LENGTH> {
        match self {
            Value::Boolean(_) => ByteArray::copy(&self.encode_boolean().bytes()),
            Value::Long(_) => ByteArray::copy(&self.encode_long().bytes()),
            Value::Double(_) => ByteArray::copy(&self.encode_double().bytes()),
            Value::Decimal(_) => ByteArray::copy(&self.encode_decimal().bytes()),
            Value::Date(_) => ByteArray::copy(&self.encode_date().bytes()),
            Value::DateTime(_) => ByteArray::copy(&self.encode_date_time().bytes()),
            Value::DateTimeTZ(_) => ByteArray::copy(&self.encode_date_time_tz().bytes()),
            Value::Duration(_) => ByteArray::copy(&self.encode_duration().bytes()),
            Value::String(_) => ByteArray::copy(self.encode_string::<INLINE_LENGTH>().bytes().bytes()),
            Value::Struct(_) => ByteArray::copy(self.encode_struct::<INLINE_LENGTH>().bytes().bytes()),
        }
    }
}

impl<'a> fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(bool) => write!(f, "{bool}"),
            Value::Long(long) => write!(f, "{long}"),
            Value::Double(double) => write!(f, "{double}"),
            Value::Decimal(decimal) => write!(f, "{decimal}[decimal]"),
            Value::Date(date) => write!(f, "{date}"),
            Value::DateTime(datetime) => write!(f, "{datetime}"),
            Value::DateTimeTZ(datetime_tz) => write!(f, "{datetime_tz}"),
            Value::Duration(duration) => write!(f, "{duration}"),
            Value::String(string) => write!(f, "\"{string}\""),
            // TODO: this string will not have field names, only field IDs!
            Value::Struct(struct_) => write!(f, "{struct_}"),
        }
    }
}

pub trait NativeValueConvertible: Sized {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()>;

    fn to_db_value(self) -> Value<'static>;
}

impl NativeValueConvertible for bool {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Boolean;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Boolean(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Boolean(self)
    }
}

impl NativeValueConvertible for f64 {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Double;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Double(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Double(self)
    }
}

impl NativeValueConvertible for i64 {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Long;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Long(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Long(self)
    }
}

impl NativeValueConvertible for String {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::String;
    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::String(value) => Ok(value.deref().to_owned()),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::String(Cow::Owned(self))
    }
}

impl NativeValueConvertible for Decimal {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Decimal;
    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Decimal(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Decimal(self)
    }
}

impl NativeValueConvertible for NaiveDate {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Date;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Date(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Date(self)
    }
}

impl NativeValueConvertible for NaiveDateTime {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::DateTime;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::DateTime(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::DateTime(self)
    }
}

impl NativeValueConvertible for DateTime<TimeZone> {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::DateTimeTZ;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::DateTimeTZ(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::DateTimeTZ(self)
    }
}

impl NativeValueConvertible for Duration {
    const VALUE_TYPE_CATEGORY: ValueTypeCategory = ValueTypeCategory::Duration;

    fn from_db_value(value: Value<'static>) -> Result<Self, ()> {
        match value {
            Value::Duration(value) => Ok(value),
            _ => Err(()),
        }
    }

    fn to_db_value(self) -> Value<'static> {
        Value::Duration(self)
    }
}
