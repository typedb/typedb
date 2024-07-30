/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use encoding::value::value_type::ValueTypeCategory;
use typeql::common::token::ValueType;

pub(crate) fn as_value_type_category(value_type: &ValueType) -> ValueTypeCategory {
    match value_type {
        ValueType::Boolean => ValueTypeCategory::Boolean,
        ValueType::Date => ValueTypeCategory::Date,
        ValueType::DateTime => ValueTypeCategory::DateTime,
        ValueType::DateTimeTZ => ValueTypeCategory::DateTimeTZ,
        ValueType::Decimal => ValueTypeCategory::Decimal,
        ValueType::Double => ValueTypeCategory::Double,
        ValueType::Duration => ValueTypeCategory::Duration,
        ValueType::Long => ValueTypeCategory::Long,
        ValueType::String => ValueTypeCategory::String,
    }
}
