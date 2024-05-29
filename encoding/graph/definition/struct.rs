/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::value::value_type::ValueType;

/// Restrictions: maximum number fields is StructFieldNumber::MAX
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructValueType {
    name: String,
    fields: Vec<StructField>,
    field_names: HashMap<String, StructFieldNumber>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructField {
    optional: bool,
    value_type: ValueType,
    index: StructFieldNumber,
}

pub(crate) type StructFieldNumber = u16;