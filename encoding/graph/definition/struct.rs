/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use resource::constants::encoding::StructFieldIDUInt;

use crate::value::value_type::ValueType;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    name: String,
    fields: Vec<StructDefinitionField>,
    field_names: HashMap<String, StructFieldIDUInt>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinitionField {
    optional: bool,
    // value_type: ValueType, // TODO
    index: StructFieldIDUInt,
}
