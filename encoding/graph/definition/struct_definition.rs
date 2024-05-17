/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


/// Restrictions: maximum number fields is StructFieldNumber::MAX
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    name: String,
    fields: Vec<StructFieldDefinition>,
    field_names: HashMap<String, StructFieldNumber>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructFieldDefinition {
    optional: bool,
    value_type: ValueType,
}

pub(crate) type StructFieldNumber = u16;