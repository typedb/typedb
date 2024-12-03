/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
};

use bytes::Bytes;
use resource::constants::{encoding::StructFieldIDUInt, snapshot::BUFFER_VALUE_INLINE};
use serde::{Deserialize, Serialize};

use crate::{
    error::EncodingError, graph::definition::DefinitionValueEncoding, layout::prefix::Prefix,
    value::value_type::ValueType,
};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    pub name: String,
    pub fields: HashMap<StructFieldIDUInt, StructDefinitionField>,
    pub field_names: HashMap<String, StructFieldIDUInt>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinitionField {
    pub index: StructFieldIDUInt,
    pub optional: bool,
    pub value_type: ValueType,
}

impl fmt::Display for StructDefinitionField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Value type: {}, optional: {}", self.value_type, self.optional)
    }
}

impl StructDefinition {
    pub const PREFIX: Prefix = Prefix::DefinitionStruct;

    pub fn new(name: String) -> StructDefinition {
        StructDefinition { name, fields: HashMap::new(), field_names: HashMap::new() }
    }

    pub fn add_field(&mut self, field_name: &str, value_type: ValueType, optional: bool) -> Result<(), EncodingError> {
        if self.fields.len() > StructFieldIDUInt::MAX as usize {
            Err(EncodingError::StructAlreadyHasMaximumNumberOfFields { struct_name: self.name.clone() })
        } else {
            match self.field_names.entry(field_name.to_string()) {
                Entry::Vacant(entry) => {
                    let index = (0..self.fields.len() as StructFieldIDUInt)
                        .find(|idx| !self.fields.contains_key(idx))
                        .unwrap_or(self.fields.len() as StructFieldIDUInt);
                    self.fields.insert(index, StructDefinitionField { index, optional, value_type });
                    entry.insert(index);
                    Ok(())
                }
                Entry::Occupied(entry) => Err(EncodingError::StructDuplicateFieldDefinition {
                    struct_name: self.name.clone(),
                    field_name: entry.key().clone(),
                }),
            }
        }
    }

    pub fn delete_field(&mut self, field_name: &str) -> Result<(), EncodingError> {
        if !self.field_names.contains_key(field_name) {
            Err(EncodingError::StructFieldUnresolvable {
                struct_name: self.name.clone(),
                field_path: vec![field_name.to_string()],
            })
        } else {
            let field_idx = self.field_names.remove(field_name).unwrap();
            self.fields.remove(&field_idx).unwrap();
            Ok(())
        }
    }

    pub fn get_field(&self, field_name: &str) -> Option<&StructDefinitionField> {
        if let Some(id) = self.field_names.get(field_name) {
            self.fields.get(id)
        } else {
            None
        }
    }
}

impl DefinitionValueEncoding for StructDefinition {
    fn from_bytes(value: &[u8]) -> Self {
        bincode::deserialize(value).unwrap()
    }

    fn into_bytes(self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl StructDefinitionField {
    pub fn has_optionality_and_value_type(&self, optional: bool, value_type: ValueType) -> bool {
        self.optional == optional && self.value_type == value_type
    }
}
