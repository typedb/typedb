/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use bytes::{byte_reference::ByteReference, Bytes};
use resource::constants::{encoding::StructFieldIDUInt, snapshot::BUFFER_VALUE_INLINE};
use serde::{Deserialize, Serialize};

use crate::{graph::definition::DefinitionValueEncoding, value::value_type::ValueType, AsBytes};

// TODO: Revisit to think about serialisation.
// Storing index in the StructDefinitionField opens the door for duplicates?
// We also have redundancy in storing the StructFieldIDUInt twice.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    pub fields: Vec<StructDefinitionField>,
    pub field_names: HashMap<String, StructFieldIDUInt>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinitionField {
    pub index: StructFieldIDUInt,
    pub optional: bool,
    pub value_type: ValueType, // TODO
}

impl StructDefinition {
    pub fn define(definitions: HashMap<String, (ValueType, bool)>) -> StructDefinition {
        let mut fields: Vec<StructDefinitionField> = Vec::with_capacity(definitions.len());
        let mut field_names = HashMap::with_capacity(definitions.len());
        for (i, (name, (value_type, optional))) in definitions.into_iter().enumerate() {
            let index = i as StructFieldIDUInt;
            fields.push(StructDefinitionField { index, optional, value_type });
            field_names.insert(name, index);
        }
        StructDefinition { fields, field_names }
    }
}

impl DefinitionValueEncoding for StructDefinition {
    fn from_bytes<'b>(value: ByteReference<'b>) -> Self {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_bytes(self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}
