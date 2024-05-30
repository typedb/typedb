/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use resource::constants::encoding::StructFieldIDUInt;
use serde::{Deserialize, Serialize};
use bytes::byte_reference::ByteReference;
use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use crate::AsBytes;
use crate::graph::definition::DefinitionValueEncoding;
use crate::value::value_type::ValueType;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    name: String,
    fields: Vec<StructDefinitionField>,
    field_names: HashMap<String, StructFieldIDUInt>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct StructDefinitionField {
    index: StructFieldIDUInt,
    optional: bool,
    value_type: ValueType, // TODO
}

impl DefinitionValueEncoding for StructDefinition {
    fn from_bytes<'b>(value: ByteReference<'b>) -> Self {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_bytes(self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}