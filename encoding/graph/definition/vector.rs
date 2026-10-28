/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{Deserialize, Serialize};

use crate::{
    graph::definition::DefinitionValueEncoding, layout::prefix::Prefix, value::value_type::VectorTypeParameters,
};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub struct VectorDefinition {
    pub parameters: VectorTypeParameters,
}

impl VectorDefinition {
    pub const PREFIX: Prefix = Prefix::DefinitionVector;

    pub fn new(parameters: VectorTypeParameters) -> Self {
        Self { parameters }
    }
}

impl DefinitionValueEncoding for VectorDefinition {
    fn from_bytes(value: &[u8]) -> Self {
        bincode::deserialize(value).unwrap()
    }

    fn into_bytes(self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}
