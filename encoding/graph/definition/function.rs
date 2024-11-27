/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

use crate::{layout::prefix::Prefix, value::string_bytes::StringBytes, AsBytes};

#[derive(Clone, Debug)]
pub struct FunctionDefinition<'a> {
    bytes: Bytes<'a, BUFFER_VALUE_INLINE>,
}

impl<'a> FunctionDefinition<'a> {
    pub const PREFIX: Prefix = Prefix::DefinitionFunction;

    pub fn new(bytes: Bytes<'a, BUFFER_VALUE_INLINE>) -> Self {
        Self { bytes }
    }

    pub fn build_owned(definition: &str) -> FunctionDefinition<'static> {
        FunctionDefinition { bytes: StringBytes::build_owned(definition).into_bytes() }
    }

    pub fn build_ref(definition: &'a str) -> Self {
        Self { bytes: StringBytes::build_owned(definition).into_bytes() }
    }

    pub fn as_str(&self) -> String {
        StringBytes::<0>::new(Bytes::Reference(&self.bytes)).as_str().to_owned()
    }

    pub fn into_bytes(self) -> Bytes<'a, BUFFER_VALUE_INLINE> {
        self.bytes
    }
}
