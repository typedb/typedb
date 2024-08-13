/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::definition::{definition_key::DefinitionKey, function::FunctionDefinition};
use ir::program::function_signature::FunctionIDAPI;

use crate::FunctionError;

pub type SchemaFunction = Function<DefinitionKey<'static>>;

#[derive(Debug)]
pub struct Function<FunctionIDType: FunctionIDAPI> {
    pub(crate) function_id: FunctionIDType,
    pub(crate) parsed: typeql::schema::definable::Function,
}

impl<FunctionIDType: FunctionIDAPI> Function<FunctionIDType> {
    pub fn function_id(&self) -> FunctionIDType {
        self.function_id.clone()
    }

    pub fn name(&self) -> String {
        self.parsed.signature.ident.as_str().to_owned()
    }
}

impl<FunctionIDType: FunctionIDAPI> Function<FunctionIDType> {
    pub(crate) fn build(function_id: FunctionIDType, definition: FunctionDefinition) -> Result<Self, FunctionError> {
        let parsed = typeql::parse_definition_function(definition.as_str().as_str())
            .map_err(|source| FunctionError::ParseError { source })?;
        Ok(Self { function_id, parsed })
    }
}
