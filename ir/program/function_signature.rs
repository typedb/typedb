/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use encoding::graph::definition::definition_key::DefinitionKey;
use primitive::maybe_owns::MaybeOwns;

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::FunctionReadError,
    translator::function_builder::TypeQLFunctionBuilder,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FunctionID {
    Schema(DefinitionKey<'static>),
    Preamble(usize),
}

pub struct FunctionSignature {
    pub(crate) function_id: FunctionID,
    pub(crate) arguments: Vec<(VariableCategory, VariableOptionality)>,
    pub(crate) returns: Vec<(VariableCategory, VariableOptionality)>,
    pub(crate) return_is_stream: bool,
}

impl FunctionSignature {
    pub fn new(
        function_id: FunctionID,
        arguments: Vec<(VariableCategory, VariableOptionality)>,
        returns: Vec<(VariableCategory, VariableOptionality)>,
        return_is_stream: bool,
    ) -> FunctionSignature {
        Self { function_id, arguments, returns, return_is_stream }
    }

    pub fn function_id(&self) -> FunctionID {
        self.function_id.clone()
    }
}

impl FunctionID {
    pub fn as_definition_key(&self) -> Option<DefinitionKey<'static>> {
        if let FunctionID::Schema(definition_key) = self {
            Some(definition_key.clone())
        } else {
            None
        }
    }

    pub fn as_preamble(&self) -> Option<usize> {
        if let FunctionID::Preamble(index) = self {
            Some(*index)
        } else {
            None
        }
    }

    pub fn as_usize(&self) -> usize {
        match self {
            FunctionID::Schema(id) => id.definition_id().as_uint() as usize,
            FunctionID::Preamble(id) => *id,
        }
    }
}

impl Display for FunctionID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionID::Schema(definition_key) => {
                write!(f, "SchemaFunction#{}", definition_key.definition_id().as_uint())
            }
            FunctionID::Preamble(index) => write!(f, "QueryFunction#{}", index),
        }
    }
}

pub trait FunctionIDTrait: Clone + Into<FunctionID> {
    fn as_usize(&self) -> usize;
}

impl FunctionIDTrait for DefinitionKey<'static> {
    fn as_usize(&self) -> usize {
        self.definition_id().as_uint() as usize
    }
}

impl FunctionIDTrait for usize {
    fn as_usize(&self) -> usize {
        *self
    }
}

impl Into<FunctionID> for usize {
    fn into(self) -> FunctionID {
        FunctionID::Preamble(self)
    }
}

impl Into<FunctionID> for DefinitionKey<'static> {
    fn into(self) -> FunctionID {
        FunctionID::Schema(self)
    }
}

pub trait FunctionManagerIndexInjectionTrait {
    fn get_function_signature(&self, name: &str)
        -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError>;
}

pub struct FunctionSignatureIndex<'func, SchemaIndex: FunctionManagerIndexInjectionTrait> {
    schema: &'func SchemaIndex,
    buffered: HashMap<String, FunctionSignature>,
}

impl<'func, SchemaIndex: FunctionManagerIndexInjectionTrait> FunctionSignatureIndex<'func, SchemaIndex> {
    pub fn into_parts(self) -> (&'func SchemaIndex, HashMap<String, FunctionSignature>) {
        (self.schema, self.buffered)
    }
}

impl<'func, SchemaIndex: FunctionManagerIndexInjectionTrait> FunctionSignatureIndex<'func, SchemaIndex> {
    pub fn new(schema: &'func SchemaIndex, buffered: HashMap<String, FunctionSignature>) -> Self {
        Self { schema, buffered }
    }

    pub fn build<'a>(
        schema: &'func SchemaIndex,
        buffered_typeql: impl Iterator<Item = (FunctionID, &'a typeql::Function)>,
    ) -> Self {
        let buffered = buffered_typeql
            .map(|(function_id, function)| {
                (
                    function.signature.ident.ident.clone(),
                    TypeQLFunctionBuilder::build_signature(function_id.into(), &function),
                )
            })
            .collect();
        FunctionSignatureIndex::new(schema, buffered)
    }
}

impl<'func, SchemaIndex: FunctionManagerIndexInjectionTrait> FunctionSignatureIndex<'func, SchemaIndex> {
    pub fn get_function_signature(
        &self,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError> {
        if let Some(signature) = self.buffered.get(name) {
            Ok(Some(MaybeOwns::Borrowed(signature)))
        } else {
            self.schema.get_function_signature(name)
        }
    }
}

pub struct EmptySchemaFunctionIndex {}

impl FunctionManagerIndexInjectionTrait for EmptySchemaFunctionIndex {
    fn get_function_signature(
        &self,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError> {
        Ok(None)
    }
}
