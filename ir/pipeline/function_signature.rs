/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, mem, ops::BitXor};

use encoding::graph::definition::definition_key::DefinitionKey;
use primitive::maybe_owns::MaybeOwns;
use structural_equality::StructuralEquality;

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    pipeline::FunctionReadError,
    translation::function::build_signature,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FunctionID {
    Schema(DefinitionKey),
    Preamble(usize),
}

#[derive(Debug)]
pub struct FunctionSignature {
    pub(crate) function_id: FunctionID,
    pub(crate) arguments: Vec<VariableCategory>, // TODO: Arguments cannot be optional
    pub(crate) returns: Vec<(VariableCategory, VariableOptionality)>,
    pub(crate) return_is_stream: bool,
}

impl FunctionSignature {
    pub fn new(
        function_id: FunctionID,
        arguments: Vec<VariableCategory>,
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
    pub fn as_definition_key(&self) -> Option<DefinitionKey> {
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

impl StructuralEquality for FunctionID {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self)).bitxor(match self {
            FunctionID::Schema(key) => StructuralEquality::hash(&(key.definition_id().as_uint() as usize)),
            FunctionID::Preamble(id) => StructuralEquality::hash(id),
        })
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Schema(key), Self::Schema(other_key)) => StructuralEquality::equals(
                &(key.definition_id().as_uint() as usize),
                &(other_key.definition_id().as_uint() as usize),
            ),
            (Self::Preamble(id), Self::Preamble(other_id)) => id.equals(other_id),
            // note: this style forces updating the match when the variants change
            (Self::Schema { .. }, _) | (Self::Preamble { .. }, _) => false,
        }
    }
}

impl fmt::Display for FunctionID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionID::Schema(definition_key) => {
                write!(f, "SchemaFunction#{}", definition_key.definition_id().as_uint())
            }
            FunctionID::Preamble(index) => write!(f, "QueryFunction#{}", index),
        }
    }
}

pub trait FunctionIDAPI:
    fmt::Debug + Clone + TryFrom<FunctionID> + Into<FunctionID> + std::hash::Hash + Eq + Ord
{
}

impl FunctionIDAPI for DefinitionKey {}
impl FunctionIDAPI for usize {}

impl From<usize> for FunctionID {
    fn from(val: usize) -> Self {
        FunctionID::Preamble(val)
    }
}

impl From<DefinitionKey> for FunctionID {
    fn from(val: DefinitionKey) -> Self {
        FunctionID::Schema(val)
    }
}

impl TryFrom<FunctionID> for usize {
    type Error = ();
    fn try_from(value: FunctionID) -> Result<Self, Self::Error> {
        match value {
            FunctionID::Schema(_) => Err(()),
            FunctionID::Preamble(id) => Ok(id),
        }
    }
}

impl TryFrom<FunctionID> for DefinitionKey {
    type Error = ();

    fn try_from(value: FunctionID) -> Result<DefinitionKey, Self::Error> {
        match value {
            FunctionID::Schema(id) => Ok(id),
            FunctionID::Preamble(_) => Err(()),
        }
    }
}

pub trait FunctionSignatureIndex {
    fn get_function_signature(&self, name: &str)
        -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError>;
}

#[derive(Debug)]
pub struct HashMapFunctionSignatureIndex {
    index: HashMap<String, FunctionSignature>,
}

impl HashMapFunctionSignatureIndex {
    pub fn build<'func>(buffered_typeql: impl Iterator<Item = (FunctionID, &'func typeql::Function)>) -> Self {
        let index = buffered_typeql
            .map(|(function_id, function)| {
                (function.signature.ident.as_str_unchecked().to_owned(), build_signature(function_id, function))
            })
            .collect();
        Self { index }
    }

    pub fn empty() -> Self {
        Self::build([].into_iter())
    }

    pub fn into_map(self) -> HashMap<String, FunctionSignature> {
        self.index
    }
}

impl FunctionSignatureIndex for HashMapFunctionSignatureIndex {
    fn get_function_signature(
        &self,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError> {
        if let Some(signature) = self.index.get(name) {
            Ok(Some(MaybeOwns::Borrowed(signature)))
        } else {
            Ok(None)
        }
    }
}
