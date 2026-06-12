/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, io::Read};

use answer::variable::Variable;
use bytes::byte_array::ByteArray;
use encoding::graph::thing::THING_VERTEX_MAX_LENGTH;
use itertools::Itertools;
use typeql::common::Span;

use crate::{
    RepresentationError,
    pattern::variable_category::VariableCategory,
    pipeline::{ParameterRegistry, VariableRegistry, block::BlockBuilderContext, reduce::Reducer},
};

mod constraints;
mod expression;
pub mod fetch;
pub mod function;
pub mod literal;
pub mod match_;
pub mod modifiers;
pub mod pipeline;
pub mod reduce;
pub mod tokens;
pub mod writes;

use crate::pattern::variable_category::VariableOptionality;

#[derive(Debug, Clone)]
pub struct PipelineTranslationContext {
    pub variable_registry: VariableRegistry, // TODO: Unpub
    last_stage_visible_variables: HashMap<String, Variable>,
}

impl Default for PipelineTranslationContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineTranslationContext {
    pub fn new() -> Self {
        Self { variable_registry: VariableRegistry::new(), last_stage_visible_variables: HashMap::new() }
    }

    pub fn new_function_pipeline(
        input_variables: Vec<(String, Option<Span>, (VariableCategory, VariableOptionality))>,
    ) -> Result<(Self, Vec<Variable>), Box<RepresentationError>> {
        debug_assert!(input_variables.iter().all(|(_, _, (_, opt))| VariableOptionality::Required == *opt));
        let mut variables = Vec::with_capacity(input_variables.len());
        let mut this = Self::new();
        for input_variable in input_variables {
            variables.push(this.register_input_variable(input_variable)?);
        }
        Ok((this, variables))
    }

    pub(crate) fn register_input_variable(
        &mut self,
        input_variable: (String, Option<Span>, (VariableCategory, VariableOptionality)),
    ) -> Result<Variable, Box<RepresentationError>> {
        let (name, source_span, (category, optionality)) = input_variable;
        let variable =
            self.variable_registry.register_input_variable(name.as_str(), category, optionality, source_span)?;
        self.last_stage_visible_variables.insert(name.clone(), variable);
        Ok(variable)
    }

    pub fn new_block_builder_context<'a>(
        &'a mut self,
        parameters: &'a mut ParameterRegistry,
    ) -> BlockBuilderContext<'a> {
        let Self { variable_registry, last_stage_visible_variables: visible_variables } = self;
        BlockBuilderContext::new(variable_registry, visible_variables, parameters)
    }

    pub(crate) fn register_reduced_variable(
        &mut self,
        name: &str,
        variable_category: VariableCategory,
        is_optional: bool,
        source_span: Option<Span>,
        reducer: Reducer,
    ) -> Result<Variable, Box<RepresentationError>> {
        if let Some(existing_variable) = self.last_stage_visible_variables.get(name) {
            return Err(Box::new(RepresentationError::ReduceAssignsToExistingVariable {
                variable: name.clone().to_owned(),
                source_span,
                existing_span: self.variable_registry.source_span(*existing_variable),
            }));
        }
        let variable = self.variable_registry.register_reduce_output_variable(
            name.to_owned(),
            variable_category,
            is_optional,
            source_span,
            reducer,
        )?;
        self.last_stage_visible_variables.insert(name.to_owned(), variable);
        Ok(variable)
    }

    pub fn get_variable(&self, variable: &str) -> Option<Variable> {
        self.last_stage_visible_variables.get(variable).cloned()
    }
}

macro_rules! verify_variable_available {
    ($context:ident, $var:expr => $error:ident ) => {
        match $context.get_variable(
            $var.name()
                .ok_or(Box::new(RepresentationError::NonAnonymousVariableExpected { source_span: $var.span() }))?,
        ) {
            Some(translated) => Ok(translated),
            None => Err(Box::new(RepresentationError::$error {
                variable: $var.name().unwrap().to_owned(),
                source_span: $var.span(),
            })),
        }
    };
}
pub(super) use verify_variable_available;

pub fn parse_iid(iid: &str) -> Result<ByteArray<THING_VERTEX_MAX_LENGTH>, ()> {
    fn from_hex(c: u8) -> Result<u8, ()> {
        // relying on the fact that typeql ensures only hex digits
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(()),
        }
    }

    let iid = &iid["0x".len()..];

    let mut bytes = [0u8; THING_VERTEX_MAX_LENGTH];
    for (i, (hi, lo)) in iid.bytes().tuples().enumerate() {
        bytes[i] = (from_hex(hi)? << 4) + from_hex(lo)?;
    }
    let len = iid.as_bytes().len() / 2;
    Ok(ByteArray::inline(bytes, len))
}
