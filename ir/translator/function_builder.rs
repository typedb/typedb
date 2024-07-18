/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use typeql::{
    schema::definable::function::{Output, ReturnSingle, ReturnStatement, ReturnStream, SingleOutput},
    TypeAny,
};

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::{
        block::FunctionalBlock,
        function::{FunctionIR, Reducer, ReturnOperationIR},
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex},
        FunctionDefinitionError,
    },
    translator::block_builder::TypeQLBuilder,
};

pub struct TypeQLFunctionBuilder {}

impl TypeQLFunctionBuilder {
    pub fn build_ir(
        function_index: &impl FunctionSignatureIndex,
        function: &typeql::Function,
    ) -> Result<FunctionIR, FunctionDefinitionError> {
        let block = TypeQLBuilder::build_match(function_index, &function.body)
            .map_err(|source| FunctionDefinitionError::PatternDefinition { source })?;

        let return_operation = match &function.return_stmt {
            ReturnStatement::Stream(stream) => Self::build_return_stream(&block, stream),
            ReturnStatement::Single(single) => Self::build_return_single(&block, single),
        }?;

        let arguments: Vec<Variable> = function
            .signature
            .args
            .iter()
            .map(|typeql_arg| {
                Self::get_variable_in_block(&block, &typeql_arg.var, |var| {
                    FunctionDefinitionError::FunctionArgumentUnused {
                        argument_variable: var.name().unwrap().to_string(),
                    }
                })
            })
            .collect::<Result<Vec<_>, FunctionDefinitionError>>()?;

        Ok(FunctionIR::new(block, arguments, return_operation))
    }

    pub fn build_signature(function_id: FunctionID, function: &typeql::Function) -> FunctionSignature {
        let args = function
            .signature
            .args
            .iter()
            .map(|arg| Self::type_any_to_category_and_optionality(&arg.type_))
            .collect::<Vec<_>>();

        let return_is_stream = matches!(function.signature.output, Output::Stream(_));
        let returns = match &function.signature.output {
            Output::Stream(stream) => &stream.types,
            Output::Single(single) => &single.types,
        }
        .iter()
        .map(|type_any| Self::type_any_to_category_and_optionality(type_any))
        .collect::<Vec<_>>();
        FunctionSignature::new(function_id.clone().into(), args, returns, return_is_stream)
    }

    fn type_any_to_category_and_optionality(type_any: &typeql::TypeAny) -> (VariableCategory, VariableOptionality) {
        match type_any {
            TypeAny::Type(_) => (VariableCategory::Thing, VariableOptionality::Required),
            TypeAny::Optional(_) => (VariableCategory::Thing, VariableOptionality::Optional),
            TypeAny::List(_) => (VariableCategory::ThingList, VariableOptionality::Required),
        }
    }

    fn build_return_stream(
        block: &FunctionalBlock,
        stream: &ReturnStream,
    ) -> Result<ReturnOperationIR, FunctionDefinitionError> {
        let variables = stream
            .vars
            .iter()
            .map(|typeql_var| {
                Self::get_variable_in_block(block, typeql_var, |var| {
                    FunctionDefinitionError::ReturnVariableUnavailable { variable: var.name().unwrap().to_string() }
                })
            })
            .collect::<Result<Vec<Variable>, FunctionDefinitionError>>()?;
        Ok(ReturnOperationIR::Stream(variables))
    }

    fn build_return_single(
        block: &FunctionalBlock,
        single: &ReturnSingle,
    ) -> Result<ReturnOperationIR, FunctionDefinitionError> {
        let reducers = single
            .outputs
            .iter()
            .map(|output| Self::build_return_single_output(block, output))
            .collect::<Result<Vec<Reducer>, FunctionDefinitionError>>()?;
        Ok(ReturnOperationIR::Single(reducers))
    }

    fn build_return_single_output(
        block: &FunctionalBlock,
        single_output: &SingleOutput,
    ) -> Result<Reducer, FunctionDefinitionError> {
        todo!()
    }

    fn get_variable_in_block<F>(
        block: &FunctionalBlock,
        typeql_var: &typeql::Variable,
        err: F,
    ) -> Result<Variable, FunctionDefinitionError>
    where
        F: FnOnce(&typeql::Variable) -> FunctionDefinitionError,
    {
        block
            .context()
            .get_variable_named(typeql_var.name().unwrap(), block.scope_id())
            .map_or_else(|| Err(err(typeql_var)), |var| Ok(var.clone()))
    }
}
