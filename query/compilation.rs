/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::{HashMap, HashSet},
    iter::zip,
    sync::Arc,
};

use answer::variable::Variable;
use compiler::{
    delete::program::DeleteProgram,
    insert::program::InsertProgram,
    match_::{inference::annotated_functions::AnnotatedUnindexedFunctions, planner::pattern_plan::MatchProgram},
    modifiers::{LimitProgram, OffsetProgram, RequireProgram, SelectProgram, SortProgram},
    reduce::{ReduceInstruction, ReduceProgram},
    VariablePosition,
};
use concept::thing::statistics::Statistics;
use ir::program::{function::Function, VariableRegistry};

use crate::{annotation::AnnotatedStage, error::QueryError};

pub struct CompiledPipeline {
    pub(super) compiled_functions: Vec<CompiledFunction>,
    pub(super) compiled_stages: Vec<CompiledStage>,
    pub(super) output_variable_positions: HashMap<Variable, VariablePosition>,
}

pub struct CompiledFunction {
    plan: MatchProgram,
    returns: HashMap<Variable, VariablePosition>,
}

pub enum CompiledStage {
    Match(MatchProgram),
    Insert(InsertProgram),
    Delete(DeleteProgram),

    Select(SelectProgram),
    Sort(SortProgram),
    Offset(OffsetProgram),
    Limit(LimitProgram),
    Require(RequireProgram),
    Reduce(ReduceProgram),
}

impl CompiledStage {
    fn output_row_mapping(&self) -> HashMap<Variable, VariablePosition> {
        match self {
            CompiledStage::Match(program) => program.variable_positions().to_owned(),
            CompiledStage::Insert(program) => program
                .output_row_schema
                .iter()
                .filter_map(|opt| opt.map(|(v, _)| v))
                .enumerate()
                .map(|(i, v)| (v, VariablePosition::new(i as u32)))
                .collect(),
            CompiledStage::Delete(program) => program
                .output_row_schema
                .iter()
                .enumerate()
                .filter_map(|(i, v)| v.map(|v| (v, VariablePosition::new(i as u32))))
                .collect(),
            CompiledStage::Select(program) => program.output_row_mapping.clone(),
            CompiledStage::Sort(program) => program.output_row_mapping.clone(),
            CompiledStage::Offset(program) => program.output_row_mapping.clone(),
            CompiledStage::Limit(program) => program.output_row_mapping.clone(),
            CompiledStage::Require(program) => program.output_row_mapping.clone(),
            CompiledStage::Reduce(program) => program.output_row_mapping.clone(),
        }
    }
}

pub(super) fn compile_pipeline(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    annotated_functions: AnnotatedUnindexedFunctions,
    annotated_stages: Vec<AnnotatedStage>,
) -> Result<CompiledPipeline, QueryError> {
    let compiled_functions = annotated_functions
        .iter_functions()
        .map(|function| compile_function(statistics, variable_registry.clone(), function))
        .collect::<Result<Vec<_>, _>>()?;

    let mut compiled_stages = Vec::with_capacity(annotated_stages.len());
    for stage in annotated_stages {
        let input_variable_positions =
            compiled_stages.last().map(|stage: &CompiledStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

        let compiled_stage = compile_stage(statistics, variable_registry.clone(), &input_variable_positions, stage)?;
        compiled_stages.push(compiled_stage);
    }
    let output_variable_positions =
        compiled_stages.last().map(|stage: &CompiledStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

    Ok(CompiledPipeline { compiled_functions, compiled_stages, output_variable_positions })
}

fn compile_function(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    function: &Function,
) -> Result<CompiledFunction, QueryError> {
    todo!()
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    input_variables: &HashMap<Variable, VariablePosition>,
    annotated_stage: AnnotatedStage,
) -> Result<CompiledStage, QueryError> {
    match &annotated_stage {
        AnnotatedStage::Match { block, block_annotations, compiled_expressions } => {
            let plan = MatchProgram::compile(
                block,
                input_variables,
                block_annotations,
                variable_registry,
                compiled_expressions,
                statistics,
            );
            Ok(CompiledStage::Match(plan))
        }
        AnnotatedStage::Insert { block, annotations } => {
            let plan = compiler::insert::program::compile(
                variable_registry,
                block.conjunction().constraints(),
                input_variables,
                annotations,
            )
            .map_err(|source| QueryError::WriteCompilation { source })?;
            Ok(CompiledStage::Insert(plan))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations } => {
            let plan = compiler::delete::program::compile(
                input_variables,
                annotations,
                block.conjunction().constraints(),
                deleted_variables,
            )
            .map_err(|source| QueryError::WriteCompilation { source })?;
            Ok(CompiledStage::Delete(plan))
        }
        AnnotatedStage::Select(select) => {
            let mut retained_positions = HashSet::with_capacity(select.variables.len());
            let mut output_row_mapping = HashMap::with_capacity(select.variables.len());
            for &variable in &select.variables {
                let pos = input_variables[&variable];
                retained_positions.insert(pos);
                output_row_mapping.insert(variable, pos);
            }
            Ok(CompiledStage::Select(SelectProgram { retained_positions, output_row_mapping }))
        }
        AnnotatedStage::Sort(sort) => Ok(CompiledStage::Sort(SortProgram {
            sort_on: sort.variables.clone(),
            output_row_mapping: input_variables.clone(),
        })),
        AnnotatedStage::Offset(offset) => Ok(CompiledStage::Offset(OffsetProgram {
            offset: offset.offset(),
            output_row_mapping: input_variables.clone(),
        })),
        AnnotatedStage::Limit(limit) => {
            Ok(CompiledStage::Limit(LimitProgram { limit: limit.limit(), output_row_mapping: input_variables.clone() }))
        }
        AnnotatedStage::Require(require) => {
            let mut required_positions = HashSet::with_capacity(require.variables.len());
            for &variable in &require.variables {
                let pos = input_variables[&variable];
                required_positions.insert(pos);
            }
            Ok(CompiledStage::Require(RequireProgram {
                required: required_positions,
                output_row_mapping: input_variables.clone(),
            }))
        }
        AnnotatedStage::Reduce(reduce, typed_reducers) => {
            debug_assert_eq!(reduce.assigned_reductions.len(), typed_reducers.len());
            let mut output_row_mapping = HashMap::new();
            let mut input_group_positions = Vec::with_capacity(reduce.within_group.len());
            for variable in reduce.within_group.iter() {
                output_row_mapping.insert(variable.clone(), VariablePosition::new(input_group_positions.len() as u32));
                input_group_positions.push(input_variables.get(variable).unwrap().clone());
            }
            let mut reduction_inputs = Vec::with_capacity(reduce.assigned_reductions.len());
            for ((assigned_variable, _), reducer_on_variable) in
                zip(reduce.assigned_reductions.iter(), typed_reducers.iter())
            {
                output_row_mapping.insert(
                    assigned_variable.clone(),
                    VariablePosition::new((input_group_positions.len() + reduction_inputs.len()) as u32),
                );
                let reducer_on_position = match &reducer_on_variable {
                    ReduceInstruction::Count => ReduceInstruction::Count,
                    ReduceInstruction::CountVar(variable) => {
                        ReduceInstruction::CountVar(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::SumLong(variable) => {
                        ReduceInstruction::SumLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::SumDouble(variable) => {
                        ReduceInstruction::SumDouble(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MaxLong(variable) => {
                        ReduceInstruction::MaxLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MaxDouble(variable) => {
                        ReduceInstruction::MaxDouble(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MinLong(variable) => {
                        ReduceInstruction::MinLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MinDouble(variable) => {
                        ReduceInstruction::MinDouble(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MeanLong(variable) => {
                        ReduceInstruction::MeanLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MeanDouble(variable) => {
                        ReduceInstruction::MeanDouble(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MedianLong(variable) => {
                        ReduceInstruction::MedianLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::MedianDouble(variable) => {
                        ReduceInstruction::MedianDouble(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::StdLong(variable) => {
                        ReduceInstruction::StdLong(input_variables.get(variable).unwrap().clone())
                    }
                    ReduceInstruction::StdDouble(variable) => {
                        ReduceInstruction::StdDouble(input_variables.get(variable).unwrap().clone())
                    }
                };
                reduction_inputs.push(reducer_on_position);
            }
            Ok(CompiledStage::Reduce(ReduceProgram { reduction_inputs, input_group_positions, output_row_mapping }))
        }
    }
}
