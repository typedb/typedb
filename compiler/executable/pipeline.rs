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
use concept::thing::statistics::Statistics;
use ir::pipeline::VariableRegistry;
use crate::annotation::fetch::AnnotatedFetch;
use crate::annotation::function::{AnnotatedFunction, AnnotatedUnindexedFunctions};
use crate::annotation::pipeline::AnnotatedStage;
use crate::executable::delete::executable::DeleteExecutable;
use crate::executable::ExecutableError;
use crate::executable::fetch::executable::FetchExecutable;
use crate::executable::function::{compile_function, ExecutableFunction};
use crate::executable::insert::executable::InsertExecutable;
use crate::executable::match_::planner::match_executable::MatchExecutable;
use crate::executable::modifiers::{LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable};
use crate::executable::reduce::{ReduceExecutable, ReduceInstruction};
use crate::VariablePosition;

pub struct ExecutablePipeline {
    pub executable_functions: Vec<ExecutableFunction>,
    pub executable_stages: Vec<ExecutableStage>,
    pub stages_variable_positions: HashMap<Variable, VariablePosition>,
    pub executable_fetch: Option<FetchExecutable>,
}

pub enum ExecutableStage {
    Match(MatchExecutable),
    Insert(InsertExecutable),
    Delete(DeleteExecutable),

    Select(SelectExecutable),
    Sort(SortExecutable),
    Offset(OffsetExecutable),
    Limit(LimitExecutable),
    Require(RequireExecutable),
    Reduce(ReduceExecutable),
}

impl ExecutableStage {
    fn output_row_mapping(&self) -> HashMap<Variable, VariablePosition> {
        match self {
            ExecutableStage::Match(executable) => executable.variable_positions().to_owned(),
            ExecutableStage::Insert(executable) => executable
                .output_row_schema
                .iter()
                .filter_map(|opt| opt.map(|(v, _)| v))
                .enumerate()
                .map(|(i, v)| (v, VariablePosition::new(i as u32)))
                .collect(),
            ExecutableStage::Delete(executable) => executable
                .output_row_schema
                .iter()
                .enumerate()
                .filter_map(|(i, v)| v.map(|v| (v, VariablePosition::new(i as u32))))
                .collect(),
            ExecutableStage::Select(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Sort(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Offset(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Limit(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Require(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Reduce(executable) => executable.output_row_mapping.clone(),
        }
    }
}

pub fn compile_pipeline(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    annotated_functions: AnnotatedUnindexedFunctions,
    annotated_stages: Vec<AnnotatedStage>,
    annotated_fetch: Option<AnnotatedFetch>,
) -> Result<ExecutablePipeline, ExecutableError> {
    let executable_functions = annotated_functions
        .iter_functions()
        .map(|function| compile_function(statistics, variable_registry.clone(), function))
        .collect::<Result<Vec<_>, _>>()?;

    let mut executable_stages = Vec::with_capacity(annotated_stages.len());
    for stage in annotated_stages {
        let input_variable_positions =
            executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

        let executable_stage = compile_stage(statistics, variable_registry.clone(), &input_variable_positions, stage)?;
        executable_stages.push(executable_stage);
    }
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

    let executable_fetch = annotated_fetch.map(|fetch| compile_fetch(fetch));
    Ok(ExecutablePipeline { executable_functions, executable_stages, stages_variable_positions, executable_fetch })
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    input_variables: &HashMap<Variable, VariablePosition>,
    annotated_stage: AnnotatedStage,
) -> Result<ExecutableStage, ExecutableError> {
    match &annotated_stage {
        AnnotatedStage::Match { block, block_annotations, executable_expressions } => {
            let plan = crate::executable::match_::planner::compile(
                block,
                input_variables,
                block_annotations,
                variable_registry,
                executable_expressions,
                statistics,
            );
            Ok(ExecutableStage::Match(plan))
        }
        AnnotatedStage::Insert { block, annotations } => {
            let plan = crate::executable::insert::executable::compile(
                variable_registry,
                block.conjunction().constraints(),
                input_variables,
                annotations,
            )
            .map_err(|source| ExecutableError::InsertExecutableCompilation { source })?;
            Ok(ExecutableStage::Insert(plan))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations } => {
            let plan = crate::executable::delete::executable::compile(
                input_variables,
                annotations,
                block.conjunction().constraints(),
                deleted_variables,
            )
            .map_err(|source| ExecutableError::DeleteExecutableCompilation { source })?;
            Ok(ExecutableStage::Delete(plan))
        }
        AnnotatedStage::Select(select) => {
            let mut retained_positions = HashSet::with_capacity(select.variables.len());
            let mut output_row_mapping = HashMap::with_capacity(select.variables.len());
            for &variable in &select.variables {
                let pos = input_variables[&variable];
                retained_positions.insert(pos);
                output_row_mapping.insert(variable, pos);
            }
            Ok(ExecutableStage::Select(SelectExecutable { retained_positions, output_row_mapping }))
        }
        AnnotatedStage::Sort(sort) => Ok(ExecutableStage::Sort(SortExecutable {
            sort_on: sort.variables.clone(),
            output_row_mapping: input_variables.clone(),
        })),
        AnnotatedStage::Offset(offset) => Ok(ExecutableStage::Offset(OffsetExecutable {
            offset: offset.offset(),
            output_row_mapping: input_variables.clone(),
        })),
        AnnotatedStage::Limit(limit) => Ok(ExecutableStage::Limit(LimitExecutable {
            limit: limit.limit(),
            output_row_mapping: input_variables.clone(),
        })),
        AnnotatedStage::Require(require) => {
            let mut required_positions = HashSet::with_capacity(require.variables.len());
            for &variable in &require.variables {
                let pos = input_variables[&variable];
                required_positions.insert(pos);
            }
            Ok(ExecutableStage::Require(RequireExecutable {
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
            let mut reductions = Vec::with_capacity(reduce.assigned_reductions.len());
            for ((assigned_variable, _), reducer_on_variable) in
                zip(reduce.assigned_reductions.iter(), typed_reducers.iter())
            {
                output_row_mapping.insert(
                    assigned_variable.clone(),
                    VariablePosition::new((input_group_positions.len() + reductions.len()) as u32),
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
                reductions.push(reducer_on_position);
            }
            Ok(ExecutableStage::Reduce(ReduceExecutable {
                reductions: reductions,
                input_group_positions,
                output_row_mapping,
            }))
        }
    }
}

fn compile_fetch(fetch: AnnotatedFetch) -> FetchExecutable {
    todo!()
}
