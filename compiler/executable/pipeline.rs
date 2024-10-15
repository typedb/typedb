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

use crate::{
    annotation::{fetch::AnnotatedFetch, function::AnnotatedUnindexedFunctions, pipeline::AnnotatedStage},
    executable::{
        delete::executable::DeleteExecutable,
        fetch::executable::{compile_fetch, ExecutableFetch},
        function::{compile_function, ExecutableFunction},
        insert::executable::InsertExecutable,
        match_::planner::match_executable::MatchExecutable,
        modifiers::{LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable},
        reduce::ReduceExecutable,
        ExecutableCompilationError,
    },
    VariablePosition,
};
use crate::executable::match_::planner::function_plan::FunctionPlanRegistry;

pub struct ExecutablePipeline {
    pub executable_functions: Vec<ExecutableFunction>,
    pub executable_stages: Vec<ExecutableStage>,
    pub executable_fetch: Option<ExecutableFetch>,
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
    pub fn output_row_mapping(&self) -> HashMap<Variable, VariablePosition> {
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
    input_variables: HashSet<Variable>,
) -> Result<ExecutablePipeline, ExecutableCompilationError> {
    // TODO: Where do the schema functions come from?
    let executable_functions = annotated_functions
        .into_iter_functions()
        .map(|function| compile_function(statistics, schema_functions, function))
        .collect::<Result<Vec<_>, _>>()?;
    let schema_and_preamble_functions: FunctionPlanRegistry = todo!();
    let (executable_stages, executable_fetch) =
        compile_stages_and_fetch(statistics, variable_registry, &schema_and_preamble_functions, annotated_stages, annotated_fetch, input_variables)?;
    Ok(ExecutablePipeline { executable_functions, executable_stages, executable_fetch })
}

pub fn compile_stages_and_fetch(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    available_functions: &FunctionPlanRegistry,
    annotated_stages: Vec<AnnotatedStage>,
    annotated_fetch: Option<AnnotatedFetch>,
    input_variables: HashSet<Variable>,
) -> Result<(Vec<ExecutableStage>, Option<ExecutableFetch>), ExecutableCompilationError> {
    let (executable_stages, _) =
        compile_pipeline_stages(statistics, variable_registry.clone(), available_functions, annotated_stages, input_variables)?;
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

    let executable_fetch = annotated_fetch
        .map(|fetch| {
            compile_fetch(statistics, available_functions, fetch, &stages_variable_positions)
                .map_err(|err| ExecutableCompilationError::FetchCompliation { typedb_source: err })
        })
        .transpose()?;
    Ok((executable_stages, executable_fetch))
}

pub(crate) fn compile_pipeline_stages(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    functions: &FunctionPlanRegistry,
    annotated_stages: Vec<AnnotatedStage>,
    input_variables: HashSet<Variable>,
) -> Result<(Vec<ExecutableStage>, HashMap<Variable, VariablePosition>), ExecutableCompilationError> {
    let mut executable_stages = Vec::with_capacity(annotated_stages.len());
    let pipeline_input_variable_positions = input_variables
        .iter()
        .enumerate()
        .map(|(i, var)| (*var, VariablePosition { position: i as u32 }))
        .collect::<HashMap<_,_>>();
    for stage in annotated_stages {
        let stage_input_variable_positions =
            executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping())
                .unwrap_or_else(|| pipeline_input_variable_positions.clone());
        let executable_stage = compile_stage(statistics, variable_registry.clone(), &stage_input_variable_positions, stage, functions)?;
        executable_stages.push(executable_stage);
    }
    Ok((executable_stages, pipeline_input_variable_positions))
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: Arc<VariableRegistry>,
    input_variables: &HashMap<Variable, VariablePosition>,
    annotated_stage: AnnotatedStage,
    functions: &FunctionPlanRegistry,
) -> Result<ExecutableStage, ExecutableCompilationError> {
    match &annotated_stage {
        AnnotatedStage::Match { block, block_annotations, executable_expressions } => {
            let plan = crate::executable::match_::planner::compile(
                block,
                input_variables,
                block_annotations,
                variable_registry,
                executable_expressions,
                statistics,
                functions,
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
            .map_err(|source| ExecutableCompilationError::InsertExecutableCompilation { source })?;
            Ok(ExecutableStage::Insert(plan))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations } => {
            let plan = crate::executable::delete::executable::compile(
                input_variables,
                annotations,
                block.conjunction().constraints(),
                deleted_variables,
            )
            .map_err(|source| ExecutableCompilationError::DeleteExecutableCompilation { source })?;
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
                let reducer_on_position = reducer_on_variable.clone().map(input_variables);
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
