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
use ir::{
    pattern::{conjunction::Conjunction, nested_pattern::NestedPattern},
    pipeline::{function_signature::FunctionID, reduce::AssignedReduction, VariableRegistry},
};

use crate::{
    annotation::{
        fetch::{AnnotatedFetch, AnnotatedFetchObject, AnnotatedFetchSome},
        function::{AnnotatedPreambleFunctions, AnnotatedSchemaFunctions},
        pipeline::AnnotatedStage,
    },
    executable::{
        delete::executable::DeleteExecutable,
        fetch::executable::{compile_fetch, ExecutableFetch},
        function::{executable::compile_functions, ExecutableFunctionRegistry, FunctionCallCostProvider},
        insert::{self, executable::InsertExecutable},
        match_::planner::{match_executable::MatchExecutable, vertex::Cost},
        modifiers::{
            DistinctExecutable, LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable,
        },
        put::PutExecutable,
        reduce::{ReduceExecutable, ReduceRowsExecutable},
        update::executable::UpdateExecutable,
        ExecutableCompilationError,
    },
    VariablePosition,
};

#[derive(Debug, Clone)]
pub struct ExecutablePipeline {
    pub executable_functions: ExecutableFunctionRegistry,
    pub executable_stages: Vec<ExecutableStage>,
    pub executable_fetch: Option<Arc<ExecutableFetch>>,
}

#[derive(Debug, Clone)]
pub enum ExecutableStage {
    Match(Arc<MatchExecutable>),
    Insert(Arc<InsertExecutable>),
    Update(Arc<UpdateExecutable>),
    Put(Arc<PutExecutable>),
    Delete(Arc<DeleteExecutable>),

    Select(Arc<SelectExecutable>),
    Sort(Arc<SortExecutable>),
    Offset(Arc<OffsetExecutable>),
    Limit(Arc<LimitExecutable>),
    Require(Arc<RequireExecutable>),
    Distinct(Arc<DistinctExecutable>),
    Reduce(Arc<ReduceExecutable>),
}

impl ExecutableStage {
    pub fn output_row_mapping(&self) -> HashMap<Variable, VariablePosition> {
        match self {
            ExecutableStage::Match(executable) => executable
                .variable_positions()
                .iter()
                .filter(|(_, pos)| executable.selected_variables().contains(pos))
                .map(|(&var, &pos)| (var, pos))
                .collect(),
            ExecutableStage::Insert(executable) => insert_row_schema_to_mapping(&executable.output_row_schema),
            ExecutableStage::Update(executable) => insert_row_schema_to_mapping(&executable.output_row_schema),
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
            ExecutableStage::Distinct(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Reduce(executable) => executable.output_row_mapping.clone(),
            ExecutableStage::Put(executable) => executable.output_row_mapping().clone(),
        }
    }
}

fn insert_row_schema_to_mapping(
    output_row_schema: &[Option<(Variable, insert::VariableSource)>],
) -> HashMap<Variable, VariablePosition> {
    output_row_schema
        .iter()
        .enumerate()
        .filter_map(|(i, opt)| opt.map(|(v, _)| (i, v)))
        .map(|(i, v)| (v, VariablePosition::new(i as u32)))
        .collect()
}

pub fn compile_pipeline_and_functions(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    annotated_schema_functions: &AnnotatedSchemaFunctions,
    annotated_preamble: AnnotatedPreambleFunctions,
    annotated_stages: Vec<AnnotatedStage>,
    annotated_fetch: Option<AnnotatedFetch>,
    input_variables: &HashSet<Variable>,
) -> Result<ExecutablePipeline, ExecutableCompilationError> {
    // TODO: we could cache compiled schema functions so we dont have to re-compile with every query here
    let referenced_functions = find_referenced_functions(
        annotated_schema_functions,
        &annotated_preamble,
        &annotated_stages,
        annotated_fetch.as_ref(),
    );
    let referenced_schema_functions = annotated_schema_functions
        .iter()
        .filter_map(|(fid, function)| {
            referenced_functions.contains(&fid.clone().into()).then(|| (fid.clone(), function.clone()))
        })
        .collect();
    let arced_executable_schema_functions =
        Arc::new(compile_functions(statistics, &ExecutableFunctionRegistry::empty(), referenced_schema_functions)?);
    let schema_function_registry =
        ExecutableFunctionRegistry::new(arced_executable_schema_functions.clone(), HashMap::new());

    let referenced_preamble_functions = annotated_preamble
        .into_iter()
        .enumerate()
        .filter_map(|(fid, function)| {
            referenced_functions.contains(&fid.clone().into()).then(|| (fid.clone(), function.clone()))
        })
        .collect();
    let executable_preamble_functions =
        compile_functions(statistics, &schema_function_registry, referenced_preamble_functions)?;

    let schema_and_preamble_functions: ExecutableFunctionRegistry =
        ExecutableFunctionRegistry::new(arced_executable_schema_functions, executable_preamble_functions);
    let (_input_positions, executable_stages, executable_fetch) = compile_stages_and_fetch(
        statistics,
        variable_registry,
        &schema_and_preamble_functions,
        annotated_stages,
        annotated_fetch,
        input_variables,
    )?;
    Ok(ExecutablePipeline { executable_functions: schema_and_preamble_functions, executable_stages, executable_fetch })
}

pub fn compile_stages_and_fetch(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    available_functions: &ExecutableFunctionRegistry,
    annotated_stages: Vec<AnnotatedStage>,
    annotated_fetch: Option<AnnotatedFetch>,
    input_variables: &HashSet<Variable>,
) -> Result<
    (HashMap<Variable, VariablePosition>, Vec<ExecutableStage>, Option<Arc<ExecutableFetch>>),
    ExecutableCompilationError,
> {
    let (input_positions, executable_stages, _) = compile_pipeline_stages(
        statistics,
        variable_registry,
        available_functions,
        annotated_stages,
        input_variables.iter().copied(),
        None,
    )?;
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

    let executable_fetch = annotated_fetch
        .map(|fetch| {
            compile_fetch(statistics, available_functions, fetch, &stages_variable_positions)
                .map_err(|err| ExecutableCompilationError::FetchCompilation { typedb_source: err })
        })
        .transpose()?
        .map(Arc::new);
    Ok((input_positions, executable_stages, executable_fetch))
}

pub(crate) fn compile_pipeline_stages(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    call_cost_provider: &impl FunctionCallCostProvider,
    annotated_stages: Vec<AnnotatedStage>,
    input_variables: impl Iterator<Item = Variable>,
    function_return: Option<&[Variable]>,
) -> Result<(HashMap<Variable, VariablePosition>, Vec<ExecutableStage>, Cost), ExecutableCompilationError> {
    let mut executable_stages: Vec<ExecutableStage> = Vec::with_capacity(annotated_stages.len());
    let input_variable_positions =
        input_variables.enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))).collect();

    for stage in annotated_stages {
        // TODO: We can filter out the variables that are no longer needed in the future stages, but are carried as selected variables from the previous one
        let executable_stage = match executable_stages.last().map(|stage| stage.output_row_mapping()) {
            Some(row_mapping) => {
                compile_stage(statistics, variable_registry, call_cost_provider, &row_mapping, function_return, stage)?
            }
            None => compile_stage(
                statistics,
                variable_registry,
                call_cost_provider,
                &input_variable_positions,
                function_return,
                stage,
            )?,
        };
        executable_stages.push(executable_stage);
    }
    let total_cost =
        executable_stages
            .iter()
            .filter_map(|stage| {
                if let ExecutableStage::Match(m) = stage {
                    Some(m.planner_statistics().query_cost)
                } else {
                    None
                }
            })
            .reduce(|x, y| x.chain(y))
            .unwrap_or(Cost { cost: 1.0, io_ratio: 1.0 });
    Ok((input_variable_positions, executable_stages, total_cost))
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    call_cost_provider: &impl FunctionCallCostProvider,
    input_variables: &HashMap<Variable, VariablePosition>,
    function_return: Option<&[Variable]>,
    annotated_stage: AnnotatedStage,
) -> Result<ExecutableStage, ExecutableCompilationError> {
    match &annotated_stage {
        AnnotatedStage::Match { block, block_annotations, executable_expressions, .. } => {
            let mut selected_variables: HashSet<_> = function_return.unwrap_or(&[]).iter().copied().collect();
            selected_variables.extend(input_variables.keys().copied());
            selected_variables.extend(block.conjunction().named_producible_variables(block.block_context()));
            let plan = crate::executable::match_::planner::compile(
                block,
                input_variables,
                &selected_variables,
                block_annotations,
                variable_registry,
                executable_expressions,
                statistics,
                call_cost_provider,
            )
            .map_err(|source| ExecutableCompilationError::MatchCompilation { typedb_source: source })?;
            Ok(ExecutableStage::Match(Arc::new(plan)))
        }
        AnnotatedStage::Insert { block, annotations, source_span } => {
            let plan = crate::executable::insert::executable::compile(
                block.conjunction().constraints(),
                input_variables,
                annotations,
                variable_registry,
                None,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::InsertExecutableCompilation { typedb_source })?;
            Ok(ExecutableStage::Insert(Arc::new(plan)))
        }
        AnnotatedStage::Update { block, annotations, source_span } => {
            let plan = crate::executable::update::executable::compile(
                block.conjunction().constraints(),
                input_variables,
                annotations,
                variable_registry,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::UpdateExecutableCompilation { typedb_source })?;
            Ok(ExecutableStage::Update(Arc::new(plan)))
        }
        AnnotatedStage::Put { block, match_annotations, insert_annotations, source_span } => {
            let mut selected_variables: HashSet<_> = function_return.unwrap_or(&[]).iter().copied().collect();
            selected_variables.extend(input_variables.keys().copied());
            selected_variables.extend(block.conjunction().named_producible_variables(block.block_context()));
            let match_plan = crate::executable::match_::planner::compile(
                block,
                input_variables,
                &selected_variables,
                match_annotations,
                variable_registry,
                &HashMap::new(),
                statistics,
                call_cost_provider,
            )
            .map_err(|source| ExecutableCompilationError::PutMatchCompilation { typedb_source: source })?;
            let insert_plan = crate::executable::insert::executable::compile(
                block.conjunction().constraints(),
                input_variables,
                insert_annotations,
                variable_registry,
                Some(match_plan.variable_positions().clone()),
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::PutInsertCompilation { typedb_source })?;
            Ok(ExecutableStage::Put(Arc::new(PutExecutable::new(match_plan, insert_plan))))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations, source_span } => {
            let plan = crate::executable::delete::executable::compile(
                input_variables,
                annotations,
                variable_registry,
                block.conjunction().constraints(),
                deleted_variables,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::DeleteExecutableCompilation { typedb_source })?;
            Ok(ExecutableStage::Delete(Arc::new(plan)))
        }
        AnnotatedStage::Select(select) => {
            let mut retained_positions = HashSet::with_capacity(select.variables.len());
            let mut removed_positions =
                HashSet::with_capacity(input_variables.len().saturating_sub(select.variables.len()));
            let mut output_row_mapping = HashMap::with_capacity(select.variables.len());
            for (&variable, &pos) in input_variables.iter() {
                if select.variables.contains(&variable) {
                    retained_positions.insert(pos);
                    output_row_mapping.insert(variable, pos);
                } else {
                    removed_positions.insert(pos);
                }
            }
            Ok(ExecutableStage::Select(Arc::new(SelectExecutable::new(
                retained_positions,
                output_row_mapping,
                removed_positions,
            ))))
        }
        AnnotatedStage::Sort(sort) => {
            Ok(ExecutableStage::Sort(Arc::new(SortExecutable::new(sort.variables.clone(), input_variables.clone()))))
        }
        AnnotatedStage::Offset(offset) => {
            Ok(ExecutableStage::Offset(Arc::new(OffsetExecutable::new(offset.offset(), input_variables.clone()))))
        }
        AnnotatedStage::Limit(limit) => {
            Ok(ExecutableStage::Limit(Arc::new(LimitExecutable::new(limit.limit(), input_variables.clone()))))
        }
        AnnotatedStage::Require(require) => {
            let mut required_positions = HashSet::with_capacity(require.variables.len());
            for &variable in &require.variables {
                let pos = input_variables[&variable];
                required_positions.insert(pos);
            }
            Ok(ExecutableStage::Require(Arc::new(RequireExecutable::new(required_positions, input_variables.clone()))))
        }
        AnnotatedStage::Distinct(distinct) => {
            Ok(ExecutableStage::Distinct(Arc::new(DistinctExecutable::new(input_variables.clone()))))
        }
        AnnotatedStage::Reduce(reduce, typed_reducers) => {
            debug_assert_eq!(reduce.assigned_reductions.len(), typed_reducers.len());
            let mut output_row_mapping = HashMap::new();
            let mut input_group_positions = Vec::with_capacity(reduce.groupby.len());
            for variable in reduce.groupby.iter() {
                output_row_mapping.insert(*variable, VariablePosition::new(input_group_positions.len() as u32));
                input_group_positions.push(input_variables[variable]);
            }
            let mut reductions = Vec::with_capacity(reduce.assigned_reductions.len());
            for (&AssignedReduction { assigned, .. }, reducer_on_variable) in
                zip(reduce.assigned_reductions.iter(), typed_reducers.iter())
            {
                output_row_mapping
                    .insert(assigned, VariablePosition::new((input_group_positions.len() + reductions.len()) as u32));
                let reducer_on_position = reducer_on_variable.clone().map(input_variables);
                reductions.push(reducer_on_position);
            }
            Ok(ExecutableStage::Reduce(Arc::new(ReduceExecutable::new(
                ReduceRowsExecutable { reductions, input_group_positions },
                output_row_mapping,
            ))))
        }
    }
}

fn find_referenced_functions(
    annotated_schema_functions: &AnnotatedSchemaFunctions,
    preamble: &AnnotatedPreambleFunctions,
    stages: &[AnnotatedStage],
    fetch: Option<&AnnotatedFetch>,
) -> HashSet<FunctionID> {
    let mut referenced_functions = HashSet::new();
    find_referenced_functions_in_pipeline(annotated_schema_functions, preamble, stages, &mut referenced_functions);
    if let Some(fetch) = fetch {
        find_referenced_functions_in_fetch(
            annotated_schema_functions,
            preamble,
            &fetch.object,
            &mut referenced_functions,
        );
    }
    referenced_functions
}
fn find_referenced_functions_in_pipeline(
    annotated_schema_functions: &AnnotatedSchemaFunctions,
    preamble: &AnnotatedPreambleFunctions,
    stages: &[AnnotatedStage],
    referenced_functions: &mut HashSet<FunctionID>,
) {
    stages
        .iter()
        .filter_map(
            |stage| {
                if let AnnotatedStage::Match { block, .. } = stage {
                    Some(block.conjunction())
                } else {
                    None
                }
            },
        )
        .for_each(|conjunction| {
            find_referenced_functions_in_conjunction(
                annotated_schema_functions,
                preamble,
                conjunction,
                referenced_functions,
            );
        });
}

fn find_referenced_functions_in_conjunction(
    annotated_schema_functions: &AnnotatedSchemaFunctions,
    preamble: &AnnotatedPreambleFunctions,
    conjunction: &Conjunction,
    referenced_functions: &mut HashSet<FunctionID>,
) {
    conjunction
        .constraints()
        .iter()
        .filter_map(|constraint| constraint.as_function_call_binding().map(|call| call.function_call().function_id()))
        .for_each(|function_id| {
            if !referenced_functions.contains(&function_id) {
                let function = match &function_id {
                    FunctionID::Schema(key) => annotated_schema_functions.get(key).unwrap(),
                    FunctionID::Preamble(key) => preamble.get(*key).unwrap(),
                };
                referenced_functions.insert(function_id);
                find_referenced_functions_in_pipeline(
                    annotated_schema_functions,
                    preamble,
                    &function.stages,
                    referenced_functions,
                );
            }
        });
    conjunction.nested_patterns().iter().for_each(|nested| match nested {
        NestedPattern::Negation(inner) => find_referenced_functions_in_conjunction(
            annotated_schema_functions,
            preamble,
            inner.conjunction(),
            referenced_functions,
        ),
        NestedPattern::Optional(inner) => find_referenced_functions_in_conjunction(
            annotated_schema_functions,
            preamble,
            inner.conjunction(),
            referenced_functions,
        ),
        NestedPattern::Disjunction(disjunction) => disjunction.conjunctions().iter().for_each(|inner| {
            find_referenced_functions_in_conjunction(annotated_schema_functions, preamble, inner, referenced_functions);
        }),
    })
}

fn find_referenced_functions_in_fetch(
    annotated_schema_functions: &AnnotatedSchemaFunctions,
    preamble: &AnnotatedPreambleFunctions,
    fetch: &AnnotatedFetchObject,
    referenced_functions: &mut HashSet<FunctionID>,
) {
    if let AnnotatedFetchObject::Entries(entries) = fetch {
        entries.values().for_each(|fetch_object| {
            match fetch_object {
                AnnotatedFetchSome::SingleFunction(function) | AnnotatedFetchSome::ListFunction(function) => {
                    find_referenced_functions_in_pipeline(
                        annotated_schema_functions,
                        preamble,
                        &function.stages,
                        referenced_functions,
                    );
                }
                AnnotatedFetchSome::ListSubFetch(sub_fetch) => {
                    find_referenced_functions_in_pipeline(
                        annotated_schema_functions,
                        preamble,
                        &sub_fetch.stages,
                        referenced_functions,
                    );
                    find_referenced_functions_in_fetch(
                        annotated_schema_functions,
                        preamble,
                        &sub_fetch.fetch.object,
                        referenced_functions,
                    )
                }
                AnnotatedFetchSome::Object(inner) => {
                    find_referenced_functions_in_fetch(
                        annotated_schema_functions,
                        preamble,
                        inner,
                        referenced_functions,
                    );
                }
                AnnotatedFetchSome::SingleVar(_)
                | AnnotatedFetchSome::SingleAttribute(_, _)
                | AnnotatedFetchSome::ListAttributesAsList(_, _)
                | AnnotatedFetchSome::ListAttributesFromList(_, _) => {
                    // Nothing
                }
            }
        })
    }
}
