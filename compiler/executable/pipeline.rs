/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet},
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{conjunction::Conjunction, nested_pattern::NestedPattern, Pattern, Vertex},
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
        match_::planner::conjunction_executable::ConjunctionExecutable,
        modifiers::{
            DistinctExecutable, LimitExecutable, OffsetExecutable, RequireExecutable, SelectExecutable, SortExecutable,
        },
        put::PutExecutable,
        reduce::{ReduceExecutable, ReduceRowsExecutable},
        update::executable::UpdateExecutable,
        ExecutableCompilationError,
    },
    query_structure::ParametrisedPipelineStructure,
    VariablePosition,
};

#[derive(Debug, Default, Clone)]
pub struct TypePopulations {
    counts: HashMap<Type, u64>,
}

impl TypePopulations {
    fn update(&mut self, types: &BTreeSet<Type>, statistics: &Statistics) {
        for &ty in types {
            self.counts.entry(ty).or_insert_with(|| match ty {
                Type::Entity(ty) => statistics.entity_counts.get(&ty).copied().unwrap_or_default(),
                Type::Relation(ty) => statistics.relation_counts.get(&ty).copied().unwrap_or_default(),
                Type::Attribute(ty) => statistics.attribute_counts.get(&ty).copied().unwrap_or_default(),
                Type::RoleType(ty) => statistics.role_counts.get(&ty).copied().unwrap_or_default(),
            });
        }
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.counts.extend(other.counts)
    }
}

impl<'a> IntoIterator for &'a TypePopulations {
    type Item = (&'a Type, &'a u64);

    type IntoIter = hash_map::Iter<'a, Type, u64>;

    fn into_iter(self) -> Self::IntoIter {
        self.counts.iter()
    }
}

#[derive(Debug, Clone)]
pub struct ExecutablePipeline {
    pub executable_functions: ExecutableFunctionRegistry,
    pub executable_stages: Vec<ExecutableStage>,
    pub executable_fetch: Option<Arc<ExecutableFetch>>,
    pub pipeline_structure: Arc<ParametrisedPipelineStructure>,
    pub type_populations: TypePopulations,
}

#[derive(Debug, Clone)]
pub enum ExecutableStage {
    Match(Arc<ConjunctionExecutable>),
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
    pipeline_structure: Arc<ParametrisedPipelineStructure>,
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
        .filter(|&(fid, _)| referenced_functions.contains(&fid.clone().into()))
        .map(|(fid, function)| (fid.clone(), function.clone()))
        .collect();
    let arced_executable_schema_functions =
        Arc::new(compile_functions(statistics, &ExecutableFunctionRegistry::empty(), referenced_schema_functions)?);
    let schema_function_registry =
        ExecutableFunctionRegistry::new(arced_executable_schema_functions.clone(), HashMap::new());

    let referenced_preamble_functions = annotated_preamble
        .into_iter()
        .enumerate()
        .filter(|&(fid, _)| referenced_functions.contains(&fid.into()))
        .collect();
    let executable_preamble_functions =
        compile_functions(statistics, &schema_function_registry, referenced_preamble_functions)?;

    let schema_and_preamble_functions: ExecutableFunctionRegistry =
        ExecutableFunctionRegistry::new(arced_executable_schema_functions, executable_preamble_functions);
    let (_input_positions, executable_stages, executable_fetch, type_populations) = compile_stages_and_fetch(
        statistics,
        variable_registry,
        &schema_and_preamble_functions,
        &annotated_stages,
        annotated_fetch,
        input_variables,
    )?;
    debug_assert!(!executable_stages.is_empty());
    Ok(ExecutablePipeline {
        pipeline_structure,
        executable_functions: schema_and_preamble_functions,
        executable_stages,
        executable_fetch,
        type_populations,
    })
}

pub fn compile_stages_and_fetch(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    available_functions: &ExecutableFunctionRegistry,
    annotated_stages: &[AnnotatedStage],
    annotated_fetch: Option<AnnotatedFetch>,
    input_variables: &HashSet<Variable>,
) -> Result<
    (HashMap<Variable, VariablePosition>, Vec<ExecutableStage>, Option<Arc<ExecutableFetch>>, TypePopulations),
    ExecutableCompilationError,
> {
    let (input_positions, executable_stages, mut type_populations) = compile_pipeline_stages(
        statistics,
        variable_registry,
        available_functions,
        annotated_stages,
        input_variables.iter().copied(),
        None,
    )?;
    let stages_variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

    if let Some(fetch) = annotated_fetch {
        let (executable_fetch, fetch_type_populations) =
            compile_fetch(statistics, available_functions, fetch, &stages_variable_positions)
                .map_err(|err| ExecutableCompilationError::FetchCompilation { typedb_source: err })?;
        type_populations.extend(fetch_type_populations);
        Ok((input_positions, executable_stages, Some(Arc::new(executable_fetch)), type_populations))
    } else {
        Ok((input_positions, executable_stages, None, type_populations))
    }
}

pub(crate) fn compile_pipeline_stages(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    call_cost_provider: &impl FunctionCallCostProvider,
    annotated_stages: &[AnnotatedStage],
    input_variables: impl Iterator<Item = Variable>,
    function_return: Option<&[Variable]>,
) -> Result<(HashMap<Variable, VariablePosition>, Vec<ExecutableStage>, TypePopulations), ExecutableCompilationError> {
    let mut executable_stages: Vec<ExecutableStage> = Vec::with_capacity(annotated_stages.len());
    let input_variable_positions =
        input_variables.enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))).collect();
    let mut last_match_annotations = None;
    let mut type_populations = TypePopulations::default();
    for stage in annotated_stages {
        // TODO: We can filter out the variables that are no longer needed in the future stages, but are carried as selected variables from the previous one
        let (executable_stage, referenced_types) =
            match executable_stages.last().map(|stage| stage.output_row_mapping()) {
                Some(row_mapping) => compile_stage(
                    statistics,
                    variable_registry,
                    call_cost_provider,
                    &row_mapping,
                    last_match_annotations.unwrap_or(&BTreeMap::new()),
                    function_return,
                    stage,
                )?,
                None => compile_stage(
                    statistics,
                    variable_registry,
                    call_cost_provider,
                    &input_variable_positions,
                    last_match_annotations.unwrap_or(&BTreeMap::new()),
                    function_return,
                    stage,
                )?,
            };
        if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
            last_match_annotations =
                Some(block_annotations.type_annotations_of(block.conjunction()).unwrap().vertex_annotations())
        }
        type_populations.update(&referenced_types, statistics);
        executable_stages.push(executable_stage);
    }
    Ok((input_variable_positions, executable_stages, type_populations))
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    call_cost_provider: &impl FunctionCallCostProvider,
    stage_input_positions: &HashMap<Variable, VariablePosition>,
    stage_input_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<answer::Type>>>,
    function_return: Option<&[Variable]>,
    annotated_stage: &AnnotatedStage,
) -> Result<(ExecutableStage, BTreeSet<Type>), ExecutableCompilationError> {
    match annotated_stage {
        AnnotatedStage::Match { block, block_annotations, executable_expressions, .. } => {
            // TODO: technically, we only need to select variables that are used _later_ in the pipeline, not everything
            let mut selected_variables: HashSet<_> = function_return.unwrap_or(&[]).iter().copied().collect();
            selected_variables.extend(stage_input_positions.keys().copied());
            selected_variables.extend(block.conjunction().named_visible_binding_variables(block.block_context()));
            let plan = crate::executable::match_::planner::compile(
                block,
                stage_input_annotations,
                stage_input_positions,
                selected_variables,
                block_annotations,
                variable_registry,
                executable_expressions,
                statistics,
                call_cost_provider,
            )
            .map_err(|source| ExecutableCompilationError::MatchCompilation { typedb_source: source })?;
            Ok((ExecutableStage::Match(Arc::new(plan)), block_annotations.referenced_types()))
        }
        AnnotatedStage::Insert { block, annotations, source_span } => {
            let plan = crate::executable::insert::executable::compile(
                block,
                stage_input_positions,
                annotations,
                variable_registry,
                None,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::InsertExecutableCompilation { typedb_source })?;
            Ok((ExecutableStage::Insert(Arc::new(plan)), BTreeSet::new()))
        }
        AnnotatedStage::Update { block, annotations, source_span } => {
            let plan = crate::executable::update::executable::compile(
                block,
                stage_input_positions,
                annotations,
                variable_registry,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::UpdateExecutableCompilation { typedb_source })?;
            Ok((ExecutableStage::Update(Arc::new(plan)), BTreeSet::new()))
        }
        AnnotatedStage::Put { block, match_annotations, insert_annotations, source_span } => {
            let mut selected_variables: HashSet<_> = function_return.unwrap_or(&[]).iter().copied().collect();
            selected_variables.extend(stage_input_positions.keys().copied());
            selected_variables.extend(block.conjunction().named_visible_binding_variables(block.block_context()));
            let match_plan = crate::executable::match_::planner::compile(
                block,
                stage_input_annotations,
                stage_input_positions,
                selected_variables,
                match_annotations,
                variable_registry,
                &HashMap::new(),
                statistics,
                call_cost_provider,
            )
            .map_err(|source| ExecutableCompilationError::PutMatchCompilation { typedb_source: source })?;
            let insert_plan = crate::executable::insert::executable::compile(
                block,
                stage_input_positions,
                insert_annotations,
                variable_registry,
                Some(match_plan.variable_positions().clone()),
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::PutInsertCompilation { typedb_source })?;
            Ok((
                ExecutableStage::Put(Arc::new(PutExecutable::new(match_plan, insert_plan))),
                match_annotations.referenced_types(),
            ))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations, source_span } => {
            let plan = crate::executable::delete::executable::compile(
                stage_input_positions,
                annotations,
                variable_registry,
                block,
                deleted_variables,
                *source_span,
            )
            .map_err(|typedb_source| ExecutableCompilationError::DeleteExecutableCompilation { typedb_source })?;
            Ok((ExecutableStage::Delete(Arc::new(plan)), BTreeSet::new()))
        }
        AnnotatedStage::Select(select) => {
            let mut retained_positions = HashSet::with_capacity(select.variables.len());
            let mut removed_positions =
                HashSet::with_capacity(stage_input_positions.len().saturating_sub(select.variables.len()));
            let mut output_row_mapping = HashMap::with_capacity(select.variables.len());
            for (&variable, &pos) in stage_input_positions.iter() {
                if select.variables.contains(&variable) {
                    retained_positions.insert(pos);
                    output_row_mapping.insert(variable, pos);
                } else {
                    removed_positions.insert(pos);
                }
            }
            Ok((
                ExecutableStage::Select(Arc::new(SelectExecutable::new(
                    retained_positions,
                    output_row_mapping,
                    removed_positions,
                ))),
                BTreeSet::new(),
            ))
        }
        AnnotatedStage::Sort(sort) => Ok((
            ExecutableStage::Sort(Arc::new(SortExecutable::new(sort.variables.clone(), stage_input_positions.clone()))),
            BTreeSet::new(),
        )),
        AnnotatedStage::Offset(offset) => Ok((
            ExecutableStage::Offset(Arc::new(OffsetExecutable::new(offset.offset(), stage_input_positions.clone()))),
            BTreeSet::new(),
        )),
        AnnotatedStage::Limit(limit) => Ok((
            ExecutableStage::Limit(Arc::new(LimitExecutable::new(limit.limit(), stage_input_positions.clone()))),
            BTreeSet::new(),
        )),
        AnnotatedStage::Require(require) => {
            let mut required_positions = HashSet::with_capacity(require.variables.len());
            for &variable in &require.variables {
                let pos = stage_input_positions[&variable];
                required_positions.insert(pos);
            }
            Ok((
                ExecutableStage::Require(Arc::new(RequireExecutable::new(
                    required_positions,
                    stage_input_positions.clone(),
                ))),
                BTreeSet::new(),
            ))
        }
        AnnotatedStage::Distinct(_distinct) => Ok((
            ExecutableStage::Distinct(Arc::new(DistinctExecutable::new(stage_input_positions.clone()))),
            BTreeSet::new(),
        )),
        AnnotatedStage::Reduce(reduce, typed_reducers) => {
            debug_assert_eq!(reduce.assigned_reductions.len(), typed_reducers.len());
            let mut output_row_mapping = HashMap::new();
            let mut input_group_positions = Vec::with_capacity(reduce.groupby.len());
            for variable in reduce.groupby.iter() {
                output_row_mapping.insert(*variable, VariablePosition::new(input_group_positions.len() as u32));
                input_group_positions.push(stage_input_positions[variable]);
            }
            let mut reductions = Vec::with_capacity(reduce.assigned_reductions.len());
            for (&AssignedReduction { assigned, .. }, reducer_on_variable) in
                zip(reduce.assigned_reductions.iter(), typed_reducers.iter())
            {
                output_row_mapping
                    .insert(assigned, VariablePosition::new((input_group_positions.len() + reductions.len()) as u32));
                let reducer_on_position = reducer_on_variable.clone().map(stage_input_positions);
                reductions.push(reducer_on_position);
            }
            Ok((
                ExecutableStage::Reduce(Arc::new(ReduceExecutable::new(
                    ReduceRowsExecutable { reductions, input_group_positions },
                    output_row_mapping,
                ))),
                BTreeSet::new(),
            ))
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
