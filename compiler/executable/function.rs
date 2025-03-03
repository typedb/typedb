/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{
        function_signature::{FunctionID, FunctionIDAPI},
        ParameterRegistry,
    },
};
use itertools::Itertools;
use typeql::schema::definable::function::SingleSelector;

use crate::{
    annotation::{
        function::{AnnotatedFunction, AnnotatedFunctionReturn},
        pipeline::AnnotatedStage,
    },
    executable::{
        match_::planner::vertex::Cost,
        next_executable_id,
        pipeline::{compile_pipeline_stages, ExecutableStage},
        reduce::ReduceRowsExecutable,
        ExecutableCompilationError,
    },
    VariablePosition,
};

#[derive(Debug, Clone)]
pub struct ExecutableFunction {
    pub executable_id: u64,
    pub executable_stages: Vec<ExecutableStage>,
    pub argument_positions: HashMap<Variable, VariablePosition>,
    pub returns: ExecutableReturn,
    pub is_tabled: FunctionTablingType,
    pub parameter_registry: Arc<ParameterRegistry>,
    pub single_call_cost: Cost,
}

#[derive(Debug, Clone)]
pub enum ExecutableReturn {
    Stream(Vec<VariablePosition>),
    Single(SingleSelector, Vec<VariablePosition>),
    Check,
    Reduce(Arc<ReduceRowsExecutable>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionTablingType {
    Tabled(FunctionID),
    Untabled,
}

pub trait FunctionCallCostProvider {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost;
}

#[derive(Clone)]
pub struct ExecutableFunctionRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
    preamble_functions: HashMap<usize, ExecutableFunction>,
}

impl fmt::Debug for ExecutableFunctionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutableFunctionRegistry { omitted }")
    }
}

impl ExecutableFunctionRegistry {
    pub(crate) fn new(
        schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
        preamble_functions: HashMap<usize, ExecutableFunction>,
    ) -> Self {
        Self { schema_functions, preamble_functions }
    }

    pub fn empty() -> Self {
        Self::new(Arc::new(HashMap::new()), HashMap::new())
    }

    pub fn get(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        match function_id {
            FunctionID::Schema(id) => self.schema_functions.get(id),
            FunctionID::Preamble(id) => self.preamble_functions.get(id),
        }
    }

    pub(crate) fn schema_functions(&self) -> Arc<HashMap<DefinitionKey, ExecutableFunction>> {
        self.schema_functions.clone()
    }
}

impl FunctionCallCostProvider for ExecutableFunctionRegistry {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        self.get(function_id).unwrap().single_call_cost
    }
}

pub(crate) fn compile_single_untabled_function(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    to_compile: AnnotatedFunction,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    compile_function(statistics, to_compile, cached_plans, FunctionTablingType::Untabled)
}

pub(crate) fn compile_functions<FIDType: FunctionIDAPI>(
    statistics: &Statistics,
    cached_plans: &ExecutableFunctionRegistry,
    mut to_compile: HashMap<FIDType, AnnotatedFunction>,
) -> Result<HashMap<FIDType, ExecutableFunction>, ExecutableCompilationError> {
    // TODO: Cache compiled schema functions?
    let (post_order, dependencies, tabling_types) = determine_dependencies_and_tabling_types(&to_compile)?;
    let mut context = FunctionCompilationContext::new(cached_plans, to_compile, dependencies, tabling_types);
    // Now plan & compile this function.

    for fid in post_order {
        // Going in post-order ensures dependencies are compiled first, and we have a cost available.
        if let Some(function) = context.to_compile.remove(&fid) {
            let compiled_function = compile_function(statistics, function, &context, context.tabling_types.get(&fid).unwrap().clone())?;
            context.compiled.insert(fid.clone(), compiled_function);
        }
    }
    Ok(context.compiled)
}

// Private
struct FunctionCompilationContext<'a, FIDType: FunctionIDAPI> {
    precompiled: &'a ExecutableFunctionRegistry,
    to_compile: HashMap<FIDType, AnnotatedFunction>,
    compiled: HashMap<FIDType, ExecutableFunction>,
    dependencies: HashMap<FIDType, HashSet<FIDType>>,
    tabling_types: HashMap<FIDType, FunctionTablingType>,
}

impl<'a, FIDType: FunctionIDAPI> FunctionCompilationContext<'a, FIDType> {
    fn new(cached_plans: &'a ExecutableFunctionRegistry, to_compile: HashMap<FIDType, AnnotatedFunction>, dependencies: HashMap<FIDType, HashSet<FIDType>>, tabling_types: HashMap<FIDType, FunctionTablingType>) -> Self {
        FunctionCompilationContext { precompiled: cached_plans, to_compile, compiled: HashMap::new(), dependencies, tabling_types }
    }

    pub(crate) fn get_executable_function(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        if let Some(plan) = self.precompiled.get(function_id) {
            Some(plan)
        } else if let Ok(key) = &FIDType::try_from(function_id.clone()) {
            self.compiled.get(&key)
        } else {
            None
        }
    }

    fn cycle_breaking_cost(&self) -> Cost {
        Cost { cost: 1.0, io_ratio: 1.0 } // TODO: Improve. This should simulate depth 1 recursion.
    }
}

impl<'a, FIDType: FunctionIDAPI> FunctionCallCostProvider for FunctionCompilationContext<'a, FIDType> {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        if let Some(function) = self.get_executable_function(function_id) {
            function.single_call_cost.clone()
        } else {
            debug_assert!(matches!(
                FIDType::try_from(function_id.clone()).map(|id| matches!(self.tabling_types.get(&id), Some(FunctionTablingType::Tabled(_)))),
                Ok(true)
            ));
            self.cycle_breaking_cost()
        }
    }
}

fn determine_dependencies_and_tabling_types<FIDType: FunctionIDAPI>(
    to_compile: &HashMap<FIDType, AnnotatedFunction>,
) -> Result<(Vec<FIDType>, HashMap<FIDType, HashSet<FIDType>>, HashMap<FIDType, FunctionTablingType>), ExecutableCompilationError> {
    // Quick kosaraju for SCC finding
    let mut forward_dependency_graph = HashMap::new();
    let mut reverse_dependency_graph = HashMap::new();
    let mut order = Vec::new();
    for fid in to_compile.keys() {
        construct_dependency_graphs(&to_compile, fid.clone(), &mut forward_dependency_graph, &mut reverse_dependency_graph, &mut order)?;
    }
    debug_assert!(to_compile.keys().all(|k| order.contains(k)));
    let mut scc = HashSet::new();
    let mut scc_mapping = HashMap::new();
    for root in order.iter().rev()  {
        if scc_mapping.contains_key(root) {
            continue;
        }
        scc.clear();
        collect_scc(root.clone(), &reverse_dependency_graph, &mut scc);
        scc.iter().for_each(|fid| {
            scc_mapping.insert(fid.clone(), root);
        });
    }

    let mut tabling_types = HashMap::new();
    let mut open = HashSet::new();
    let mut closed = HashSet::new();
    for fid in forward_dependency_graph.keys() {
        determine_tabling_types(&forward_dependency_graph, &scc_mapping, &mut tabling_types, &mut open, &mut closed, fid);
    }

    Ok((order, forward_dependency_graph, tabling_types))
}

fn construct_dependency_graphs<FIDType: FunctionIDAPI>(
    to_compile: &HashMap<FIDType, AnnotatedFunction>,
    fid: FIDType,
    forward_dependencies: &mut HashMap<FIDType, HashSet<FIDType>>,
    reversed_dependencies: &mut HashMap<FIDType, HashSet<FIDType>>,
    post_order: &mut Vec<FIDType>,
) -> Result<(), ExecutableCompilationError> {
    if forward_dependencies.contains_key(&fid) {
        debug_assert!(reversed_dependencies.contains_key(&fid));
        return Ok(());
    }
    debug_assert!(!reversed_dependencies.contains_key(&fid));
    reversed_dependencies.insert(fid.clone(), HashSet::new());

    let function = to_compile.get(&fid).unwrap();
    let mut all_called_ids = all_calls_in_pipeline(function.stages.as_slice())
        .iter()
        .filter_map(|id| FIDType::try_from(id.clone()).ok())
        .filter(|id| to_compile.contains_key(id))
        .collect::<HashSet<_>>();
    forward_dependencies.insert(fid.clone(), all_called_ids.clone());

    for called_id in &all_called_ids {
        construct_dependency_graphs(to_compile, called_id.clone(), forward_dependencies, reversed_dependencies, post_order);
        reversed_dependencies.get_mut(called_id).unwrap().insert(fid.clone());
    }
    post_order.push(fid);
    Ok(())
}


fn collect_scc<FIDType: FunctionIDAPI>(
    fid: FIDType,
    reversed_dependency_graph: &HashMap<FIDType, HashSet<FIDType>>,
    scc: &mut HashSet<FIDType>,
) {
    if scc.insert(fid.clone()) {
        for other_fid in reversed_dependency_graph.get(&fid).unwrap() {
            collect_scc(other_fid.clone(), reversed_dependency_graph, scc);
        }
    }
}

fn determine_tabling_types<FIDType:FunctionIDAPI>(
    dependencies: &HashMap<FIDType, HashSet<FIDType>>,
    scc_mapping: &HashMap<FIDType, &FIDType>,
    tabling_types: &mut HashMap<FIDType, FunctionTablingType>,
    open: &mut HashSet<FIDType>,
    closed: &mut HashSet<FIDType>,
    fid: &FIDType,
) {
    if closed.contains(fid) {
        return;
    }
    if open.contains(fid) {
        tabling_types.insert(fid.clone(), FunctionTablingType::Tabled((*scc_mapping.get(fid).unwrap()).clone().into()));
        return;
    }
    open.insert(fid.clone());
    dependencies.get(fid).unwrap().iter().for_each(|dependency_fid| {
        determine_tabling_types(dependencies, scc_mapping, tabling_types, open, closed, dependency_fid);
    });
    open.remove(&fid);
    closed.insert(fid.clone());
    if !tabling_types.contains_key(fid) {
        tabling_types.insert(fid.clone(), FunctionTablingType::Untabled);
    }
}

fn compile_function(
    statistics: &Statistics,
    function: AnnotatedFunction,
    call_cost_provider: &impl FunctionCallCostProvider,
    is_tabled: FunctionTablingType,
) -> Result<ExecutableFunction, ExecutableCompilationError> {
    debug_assert!(all_calls_in_pipeline(function.stages.as_slice()).iter().all(|f| {
        call_cost_provider.get_call_cost(f);
        true // The call above will crash if the assertion fails.
    }));
    let AnnotatedFunction { variable_registry, parameter_registry, arguments, stages, return_, .. } = function;
    let (argument_positions, executable_stages, _) = compile_pipeline_stages(
        statistics,
        &variable_registry,
        call_cost_provider,
        stages,
        arguments.into_iter(),
        &return_.referenced_variables(),
    )?;

    let returns = compile_return_operation(&executable_stages, return_)?;
    debug_assert!(executable_stages.iter().any(|stage| matches!(stage, ExecutableStage::Match(_))));
    let single_call_cost =
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
            .unwrap();
    Ok(ExecutableFunction {
        executable_id: next_executable_id(),
        executable_stages,
        argument_positions,
        returns,
        parameter_registry: Arc::new(parameter_registry),
        is_tabled,
        single_call_cost,
    })
}

fn compile_return_operation(
    executable_stages: &[ExecutableStage],
    return_: AnnotatedFunctionReturn,
) -> Result<ExecutableReturn, ExecutableCompilationError> {
    let variable_positions =
        executable_stages.last().map(|stage: &ExecutableStage| stage.output_row_mapping()).unwrap_or_default();
    match return_ {
        AnnotatedFunctionReturn::Stream { variables, .. } => {
            Ok(ExecutableReturn::Stream(variables.iter().map(|var| variable_positions[var]).collect()))
        }
        AnnotatedFunctionReturn::Single { selector, variables, .. } => {
            Ok(ExecutableReturn::Single(selector, variables.iter().map(|var| variable_positions[var]).collect()))
        }
        | AnnotatedFunctionReturn::ReduceCheck {} => Ok(ExecutableReturn::Check),
        | AnnotatedFunctionReturn::ReduceReducer { instructions } => {
            let reductions = instructions.into_iter().map(|reducer| reducer.map(&variable_positions)).collect();
            Ok(ExecutableReturn::Reduce(Arc::new(ReduceRowsExecutable {
                reductions,
                input_group_positions: Vec::new(),
            })))
        }
    }
}

fn all_calls_in_pipeline(stages: &[AnnotatedStage]) -> HashSet<FunctionID> {
    let match_stage_conjunctions = stages.iter().filter_map(|stage| match stage {
        AnnotatedStage::Match { block, .. } => Some(block.conjunction()),
        _ => None,
    });
    let mut call_accumulator = HashSet::new();
    for conjunction in match_stage_conjunctions {
        all_calls_in_conjunction(conjunction, &mut call_accumulator);
    }
    call_accumulator
}

fn all_calls_in_conjunction(conjunction: &Conjunction, call_accumulator: &mut HashSet<FunctionID>) {
    for constraint in conjunction.constraints() {
        if let Constraint::FunctionCallBinding(binding) = constraint {
            call_accumulator.insert(binding.function_call().function_id());
        }
    }
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for inner in disjunction.conjunctions() {
                    all_calls_in_conjunction(inner, call_accumulator);
                }
            }
            NestedPattern::Optional(optional) => {
                all_calls_in_conjunction(optional.conjunction(), call_accumulator);
            }
            NestedPattern::Negation(negation) => {
                all_calls_in_conjunction(negation.conjunction(), call_accumulator);
            }
        }
    }
}
