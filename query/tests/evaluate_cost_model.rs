/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, trace};
use compiler::annotation::function::AnnotatedSchemaFunctions;
use compiler::annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedStage};
use compiler::executable::function::ExecutableFunctionRegistry;
use compiler::executable::match_::planner::{compile, MatchCompilationError};
use compiler::executable::match_::planner::match_executable::MatchExecutable;
use compiler::ExecutorVariable;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use executor::{ExecutionInterrupt, InterruptType};
use executor::pipeline::initial::InitialStage;
use executor::pipeline::match_::MatchStageExecutor;
use executor::pipeline::stage::{ExecutionContext, ReadPipelineStage, StageAPI};
use ir::pipeline::function_signature::HashMapFunctionSignatureIndex;
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use ir::translation::match_::translate_match;
use ir::translation::pipeline::{translate_pipeline, TranslatedPipeline};
use lending_iterator::LendingIterator;
use resource::profile::QueryProfile;
use storage::snapshot::ReadableSnapshot;

fn run_plan(
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    executable: Arc<MatchExecutable>,
    expected_answer_count: usize,
) -> QueryProfile {
    let query_profile = Arc::new(QueryProfile::new(true));
    let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);
    let interrupt = ExecutionInterrupt::new(query_interrupt_receiver);
    let finished = Arc::new(AtomicBool::new(false));
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5000));
        if !finished.load(Ordering::SeqCst) {
            match query_interrupt_sender.send(InterruptType::TransactionClosed) {
                Ok(()) => {}, // oops, we timed out
                Err(_) => {}, // great, the request finished already
            }
        }
    });
    let context = ExecutionContext::new_with_profile(snapshot, thing_manager, parameters.clone(), query_profile);
    let initial_stage =ReadPipelineStage::Initial(Box::new(InitialStage::new_empty(context)));
    let executor = MatchStageExecutor::new(executable.clone(), initial_stage, Arc::new(ExecutableFunctionRegistry::empty()));
    let (iterator, context) = executor.into_iterator(interrupt).unwrap();
    let answer_count = iterator.count();
    assert_eq!(answer_count, expected_answer_count);
    Arc::into_inner(context.profile).unwrap()
}

fn plan_query(snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, statistics: &Statistics, query: &str) -> Vec<SampledConjunctionPlan> {
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let mut translated = translate_pipeline(&snapshot, &HashMapFunctionSignatureIndex::empty(), &pipeline).unwrap();
    assert_eq!(translated.translated_stages.len(), 1);
    debug_assert!(translated.translated_preamble.is_empty() && translated.translated_fetch.is_none() && translated.translated_stages.len() == 1);
    let TranslatedPipeline { translated_stages, mut variable_registry, value_parameters, .. } = translated;
    let annotated = annotate_preamble_and_pipeline(
        snapshot,
        type_manager,
        Arc::new(AnnotatedSchemaFunctions::new()),
        &mut variable_registry,
        &value_parameters,
        Vec::new(),
        translated_stages,
        None,
    ).unwrap();
    sample_plans(&annotated.annotated_stages[0], variable_registry, statistics, )
}

struct SampledConjunctionPlan {
    executable: MatchExecutable,
}

fn sample_plans(match_stage: &AnnotatedStage, variable_registry: VariableRegistry, statistics: &Statistics) -> Vec<SampledConjunctionPlan> {
    let AnnotatedStage::Match { block, block_annotations, executable_expressions, .. } = match_stage else { unreachable!("Only supports single match") };
    let plan = plan_conjunction(
        block.conjunction(),
        block.block_context(),
        &HashSet::new(),
        &block.variables().collect(),
        block_annotations,
        variable_registry,
        executable_expressions,
        statistics,
        &ExecutableFunctionRegistry::empty(),
    )
        .map_err(|source| MatchCompilationError::PlanningError { typedb_source: source })?
        .lower(
            &HashMap::new(),
            [].iter(),
            block.variables().copied(),
            &HashMap::new(),
            &variable_registry,
            None,
        )
        .map_err(|source| MatchCompilationError::PlanningError { typedb_source: source })?
        .finish(&variable_registry);

    trace!("Finished planning conjunction:\n{conjunction}");
    debug!("Lowered plan:\n{plan}");

}

#[test]
fn foo() {

}
