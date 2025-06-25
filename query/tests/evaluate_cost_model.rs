/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::broadcast;
use compiler::annotation::function::AnnotatedSchemaFunctions;
use compiler::annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline, AnnotatedStage};
use compiler::executable::function::ExecutableFunctionRegistry;
use compiler::executable::match_::planner::match_executable::MatchExecutable;
use compiler::executable::match_::planner::plan::test::get_multiple_plans_for_simple_conjunction_with;
use compiler::executable::match_::planner::vertex::Cost;
use compiler::executable::match_::planner::vertex::test::cost_of;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::{ExecutionInterrupt, InterruptType};
use executor::pipeline::initial::InitialStage;
use executor::pipeline::match_::MatchStageExecutor;
use executor::pipeline::stage::{ExecutionContext, ReadPipelineStage, StageAPI};
use function::function_manager::FunctionManager;
use ir::pipeline::function_signature::HashMapFunctionSignatureIndex;
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use ir::translation::pipeline::{translate_pipeline, TranslatedPipeline};
use lending_iterator::LendingIterator;
use query::query_cache::QueryCache;
use query::query_manager::QueryManager;
use resource::profile::{CommitProfile, QueryProfile, StageProfile};
use resource::profile::test::StageProfileSummary;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, ReadableSnapshot};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;


struct TestContext { // TODO: I Use this everywhere. I should pull it into test_utils now.
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

fn setup_common(schema: &str) -> TestContext {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    TestContext { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn run_plan(
    test_context: &TestContext,
    parameters: Arc<ParameterRegistry>,
    executable: Arc<MatchExecutable>,
    // expected_answer_count: usize,
) -> QueryProfile {
    let query_profile = Arc::new(QueryProfile::new(true));
    let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);
    let interrupt = ExecutionInterrupt::new(query_interrupt_receiver);
    let finished = Arc::new(AtomicBool::new(false));
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5000));
        if !finished.load(Ordering::SeqCst) {
            match query_interrupt_sender.send(InterruptType::TransactionClosed) {
                Ok(_) => {}, // oops, we timed out
                Err(_) => {}, // great, the request finished already
            }
        }
    });
    let snapshot = Arc::new(test_context.storage.clone().open_snapshot_read());
    let context = ExecutionContext::new_with_profile(snapshot, test_context.thing_manager.clone(), parameters.clone(), query_profile);
    let initial_stage =ReadPipelineStage::Initial(Box::new(InitialStage::new_empty(context)));
    let executor = MatchStageExecutor::new(executable.clone(), initial_stage, Arc::new(ExecutableFunctionRegistry::empty()));
    let (iterator, context) = executor.into_iterator(interrupt).map_err(|(boxed_err,_)| boxed_err).unwrap();
    let answer_count = iterator.count();
    // assert_eq!(answer_count, expected_answer_count);
    Arc::into_inner(context.profile).unwrap()
}

fn translate_and_annotate(context: &TestContext, query: &str) -> (AnnotatedPipeline, VariableRegistry, ParameterRegistry) {
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let snapshot = context.storage.clone().open_snapshot_read();
    let mut translated = translate_pipeline(&snapshot, &HashMapFunctionSignatureIndex::empty(), &pipeline).unwrap();
    assert_eq!(translated.translated_stages.len(), 1);
    debug_assert!(translated.translated_preamble.is_empty() && translated.translated_fetch.is_none() && translated.translated_stages.len() == 1);
    let TranslatedPipeline { translated_stages, mut variable_registry, value_parameters, .. } = translated;
    let annotated = annotate_preamble_and_pipeline(
        &snapshot,
        &context.type_manager,
        Arc::new(AnnotatedSchemaFunctions::new()),
        &mut variable_registry,
        &value_parameters,
        Vec::new(),
        translated_stages,
        None,
    ).unwrap();
    (annotated, variable_registry, value_parameters)
}

struct SampledConjunctionPlan {
    executable: MatchExecutable,
    cost: Cost
}

fn sample_plans(match_stage: &AnnotatedStage, variable_registry: &VariableRegistry, statistics: &Statistics) -> Vec<SampledConjunctionPlan> {
    let AnnotatedStage::Match { block, block_annotations, executable_expressions, .. } = match_stage else { unreachable!("Only supports single match") };
    get_multiple_plans_for_simple_conjunction_with(
        block, block_annotations, variable_registry, executable_expressions, statistics
    ).unwrap().into_iter().map(|(executable, cost)| {
        SampledConjunctionPlan { executable, cost }
    }).collect()
}

fn derive_cost_from_profile_summary(summary: StageProfileSummary) -> Cost {
    cost_of(summary.raw_seek, summary.raw_advance, summary.last_step_rows as f64)
}

#[test]
fn foo() {
    let q = "match $x owns $y;";
    let context = setup_common("define entity person; attribute name value string; person owns name;");
    let (annotated, variable_registry, parameters) = translate_and_annotate(&context, q);
    let parameters = Arc::new(parameters);
    let plans = sample_plans(&annotated.annotated_stages[0], &variable_registry, context.thing_manager.statistics());
    let mut costs_for_comparison = Vec::new();
    for SampledConjunctionPlan { executable, cost } in plans {
        let executable_id = executable.executable_id();
        let profile = run_plan(&context, parameters.clone(), Arc::new(executable));
        let guard = profile.stage_profiles().read().unwrap();
        let stage_profile = guard.get(&executable_id).unwrap();
        let cost_from_profile = derive_cost_from_profile_summary(StageProfileSummary::from(stage_profile));
        costs_for_comparison.push((cost, cost_from_profile));
    }
    print_costs_for_comparison(&costs_for_comparison);
}

fn print_costs_for_comparison(costs_for_comparison: &Vec<(Cost, Cost)>) {
    for (plan_cost, execution_cost) in costs_for_comparison {
        println!("{:.3}\t|{:.3}", plan_cost.cost, execution_cost.cost);
    }
}
