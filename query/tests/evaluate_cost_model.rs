/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::broadcast;
use compiler::annotation::function::AnnotatedSchemaFunctions;
use compiler::annotation::pipeline::{annotate_preamble_and_pipeline, AnnotatedPipeline, AnnotatedStage};
use compiler::executable::function::ExecutableFunctionRegistry;
use compiler::executable::match_::planner::match_executable::MatchExecutable;
use compiler::executable::match_::planner::plan::test::{get_multiple_plans_for_simple_conjunction_with, SampledConjunctionPlan};
use compiler::executable::match_::planner::vertex::Cost;
use compiler::executable::match_::planner::vertex::test::cost_of;
use database::Database;
use database::transaction::TransactionRead;
use executor::{ExecutionInterrupt, InterruptType};
use executor::pipeline::initial::InitialStage;
use executor::pipeline::match_::MatchStageExecutor;
use executor::pipeline::stage::{ExecutionContext, ReadPipelineStage, StageAPI};
use ir::pipeline::function_signature::HashMapFunctionSignatureIndex;
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use ir::translation::pipeline::{translate_pipeline, TranslatedPipeline};
use lending_iterator::LendingIterator;
use options::TransactionOptions;
use resource::profile::{QueryProfile};
use resource::profile::test::{StorageCounterCopy};
use storage::durability_client::WALClient;
use storage::snapshot::{ReadableSnapshot};

const MAX_COST_TO_EVALUATE: f64 = 1e6;

fn open_database(path: &PathBuf) -> Database<WALClient> {
    assert!(std::fs::exists(path).unwrap() && std::fs::exists(&path.join("wal")).unwrap() && std::fs::exists(&path.join("storage")).unwrap());
    Database::open(path).unwrap()
}

fn run_plan(
    database: Arc<Database<WALClient>>,
    parameters: Arc<ParameterRegistry>,
    executable: Arc<MatchExecutable>,
    // expected_answer_count: usize,
) -> QueryProfile {
    const CHECK_FINISHED_EVERY: Duration = Duration::from_millis(100);
    const INTERRUPT_AFTER: Duration = Duration::from_millis(5000);
    let total_checks = INTERRUPT_AFTER.div_duration_f64(CHECK_FINISHED_EVERY).round() as usize;
    let query_profile = Arc::new(QueryProfile::new(true));
    let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);
    let interrupt = ExecutionInterrupt::new(query_interrupt_receiver);
    let finished = Arc::new(AtomicBool::new(false));
    let finished_for_the_arc = finished.clone();
    let tx = TransactionRead::open(database, TransactionOptions::default()).unwrap();
    let handle = thread::spawn(move || {
        for _ in 0..total_checks {
            thread::sleep(CHECK_FINISHED_EVERY);
            if finished_for_the_arc.load(Ordering::SeqCst) {
                break;
            }
        }
        if !finished_for_the_arc.load(Ordering::SeqCst) {
            println!("Sending interrupt!");
            match query_interrupt_sender.send(InterruptType::TransactionClosed) {
                Ok(_) => {}, // oops, we timed out
                Err(_) => {}, // great, the request finished already
            }
        }
    });
    let context = ExecutionContext::new_with_profile(tx.snapshot.clone_inner(), tx.thing_manager.clone(), parameters.clone(), query_profile);
    let initial_stage =ReadPipelineStage::Initial(Box::new(InitialStage::new_empty(context)));
    let executor = MatchStageExecutor::new(executable.clone(), initial_stage, Arc::new(ExecutableFunctionRegistry::empty()));
    let (iterator, context) = executor.into_iterator(interrupt).map_err(|(boxed_err,_)| boxed_err).unwrap();
    let _answer_count = iterator.count();
    // assert_eq!(_answer_count, expected_answer_count);
    finished.store(true, Ordering::Relaxed);
    handle.join().unwrap();
    Arc::into_inner(context.profile).unwrap()
}

fn translate_and_annotate(database: Arc<Database<WALClient>>, query: &str) -> (AnnotatedPipeline, VariableRegistry, ParameterRegistry) {
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let tx = TransactionRead::open(database, TransactionOptions::default()).unwrap();
    let mut translated = translate_pipeline(tx.snapshot.as_ref(), &HashMapFunctionSignatureIndex::empty(), &pipeline).unwrap();
    assert_eq!(translated.translated_stages.len(), 1);
    debug_assert!(translated.translated_preamble.is_empty() && translated.translated_fetch.is_none() && translated.translated_stages.len() == 1);
    let TranslatedPipeline { translated_stages, mut variable_registry, value_parameters, .. } = translated;
    let annotated = annotate_preamble_and_pipeline(
        tx.snapshot.as_ref(),
        tx.type_manager.as_ref(),
        Arc::new(AnnotatedSchemaFunctions::new()),
        &mut variable_registry,
        &value_parameters,
        Vec::new(),
        translated_stages,
        None,
    ).unwrap();
    (annotated, variable_registry, value_parameters)
}


fn sample_plans(database: Arc<Database<WALClient>>, match_stage: &AnnotatedStage, variable_registry: &VariableRegistry, n_plans: usize) -> Vec<SampledConjunctionPlan> {
    let AnnotatedStage::Match { block, block_annotations, executable_expressions, .. } = match_stage else { unreachable!("Only supports single match") };
    let tx = TransactionRead::open(database, TransactionOptions::default()).unwrap();
    get_multiple_plans_for_simple_conjunction_with(
        block, block_annotations, variable_registry, executable_expressions, tx.thing_manager.statistics(), n_plans
    ).unwrap().into_iter().filter(|plan| plan.total_cost.cost < MAX_COST_TO_EVALUATE)
    .collect()
}

struct CostComparison {
    plan_cumulative_cost: Cost,
    plan_per_step_estimated_cost: Vec<Cost>, // zip with steps in profile
    executed_per_step_counters: Vec<StorageCounterCopy>,
}

#[test]
fn foo() {
    let q = "match $d isa DISTRICT, has D_ID 11; $o links (customer: $c, district: $d), isa ORDER, has O_ID $o_id, has O_NEW_ORDER true; $c isa CUSTOMER, has C_ID $c_id;";
    let database = Arc::new(open_database(&PathBuf::from("../target/debug/data/tpcc")));
    let n_plans = 10;
    let (annotated, variable_registry, parameters) = translate_and_annotate(database.clone(), q);
    let parameters = Arc::new(parameters);
    let plans = sample_plans(database.clone(), &annotated.annotated_stages[0], &variable_registry, n_plans);
    let actual_n_plans = plans.len();
    println!("Sampled {} plans (desired: {}). Executing...", plans.len(), actual_n_plans);
    let mut costs_for_comparison = Vec::new();
    for (i, SampledConjunctionPlan { executable, total_cost, per_step_estimated_cost }) in plans.into_iter().enumerate() {
        eprintln!("{:#?}", &executable.steps());
        let executable_id = executable.executable_id();
        let profile = run_plan(database.clone(), parameters.clone(), Arc::new(executable));
        let guard = profile.stage_profiles().read().unwrap();
        let stage_profile = guard.get(&executable_id).unwrap().clone();
        let executed_per_step_counters = StorageCounterCopy::from(&stage_profile);
        let cost_comparison = CostComparison {
            plan_cumulative_cost: total_cost,
            plan_per_step_estimated_cost: per_step_estimated_cost,
            executed_per_step_counters,
        };
        costs_for_comparison.push(cost_comparison);
        println!("Completed {}/{}", i+1, actual_n_plans);
    }
    print_costs_for_comparison(&costs_for_comparison);
}

fn print_costs_for_comparison(costs_for_comparison: &Vec<CostComparison>) {
    println!("Plan\t|Executed");
    for cost_comparison in costs_for_comparison {
        let CostComparison { plan_cumulative_cost, executed_per_step_counters, .. } = &cost_comparison;
        let total_cost_from_profile = executed_per_step_counters.iter().fold(StorageCounterCopy::default(), |a,b| a.add(b));
        let last_stage_rows = executed_per_step_counters.last().unwrap().rows;
        let cost_from_execution = cost_of(total_cost_from_profile.raw_seek, total_cost_from_profile.raw_advance, last_stage_rows as f64);
        println!("{:.3}\t|{:.3}", plan_cumulative_cost.cost, cost_from_execution.cost);
    }
}
