/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// Repro for the string-equality lookup regression introduced by the #7878 (issue #7869) fix:
// an `==` on a string longer than 16 bytes must not scan every attribute sharing the first 8 bytes.

use std::{fmt::Write, sync::Arc};

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
};
use executor::ExecutionInterrupt;
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{given_rows::GivenRowsSimple, query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, SubstepProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const N: usize = 1000;
const EXTRA_ON_OWNER_0: usize = 300; // person 0 additionally owns this many long names sharing the 8-byte prefix
const LONG_PREFIX: &str = "https://example.com/item/"; // > 16 bytes: hashed IDs, all share first 8 bytes
const SHORT_PREFIX: &str = "n-"; // "n-00500" = 7 bytes: inline IDs

fn long_name(i: usize) -> String {
    format!("{LONG_PREFIX}{i:05}")
}
fn short_name(i: usize) -> String {
    format!("{SHORT_PREFIX}{i:05}")
}

fn define_schema(storage: Arc<MVCCStorage<WALClient>>, tm: &TypeManager, thm: &ThingManager, fm: &FunctionManager) {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_str = r#"
    define
      attribute name value string;
      attribute id value integer;
      entity person owns name @card(0..), owns id @key;
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_structure().into_schema();
    QueryManager::new(None).execute_schema(&mut snapshot, tm, thm, fm, &schema_query, query_str).unwrap();
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

fn insert(storage: Arc<MVCCStorage<WALClient>>, tm: &TypeManager, thm: Arc<ThingManager>, fm: Arc<FunctionManager>) {
    let mut q = String::from("insert\n");
    for i in 0..N {
        writeln!(q, r#"$p{i} isa person, has id {i}, has name "{}", has name "{}";"#, long_name(i), short_name(i))
            .unwrap();
    }
    for j in 0..EXTRA_ON_OWNER_0 {
        writeln!(q, r#"$p0 has name "{LONG_PREFIX}extra/{j:05}";"#).unwrap();
    }
    let snapshot = storage.clone().open_snapshot_write();
    let query = typeql::parse_query(&q).unwrap().into_structure().into_pipeline();
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_write_pipeline(snapshot, tm, thm, fm, &query, None::<GivenRowsSimple>, &q)
        .unwrap();
    let (_iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let snapshot = Arc::into_inner(context.snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

struct Outcome {
    rows: usize,
    seeks: u64,
    advances: u64,
    rendered: String,
}

fn sum_counters(pattern: &PatternProfile, seeks: &mut u64, advances: &mut u64) {
    for substep in pattern.substeps().read().unwrap().iter() {
        match substep {
            SubstepProfile::StepProfile(step) => {
                let c = step.storage_counters();
                *seeks += c.get_raw_seek().unwrap_or(0);
                *advances += c.get_raw_advance().unwrap_or(0);
            }
            SubstepProfile::PatternProfile(p) => sum_counters(p, seeks, advances),
            SubstepProfile::QueryProfile { .. } => {}
        }
    }
}

fn run(
    storage: Arc<MVCCStorage<WALClient>>,
    tm: &TypeManager,
    thm: Arc<ThingManager>,
    fm: Arc<FunctionManager>,
    query_str: &str,
) -> Outcome {
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(storage.open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_read_pipeline(snapshot, tm, thm, fm, &query, None::<GivenRowsSimple>, query_str)
        .unwrap();
    let (iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let rows = iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).count();
    assert!(context.profile.is_enabled(), "profile must be enabled (TRACE logging)");
    let (mut seeks, mut advances) = (0, 0);
    for (_, stage) in context.profile.stage_profiles().read().unwrap().iter() {
        if let Some(pattern) = stage.pattern_profile() {
            sum_counters(&pattern, &mut seeks, &mut advances);
        }
    }
    Outcome { rows, seeks, advances, rendered: format!("{}", context.profile) }
}

#[test]
fn string_equality_lookups() {
    init_logging();
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (tm, thm) = load_managers(storage.clone(), None);
    let fm = Arc::new(FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None));
    define_schema(storage.clone(), &tm, &thm, &fm);
    insert(storage.clone(), &tm, thm.clone(), fm.clone());
    // rebuild the thing manager with statistics synchronised to the inserted data, so the planner
    // sees real counts (as a running server would) rather than costing every step identically
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise(storage.as_ref()).unwrap();
    let thm = Arc::new(ThingManager::new(
        Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap()),
        tm.clone(),
        Arc::new(statistics),
    ));

    let l500 = long_name(500);
    let s500 = short_name(500);
    let l900 = long_name(900);
    let l100 = long_name(100);
    let l0 = long_name(0);

    // (label, query, expected rows, max advances allowed for a point-ish lookup)
    let cases: Vec<(&str, String, usize, Option<u64>)> = vec![
        ("EQ isa long", format!(r#"match $n isa name; $n == "{l500}";"#), 1, Some(10)),
        ("EQ has-reverse long", format!(r#"match $p isa person, has name $n; $n == "{l500}";"#), 1, Some(10)),
        ("EQ has literal long", format!(r#"match $p isa person, has name "{l500}";"#), 1, Some(10)),
        ("EQ bound-owner long", format!(r#"match $p isa person, has id 500; $p has name $n; $n == "{l500}";"#), 1, Some(10)),
        ("EQ bound-owner0 long", format!(r#"match $p isa person, has id 0; $p has name $n; $n == "{l0}";"#), 1, Some(10)),
        ("EQ bound-owner0 literal", format!(r#"match $p isa person, has id 0; $p has name "{l0}";"#), 1, Some(10)),
        ("EQ isa short", format!(r#"match $n isa name; $n == "{s500}";"#), 1, Some(10)),
        ("EQ has literal short", format!(r#"match $p isa person, has name "{s500}";"#), 1, Some(10)),
        ("EQ bound-owner short", format!(r#"match $p isa person, has id 500; $p has name $n; $n == "{s500}";"#), 1, Some(10)),
        // ordering correctness (issue #7869 must stay fixed). Note "n-..." sorts after "https://...", and the 300 "extra/" names sort above l900, hence 1399 / 2.
        ("GT isa long", format!(r#"match $n isa name; $n > "{l900}";"#), 1399, None),
        ("GT has-reverse long", format!(r#"match $p isa person, has name $n; $n > "{l900}";"#), 1399, None),
        ("LT isa long", format!(r#"match $n isa name; $n < "{l100}";"#), 100, None),
        ("LT has-reverse long", format!(r#"match $p isa person, has name $n; $n < "{l100}";"#), 100, None),
        ("GE bound-owner long", format!(r#"match $p isa person, has id 500; $p has name $n; $n >= "{l500}";"#), 2, None),
        ("GT/LE has-reverse long", format!(r#"match $p isa person, has name $n; $n > "{l100}"; $n <= "{l900}";"#), 800, None),
    ];

    let mut failures = Vec::new();
    println!("\n{:<26} {:>6} {:>8} {:>10}", "case", "rows", "seeks", "advances");
    for (label, query, expected_rows, max_advances) in &cases {
        let out = run(storage.clone(), &tm, thm.clone(), fm.clone(), query);
        println!("{:<26} {:>6} {:>8} {:>10}", label, out.rows, out.seeks, out.advances);
        if std::env::var("PRINT_PROFILE").is_ok() {
            println!("{}\n{}", query, out.rendered);
        }
        if out.rows != *expected_rows {
            failures.push(format!("{label}: expected {expected_rows} rows, got {} \n{}", out.rows, out.rendered));
        }
        if let Some(max) = max_advances {
            if out.advances > *max {
                failures.push(format!("{label}: {} advances (limit {max}) \n{}", out.advances, out.rendered));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}
