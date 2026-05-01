/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::ExecutionInterrupt;
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::CommitProfile;
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn define_schema(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    function_manager: &FunctionManager,
) {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_manager = QueryManager::new(None);

    let query_str = r#"
    define
      attribute name value string;
      attribute age value integer;
      attribute nickname value string;
      relation friendship relates friend @card(0..);
      entity person owns name @card(0..),
                    owns age,
                    owns nickname @card(0..1),
                    plays friendship:friend @card(0..);
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, type_manager, thing_manager, function_manager, schema_query, query_str)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn insert_data(
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_string: &str,
) {
    let snapshot = storage.clone().open_snapshot_write();
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    let query = typeql::parse_query(query_string).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_write_pipeline(snapshot, type_manager, thing_manager, function_manager, &query, query_string)
        .unwrap();
    let (_iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let snapshot = Arc::into_inner(context.snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

#[test]
fn query_profile_display_does_not_panic() {
    // Enable TRACE-level tracing so that `QueryProfile::new(tracing::enabled!(Level::TRACE))`
    // in `prepare_read_pipeline` produces an enabled profile, otherwise the executor builds
    // a disabled profile and most of the IndentDisplay tree never gets exercised.
    init_logging();

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    define_schema(storage.clone(), type_manager.as_ref(), thing_manager.as_ref(), &function_manager);
    insert_data(
        storage.clone(),
        type_manager.as_ref(),
        thing_manager.clone(),
        &function_manager,
        r#"
        insert
          $x isa person, has age 10, has name "Alice", has nickname "Ally";
          $y isa person, has age 11, has name "Bob";
          $z isa person, has age 12, has name "Charlie";
          $p isa person, has age 13, has name "Dixie";
          $q isa person, has age 14, has name "Ellie";
          (friend: $x, friend: $y) isa friendship;
          (friend: $y, friend: $z) isa friendship;
          (friend: $z, friend: $p) isa friendship;
          (friend: $p, friend: $q) isa friendship;
    "#,
    );

    // A read pipeline exercising every nested-pattern variant the profile tree can hold,
    // plus several stage types beyond Match:
    //   - inlined function call  (`let $age in get_age(...)`)            -> SubstepProfile::QueryProfile
    //   - disjunction            (`{ ... } or { ... }`)                  -> SubstepProfile::PatternProfile
    //   - negation               (`not { ... }`)                         -> SubstepProfile::PatternProfile
    //   - optional               (`try { ... }`)                         -> SubstepProfile::PatternProfile
    //   - basic match steps      (isa, has, comparisons)                 -> SubstepProfile::StepProfile
    //   - sort / select / offset / limit pipeline stages                 -> additional StageProfile entries
    let query_str = r#"
        with
        fun get_age($p_arg: person) -> { age }:
        match
            $p_arg has age $age_return;
        return { $age_return };

        match
            $x isa person, has name $name;
            let $age in get_age($x);
            { $age >= 12; } or { $age <= 10; };
            not { $x has name "Charlie"; };
            try { $x has nickname $nick; };
        sort $name asc;
        select $x, $name, $age;
        offset 0;
        limit 10;
    "#;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let pipeline = QueryManager::new(Some(Arc::new(QueryCache::new())))
        .prepare_read_pipeline(snapshot, &type_manager, thing_manager.clone(), &function_manager, &query, query_str)
        .unwrap();

    let (iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    // Drain the iterator so the executor populates step counters before we render the profile.
    let _: Vec<_> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).into_iter().collect();

    assert!(context.profile.is_enabled(), "expected profile to be enabled under TRACE-level tracing");
    // The actual test: format the whole profile tree via Display (which delegates to
    // IndentDisplay::indent_fmt) and ensure it neither panics nor produces empty output.
    let rendered = format!("{}", context.profile);
    println!("{}", rendered);
    assert!(rendered.contains("Query profile"), "rendered profile missing root header: {}", rendered);
    assert!(rendered.contains("Stage"), "rendered profile missing any Stage entry: {}", rendered);
}
