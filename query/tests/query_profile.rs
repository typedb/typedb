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
use itertools::Itertools;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StageProfile, SubstepProfile};
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

/// Counts and structure for the substeps under a [`PatternProfile`].
///
/// Leaf steps are summarized as a count rather than carried individually, because
/// the planner sometimes merges adjacent checks into a single step (so the leaf
/// count for a given query is not strictly stable). Nested patterns and nested
/// queries preserve their own structure recursively, and child vectors are kept
/// sorted by canonical shape so that planner-driven re-orderings don't make
/// otherwise-equal trees compare unequal.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PatternProfileShape {
    step_count: usize,
    nested_patterns: Vec<PatternProfileShape>,
    nested_queries: Vec<QueryProfileShape>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct StageProfileShape {
    pattern: Option<PatternProfileShape>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct QueryProfileShape {
    stages: Vec<StageProfileShape>,
}

impl QueryProfileShape {
    fn from_profile(profile: &QueryProfile) -> Self {
        let stage_profiles = profile.stage_profiles().read().unwrap();
        let stages = stage_profiles
            .iter()
            .sorted_by_key(|(id, _)| *id)
            .map(|(_, stage)| StageProfileShape::from_stage(stage))
            .collect();
        Self { stages }
    }
}

impl StageProfileShape {
    fn from_stage(stage: &StageProfile) -> Self {
        Self { pattern: stage.pattern_profile().map(|p| PatternProfileShape::from_pattern(&p)) }
    }
}

impl PatternProfileShape {
    fn from_pattern(pattern: &PatternProfile) -> Self {
        let mut step_count = 0;
        let mut nested_patterns = Vec::new();
        let mut nested_queries = Vec::new();
        for substep in pattern.substeps().read().unwrap().iter() {
            match substep {
                SubstepProfile::StepProfile(_) => step_count += 1,
                SubstepProfile::PatternProfile(p) => nested_patterns.push(Self::from_pattern(p)),
                SubstepProfile::QueryProfile { profile, .. } => {
                    nested_queries.push(QueryProfileShape::from_profile(profile))
                }
            }
        }
        // Canonicalize so callers can compare against an expected multiset of children.
        nested_patterns.sort();
        nested_queries.sort();
        Self { step_count, nested_patterns, nested_queries }
    }

    /// A pattern with `step_count` leaf step substeps and no nested patterns or queries —
    /// "leaf" in the structural-recursion sense, not in the "single step" sense.
    fn leaf(step_count: usize) -> Self {
        Self { step_count, nested_patterns: vec![], nested_queries: vec![] }
    }
}

#[test]
fn query_profile_tree_structure() {
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

    // Render via Display so structural-assertion failures can include the tree for
    // diagnosis (and so we transitively exercise that Display does not panic).
    let rendered = format!("{}", context.profile);

    // Structural assertions. We deliberately do NOT pin the exact leaf-step count of
    // the top-level match conjunction: the planner may merge several check steps into
    // a single intersection step depending on cost/heuristic ties. We *do* pin the
    // number and shape of nested patterns and queries, since those reflect the user-
    // visible match clauses (optional / negation / disjunction / function call).
    let actual = QueryProfileShape::from_profile(&context.profile);

    // 2 stages total: Match + Sort.
    // (`select`, `offset`, `limit` are stream modifiers and don't allocate StageProfiles.)
    assert_eq!(actual.stages.len(), 2, "expected exactly 2 stages, got {:#?}\n{}", actual.stages, rendered);

    let match_pattern = actual.stages[0].pattern.as_ref().expect("Match stage should have a populated pattern profile");
    let sort_pattern = actual.stages[1].pattern.as_ref().expect("Sort stage should have a populated pattern profile");

    // Match pattern: leaf-step count is planner-dependent but always >= 1
    // (the sorted iterator intersection on $x has $name).
    assert!(
        match_pattern.step_count >= 1,
        "match step_count = {}, expected at least 1\n{}",
        match_pattern.step_count,
        rendered
    );

    // Match pattern's three nested patterns are: Optional, Negation, and a Disjunction
    // with two single-step branches. Order is canonicalized in `from_pattern`, so we
    // assert against the sorted multiset directly.
    let mut expected_nested_patterns = vec![
        PatternProfileShape::leaf(2), // optional
        PatternProfileShape::leaf(2), // negation
        PatternProfileShape {
            step_count: 0,
            nested_patterns: vec![PatternProfileShape::leaf(1), PatternProfileShape::leaf(1)],
            nested_queries: vec![],
        }, // disjunction with two single-step branches
    ];
    expected_nested_patterns.sort();
    assert_eq!(match_pattern.nested_patterns, expected_nested_patterns, "match nested patterns mismatch\n{}", rendered);

    // Match pattern has exactly one nested query: the inlined `get_age` function call.
    // That sub-profile has one stage (the function's match) whose pattern has 2 leaf steps.
    let expected_fn_call =
        QueryProfileShape { stages: vec![StageProfileShape { pattern: Some(PatternProfileShape::leaf(2)) }] };
    assert_eq!(match_pattern.nested_queries, vec![expected_fn_call], "match nested queries mismatch\n{}", rendered);

    // Sort stage: a flat pattern with at least one leaf step ("Sort execution") and no
    // nested patterns or queries. Step count isn't pinned, in case the executor later
    // adds more bookkeeping steps to the Sort stage.
    assert!(sort_pattern.step_count >= 1, "sort step_count = {}\n{}", sort_pattern.step_count, rendered);
    assert!(sort_pattern.nested_patterns.is_empty(), "sort has nested patterns\n{}", rendered);
    assert!(sort_pattern.nested_queries.is_empty(), "sort has nested queries\n{}", rendered);
}
