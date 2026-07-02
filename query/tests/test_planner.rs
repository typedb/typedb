/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::executable::match_::planner::conjunction_executable::{ExecutionStep, IntersectionStep};
use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{type_cache::TypeCache, TypeManager},
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
};
use executor::pipeline::match_::MatchStageExecutor;
use executor::pipeline::stage::ReadStageIterator;
use executor::{
    pipeline::{
        pipeline::Pipeline,
        stage::{ExecutionContext, ReadPipelineStage, StageIterator},
    },
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use options::InternalQueryOptions;
use query::given_rows::GivenRowsSimple;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StepProfile, SubstepProfile};
use std::{collections::HashMap, sync::Arc};
use itertools::Itertools;
use compiler::executable::match_::instructions::ConstraintInstruction;
use compiler::executable::match_::instructions::thing::HasInstruction;
use storage::snapshot::ReadableSnapshot;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot},
    MVCCStorage,
};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: Arc<FunctionManager>,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

impl Context {
    fn refresh(&mut self) {
        let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
        statistics.may_synchronise(self.storage.as_ref()).unwrap();

        let definition_key_gen = self.type_manager.definition_key_generator();
        let vertex_gen = self.type_manager.type_vertex_generator();
        let cache = Arc::new(TypeCache::new(self.storage.clone(), self.storage.snapshot_watermark()).unwrap());
        let type_manager = Arc::new(TypeManager::new(definition_key_gen, vertex_gen, Some(cache)));

        let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(self.storage.clone()).unwrap());
        let thing_manager =
            Arc::new(ThingManager::new(thing_vertex_generator, type_manager.clone(), Arc::new(statistics)));

        self.type_manager = type_manager;
        self.thing_manager = thing_manager;
        self.query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    }
}

fn setup() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = Arc::new(FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None));
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn define_schema(context: &mut Context, query: &str) {
    let mut snapshot = context.storage.clone().open_snapshot_schema();
    let schema_query = typeql::parse_query(query).unwrap().into_structure().into_schema();
    context
        .query_manager
        .execute_schema(
            &mut snapshot,
            &context.type_manager,
            &context.thing_manager,
            &context.function_manager,
            schema_query,
            query,
            InternalQueryOptions::default(),
        )
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn commit_writes(context: &mut Context, queries: &[String]) {
    let mut snapshot = context.storage.clone().open_snapshot_write();
    for query in queries {
        let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context
            .query_manager
            .prepare_write_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                context.function_manager.clone(),
                &parsed_query,
                None::<GivenRowsSimple>,
                query,
                InternalQueryOptions::default(),
            )
            .unwrap();
        // into_rows_iterator executes eagerly
        let (_iterator, exec_context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        snapshot = Arc::into_inner(exec_context.snapshot).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn compile_read(
    context: &Context,
    query: &str,
) -> Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            context.function_manager.clone(),
            &parsed_query,
            None::<GivenRowsSimple>,
            query,
            InternalQueryOptions { force_query_profile: true },
        )
        .unwrap();
    pipeline
}

fn execute_read(
    pipeline: Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>>,
) -> (usize, Arc<QueryProfile>) {
    let (iterator, ExecutionContext { profile, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let rows = iterator.collect_owned().unwrap().len();
    (rows, profile)
}

// --- DataSpec: declarative test-data builder -------------------------------------------------

struct DataSpec {
    instances: Vec<InstanceSpec>,
    has: Vec<HasSpec>,
}

struct InstanceSpec {
    type_: &'static str,
    count: usize,
    /// Optionally each instance a `has` edge to a key attribute using integers `0..count`.
    key: Option<&'static str>,
}

// NOTE: only integer right now
type AttributeGenerator = Box<dyn Fn(usize) -> i64>;

fn sequential() -> AttributeGenerator {
    Box::new(|i| i as i64)
}

fn cyclic(modulus: usize) -> AttributeGenerator {
    Box::new(move |i| (i % modulus) as i64)
}

fn offset_unique(start: i64) -> AttributeGenerator {
    Box::new(move |i| i as i64 + start)
}

struct HasSpec {
    owner_type: &'static str,
    attr_type: &'static str,
    /// Per-owner cap on the number of has's produced
    count_each: usize,
    /// Total number of `has` edges to produce - given to owners round-robin.
    count_total: usize,
    /// Maps edge index `0..count_total` to an integer attribute value. Repeating values
    attribute_generator: AttributeGenerator,
}

fn load_data(context: &mut Context, spec: DataSpec) {
    let mut instance_counts: HashMap<&'static str, usize> = HashMap::new();
    let mut queries: Vec<String> = Vec::new();

    for InstanceSpec { type_, count, key } in &spec.instances {
        if *count == 0 {
            continue;
        }
        let mut q = String::from("insert\n");
        for i in 0..*count {
            // typeql disallows underscore-prefixed variables, so name with a letter prefix.
            match key {
                Some(key_label) => {
                    q.push_str(&format!("  $x_{type_}_{i} isa {type_}, has {key_label} {i};\n"));
                }
                None => {
                    q.push_str(&format!("  $x_{type_}_{i} isa {type_};\n"));
                }
            }
        }
        queries.push(q);
        *instance_counts.entry(*type_).or_insert(0) += *count;
    }

    for HasSpec { owner_type, attr_type, count_each, count_total, attribute_generator } in &spec.has {
        let owner_count = instance_counts.get(owner_type).copied().unwrap_or(0);
        assert!(owner_count > 0, "HasSpec references owner_type '{owner_type}' with no prior InstanceSpec inserts",);
        let max_edges = owner_count.saturating_mul(*count_each);
        assert!(
            *count_total <= max_edges,
            "HasSpec count_total={count_total} exceeds owner_count*count_each={max_edges} for type '{owner_type}'",
        );
        if *count_total == 0 {
            continue;
        }
        // HasSpec needs to address individual owners by key value, so the owner type
        // must have had a `key` set on its InstanceSpec.
        let key_label =
            spec.instances.iter().find(|i| i.type_ == *owner_type).and_then(|i| i.key).unwrap_or_else(|| {
                panic!(
                    "HasSpec owner_type '{owner_type}' has no InstanceSpec with a key; \
                     can't address individual owners without one"
                )
            });
        // Assign edges to owners round-robin: edge `e` goes to owner `e % owner_count`.
        let mut owner_edge_counts = vec![0usize; owner_count];
        for e in 0..*count_total {
            let owner_idx = e % owner_count;
            owner_edge_counts[owner_idx] += 1;
            assert!(
                owner_edge_counts[owner_idx] <= *count_each,
                "internal: round-robin exceeded count_each for owner {owner_idx}",
            );
            let value = attribute_generator(e);
            queries.push(format!(
                "match $o isa {owner_type}, has {key_label} {owner_idx}; \
                 insert $o has {attr_type} {value};"
            ));
        }
    }

    commit_writes(context, &queries);
}

// --- Helpers for inspecting QueryProfile ----------------------------------------------------

fn total_storage_ops(profile: &QueryProfile) -> (u64, u64) {
    let mut total_seeks = 0u64;
    let mut total_advances = 0u64;
    for (_id, stage) in profile.stage_profiles().read().unwrap().iter() {
        if let Some(pattern) = stage.pattern_profile() {
            visit_steps_in_pattern(&pattern, &mut |step| {
                if let Some(seek) = step.storage_counters().get_raw_seek() {
                    total_seeks += seek;
                }
                if let Some(adv) = step.storage_counters().get_raw_advance() {
                    total_advances += adv;
                }
            });
        }
    }
    (total_seeks, total_advances)
}

fn visit_steps_in_pattern(pattern: &PatternProfile, visit: &mut impl FnMut(&StepProfile)) {
    for substep in pattern.substeps().read().unwrap().iter() {
        match substep {
            SubstepProfile::StepProfile(step) => visit(step),
            SubstepProfile::PatternProfile(nested) => visit_steps_in_pattern(nested, visit),
            SubstepProfile::QueryProfile { profile, .. } => {
                for (_id, stage) in profile.stage_profiles().read().unwrap().iter() {
                    if let Some(nested_pattern) = stage.pattern_profile() {
                        visit_steps_in_pattern(&nested_pattern, visit);
                    }
                }
            }
        }
    }
}

fn get_intersection_steps(
    match_: &MatchStageExecutor<ReadStageIterator<ReadSnapshot<WALClient>>>,
) -> impl Iterator<Item = &IntersectionStep> + Clone + '_ {
    match_.executable().steps().into_iter().filter_map(|step| {
        if let ExecutionStep::Intersection(intersection) = step { Some(intersection) } else { None }
    })
}

fn instruction_count(step: &IntersectionStep) -> usize {
    step.instructions.iter().count()
}

// --- Two-owner schema & noise helpers -------------------------------------------------------

const OWNER_1: &str = "owner_1";
const OWNER_2: &str = "owner_2";
const KEY_1: &str = "key_1";
const KEY_2: &str = "key_2";
const JOIN_ATTR: &str = "join_attr";

fn define_two_owner_schema(context: &mut Context) {
    let schema = format!(
        "define \
          entity {OWNER_1} owns {KEY_1} @key, owns {JOIN_ATTR}; \
          entity {OWNER_2} owns {KEY_2} @key, owns {JOIN_ATTR}; \
          attribute {KEY_1}, value integer; \
          attribute {KEY_2}, value integer; \
          attribute {JOIN_ATTR}, value integer;"
    );
    define_schema(context, &schema);
}

fn two_owner_join_query() -> String {
    format!(
        "match \
         $e1 isa {OWNER_1}, has {JOIN_ATTR} $join; \
         $e2 isa {OWNER_2}, has {JOIN_ATTR} $join;"
    )
}

const NOISE_TYPES: &[&str] = &["noise_1", "noise_2", "noise_3", "noise_4", "noise_5"];
const NOISE_KEY: &str = "noise_id";

/// Schema shape for "noise" variants: two query entity types plus N noise entity
/// types, all owning the same `join_attr`. Noise types share a `noise_id` key so
/// load_data can address them via match-insert. We never query the noise types
/// directly — they exist to bloat the storage scan range of `Reverse[X has $join]`
/// for any owner type X, inflating merge's double-scan cost relative to sequential's
/// outer-then-bind-from cost.
fn define_two_owner_with_noise_schema(context: &mut Context, n_noise_types: usize) {
    assert!(n_noise_types <= NOISE_TYPES.len(), "only {} noise types declared in NOISE_TYPES", NOISE_TYPES.len());
    let mut schema = format!(
        "define \
          entity {OWNER_1} owns {KEY_1} @key, owns {JOIN_ATTR}; \
          entity {OWNER_2} owns {KEY_2} @key, owns {JOIN_ATTR}; \
          attribute {KEY_1}, value integer; \
          attribute {KEY_2}, value integer; \
          attribute {JOIN_ATTR}, value integer; \
          attribute {NOISE_KEY}, value integer; "
    );
    for t in &NOISE_TYPES[..n_noise_types] {
        schema.push_str(&format!("entity {t} owns {NOISE_KEY} @key, owns {JOIN_ATTR}; "));
    }
    define_schema(context, &schema);
}

/// Append InstanceSpec + HasSpec entries for noise owners to `spec`. Each noise
/// type gets `per_type` entities, each owning one `join_attr` value; values are
/// unique across all noise entities, starting at `value_start` and going up.
fn add_noise_owners(spec: &mut DataSpec, n_noise_types: usize, per_type: usize, value_start: i64) {
    let mut offset = value_start;
    for t in &NOISE_TYPES[..n_noise_types] {
        spec.instances.push(InstanceSpec { type_: t, count: per_type, key: Some(NOISE_KEY) });
        spec.has.push(HasSpec {
            owner_type: t,
            attr_type: JOIN_ATTR,
            count_each: 1,
            count_total: per_type,
            attribute_generator: offset_unique(offset),
        });
        offset += per_type as i64;
    }
}

// === Merge should win tests ====================================================================

/// Symmetric balanced full-coverage baseline. Both sides 500 owners, 1:1 with
/// values 0..499 → 500 output rows.
///
/// In this case, the advancing past an intersection immediately puts you at the next one
/// So seeking to the next intersection is replaced by advances
/// However, not clear how much the planner can know about this.
///
/// Empirical estimate (SEEK=5, ADVANCE=1):
/// - sequential chain ≈ 500 × (SEEK + 1 ADV) ≈ 3000
/// - 2-iter merge optimal   ≈ 2 × (SEEK + 500 ADV)  ≈ 1010
/// - 2-iter merge (if we can't tell the seeks don't actually need to happen) ≈ 500 × 2 x (SEEK + ADV)  ≈ 6000
///
/// Actual planner cost: 4045 (sequential)
///
/// Although in the optimal case Merge should win, this test locks in the sequential behaviour
///
/// TODO: fix bug where planner it not able to start with an intersection because both directions are not preserved as choices if direction that enables the intersection is more expensive
#[test]
fn merge_wins_symmetric_balanced() {
    const N: usize = 500;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, N, "symmetric_balanced: 1:1 full coverage → N rows");

    // seek count: 1 for the  outer iterator, 500 for each of the other iterator
    assert_eq!(seeks, 501);
    // advance count:
    //  Has iter: 1000 — the owner-type range walks every has edge: 500 × (1 join_attr + 1 key_1).
    //    The 1000th is the fail, since the first entry comes from the seek itself
    //  ReverseHas iter: one advance to fail per seek = 500
    assert_eq!(advances, 1500);

    println!("{}", profile);
}


/// Zipper test that should be optimal by seeking between matches to skip gaps
/// 500 matching values + 2 decoys per side per match.
/// Side A owners hold {match, match+1, match+2}; side B owners hold {match, match+3,
/// match+4}. Stride 5 between matches. 2 additional owners per side cause seek mismatches between matches.
///
/// Empirical estimate (SEEK=5, ADVANCE=1):
/// - sequential: 1 SEEK + ~1500 ADV + 1500 (SEEK + ADV) ~= 10505
/// - has merge: 500 x 2 SEEK + 500 x ~2 ADV ~= 6000
///
/// How beneficial the merge is vs sequential depends on actual cost of seek vs advance
/// and the size of gaps between intersections.
///
/// This test locks in the chosen sequential behaviour for the time being
///
/// TODO: fix bug where planner it not able to start with an intersection because both directions are not preserved as choices if direction that enables the intersection is more expensive
#[test]
fn merge_wins_true_zipper() {
    const N_OWNERS: usize = 500;
    const STRIDE: i64 = 5;
    const HAS_PER_OWNER: usize = 3; // 1 match + 2 decoys per side
    const HAS_PER_SIDE: usize = N_OWNERS * HAS_PER_OWNER;
    // Side A decoy offsets from match: {+1, +2}. Side B decoys: {+3, +4}.
    const A_DECOY_OFFSETS: &[i64] = &[1, 2];
    const B_DECOY_OFFSETS: &[i64] = &[3, 4];

    let zipper_gen = |decoy_offsets: &'static [i64]| -> AttributeGenerator {
        Box::new(move |e| {
            let owner_idx = (e % N_OWNERS) as i64;
            // round e: 0..N_OWNERS is the match has, N_OWNERS..2*N is first decoy, etc.
            let round = e / N_OWNERS;
            let offset = if round == 0 { 0 } else { decoy_offsets[round - 1] };
            owner_idx * STRIDE + offset
        })
    };

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: HAS_PER_OWNER,
                count_total: HAS_PER_SIDE,
                attribute_generator: zipper_gen(A_DECOY_OFFSETS),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: HAS_PER_OWNER,
                count_total: HAS_PER_SIDE,
                attribute_generator: zipper_gen(B_DECOY_OFFSETS),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, N_OWNERS, "true_zipper: 500 matching values × 1 × 1 = 500 rows");

    // seek count: 1 for the outer Has iterator, 1500 for the HasReverse iterator (one probe per scanned has edge)
    assert_eq!(seeks, 1501);
    // advance count:
    //  Has iter: 2000 — the owner-type range walks every has edge: 500 owners × (3 join_attr + 1 key_1).
    //    The last is the fail, since the first entry comes from the seek itself
    //  ReverseHas iter: one failing advance per successful probe = 500 (the edge itself comes from the
    //    seek); the 1000 miss probes detect the miss on the seek itself
    assert_eq!(advances, 2500);

    println!("{}", profile);
}

/// Moderate per-value fan-out. Both sides 500 owners cyclic over 50 distinct
/// values (10 owners per value per side). Output = 50 × 10 × 10 = 5000 rows
/// (cartesian within each value).
///
/// Empirical estimate (SEEK=5, ADVANCE=1) [note: cartesian currently uses re-seeking iterators at expense of memory]:
/// - sequential [Has, ReverseHas] = (1 SEEK + 500 ADV) + 500 (SEEK + 10 ADV) = 8505
/// - sequential [ReverseHas, ReverseHas] = <identical> = 8505
/// - 2-iter ReverseHas merge = 50 x 2 (SEEK + 10 [skip to next attr] ADV) + Cartesian = 50x(SEEK + 10 ADV + 10x(SEEK + 10 ADV)) = 10700
///
/// TODO: if we fully optimize cartesian to run over buffered skipped rows:
/// - 2-iter ReverseHas merge = 50 x 2 (SEEK + 10 [skip to next attr] ADV) = 2500
///
/// Actual planner cost: 8635 (sequential)
///
/// This test locks in the sequential behaviour
///
/// TODO: current cartesian is expensive to compute so this might be the right plan for now!
#[test]
fn merge_wins_cartesian() {
    const N_OWNERS: usize = 500;
    const DISTINCT_VALUES: usize = 50;
    const OWNERS_PER_VALUE: usize = N_OWNERS / DISTINCT_VALUES; // 10
    const EXPECTED_ROWS: usize = DISTINCT_VALUES * OWNERS_PER_VALUE * OWNERS_PER_VALUE; // 5000

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, EXPECTED_ROWS, "moderate_cartesian: 50 values × 10 × 10 = 5000 rows");

    // seek count: 1 for the outer Has iterator, 500 for the HasReverse iterator (one probe per scanned has edge)
    assert_eq!(seeks, 501);
    // advance count:
    //  Has iter: 1000 — the owner-type range walks every has edge: 500 × (1 join_attr + 1 key)
    //  ReverseHas iter: 10 per probe = 5000 — each probe walks its 10-owner cluster: the first owner
    //    comes from the seek, 9 advances walk the rest, and the 10th is the fail
    assert_eq!(advances, 6000);

    println!("{}", profile);
}

// --- Multi-attribute filter helpers (single-entity, K-pattern intersection) -----------------

const WIDGET: &str = "widget";
const WIDGET_KEY: &str = "widget_key";
const MULTI_ATTR_TYPES: &[&str] = &[
    "m_attr_0", "m_attr_1", "m_attr_2", "m_attr_3", "m_attr_4", "m_attr_5", "m_attr_6", "m_attr_7", "m_attr_8",
    "m_attr_9",
];

fn define_multi_attr_schema(context: &mut Context, k_attrs: usize) {
    assert!(k_attrs >= 2 && k_attrs <= MULTI_ATTR_TYPES.len());
    let mut schema = format!("define entity {WIDGET} owns {WIDGET_KEY} @key");
    for i in 0..k_attrs {
        schema.push_str(&format!(", owns {}", MULTI_ATTR_TYPES[i]));
    }
    schema.push_str(&format!("; attribute {WIDGET_KEY}, value integer;"));
    for i in 0..k_attrs {
        schema.push_str(&format!(" attribute {}, value integer;", MULTI_ATTR_TYPES[i]));
    }
    define_schema(context, &schema);
}

fn multi_attr_query(k_attrs: usize, fixed_value: i64) -> String {
    let mut q = format!("match $x isa {WIDGET}");
    for i in 0..k_attrs {
        q.push_str(&format!(", has {} {fixed_value}", MULTI_ATTR_TYPES[i]));
    }
    q.push(';');
    q
}

fn build_multi_attr_spec(n_owners: usize, k_attrs: usize, m_values: usize) -> DataSpec {
    assert!(k_attrs <= MULTI_ATTR_TYPES.len());
    let mut spec = DataSpec {
        instances: vec![InstanceSpec { type_: WIDGET, count: n_owners, key: Some(WIDGET_KEY) }],
        has: vec![],
    };
    for i in 0..k_attrs {
        // Base-M digit-position: widget j gets value (j / m^i) % m for attr_i.
        // This gives a uniform combinatorial assignment over (val_0, ..., val_{K-1}).
        let divisor: usize = m_values.pow(i as u32);
        let modulus = m_values;
        spec.has.push(HasSpec {
            owner_type: WIDGET,
            attr_type: MULTI_ATTR_TYPES[i],
            count_each: 1,
            count_total: n_owners,
            attribute_generator: Box::new(move |edge_idx| {
                // With count_total = n_owners and count_each = 1, edge_idx == owner_idx.
                ((edge_idx / divisor) % modulus) as i64
            }),
        });
    }
    spec
}

/// Known multi-attribute filter on a single entity. One entity owning K=3
/// integer attributes; query constrains all 3 to a fixed value:
///   `match $x isa widget, has attr_0 0, has attr_1 0, has attr_2 0;`
///
/// Data: N=1000 widgets, K=3 attrs × M=3 values each. Values are assigned by
/// base-M digit positions, so each combination appears N/M^K = ~37 times.
///
/// Merge can co-walk all K owner-sorted iters in a single pass on $x
/// Sequential would have to drive from one filter and bind-from probe each of the other patterns
///
/// Empirical estimate (SEEK=5, ADVANCE=1):
/// - sequential HasReverse cascade ≈ 1 SEEK + 333 ADV + 333 x (SEEK + ADV) + 111 x (SEEK + ADV) ≈ 3000
/// - K=3-iter HasReverse merge     ≈ 3 × (between 37 and 111 tested candidates) (SEEK + ADV) ≈ 1500
#[test]
fn merge_wins_multi_attr_filter() {
    const N: usize = 1_000;
    const K: usize = 3;
    const M: usize = 3;
    const EXPECTED_ROWS: usize = N / (M * M * M); // 1000 / 27 = 37

    let mut context = setup();
    define_multi_attr_schema(&mut context, K);
    load_data(&mut context, build_multi_attr_spec(N, K, M));

    let pipeline = compile_read(&context, &multi_attr_query(K, 0));

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    // three IsaReverse steps resolve the literal-valued attribute instances, then one
    // K-way merge intersection on $x — the planner picks merge correctly here
    assert_eq!(steps.len(), K + 1);
    for i in 0..K {
        assert_eq!(instruction_count(steps[i]), 1);
        assert!(matches!(steps[i].instructions[0].0, ConstraintInstruction::IsaReverse(_)));
    }
    assert_eq!(instruction_count(steps[K]), K);
    for i in 0..K {
        assert!(matches!(steps[K].instructions[i].0, ConstraintInstruction::HasReverse(_)));
    }

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    // Each base-M combination appears floor(N / M^K) or ceil(N / M^K) times due to
    // integer arithmetic in the digit-position generator. Allow ±a few rows of slack.
    let lo = EXPECTED_ROWS.saturating_sub(2);
    let hi = EXPECTED_ROWS + 2;
    assert!(
        (lo..=hi).contains(&rows),
        "multi_attr_filter: expected ~{EXPECTED_ROWS} rows (= N / M^K with N={N}, M={M}, K={K}); \
         got {rows}",
    );

    // seek count: each IsaReverse lookup costs 2 (type-prefix open + seek to the literal value);
    //  the merge opens its 3 iterators then pays 222 zipper catch-up seeks between intersections
    assert_eq!(seeks, 231);
    // advance count:
    //  IsaReverse lookups: 1 failing advance each (the attribute instance itself comes from the seek)
    //  merge: 336 — advancing past intersections plus walking entries the catch-up seeks don't skip
    //   (each iter holds ~333 owner-sorted entries in runs of 3^i consecutive owners)
    assert_eq!(advances, 339);

    println!("{}", profile);
}

// === Sequential should win tests ===============================================================

/// Extreme cardinality asymmetry. owner_1: 1 owner with value 0; owner_2:
/// 2000 owners with unique values 0..1999 → 1 output row.
///
/// The best plan here is a sequential join.
/// A good merge would have to be a Has from owner_1 (bound) to attr with unbound ReverseHas of owner_2?

///
/// Empirical estimate (SEEK=5, ADVANCE=1):
/// - sequential chain ≈ 1 SEEK + ADV + 1 × (SEEK + 1 ADV) ≈ 12
/// - merge:
///   -> ISA to get owner_1's -> 1 SEEK + ADV = 6
///   -> merge, Has (bound) with ReverseHas (unbound) = 1 SEEK + ADV + 1 SEEK + 1 ADV =  = 12
///   Total ~= 18?
#[test]
fn sequential_wins_tiny_outer_huge_inner() {
    const N_INNER: usize = 2000;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: 1, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: 1,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_INNER,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection: scan the 1-owner outer side
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection: bound probe into the 2000-owner inner side
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, 1, "tiny_outer_huge_inner: 1 outer × 1 matching inner = 1 row");

    // seek count: 1 for the outer Has iterator, 1 for the single HasReverse probe
    assert_eq!(seeks, 2);
    // advance count:
    //  Has iter: 2 — the single owner's has edges: 1 join_attr + 1 key; the last is the fail,
    //    since the first entry comes from the seek itself
    //  ReverseHas iter: one failing advance for the single probe (the edge itself comes from the seek)
    assert_eq!(advances, 3);

    println!("{}", profile);
}

/// Small outer, huge inner. owner_1: 100 owners with values 0..99 (full
/// coverage of own range); owner_2: 2000 owners with unique values 0..1999
/// (outer covers 5% of inner's value domain). Output: 100 rows.
///
/// In a good sequential plan, the small outer should drive bound probes into inner
/// A good merge plan is two reverse Has merge? -> this is not split by owner type, so we'd
///   be doing worst case 100 seeks to find intersections,
///   and at 1900 advances to skip unwanted values for owner_1's reverse iterator tail (we can't stop
///   early, since the ranges are reverse ranges and we don't know when to end - a owner_1 could exist at the end!)
///
/// Empirical estimate (SEEK=5, ADVANCE=1):
/// - sequential chain ≈ 1 SEEK + 100 ADV + 100 × (SEEK + 1 ADV) ≈ ~700
/// - 2-iter merge    ≈ 100 SEEK + 2000 ≈ ~2500
///
/// Actual planner cost: 813 (sequential).
#[test]
fn sequential_wins_subset_coverage() {
    const N_OUTER: usize = 100;
    const N_INNER: usize = 2000;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OUTER, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_OUTER,
                attribute_generator: sequential(), // values 0..99
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_INNER,
                attribute_generator: sequential(), // values 0..1999, outer ⊂ inner
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection: scan the 100-owner outer side
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection: bound probe into the 2000-owner inner side
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, N_OUTER, "subset_coverage: outer ⊂ inner → N_OUTER rows");

    // seek count: 1 for the outer Has iterator, 100 for the HasReverse iterator (one probe per outer edge)
    assert_eq!(seeks, 101);
    // advance count:
    //  Has iter: 200 — the owner-type range walks every has edge: 100 × (1 join_attr + 1 key)
    //  ReverseHas iter: one failing advance per probe = 100 (all probes hit, the edge comes from the seek)
    assert_eq!(advances, 300);

    println!("{}", profile);
}

/// Noise inflates the storage scan range for any `Reverse[X has $join]` iter,
/// so merge pays for the bloated range twice (once per side) while sequential
/// pays once on the outer scan plus tight bound-from probes on the inner.
/// owner_1: 100 owners values 0..99; owner_2: 100 owners values 0..99 (full
/// overlap with outer); plus 2000 noise entries with values 100..2099
/// inflating the value range. Output: 100 rows. Sequential should win.
///
/// Cost-model estimate (noise puts the effective per-side scan at ~2100):
/// - sequential chain ≈ 2100 outer + 100 × (SEEK + 1 ADV) ≈ ~2700
/// - 2-iter merge    ≈ 2 × 2100 (both iters walk the bloated range) ≈ ~4200
/// Sequential expected to win by ~1.5×.
/// Actual planner cost: 813 (sequential — planner picks correctly;
/// model is cheaper than the rough estimate because per-type scan range is type-
/// scoped and doesn't include cross-type noise).
#[test]
fn sequential_wins_noisy_inner() {
    const N_QUERY: usize = 100;
    const N_NOISE_TYPES: usize = 2;
    const N_NOISE_PER_TYPE: usize = 1000;

    let mut context = setup();
    define_two_owner_with_noise_schema(&mut context, N_NOISE_TYPES);

    let mut spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_QUERY, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_QUERY, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_QUERY,
                attribute_generator: sequential(), // values 0..99
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_QUERY,
                attribute_generator: sequential(), // values 0..99 (full overlap with owner_1)
            },
        ],
    };
    // Noise values 100..2099 — disjoint from query values, but bloat the scan range
    // of any reverse[has $join] iterator.
    add_noise_owners(&mut spec, N_NOISE_TYPES, N_NOISE_PER_TYPE, N_QUERY as i64);
    load_data(&mut context, spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    let stage = match pipeline.stages().iter().next().unwrap() {
        ReadPipelineStage::Match(stage) => stage,
        _ => unreachable!(),
    };
    let steps = get_intersection_steps(stage).collect_vec();
    assert_eq!(steps.len(), 2);
    // first intersection
    assert_eq!(instruction_count(steps[0]), 1);
    assert!(matches!(steps[0].instructions[0].0, ConstraintInstruction::Has(_)));
    // second intersection
    assert_eq!(instruction_count(steps[1]), 1);
    assert!(matches!(steps[1].instructions[0].0, ConstraintInstruction::HasReverse(_)));

    let (rows, profile) = execute_read(pipeline);
    let (seeks, advances) = total_storage_ops(&profile);
    println!("storage: seeks={seeks} advances={advances} weighted={}", seeks * 5 + advances);
    assert_eq!(rows, N_QUERY, "noisy_inner: query sides fully overlap → N_QUERY rows");

    // seek count: 1 for the outer Has iterator, 100 for the HasReverse iterator (one probe per outer edge).
    // The noise types don't intrude: both scans are type-scoped, so the bloated attribute range is never walked.
    assert_eq!(seeks, 101);
    // advance count:
    //  Has iter: 200 — the owner-type range walks every has edge: 100 × (1 join_attr + 1 key)
    //  ReverseHas iter: one failing advance per probe = 100 (full overlap, all probes hit;
    //    the edge itself comes from the seek)
    assert_eq!(advances, 300);

    println!("{}", profile);
}
