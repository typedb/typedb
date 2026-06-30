/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, RwLock},
};

use answer::Type;
use compiler::executable::pipeline::ExecutablePipeline;
use concept::thing::statistics::Statistics;
use ir::{
    pipeline::{fetch::FetchObject, function::Function},
    translation::pipeline::{TranslatedGiven, TranslatedPipeline, TranslatedStage},
};
use moka::sync::{Cache, CacheBuilder};
use resource::{
    constants::database::{
        QUERY_PARSE_CACHE_SIZE, QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION, QUERY_PLAN_CACHE_SIZE,
        QUERY_TRANSLATION_CACHE_SIZE,
    },
    perf_counters::QUERY_CACHE_FLUSH,
};
use storage::sequence_number::SequenceNumber;
use structural_equality::StructuralEquality;
use typeql::query::{Pipeline, SchemaQuery};

#[derive(Debug)]
struct ValidityRequirements {
    latest_statistics: Option<Arc<Statistics>>,
    latest_schema_commit: Option<SequenceNumber>,
}

/// Three-stage query front-end cache, each stage keyed by the raw query string:
/// - `parse_cache`: string -> parsed data pipeline. Purely syntactic, so it never needs
///   invalidation. Only data pipelines are cached; schema queries (define/redefine/undefine) are
///   one-shot, so parsing them every time is fine and keeps them off this cache.
/// - `translation_cache`: string -> translated IR. Translation resolves user-defined functions, so
///   it is flushed on schema commits (but not on statistics-only changes).
/// - `compile_cache`: translated IR -> executable pipeline. Flushed on schema commits, and when
///   statistics drift far enough to change query plans.
#[derive(Debug)]
pub struct QueryCache {
    parse_cache: Cache<String, Arc<Pipeline>>,
    translation_cache: Cache<String, TranslatedPipeline>,
    compile_cache: Cache<IRQuery, ExecutablePipeline>,
    validity_requirements: RwLock<ValidityRequirements>,
}

<<<<<<< Updated upstream
/// The outcome of parsing a query string. Transient (never cached directly): a schema query is held
/// by value, while a data pipeline is shared via `Arc` with the parse cache and the execution queue.
=======
>>>>>>> Stashed changes
#[derive(Debug)]
pub enum ParsedQuery {
    Schema(SchemaQuery),
    Pipeline(Arc<Pipeline>),
}

impl QueryCache {
    pub fn new() -> Self {
        let parse_cache = CacheBuilder::new(QUERY_PARSE_CACHE_SIZE).build();
        let translation_cache = CacheBuilder::new(QUERY_TRANSLATION_CACHE_SIZE).build();
        let compile_cache = CacheBuilder::new(QUERY_PLAN_CACHE_SIZE).support_invalidation_closures().build();
        let validity_requirements =
            RwLock::new(ValidityRequirements { latest_statistics: None, latest_schema_commit: None });
        QueryCache { parse_cache, translation_cache, compile_cache, validity_requirements }
    }

    pub fn get_parsed(&self, query: &str) -> Option<Arc<Pipeline>> {
        self.parse_cache.get(query)
    }

    pub(crate) fn insert_parsed(&self, source_query: &str, pipeline: Arc<Pipeline>) {
        self.parse_cache.insert(source_query.to_owned(), pipeline);
    }

    pub fn get_translated(&self, query: &str) -> Option<TranslatedPipeline> {
        self.translation_cache.get(query)
    }

    pub(crate) fn may_insert_translated(
        &self,
        statistics_sequence_number: SequenceNumber,
        source_query: &str,
        translated: TranslatedPipeline,
    ) {
        let read_lock = self.validity_requirements.read().unwrap();
        let may_insert = read_lock
            .latest_schema_commit
            .map_or(true, |latest_schema_commit_number| statistics_sequence_number >= latest_schema_commit_number);
        if may_insert {
            self.translation_cache.insert(source_query.to_owned(), translated);
        }
        drop(read_lock);
    }

    pub(crate) fn get_compiled(
        &self,
        preamble: Arc<Vec<Function>>,
        given: Arc<Option<TranslatedGiven>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
    ) -> Option<ExecutablePipeline> {
        let key = IRQuery::new(preamble.clone(), given, stages, fetch);
        self.compile_cache.get(&key).map(|mut found| {
            let replacement = preamble.iter().map(|func| Arc::new(func.parameters.clone())).enumerate();
            found.executable_functions.replace_preamble_parameters(replacement);
            found
        })
    }

    pub(crate) fn may_insert_compiled(
        &self,
        statistics_sequence_number: SequenceNumber,
        preamble: Arc<Vec<Function>>,
        given: Arc<Option<TranslatedGiven>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
        pipeline: ExecutablePipeline,
    ) {
        let key = IRQuery::new(preamble, given, stages, fetch);
        let read_lock = self.validity_requirements.read().unwrap();
        let ValidityRequirements { latest_schema_commit, latest_statistics } = &*read_lock;
        let may_insert = latest_schema_commit
            .map_or(true, |latest_schema_commit_number| statistics_sequence_number >= latest_schema_commit_number)
            && latest_statistics
                .as_ref()
                .map_or(true, |stats| !is_pipeline_type_populations_outdated(&stats, &pipeline));
        if may_insert {
            self.compile_cache.insert(key, pipeline);
        }
        drop(read_lock);
    }

    pub fn set_statistics_and_invalidate_outdated(&self, new_statistics: Arc<Statistics>) {
        // Statistics changes only affect query plans (executables), not translation, so the parse
        // cache is deliberately left untouched here.
        let mut write_lock = self.validity_requirements.write().unwrap();
        (*write_lock).latest_statistics = Some(new_statistics.clone());
        drop(write_lock);
        let _predicate_id = self
            .compile_cache
            .invalidate_entries_if(move |_, pipeline| is_pipeline_type_populations_outdated(&*new_statistics, pipeline))
            .unwrap();
    }

    pub fn force_reset(&self, statistics: &Statistics) {
        // Schema commits can change function resolution, so the translation and compile caches must
        // be flushed. The parse cache is purely syntactic and remains valid.
        let mut write_lock = self.validity_requirements.write().unwrap();
        (*write_lock).latest_schema_commit = Some(statistics.sequence_number);
        drop(write_lock);
        self.translation_cache.invalidate_all();
        self.compile_cache.invalidate_all();
        QUERY_CACHE_FLUSH.increment();
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new()
    }
}

fn is_pipeline_type_populations_outdated(statistics: &Statistics, pipeline: &ExecutablePipeline) -> bool {
    let mut total_increase = 1.0;
    let mut total_decrease = 1.0;
    for (&ty, &pop) in &pipeline.type_populations {
        let type_count = match ty {
            Type::Entity(ty) => statistics.entity_counts.get(&ty).copied().unwrap_or_default(),
            Type::Relation(ty) => statistics.relation_counts.get(&ty).copied().unwrap_or_default(),
            Type::Attribute(ty) => statistics.attribute_counts.get(&ty).copied().unwrap_or_default(),
            Type::RoleType(ty) => statistics.role_counts.get(&ty).copied().unwrap_or_default(),
        };
        match u64::max(type_count, 1) as f64 / u64::max(pop, 1) as f64 {
            increase @ 1.0.. => total_increase *= increase,
            decrease @ ..1.0 => total_decrease /= decrease,
            _ => panic!("NaN?!"),
        }
    }
    total_increase >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
        || total_decrease >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
}

#[derive(Debug)]
struct IRQuery {
    preamble: Arc<Vec<Function>>,
    given: Arc<Option<TranslatedGiven>>,
    stages: Arc<Vec<TranslatedStage>>,
    fetch: Arc<Option<FetchObject>>,
}

impl IRQuery {
    fn new(
        preamble: Arc<Vec<Function>>,
        given: Arc<Option<TranslatedGiven>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
    ) -> Self {
        Self { preamble: preamble, given, stages, fetch }
    }
}

impl Hash for IRQuery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_into(state);
    }
}

impl PartialEq<Self> for IRQuery {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Eq for IRQuery {}

impl StructuralEquality for IRQuery {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.preamble.hash_into(&mut hasher);
        self.given.hash_into(&mut hasher);
        self.stages.hash_into(&mut hasher);
        self.fetch.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.preamble.equals(&other.preamble)
            && self.given.equals(&other.given)
            && self.stages.equals(&other.stages)
            && self.fetch.equals(&other.fetch)
    }
}
