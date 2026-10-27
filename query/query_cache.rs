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
        QUERY_CACHE_MIN_ENTRY_WEIGHT, QUERY_PARSE_CACHE_SIZE, QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION,
        QUERY_PLAN_CACHE_FLUSH_STATIC_SMOOTH_THRESHOLD, QUERY_PLAN_CACHE_SIZE, QUERY_TRANSLATION_CACHE_SIZE,
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

#[derive(Debug)]
pub struct QueryCache {
    parse_cache: Cache<String, Arc<Pipeline>>,
    translation_cache: Cache<String, TranslatedPipeline>,
    executable_cache: Cache<IRQuery, (ExecutablePipeline, u32)>,
    validity_requirements: RwLock<ValidityRequirements>,
}

#[derive(Debug)]
pub enum ParsedQuery {
    Schema(SchemaQuery),
    Pipeline(Arc<Pipeline>),
}

impl QueryCache {
    pub fn new() -> Self {
        // Parsed, translated and compiled queries grow with the query, so the caches are bounded by weight, not count
        let parse_cache = CacheBuilder::new(weighted_capacity(QUERY_PARSE_CACHE_SIZE))
            .weigher(|source_query: &String, _: &Arc<Pipeline>| query_weight(source_query))
            .build();
        let translation_cache = CacheBuilder::new(weighted_capacity(QUERY_TRANSLATION_CACHE_SIZE))
            .weigher(|source_query: &String, _: &TranslatedPipeline| query_weight(source_query))
            .build();
        let executable_cache = CacheBuilder::new(weighted_capacity(QUERY_PLAN_CACHE_SIZE))
            .weigher(|_: &IRQuery, (_, weight): &(ExecutablePipeline, u32)| *weight)
            .build();
        let validity_requirements =
            RwLock::new(ValidityRequirements { latest_statistics: None, latest_schema_commit: None });
        QueryCache { parse_cache, translation_cache, executable_cache, validity_requirements }
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

    pub fn get_executable(
        &self,
        preamble: Arc<Vec<Function>>,
        given: Arc<Option<TranslatedGiven>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
    ) -> Option<ExecutablePipeline> {
        let key = IRQuery::new(preamble.clone(), given, stages, fetch);
        let (mut found, _) = self.executable_cache.get(&key)?;
        let is_outdated = self
            .validity_requirements
            .read()
            .unwrap()
            .latest_statistics
            .as_ref()
            .is_some_and(|statistics| is_pipeline_type_populations_outdated(statistics, &found));
        if is_outdated {
            // do not evict: the next compilation will update it, and eviction here could remove
            // a valid plan inserted concurrently
            return None;
        }
        let replacement = preamble.iter().map(|func| Arc::new(func.parameters.clone())).enumerate();
        found.executable_functions.replace_preamble_parameters(replacement);
        Some(found)
    }

    pub(crate) fn may_insert_executable(
        &self,
        statistics_sequence_number: SequenceNumber,
        source_query: &str,
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
            self.executable_cache.insert(key, (pipeline, query_weight(source_query)));
        }
        drop(read_lock);
    }

    pub fn set_statistics(&self, new_statistics: Arc<Statistics>) {
        self.validity_requirements.write().unwrap().latest_statistics = Some(new_statistics);
    }

    pub fn force_reset(&self, statistics: &Statistics) {
        let mut write_lock = self.validity_requirements.write().unwrap();
        (*write_lock).latest_schema_commit = Some(statistics.sequence_number);
        drop(write_lock);
        self.translation_cache.invalidate_all();
        self.executable_cache.invalidate_all();
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
        let smoothing = smoothstep(type_count, 1, QUERY_PLAN_CACHE_FLUSH_STATIC_SMOOTH_THRESHOLD);
        match u64::max(type_count, 1) as f64 / u64::max(pop, 1) as f64 {
            increase @ 1.0.. => total_increase *= increase.powf(smoothing),
            decrease @ ..1.0 => total_decrease /= decrease.powf(smoothing),
            _ => panic!("NaN?!"),
        }
    }
    total_increase >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
        || total_decrease >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
}

fn smoothstep(pop: u64, min: u64, max: u64) -> f64 {
    debug_assert!(max > min);
    let x = (pop.clamp(min, max) - min) as f64 / (max - min) as f64;
    (3.0 - 2.0 * x) * x * x
}

fn query_weight(source_query: &str) -> u32 {
    u32::try_from(source_query.len()).unwrap_or(u32::MAX).max(QUERY_CACHE_MIN_ENTRY_WEIGHT)
}

fn weighted_capacity(max_entries: u64) -> u64 {
    max_entries * u64::from(QUERY_CACHE_MIN_ENTRY_WEIGHT)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use resource::constants::database::QUERY_PARSE_CACHE_SIZE;
    use typeql::query::QueryStructure;

    use super::QueryCache;

    fn insert_query(people: usize, seed: u64) -> String {
        let statements: Vec<_> = (0..people).map(|i| format!("$p{i} isa person, has name \"{seed}-{i}\";")).collect();
        format!("insert {}", statements.join(" "))
    }

    fn fill_parse_cache(people: usize) -> QueryCache {
        let cache = QueryCache::new();
        let pipeline = match typeql::parse_query(&insert_query(1, 0)).unwrap().into_structure() {
            QueryStructure::Pipeline(pipeline) => Arc::new(pipeline),
            QueryStructure::Schema(_) => unreachable!("an insert is a data pipeline"),
        };
        for seed in 0..2 * QUERY_PARSE_CACHE_SIZE {
            cache.insert_parsed(&insert_query(people, seed), pipeline.clone());
        }
        cache.parse_cache.run_pending_tasks();
        cache
    }

    #[test]
    fn small_queries_are_bounded_by_count() {
        let cache = fill_parse_cache(1);
        assert_eq!(cache.parse_cache.entry_count(), QUERY_PARSE_CACHE_SIZE);
    }

    #[test]
    fn large_queries_are_bounded_by_length() {
        // ~37 KB per query, as when bulk loading a thousand concepts per query
        let cache = fill_parse_cache(1000);
        let count = cache.parse_cache.entry_count();
        assert!(count <= QUERY_PARSE_CACHE_SIZE / 8, "the parse cache holds {count} large queries");
    }
}
