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
    translation::pipeline::TranslatedStage,
};
use moka::sync::{Cache, CacheBuilder};
use resource::{
    constants::database::{QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION, QUERY_PLAN_CACHE_SIZE},
    perf_counters::QUERY_CACHE_FLUSH,
};
use storage::sequence_number::SequenceNumber;
use structural_equality::StructuralEquality;

#[derive(Debug)]
struct ValidityRequirements {
    latest_statistics: Option<Arc<Statistics>>,
    latest_schema_commit: Option<SequenceNumber>,
}

#[derive(Debug)]
pub struct QueryCache {
    cache: Cache<IRQuery, ExecutablePipeline>,
    validity_requirements: RwLock<ValidityRequirements>,
}

impl QueryCache {
    pub fn new() -> Self {
        let cache = CacheBuilder::new(QUERY_PLAN_CACHE_SIZE).support_invalidation_closures().build();
        let validity_requirements =
            RwLock::new(ValidityRequirements { latest_statistics: None, latest_schema_commit: None });
        QueryCache { cache, validity_requirements }
    }

    pub(crate) fn get(
        &self,
        preamble: Arc<Vec<Function>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
    ) -> Option<ExecutablePipeline> {
        let key = IRQuery::new(preamble.clone(), stages, fetch);
        self.cache.get(&key).map(|mut found| {
            let replacement = preamble.iter().map(|func| Arc::new(func.parameters.clone())).enumerate();
            found.executable_functions.replace_preamble_parameters(replacement);
            found
        })
    }

    pub(crate) fn may_insert(
        &self,
        statistics_sequence_number: SequenceNumber,
        preamble: Arc<Vec<Function>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
        pipeline: ExecutablePipeline,
    ) {
        let key = IRQuery::new(preamble, stages, fetch);
        let read_lock = self.validity_requirements.read().unwrap();
        let ValidityRequirements { latest_schema_commit, latest_statistics } = &*read_lock;
        let may_insert = latest_schema_commit
            .map_or(true, |latest_schema_commit_number| statistics_sequence_number >= latest_schema_commit_number)
            && latest_statistics
                .as_ref()
                .map_or(true, |stats| !is_pipeline_type_populations_outdated(&stats, &pipeline));
        if may_insert {
            self.cache.insert(key, pipeline);
        }
        drop(read_lock);
    }

    pub fn set_statistics_and_invalidate_outdated(&self, new_statistics: Arc<Statistics>) {
        let mut write_lock = self.validity_requirements.write().unwrap();
        (*write_lock).latest_statistics = Some(new_statistics.clone());
        drop(write_lock);
        let _predicate_id = self
            .cache
            .invalidate_entries_if(move |_, pipeline| is_pipeline_type_populations_outdated(&*new_statistics, pipeline))
            .unwrap();
    }

    pub fn force_reset(&self, statistics: &Statistics) {
        let mut write_lock = self.validity_requirements.write().unwrap();
        (*write_lock).latest_schema_commit = Some(statistics.sequence_number);
        drop(write_lock);
        self.cache.invalidate_all();
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
    preamable: Arc<Vec<Function>>,
    stages: Arc<Vec<TranslatedStage>>,
    fetch: Arc<Option<FetchObject>>,
}

impl IRQuery {
    fn new(preamable: Arc<Vec<Function>>, stages: Arc<Vec<TranslatedStage>>, fetch: Arc<Option<FetchObject>>) -> Self {
        Self { preamable, stages, fetch }
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
        self.preamable.hash_into(&mut hasher);
        self.stages.hash_into(&mut hasher);
        self.fetch.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.preamable.equals(&other.preamable) && self.stages.equals(&other.stages) && self.fetch.equals(&other.fetch)
    }
}
