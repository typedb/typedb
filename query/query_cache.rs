/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex},
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
use structural_equality::StructuralEquality;

#[derive(Debug)]
pub struct QueryCache {
    cache: Cache<IRQuery, ExecutablePipeline>,
}

impl QueryCache {
    pub fn new() -> Self {
        let cache = CacheBuilder::new(QUERY_PLAN_CACHE_SIZE).support_invalidation_closures().build();
        QueryCache { cache }
    }

    pub(crate) fn get(
        &self,
        preamble: Arc<Vec<Function>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
    ) -> Option<ExecutablePipeline> {
        let key = IRQuery::new(preamble, stages, fetch);
        self.cache.get(&key)
    }

    pub(crate) fn insert(
        &self,
        preamble: Arc<Vec<Function>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<FetchObject>>,
        pipeline: ExecutablePipeline,
    ) {
        let key = IRQuery::new(preamble, stages, fetch);
        self.cache.insert(key, pipeline);
    }

    pub fn may_evict(&self, new_statistics: &Statistics) {
        let new_statistics = new_statistics.clone(); // it's either this or clone the entire cache
        let _predicate_id = self
            .cache
            .invalidate_entries_if(move |_, pipeline| {
                let mut total_increase = 1.0;
                let mut total_decrease = 1.0;
                for (&ty, &pop) in &pipeline.type_pop {
                    let type_count = match ty {
                        Type::Entity(ty) => new_statistics.entity_counts[&ty],
                        Type::Relation(ty) => new_statistics.relation_counts[&ty],
                        Type::Attribute(ty) => new_statistics.attribute_counts[&ty],
                        Type::RoleType(_) => 1, // uhhh
                    };
                    match u64::min(type_count, 1) as f64 / u64::min(pop, 1) as f64 {
                        increase @ 1.0.. => total_increase *= increase,
                        decrease @ ..1.0 => total_decrease /= decrease,
                        _ => panic!("NaN?!"),
                    }
                }
                total_increase >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
                    || total_decrease >= QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION
            })
            .unwrap();
    }

    pub fn force_reset(&self, _statistics: &Statistics) {
        self.cache.invalidate_all();
        QUERY_CACHE_FLUSH.increment();
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new()
    }
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
