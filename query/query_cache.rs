/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex},
};

use compiler::executable::pipeline::ExecutablePipeline;
use concept::thing::statistics::Statistics;
use ir::{
    pipeline::{fetch::FetchObject, function::Function},
    translation::pipeline::TranslatedStage,
};
use moka::sync::Cache;
use resource::{
    constants::database::{QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION, QUERY_PLAN_CACHE_SIZE},
    perf_counters::QUERY_CACHE_FLUSH,
};
use storage::sequence_number::SequenceNumber;
use structural_equality::StructuralEquality;
use tracing::{event, Level};

#[derive(Debug)]
pub struct QueryCache {
    cache: Cache<IRQuery, ExecutablePipeline>,
    last_statistics: Mutex<Statistics>,
}

impl QueryCache {
    pub fn new() -> Self {
        let cache = Cache::new(QUERY_PLAN_CACHE_SIZE);
        QueryCache { cache, last_statistics: Mutex::new(Statistics::new(SequenceNumber::MIN)) }
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

    pub fn may_reset(&self, new_statistics: &Statistics) {
        let last_statistics_guard = self.last_statistics.lock().unwrap();
        let largest_change = last_statistics_guard.largest_difference_frac(new_statistics);
        if largest_change > QUERY_PLAN_CACHE_FLUSH_ANY_STATISTIC_CHANGE_FRACTION {
            event!(Level::TRACE, "Invalidating query cache given a statistic change of {}.", largest_change);
            drop(last_statistics_guard);
            self.force_reset(new_statistics);
        }
    }

    pub fn force_reset(&self, new_statistics: &Statistics) {
        let statistics = new_statistics.clone();
        *self.last_statistics.lock().unwrap() = statistics;
        self.cache.invalidate_all();
        QUERY_CACHE_FLUSH.increment();
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
