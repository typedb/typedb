/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::borrow::Borrow;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use moka::sync::Cache;

use compiler::executable::pipeline::ExecutablePipeline;
use ir::pipeline::function::Function;
use ir::translation::pipeline::TranslatedStage;
use resource::constants::database::{QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT, QUERY_PLAN_CACHE_SIZE};
use resource::perf_counters::QUERY_CACHE_FLUSH;
use structural_equality::StructuralEquality;

#[derive(Debug)]
pub struct QueryCache {
    cache: Cache<IRQuery, ExecutablePipeline>,
    statistics_size: AtomicU64,
}

impl QueryCache {
    pub fn new(statistics_size: u64) -> Self {
        let cache = Cache::new(QUERY_PLAN_CACHE_SIZE);
        QueryCache {
            cache,
            statistics_size: AtomicU64::from(statistics_size),
        }
    }

    pub(crate) fn get(&self, preamble: Arc<Vec<Function>>, stages: Arc<Vec<TranslatedStage>>) -> Option<ExecutablePipeline> {
        let key = IRQuery::new(preamble, stages, Arc::new(None));
        self.cache.get(&key)
    }

    pub(crate) fn insert(&self, preamble: Arc<Vec<Function>>, stages: Arc<Vec<TranslatedStage>>, pipeline: ExecutablePipeline) {
        let key = IRQuery::new(preamble, stages, Arc::new(None));
        self.cache.insert(key, pipeline);
    }
    
    pub fn may_reset(&self, new_statistics_size: u64) {
        let last_statistics_size = self.statistics_size.load(Ordering::SeqCst);
        let change = (last_statistics_size as f64 - new_statistics_size as f64) / (last_statistics_size as f64);
        if change.abs() > QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT {
            self.force_reset(new_statistics_size);
        }
    }
    
    pub fn force_reset(&self, new_statistics_size: u64) {
        self.cache.invalidate_all();
        self.statistics_size.store(new_statistics_size, Ordering::SeqCst);
        QUERY_CACHE_FLUSH.increment();
    }
}

#[derive(Debug)]
struct IRQuery {
    preamable: Arc<Vec<Function>>,
    stages: Arc<Vec<TranslatedStage>>,
    fetch: Arc<Option<usize>>,
}

impl IRQuery {
    fn new(
        preamable: Arc<Vec<Function>>,
        stages: Arc<Vec<TranslatedStage>>,
        fetch: Arc<Option<usize>>,
    ) -> Self {
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
        self.preamable.equals(&other.preamable) &&
            self.stages.equals(&other.stages) &&
            self.fetch.equals(&other.fetch)
    }
}
