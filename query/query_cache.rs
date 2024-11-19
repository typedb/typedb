/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use compiler::executable::pipeline::ExecutablePipeline;
use ir::pipeline::function::Function;
use ir::translation::pipeline::TranslatedStage;
use resource::constants::database::QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT;
use resource::perf_counters::QUERY_CACHE_FLUSH;
use structural_equality::StructuralEquality;
use moka::sync::Cache;

#[derive(Debug)]
pub struct QueryCache {
    // TODO: replace with actual cache type
    cache: Cache<IRQuery<'static>, ExecutablePipeline>,
    statistics_size: AtomicU64,
}

impl QueryCache {
    pub fn new(statistics_size: u64) -> Self {
        let cache = Cache::new(100);
        QueryCache { 
            cache,
            statistics_size: AtomicU64::from(statistics_size),
        }
    }

    pub(crate) fn get<'a>(&self, preamble: &'a Vec<Function>, stages: &'a Vec<TranslatedStage>) -> Option<ExecutablePipeline> {
        let key: IRQuery<'a> = IRQuery::new_ref(preamble, stages);
        self.cache.get(&key)
    }

    pub(crate) fn insert(&self, preamble: Vec<Function>, stages: Vec<TranslatedStage>, pipeline: ExecutablePipeline) {
        let key = IRQuery::new_owned(preamble, stages);
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
struct IRQuery<'a> {
    preamable: Cow<'a, Vec<Function>>,
    stages: Cow<'a, Vec<TranslatedStage>>,
    // TODO
    fetch: Cow<'a, Option<usize>>,
}

impl<'a> IRQuery<'a> {
    fn new_owned(
        preamable: Vec<Function>,
        stages: Vec<TranslatedStage>,
        // fetch: Option<FetchObject>,
    ) -> Self {
        Self {
            preamable: Cow::Owned(preamable),
            stages: Cow::Owned(stages),
            fetch: Cow::Owned(None),
        }
    }

    fn new_ref(
        preamable: &'a Vec<Function>,
        stages: &'a Vec<TranslatedStage>,
        // fetch: &'a Option<FetchObject>,
    ) -> Self {
        Self {
            preamable: Cow::Borrowed(preamable),
            stages: Cow::Borrowed(stages),
            fetch: Cow::Owned(None),
        }
    }
}

impl<'a> Hash for IRQuery<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_into(state);
    }
}

impl<'a> PartialEq<Self> for IRQuery<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl<'a> Eq for IRQuery<'a> {}

impl<'a> StructuralEquality for IRQuery<'a> {
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
