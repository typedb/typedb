/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::RwLock;

use compiler::executable::pipeline::ExecutablePipeline;
use ir::pipeline::function::Function;
use ir::translation::pipeline::TranslatedStage;
use resource::constants::database::QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT;
use resource::perf_counters::QUERY_CACHE_FLUSH;
use structural_equality::StructuralEquality;

#[derive(Debug)]
pub struct QueryCache {
    // TODO: replace with actual cache type
    cache: RwLock<(HashMap<IRQuery<'static>, ExecutablePipeline>, u64)>
}

impl QueryCache {
    pub fn new(statistics_size: u64) -> Self {
        QueryCache { cache: RwLock::new((HashMap::new(), statistics_size)) }
    }

    pub(crate) fn get(&self, preamble: &Vec<Function>, stages: &Vec<TranslatedStage>) -> Option<ExecutablePipeline> {
        let key = IRQuery::new_ref(preamble, stages);
        let guard = self.cache.read().unwrap();
        guard.0.get(&key).cloned()
    }

    pub(crate) fn insert(&self, preamble: Vec<Function>, stages: Vec<TranslatedStage>, pipeline: ExecutablePipeline) {
        let key = IRQuery::new_owned(preamble, stages);
        let mut guard = self.cache.write().unwrap();
        guard.0.insert(key, pipeline);
    }
    
    pub fn may_reset(&self, new_statistics_size: u64) {
        let read_guard = self.cache.read().unwrap();
        let last_statistics_size = read_guard.1;
        let change = (last_statistics_size as f64 - new_statistics_size as f64) / (last_statistics_size as f64);
        if change.abs() > QUERY_PLAN_CACHE_FLUSH_STATISTICS_CHANGE_PERCENT {
            drop(read_guard);
            self.force_reset(new_statistics_size);
        }
    }
    
    pub fn force_reset(&self, new_statistics_size: u64) {
        let mut write_guard = self.cache.write().unwrap();
        write_guard.0.clear();
        write_guard.1 = new_statistics_size;
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
        let hash = StructuralEquality::hash(self);
        state.write_u64(hash)
    }
}

impl<'a> PartialEq<Self> for IRQuery<'a> {
    fn eq(&self, other: &Self) -> bool {
        StructuralEquality::equal(self, other)
    }
}

impl<'a> Eq for IRQuery<'a> {}

impl<'a> StructuralEquality for IRQuery<'a> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(self.preamable.as_ref()));
        hasher.write_u64(StructuralEquality::hash(self.stages.as_ref()));
        hasher.write_u64(StructuralEquality::hash(self.fetch.as_ref()));
        hasher.finish()
    }

    fn equal(&self, other: &Self) -> bool {
        StructuralEquality::equal(self.preamable.as_ref(), other.preamable.as_ref()) &&
            StructuralEquality::equal(self.stages.as_ref(), other.stages.as_ref()) &&
            StructuralEquality::equal(self.fetch.as_ref(), other.fetch.as_ref())
    }
}
