/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    batch::FixedBatch,
    read::pattern_executor::PatternExecutor,
    row::MaybeOwnedRow,
};

pub(super) enum NestedPatternExecutor {
    Disjunction(Vec<NestedPatternBranch>, IdentityMapper),
    Negation([NestedPatternBranch; 1], NegationMapper),
}

// Bad name because I want to refactor later
pub(super) enum SubQueryResultMapperEnumMut<'a> {
    Identity(&'a mut IdentityMapper),
    Negation(&'a mut NegationMapper),
}

impl<'a> SubQueryResultMapperEnumMut<'a> {
    pub(super) fn map_output(&mut self, input: &MaybeOwnedRow<'static>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match self {
            Self::Identity(mapper) => mapper.map(input, subquery_result),
            Self::Negation(mapper) => mapper.map(input, subquery_result),
        }
    }
}

impl NestedPatternExecutor {
    pub(super) fn to_parts_mut(&mut self) -> (&mut [NestedPatternBranch], SubQueryResultMapperEnumMut<'_>) {
        match self {
            NestedPatternExecutor::Disjunction(branches, mapper) => {
                (branches.as_mut_slice(), SubQueryResultMapperEnumMut::Identity(mapper))
            }
            NestedPatternExecutor::Negation(branch, mapper) => {
                (branch.as_mut_slice(), SubQueryResultMapperEnumMut::Negation(mapper))
            },
        }
    }
}

pub(super) struct NestedPatternBranch {
    pub(super) input: Option<MaybeOwnedRow<'static>>,
    pub(super) pattern_executor: PatternExecutor,
}

impl NestedPatternBranch {
    pub(crate) fn new(pattern_executor: PatternExecutor) -> Self {
        Self { input: None, pattern_executor }
    }

    pub(super) fn prepare(&mut self, input: MaybeOwnedRow<'static>) {
        self.input = Some(input.clone());
        self.pattern_executor.prepare(FixedBatch::from(input.clone()));
    }
}


pub(super) trait SubQueryResultMapper {
    fn map(&self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult;
}

pub(super) enum SubQueryResult {
    Regular(Option<FixedBatch>),
    ShortCircuit(Option<FixedBatch>),
}

pub(super) struct NegationMapper;
pub(super) struct IdentityMapper;

impl SubQueryResultMapper for NegationMapper {
    fn map(&self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match subquery_result {
            None => SubQueryResult::ShortCircuit(Some(FixedBatch::from(input.clone().into_owned()))),
            Some(_) => SubQueryResult::ShortCircuit(None),
        }
    }
}

impl SubQueryResultMapper for IdentityMapper {
    fn map(&self, _: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        SubQueryResult::Regular(subquery_result)
    }
}
