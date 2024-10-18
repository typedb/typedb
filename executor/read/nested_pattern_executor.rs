/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::VariablePosition;

use crate::{batch::FixedBatch, read::pattern_executor::PatternExecutor, row::MaybeOwnedRow};

pub(super) enum NestedPatternExecutor {
    Disjunction(Vec<NestedPatternBranch>, IdentityMapper),
    Negation([NestedPatternBranch; 1], NegationMapper),
    InlinedFunction([NestedPatternBranch; 1], InlinedFunctionMapper),
}

// Bad name because I want to refactor later
pub(super) enum SubQueryResultMapperEnumMut<'a> {
    Identity(&'a mut IdentityMapper),
    Negation(&'a mut NegationMapper),
    InlinedFunction(&'a mut InlinedFunctionMapper),
}

impl<'a> SubQueryResultMapperEnumMut<'a> {
    pub(super) fn map_input(&mut self, input: &MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
        match self {
            Self::Identity(mapper) => mapper.map_input(input),
            Self::Negation(mapper) => mapper.map_input(input),
            Self::InlinedFunction(mapper) => mapper.map_input(input),
        }
    }
    pub(super) fn map_output(
        &mut self,
        input: &MaybeOwnedRow<'static>,
        subquery_result: Option<FixedBatch>,
    ) -> SubQueryResult {
        match self {
            Self::Identity(mapper) => mapper.map_output(input, subquery_result),
            Self::Negation(mapper) => mapper.map_output(input, subquery_result),
            Self::InlinedFunction(mapper) => mapper.map_output(input, subquery_result),
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
            }
            NestedPatternExecutor::InlinedFunction(branch, mapper) => {
                (branch.as_mut_slice(), SubQueryResultMapperEnumMut::InlinedFunction(mapper))
            }
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
    fn map_input(&self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static>;
    fn map_output(&self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult;
}

pub(super) enum SubQueryResult {
    Regular(Option<FixedBatch>),
    ShortCircuit(Option<FixedBatch>),
}

pub(super) struct NegationMapper;
pub(super) struct IdentityMapper;
pub(super) struct InlinedFunctionMapper {
    arguments: Vec<VariablePosition>, // caller input -> callee input
    assigned: Vec<VariablePosition>,  // callee return -> caller output
    output_width: u32,
}

impl SubQueryResultMapper for NegationMapper {
    fn map_input(&self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match subquery_result {
            None => SubQueryResult::ShortCircuit(Some(FixedBatch::from(input.clone().into_owned()))),
            Some(_) => SubQueryResult::ShortCircuit(None),
        }
    }
}

impl SubQueryResultMapper for IdentityMapper {
    fn map_input(&self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&self, _: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        SubQueryResult::Regular(subquery_result)
    }
}

impl InlinedFunctionMapper {
    pub(crate) fn new(arguments: Vec<VariablePosition>, assigned: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { arguments, assigned, output_width }
    }
}

impl SubQueryResultMapper for InlinedFunctionMapper {
    fn map_input(&self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        todo!()
    }

    fn map_output(&self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match subquery_result {
            None => SubQueryResult::Regular(None),
            Some(returned_batch) => {
                let mut output_batch = FixedBatch::new(self.output_width);
                for return_index in 0..returned_batch.len() {
                    // TODO: Deduplicate?
                    let returned_row = returned_batch.get_row(return_index);
                    output_batch.append(|mut output_row| {
                        for (i, element) in input.iter().enumerate() {
                            output_row.set(VariablePosition::new(i as u32), element.clone());
                        }
                        for (returned_index, output_position) in self.assigned.iter().enumerate() {
                            output_row.set(output_position.clone(), returned_row[returned_index].clone().into_owned());
                        }
                    });
                }
                SubQueryResult::Regular(Some(output_batch))
            }
        }
    }
}
