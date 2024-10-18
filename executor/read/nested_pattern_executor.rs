/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::VariablePosition;

use crate::{batch::FixedBatch, read::pattern_executor::PatternExecutor, row::MaybeOwnedRow};

// TODO: Move Offset & Limit out of here, make the mapper stateless
pub(super) enum NestedPatternExecutor {
    Disjunction(Vec<NestedPatternBranch>, IdentityMapper),
    Negation([NestedPatternBranch; 1], NegationMapper),
    InlinedFunction([NestedPatternBranch; 1], InlinedFunctionMapper),
    Offset([NestedPatternBranch; 1], OffsetMapper),
    Limit([NestedPatternBranch; 1], LimitMapper),
}

// Bad name because I want to refactor later
pub(super) enum SubQueryResultMapperEnumMut<'a> {
    Identity(&'a mut IdentityMapper),
    Negation(&'a mut NegationMapper),
    InlinedFunction(&'a mut InlinedFunctionMapper),
    Offset(&'a mut OffsetMapper),
    Limit(&'a mut LimitMapper),
}

impl<'a> SubQueryResultMapperEnumMut<'a> {
    // TODO: use
    pub(super) fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
        match self {
            Self::Identity(mapper) => mapper.prepare_and_map_input(input),
            Self::Negation(mapper) => mapper.prepare_and_map_input(input),
            Self::InlinedFunction(mapper) => mapper.prepare_and_map_input(input),
            Self::Offset(mapper) => mapper.prepare_and_map_input(input),
            Self::Limit(mapper) => mapper.prepare_and_map_input(input),
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
            Self::Offset(mapper) => mapper.map_output(input, subquery_result),
            Self::Limit(mapper) => mapper.map_output(input, subquery_result),
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
            NestedPatternExecutor::Offset(branch, mapper) => {
                (branch.as_mut_slice(), SubQueryResultMapperEnumMut::Offset(mapper))
            }
            NestedPatternExecutor::Limit(branch, mapper) => {
                (branch.as_mut_slice(), SubQueryResultMapperEnumMut::Limit(mapper))
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
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static>;
    fn map_output(&mut self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult;
}

pub(super) enum SubQueryResult {
    Retry(Option<FixedBatch>),
    Done(Option<FixedBatch>),
}

pub(super) struct NegationMapper;
pub(super) struct IdentityMapper;
pub(super) struct InlinedFunctionMapper {
    arguments: Vec<VariablePosition>, // caller input -> callee input
    assigned: Vec<VariablePosition>,  // callee return -> caller output
    output_width: u32,
}

impl SubQueryResultMapper for NegationMapper {
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&mut self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match subquery_result {
            None => SubQueryResult::Done(Some(FixedBatch::from(input.clone().into_owned()))),
            Some(_) => SubQueryResult::Done(None),
        }
    }
}

impl SubQueryResultMapper for IdentityMapper {
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned()
    }

    fn map_output(&mut self, _: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        SubQueryResult::Retry(subquery_result)
    }
}

impl InlinedFunctionMapper {
    pub(crate) fn new(arguments: Vec<VariablePosition>, assigned: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { arguments, assigned, output_width }
    }
}

impl SubQueryResultMapper for InlinedFunctionMapper {
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        input.clone().into_owned() // TODO: This is wrong
                                   // todo!()
    }

    fn map_output(&mut self, input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        match subquery_result {
            None => SubQueryResult::Done(None),
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
                SubQueryResult::Retry(Some(output_batch))
            }
        }
    }
}

// TODO: OffsetController & LimitController should be really easy.
pub(super) struct OffsetMapper {
    required: u64,
    current: u64,
}

impl OffsetMapper {
    pub(crate) fn new(offset: u64) -> Self {
        Self { required: offset, current: 0 }
    }
}

impl SubQueryResultMapper for OffsetMapper {
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        self.current = 0;
        input.clone().into_owned()
    }

    fn map_output(&mut self, _input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                SubQueryResult::Retry(Some(input_batch))
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                SubQueryResult::Retry(None)
            } else {
                let offset_in_batch = (self.required - self.current) as u32;
                let mut output_batch = FixedBatch::new(input_batch.width());
                for row_index in offset_in_batch..input_batch.len() {
                    output_batch.append(|mut output_row| {
                        let input_row = input_batch.get_row(row_index);
                        output_row.copy_from(input_row.row(), input_row.multiplicity())
                    });
                }
                self.current = self.required;
                SubQueryResult::Retry(Some(output_batch))
            }
        } else {
            SubQueryResult::Done(None)
        }
    }
}

pub(super) struct LimitMapper {
    required: u64,
    current: u64,
}

impl LimitMapper {
    pub(crate) fn new(limit: u64) -> Self {
        Self { required: limit, current: limit }
    }
}

impl SubQueryResultMapper for LimitMapper {
    fn prepare_and_map_input(&mut self, input: &MaybeOwnedRow<'_>) -> MaybeOwnedRow<'static> {
        self.current = 0;
        input.clone().into_owned()
    }

    fn map_output(&mut self, _input: &MaybeOwnedRow<'_>, subquery_result: Option<FixedBatch>) -> SubQueryResult {
        if let Some(input_batch) = subquery_result {
            if self.current >= self.required {
                SubQueryResult::Done(None)
            } else if (self.required - self.current) >= input_batch.len() as u64 {
                self.current += input_batch.len() as u64;
                SubQueryResult::Retry(Some(input_batch))
            } else {
                let mut output_batch = FixedBatch::new(input_batch.width());
                let mut i = 0;
                while self.current < self.required {
                    output_batch.append(|mut output_row| {
                        let input_row = input_batch.get_row(i);
                        output_row.copy_from(input_row.row(), input_row.multiplicity())
                    });
                    i += 1;
                    self.current += 1;
                }
                debug_assert!(self.current == self.required);
                SubQueryResult::Done(Some(output_batch))
            }
        } else {
            SubQueryResult::Done(None)
        }
    }
}

// // The reducers & sort will need to pass through a regular Batch since there's no guarantee of it passing through a fixed batch.
// // Maybe spawn off FixedBatch-es from a regular batch. This also means they can't be regular subpattern executors
// struct ReducerController {
//     input: Option<MaybeOwnedRow<'static>>,
//     grouped_reducer: GroupedReducer,
// }
//
// impl ReducerController {
//     fn new(grouped_reducer: GroupedReducer) -> Self {
//         Self { grouped_reducer, input: None }
//     }
// }
//
// impl CollectingStageController for ReducerController {
//     fn reset(&mut self) {
//         self.input = None;
//     }
//
//     fn is_active(&self) -> bool {
//         self.input.is_some()
//     }
//
//     fn prepare_and_get_subpattern_input(&mut self, row: MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
//         self.input = Some(row.clone());
//         row
//     }
//
//     fn process_result(&mut self, result: Option<FixedBatch>) -> NestedPatternControllerResult {
//
//     }
// }
