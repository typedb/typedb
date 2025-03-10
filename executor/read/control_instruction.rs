/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::VariablePosition;
use utils::impl_from_for_enum;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    read::{
        collecting_stage_executor::CollectedStageIterator, stream_modifier::StreamModifierResultMapper, BranchIndex,
        ExecutorIndex,
    },
    row::MaybeOwnedRow,
};
use crate::read::collecting_stage_executor::CollectorEnum;

#[derive(Debug)]
pub(super) enum ControlInstruction {
    // Control instructions
    PatternStart(PatternStart),
    RestoreSuspension(RestoreSuspension),
    ReshapeForReturn(ReshapeForReturn),
    Yield(Yield),

    ExecuteImmediate(ExecuteImmediate),

    MapBatchToRowsForNested(MapBatchToRowsForNested),
    ExecuteNegation(ExecuteNegation),

    ExecuteDisjunctionBranch(ExecuteDisjunctionBranch),
    ExecuteInlinedFunction(ExecuteInlinedFunction),
    ExecuteStreamModifier(ExecuteStreamModifier),

    ExecuteTabledCall(ExecuteTabledCall),
    CollectingStage(CollectingStage),
    StreamCollected(StreamCollected),
}

#[derive(Debug)]
pub(super) struct PatternStart {
    pub(super) input_batch: FixedBatch,
}

#[derive(Debug)]
pub(super) struct RestoreSuspension {
    pub(super) depth: usize,
}

#[derive(Debug)]
pub(super) struct ExecuteImmediate {
    pub(super) index: ExecutorIndex,
}

#[derive(Debug)]
pub(super) struct MapBatchToRowsForNested {
    pub(super) index: ExecutorIndex,
    pub(super) iterator: FixedBatchRowIterator,
}

#[derive(Debug)]
pub(super) struct ExecuteStreamModifier {
    pub(super) index: ExecutorIndex,
    pub(super) mapper: StreamModifierResultMapper,
    pub(super) input: MaybeOwnedRow<'static>,
}

#[derive(Debug)]
pub(super) struct ExecuteInlinedFunction {
    pub(super) index: ExecutorIndex,
    pub(super) input: MaybeOwnedRow<'static>,
}

#[derive(Debug)]
pub(super) struct ExecuteNegation {
    pub(super) index: ExecutorIndex,
    pub(super) input: MaybeOwnedRow<'static>,
}

#[derive(Debug)]
pub(super) struct ExecuteDisjunctionBranch {
    pub(super) index: ExecutorIndex,
    pub(super) branch_index: BranchIndex,
    pub(super) input: MaybeOwnedRow<'static>, // Only needed for suspend points. We can actually use an empty one, because the nested pattern has all the info
}

#[derive(Debug)]
pub(super) struct ExecuteTabledCall {
    pub(super) index: ExecutorIndex,
    pub(super) last_seen_table_size: Option<usize>,
}

#[derive(Debug)]
pub(super) struct CollectingStage {
    pub(super) index: ExecutorIndex,
    pub(super) collector: CollectorEnum,
}

#[derive(Debug)]
pub(super) struct StreamCollected {
    pub(super) index: ExecutorIndex,
    pub(super) iterator: CollectedStageIterator,
}

#[derive(Debug)]
pub(super) struct ReshapeForReturn {
    pub(super) index: ExecutorIndex,
    pub(super) to_reshape: FixedBatch,
}

#[derive(Debug)]
pub(super) struct Yield {
    pub(super) batch: FixedBatch,
}

impl ExecuteInlinedFunction {
    pub(crate) fn new(index: ExecutorIndex, input_row: MaybeOwnedRow<'_>) -> Self {
        Self { index, input: input_row.into_owned() }
    }
}

impl ExecuteNegation {
    pub(crate) fn new(index: ExecutorIndex, input_row: MaybeOwnedRow<'_>) -> Self {
        Self { index, input: input_row.into_owned() }
    }
}
impl ExecuteDisjunctionBranch {
    pub(crate) fn new(index: ExecutorIndex, branch_index: BranchIndex, input_row: MaybeOwnedRow<'_>) -> Self {
        Self { index, branch_index, input: input_row.into_owned() }
    }
}

impl ExecuteStreamModifier {
    pub(crate) fn new(index: ExecutorIndex, mapper: StreamModifierResultMapper, input_row: MaybeOwnedRow<'_>) -> Self {
        Self { index, mapper, input: input_row.into_owned() }
    }
}

impl ReshapeForReturn {
    pub(crate) fn positions_to_mapping(
        positions: &[VariablePosition],
    ) -> impl Iterator<Item = (VariablePosition, VariablePosition)> + '_ {
        positions.iter().enumerate().map(|(dst, &src)| (src, VariablePosition::new(dst as u32)))
    }
}

impl_from_for_enum!(ControlInstruction from PatternStart);
impl_from_for_enum!(ControlInstruction from RestoreSuspension);
impl_from_for_enum!(ControlInstruction from ReshapeForReturn);
impl_from_for_enum!(ControlInstruction from Yield);

impl_from_for_enum!(ControlInstruction from MapBatchToRowsForNested);

impl_from_for_enum!(ControlInstruction from ExecuteImmediate);

impl_from_for_enum!(ControlInstruction from ExecuteNegation);
impl_from_for_enum!(ControlInstruction from ExecuteDisjunctionBranch);
impl_from_for_enum!(ControlInstruction from ExecuteInlinedFunction);
impl_from_for_enum!(ControlInstruction from ExecuteStreamModifier);
impl_from_for_enum!(ControlInstruction from ExecuteTabledCall);

impl_from_for_enum!(ControlInstruction from CollectingStage);
impl_from_for_enum!(ControlInstruction from StreamCollected);
