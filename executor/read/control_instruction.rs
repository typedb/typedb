/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    read::{
        collecting_stage_executor::CollectedStageIterator,
        pattern_executor::{BranchIndex, ExecutorIndex},
        stream_modifier::StreamModifierResultMapper,
    },
    row::MaybeOwnedRow,
};

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
pub(super) struct MapRowBatchToRowForNested {
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
pub(super) struct ExecuteDisjunction {
    pub(super) index: ExecutorIndex,
    pub(super) branch_index: BranchIndex,
    pub(super) input: MaybeOwnedRow<'static>, // Only needed for suspend points. We can actually use an empty one, because the nested pattern has all the info
}

#[derive(Debug)]
pub(super) struct TabledCall {
    pub(super) index: ExecutorIndex,
}

#[derive(Debug)]
pub(super) struct CollectingStage {
    pub(super) index: ExecutorIndex,
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

#[derive(Debug)]
pub(super) enum ControlInstruction {
    PatternStart(PatternStart),
    RestoreSuspension(RestoreSuspension),

    ExecuteImmediate(ExecuteImmediate),

    MapBatchToRowForNested(MapRowBatchToRowForNested),
    ExecuteNegation(ExecuteNegation),
    ExecuteDisjunction(ExecuteDisjunction),
    ExecuteInlinedFunction(ExecuteInlinedFunction),
    ExecuteStreamModifier(ExecuteStreamModifier),

    ExecuteTabledCall(TabledCall),

    CollectingStage(CollectingStage),
    StreamCollected(StreamCollected),
    ReshapeForReturn(ReshapeForReturn),

    Yield(Yield),
}
