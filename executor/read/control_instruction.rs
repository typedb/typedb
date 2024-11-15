/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use ir::pipeline::ParameterRegistry;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    read::{
        collecting_stage_executor::CollectedStageIterator,
        pattern_executor::{BranchIndex, ExecutorIndex},
        stream_modifier::StreamModifierResultMapper,
    },
    row::MaybeOwnedRow,
};

pub(super) struct PatternStart {
    pub(super) input_batch: FixedBatch,
}

pub(super) struct RestoreSuspension {
    pub(super) depth: usize,
}

pub(super) struct ExecuteImmediate {
    pub(super) index: ExecutorIndex,
}

pub(super) struct MapRowBatchToRowForNested {
    pub(super) index: ExecutorIndex,
    pub(super) iterator: FixedBatchRowIterator,
}

pub(super) struct ExecuteStreamModifier {
    pub(super) index: ExecutorIndex,
    pub(super) mapper: StreamModifierResultMapper,
    pub(super) input: MaybeOwnedRow<'static>,
}

pub(super) struct ExecuteInlinedFunction {
    pub(super) index: ExecutorIndex,
    pub(super) parameters_override: Arc<ParameterRegistry>, // TODO: Get this straight from the executor?
    pub(super) input: MaybeOwnedRow<'static>,
}

pub(super) struct ExecuteNegation {
    pub(super) index: ExecutorIndex,
    pub(super) input: MaybeOwnedRow<'static>,
}

pub(super) struct ExecuteDisjunction {
    pub(super) index: ExecutorIndex,
    pub(super) branch_index: BranchIndex,
    pub(super) input: MaybeOwnedRow<'static>, // Only needed for suspend points. We can actually use an empty one, because the nested pattern has all the info
}

pub(super) struct TabledCall {
    pub(super) index: ExecutorIndex,
}

pub(super) struct CollectingStage {
    pub(super) index: ExecutorIndex,
}

pub(super) struct StreamCollected {
    pub(super) index: ExecutorIndex,
    pub(super) iterator: CollectedStageIterator,
}

pub(super) struct ReshapeForReturn {
    pub(super) index: ExecutorIndex,
    pub(super) to_reshape: FixedBatch,
}

pub(super) struct Yield {
    pub(super) batch: FixedBatch,
}

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
