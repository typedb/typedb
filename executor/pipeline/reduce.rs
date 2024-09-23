/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use compiler::modifiers::ReduceProgram;
use storage::snapshot::ReadableSnapshot;
use crate::batch::Batch;
use crate::ExecutionInterrupt;
use crate::pipeline::{PipelineExecutionError, WrittenRowsIterator};
use crate::pipeline::stage::{ExecutionContext, StageAPI, StageIterator};

// Sort
pub struct ReduceStageExecutor<PreviousStage> {
    program: ReduceProgram,
    previous: PreviousStage,
}

impl<PreviousStage> ReduceStageExecutor<PreviousStage>
{
    pub fn new(program: ReduceProgram, previous: PreviousStage) -> Self {
        Self { program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for ReduceStageExecutor<PreviousStage>
    where
        Snapshot: ReadableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { previous, program, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        let rows = match reduce_iterator(program, previous_iterator) {
            Ok(rows) => rows,
            Err(err) => return Err((err, context)),
        };
        Ok((WrittenRowsIterator::new(rows), context))
    }
}

fn reduce_iterator(program: ReduceProgram, iterator: impl StageIterator) -> Result<Batch, PipelineExecutionError> {
    todo!()
}
