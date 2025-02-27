/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use compiler::executable::put::PutExecutable;
use storage::snapshot::WritableSnapshot;
use crate::ExecutionInterrupt;
use crate::pipeline::{PipelineExecutionError, WrittenRowsIterator};
use crate::pipeline::stage::{ExecutionContext, StageAPI};

pub struct PutStageExecutor<PreviousStage> {
    executable: Arc<PutExecutable>,
    previous: PreviousStage,
}


impl<PreviousStage> PutStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<PutExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for PutStageExecutor<PreviousStage>
    where
        Snapshot: WritableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(self, interrupt: ExecutionInterrupt) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (Box<PipelineExecutionError>, ExecutionContext<Snapshot>)> {
        todo!()
    }
}