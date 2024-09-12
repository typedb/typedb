/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use compiler::modifiers::SortProgram;
use compiler::VariablePosition;
use ir::program::modifier::SortVariable;
use storage::snapshot::ReadableSnapshot;
use crate::batch::Batch;
use crate::ExecutionInterrupt;
use crate::pipeline::{PipelineExecutionError, StageAPI, StageIterator, WrittenRowsIterator};

pub struct SortStageExecutor<Snapshot, PreviousStage> {
    program: SortProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}


impl<Snapshot, PreviousStage> SortStageExecutor<Snapshot, PreviousStage>
    where
        Snapshot: ReadableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(program: SortProgram, previous: PreviousStage) -> Self {
        Self { program, previous, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for SortStageExecutor<Snapshot, PreviousStage>
    where
        Snapshot: ReadableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        // TODO: Do we really need this function apart from at  the final stage? If so, can't we make it a property of the pipeline instead?
        self.previous.named_selected_outputs()
    }

    fn into_iterator(self, interrupt: ExecutionInterrupt) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let (previous_iterator, mut snapshot) = self.previous.into_iterator(interrupt)?;
        // accumulate once, then we will operate in-place
        let mut batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((snapshot, err)),
        };
        let batch_len = batch.len();
        quick_sort_batch(&mut batch, self.program.sort_on, 0, batch_len);
        Ok((WrittenRowsIterator::new(batch), snapshot))
    }
}

fn quick_sort_batch(batch: &mut Batch, sort_on: Vec<SortVariable>, start: usize, end: usize) {
    // todo!("Implement")
}
