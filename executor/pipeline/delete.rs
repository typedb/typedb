/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    pipeline::{
        accumulator::{AccumulatedRowIterator, AccumulatingStageAPI, Accumulator},
        common::PipelineStageExecutor,
        stage_wrappers::WritePipelineStage,
        PipelineContext, PipelineError,
    },
    write::delete::DeleteExecutor,
};

pub type DeleteAccumulator<Snapshot: WritableSnapshot + 'static> =
    Accumulator<Snapshot, WritePipelineStage<Snapshot>, DeleteExecutor>;

pub type DeleteStage<Snapshot: WritableSnapshot + 'static> = PipelineStageExecutor<
    Snapshot,
    DeleteAccumulator<Snapshot>,
>;

impl<Snapshot: WritableSnapshot + 'static> DeleteStage<Snapshot> {
    pub fn new(upstream: Box<WritePipelineStage<Snapshot>>, executor: DeleteExecutor) -> DeleteStage<Snapshot> {
        Self::new_impl(Accumulator::new(upstream, executor))
    }
}

impl<Snapshot: WritableSnapshot + 'static> AccumulatingStageAPI<Snapshot> for DeleteExecutor {
    fn process_accumulated(
        &self,
        context: &mut PipelineContext<Snapshot>,
        rows: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError> {
        let (snapshot, thing_manager) = context.borrow_parts_mut();
        for (row, multiplicity) in rows {
            self.execute_delete(snapshot, thing_manager, &mut Row::new(row, multiplicity))
                .map_err(PipelineError::WriteError)?;
        }
        Ok(())
    }

    fn must_deduplicate_incoming_rows(&self) -> bool {
        false
    }

    fn row_width(&self) -> usize {
        self.program().output_row_schema.len() + self.program().concept_instructions.len()
    }
}
