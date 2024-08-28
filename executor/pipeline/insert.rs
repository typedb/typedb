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
        common::PipelineStageCommon,
        stage_wrappers::WritePipelineStage,
        PipelineContext, PipelineError,
    },
    write::insert::InsertExecutor,
};

pub type InsertAccumulator<Snapshot: WritableSnapshot + 'static> =
    Accumulator<Snapshot, WritePipelineStage<Snapshot>, InsertExecutor>;

pub type InsertStage<Snapshot: WritableSnapshot + 'static> = PipelineStageCommon<
    Snapshot,
    WritePipelineStage<Snapshot>,
    InsertAccumulator<Snapshot>,
    AccumulatedRowIterator<Snapshot>,
>;
impl<Snapshot: WritableSnapshot + 'static> InsertStage<Snapshot> {
    pub fn new(upstream: Box<WritePipelineStage<Snapshot>>, executor: InsertExecutor) -> InsertStage<Snapshot> {
        Self::new_impl(Accumulator::new(upstream, executor))
    }
}

impl<Snapshot: WritableSnapshot + 'static> AccumulatingStageAPI<Snapshot> for InsertExecutor {
    fn process_accumulated(
        &self,
        context: &mut PipelineContext<Snapshot>,
        rows: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError> {
        let (snapshot, thing_manager) = context.borrow_parts_mut();
        for (row, multiplicity) in rows {
            self.execute_insert(snapshot, thing_manager, &mut Row::new(row, multiplicity))
                .map_err(|source| PipelineError::WriteError(source))?;
        }
        Ok(())
    }

    fn must_deduplicate_incoming_rows(&self) -> bool {
        true
    }

    fn row_width(&self) -> usize {
        self.plan().output_row_schema.len()
    }
}
