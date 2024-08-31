/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    pipeline::{PipelineError, StageAPI, StageIterator, WrittenRowsIterator},
    write::insert::InsertExecutor,
};
use crate::batch::{Batch, FixedBatch};
use crate::row::MaybeOwnedRow;

pub struct InsertStageExecutor<Snapshot: WritableSnapshot + 'static, PreviouStage: StageAPI<Snapshot>> {
    inserter: InsertExecutor,
    previous: PreviouStage,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> InsertStageExecutor<Snapshot, PreviousStage>
    where
        Snapshot: WritableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(inserter: InsertExecutor, previous: PreviousStage) -> Self {
        Self { inserter, previous, snapshot: PhantomData::default() }
    }

    fn prepare_output_rows(output_width: u32, input_iterator: PreviousStage::OutputIterator) -> Result<Batch, PipelineError> {
        // TODO: if the previous stage is not already in Collected format, this will end up temporarily allocating 2x
        //       the current memory. However, in the other case we don't know how many rows in the output batch to allocate ahead of time
        //       and require resizing. For now we take the simpler strategy that doesn't require resizing.
        let input_batch = input_iterator.collect_owned()?;
        let total_output_rows: u64 = input_batch.get_multiplicities().iter().sum();
        let mut output_batch = Batch::new(output_width, total_output_rows as usize);
        let mut input_batch_iterator = input_batch.into_iterator();
        while let Some(row) = input_batch_iterator.next() {
            let mut row = row.map_err(|err| PipelineError::ConceptRead(err.clone()))?;
            // copy out row multiplicity M, set it to 1, then append the row M times
            let multiplicity = row.get_multiplicity();
            row.set_multiplicity(1);
            for _ in 0..multiplicity {
                output_batch.append(MaybeOwnedRow::new_from_row(&row));
            }
        }
        Ok(output_batch)
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for InsertStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        let (previous_iterator, mut snapshot, mut thing_manager) = self.previous.into_iterator()?;
        let mut output_rows = Self::prepare_output_rows(self.inserter.output_width() as u32, previous_iterator)?;

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot_ref = Arc::get_mut(&mut snapshot).unwrap();
        let thing_manager_ref = Arc::get_mut(&mut thing_manager).unwrap();
        for index in 0..output_rows.len() {
            // TODO: parallelise!
            let mut row = output_rows.get_row_mut(index);
            self.inserter
                .execute_insert(snapshot_ref, thing_manager_ref, &mut row)
                .map_err(|err| PipelineError::WriteError(err))?;
        }
        Ok((WrittenRowsIterator::new(output_rows), snapshot, thing_manager))
    }
}
