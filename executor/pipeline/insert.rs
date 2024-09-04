/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use compiler::VariablePosition;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{PipelineExecutionError, StageAPI, StageIterator, WrittenRowsIterator},
    row::MaybeOwnedRow,
    write::insert::InsertExecutor,
};

pub struct InsertStageExecutor<Snapshot: WritableSnapshot + 'static, PreviouStage: StageAPI<Snapshot>> {
    inserter: InsertExecutor,
    previous: PreviouStage,
    thing_manager: Arc<ThingManager>,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> InsertStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(inserter: InsertExecutor, previous: PreviousStage, thing_manager: Arc<ThingManager>) -> Self {
        Self { inserter, previous, thing_manager, snapshot: PhantomData::default() }
    }

    fn prepare_output_rows(
        output_width: u32,
        input_iterator: PreviousStage::OutputIterator,
    ) -> Result<Batch, PipelineExecutionError> {
        // TODO: if the previous stage is not already in Collected format, this will end up temporarily allocating 2x
        //       the current memory. However, in the other case we don't know how many rows in the output batch to allocate ahead of time
        //       and require resizing. For now we take the simpler strategy that doesn't require resizing.
        let input_batch = input_iterator.collect_owned()?;
        let total_output_rows: u64 = input_batch.get_multiplicities().iter().sum();
        let mut output_batch = Batch::new(output_width, total_output_rows as usize);
        let mut input_batch_iterator = input_batch.into_iterator_mut();
        while let Some(row) = input_batch_iterator.next() {
            let mut row = row.map_err(|err| PipelineExecutionError::ConceptRead { source: err.clone() })?;
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

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        (0..self.inserter.output_width())
            .filter_map(|position| {
                let variable = self.inserter.program().output_row_schema[position].0;
                self.inserter
                    .program()
                    .variable_registry
                    .variable_names()
                    .get(&variable)
                    .map(|name| (VariablePosition::new(position as u32), name.to_string()))
            })
            .collect()
    }

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let (previous_iterator, mut snapshot) = self.previous.into_iterator()?;
        let mut output_rows = match Self::prepare_output_rows(self.inserter.output_width() as u32, previous_iterator) {
            Ok(output_rows) => output_rows,
            Err(err) => return Err((snapshot, err)),
        };

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so we can get mut:
        let snapshot_ref = Arc::get_mut(&mut snapshot).unwrap();
        for index in 0..output_rows.len() {
            // TODO: parallelise -- though this requires our snapshots support parallel writes!
            let mut row = output_rows.get_row_mut(index);
            match self.inserter.execute_insert(snapshot_ref, self.thing_manager.as_ref(), &mut row) {
                Ok(_) => {}
                Err(err) => return Err((snapshot, PipelineExecutionError::WriteError { source: err })),
            }
        }
        Ok((WrittenRowsIterator::new(output_rows), snapshot))
    }
}
