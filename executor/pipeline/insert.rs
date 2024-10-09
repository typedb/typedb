/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::insert::{
    executable::InsertExecutable,
    instructions::{ConceptInstruction, ConnectionInstruction},
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator, WrittenRowsIterator,
    },
    row::{MaybeOwnedRow, Row},
    write::{write_instruction::AsWriteInstruction, WriteError},
    ExecutionInterrupt,
};

pub struct InsertStageExecutor<PreviousStage> {
    executable: InsertExecutable,
    previous: PreviousStage,
}

impl<PreviousStage> InsertStageExecutor<PreviousStage> {
    pub fn new(executable: InsertExecutable, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for InsertStageExecutor<PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        mut interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { executable, previous } = self;
        let (previous_iterator, mut context) = previous.into_iterator(interrupt.clone())?;
        let mut batch = match prepare_output_rows(executable.output_width() as u32, previous_iterator) {
            Ok(output_rows) => output_rows,
            Err(err) => return Err((err, context)),
        };

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so we can get mut:
        let snapshot_mut = Arc::get_mut(&mut context.snapshot).unwrap();
        for index in 0..batch.len() {
            // TODO: parallelise -- though this requires our snapshots support parallel writes!
            let mut row = batch.get_row_mut(index);

            if let Err(err) =
                execute_insert(&executable, snapshot_mut, &context.thing_manager, &context.parameters, &mut row)
            {
                return Err((PipelineExecutionError::WriteError { typedb_source: err }, context));
            }

            if index % 100 == 0 {
                if let Some(interrupt) = interrupt.check() {
                    return Err((PipelineExecutionError::Interrupted { interrupt }, context));
                }
            }
        }

        Ok((WrittenRowsIterator::new(batch), context))
    }
}

fn prepare_output_rows(output_width: u32, input_iterator: impl StageIterator) -> Result<Batch, PipelineExecutionError> {
    // TODO: if the previous stage is not already in Collected format, this will end up temporarily allocating 2x
    //       the current memory. However, in the other case we don't know how many rows in the output batch to allocate ahead of time
    //       and require resizing. For now we take the simpler strategy that doesn't require resizing.
    let input_batch = input_iterator.collect_owned()?;
    let total_output_rows: u64 = input_batch.get_multiplicities().iter().sum();
    let mut output_batch = Batch::new(output_width, total_output_rows as usize);
    let mut input_batch_iterator = input_batch.into_iterator_mut();
    while let Some(mut row) = input_batch_iterator.next() {
        // copy out row multiplicity M, set it to 1, then append the row M times
        let multiplicity = row.get_multiplicity();
        row.set_multiplicity(1);
        for _ in 0..multiplicity {
            output_batch.append(MaybeOwnedRow::new_from_row(&row));
        }
    }
    Ok(output_batch)
}

fn execute_insert(
    executable: &InsertExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), WriteError> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());
    for instruction in &executable.concept_instructions {
        match instruction {
            ConceptInstruction::PutAttribute(isa_attr) => {
                isa_attr.execute(snapshot, thing_manager, parameters, row)?;
            }
            ConceptInstruction::PutObject(isa_object) => {
                isa_object.execute(snapshot, thing_manager, parameters, row)?;
            }
        }
    }
    for instruction in &executable.connection_instructions {
        match instruction {
            ConnectionInstruction::Has(has) => {
                has.execute(snapshot, thing_manager, parameters, row)?;
            }
            ConnectionInstruction::RolePlayer(role_player) => {
                role_player.execute(snapshot, thing_manager, parameters, row)?;
            }
        };
    }
    Ok(())
}
