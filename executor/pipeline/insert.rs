/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::insert::{
        executable::{InsertExecutable, OptionalInsert},
        instructions::{ConceptInstruction, ConnectionInstruction},
        VariableSource,
    },
    VariablePosition,
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use resource::{
    constants::traversal::{BATCH_DEFAULT_CAPACITY, CHECK_INTERRUPT_FREQUENCY_ROWS},
    profile::StageProfile,
};
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
    executable: Arc<InsertExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> InsertStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<InsertExecutable>, previous: PreviousStage) -> Self {
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
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { executable, previous } = self;
        let (previous_iterator, mut context) = previous.into_iterator(interrupt.clone())?;

        let profile = context.profile.profile_stage(|| String::from("Insert"), executable.executable_id);

        // prepare_output_rows copies unmapped
        let input_output_mapping = executable
            .output_row_schema
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| match entry {
                Some((_, VariableSource::Input(src))) => Some((*src, VariablePosition::new(i as u32))),
                Some((_, VariableSource::Inserted)) | None => None,
            })
            .collect();
        let mut batch =
            match prepare_output_rows(executable.output_width() as u32, previous_iterator, &input_output_mapping) {
                Ok(output_rows) => output_rows,
                Err(err) => return Err((err, context)),
            };

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so we can get mut:
        let snapshot_mut = Arc::get_mut(&mut context.snapshot).unwrap();
        for index in 0..batch.len() {
            // TODO: parallelise -- though this requires our snapshots support parallel writes!
            let mut row = batch.get_row_mut(index);

            if let Err(typedb_source) = execute_insert(
                &executable,
                snapshot_mut,
                &context.thing_manager,
                &context.parameters,
                &mut row,
                &profile,
            ) {
                return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
            }

            if index % CHECK_INTERRUPT_FREQUENCY_ROWS == 0 {
                if let Some(interrupt) = interrupt.check() {
                    return Err((Box::new(PipelineExecutionError::Interrupted { interrupt }), context));
                }
            }
        }
        Ok((WrittenRowsIterator::new(batch), context))
    }
}

pub(crate) fn prepare_output_rows(
    output_width: u32,
    mut input_iterator: impl StageIterator,
    mapping: &Vec<(VariablePosition, VariablePosition)>,
) -> Result<Batch, Box<PipelineExecutionError>> {
    let initial_output_batch_size = input_iterator.multiplicity_sum_if_collected().unwrap_or(BATCH_DEFAULT_CAPACITY);
    let mut output_batch = Batch::new(output_width, initial_output_batch_size);
    while let Some(row) = input_iterator.next().transpose()? {
        append_row_for_insert_mapped(&mut output_batch, row.as_reference(), mapping);
    }
    debug_assert!(
        initial_output_batch_size == BATCH_DEFAULT_CAPACITY || initial_output_batch_size == output_batch.len()
    );
    Ok(output_batch)
}

pub(crate) fn append_row_for_insert_mapped(
    output_batch: &mut Batch,
    unmapped_row: MaybeOwnedRow<'_>,
    mapping: &[(VariablePosition, VariablePosition)],
) {
    // copy out row multiplicity M, set it to 1, then append the row M times
    let one = 1;
    let provenance = unmapped_row.provenance();
    let with_multiplicity_one = MaybeOwnedRow::new_borrowed(unmapped_row.row(), &one, &provenance);
    for _ in 0..unmapped_row.multiplicity() {
        output_batch.append(|mut appended| {
            appended.copy_mapped(with_multiplicity_one.as_reference(), mapping.iter().copied());
        })
    }
}

pub(crate) fn execute_insert(
    executable: &InsertExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    stage_profile: &StageProfile,
) -> Result<(), Box<WriteError>> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());
    let mut profile_index = 0;
    execute_concept_instructions(
        &executable.concept_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        stage_profile,
        &mut profile_index,
    )?;
    execute_connection_instructions(
        &executable.connection_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        stage_profile,
        &mut profile_index,
    )?;
    for optional in &executable.optional_inserts {
        execute_optional_insert(optional, snapshot, thing_manager, parameters, row, stage_profile, &mut profile_index)?;
    }
    Ok(())
}

fn execute_optional_insert(
    optional: &OptionalInsert,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    stage_profile: &StageProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    for &input in &optional.required_input_variables {
        if row.len() <= input.as_usize() || row.get(input).is_none() {
            return Ok(());
        }
    }
    execute_concept_instructions(
        &optional.concept_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        stage_profile,
        profile_index,
    )?;
    execute_connection_instructions(
        &optional.connection_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        stage_profile,
        profile_index,
    )?;
    Ok(())
}

fn execute_connection_instructions(
    connection_instructions: &[ConnectionInstruction],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    stage_profile: &StageProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    for instruction in connection_instructions {
        let step_profile = stage_profile.extend_or_get(*profile_index, || format!("{}", instruction));
        let measurement = step_profile.start_measurement();
        match instruction {
            ConnectionInstruction::Has(has) => {
                has.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
            ConnectionInstruction::Links(role_player) => {
                role_player.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        };
        measurement.end(&step_profile, 1, 1);
        *profile_index += 1;
    }
    Ok(())
}

fn execute_concept_instructions(
    concept_instructions: &[ConceptInstruction],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    stage_profile: &StageProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    for instruction in concept_instructions {
        let step_profile = stage_profile.extend_or_get(*profile_index, || format!("{}", instruction));
        let measurement = step_profile.start_measurement();
        match instruction {
            ConceptInstruction::PutAttribute(isa_attr) => {
                isa_attr.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
            ConceptInstruction::PutObject(isa_object) => {
                isa_object.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        }
        measurement.end(&step_profile, 1, 1);
        *profile_index += 1;
    }
    Ok(())
}
