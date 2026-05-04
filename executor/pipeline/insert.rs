/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use compiler::{
    VariablePosition,
    executable::insert::{
        VariableSource,
        executable::{InsertExecutable, OptionalInsert},
        instructions::{ConceptInstruction, ConnectionInstruction},
    },
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::{
    constants::traversal::{BATCH_DEFAULT_CAPACITY, CHECK_INTERRUPT_FREQUENCY_ROWS},
    profile::{PatternProfile, StepProfile},
};
use storage::snapshot::WritableSnapshot;

use crate::{
    ExecutionInterrupt,
    batch::Batch,
    pipeline::{
        PipelineExecutionError, StageIterator, WrittenRowsIterator,
        stage::{ExecutionContext, StageAPI},
    },
    row::{MaybeOwnedRow, Row},
    write::{WriteError, write_instruction::AsWriteInstruction},
};

pub struct InsertStageExecutor<InputIterator> {
    executable: Arc<InsertExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> InsertStageExecutor<InputIterator> {
    pub fn new(executable: Arc<InsertExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for InsertStageExecutor<InputIterator>
where
    Snapshot: WritableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        mut context: ExecutionContext<Snapshot>,
        mut interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { executable, .. } = self;

        let profile = context.profile.profile_stage(|| String::from("Insert"), executable.executable_id);
        let pattern_profile = profile.create_or_get_pattern(|| String::from("Insert pattern"));
        let (concept_profiles, connection_profiles, optional_concept_profiles, optional_connection_profiles) =
            build_insert_step_profiles(&executable, &pattern_profile);

        // prepare_output_rows copies unmapped
        let input_output_mapping = executable
            .output_row_schema
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| match entry {
                Some((_, VariableSource::Input(src))) => Some((*src, VariablePosition::new(i as u32))),
                Some((_, VariableSource::Inserted)) | None => None,
            })
            .collect_vec();
        let mut batch =
            match prepare_output_rows(executable.output_width() as u32, input_iterator, &input_output_mapping) {
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
                &concept_profiles,
                &connection_profiles,
                &optional_concept_profiles,
                &optional_connection_profiles,
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
    mapping: &[(VariablePosition, VariablePosition)],
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

#[allow(clippy::type_complexity)]
pub(crate) fn build_insert_step_profiles(
    executable: &InsertExecutable,
    pattern_profile: &PatternProfile,
) -> (Vec<Arc<StepProfile>>, Vec<Arc<StepProfile>>, Vec<Vec<Arc<StepProfile>>>, Vec<Vec<Arc<StepProfile>>>) {
    let mut next_index = 0;
    let concept_profiles =
        reserve_concept_step_profiles(pattern_profile, &executable.concept_instructions, &mut next_index);
    let connection_profiles =
        reserve_connection_step_profiles(pattern_profile, &executable.connection_instructions, &mut next_index);
    let mut optional_concept_profiles = Vec::with_capacity(executable.optional_inserts.len());
    let mut optional_connection_profiles = Vec::with_capacity(executable.optional_inserts.len());
    for optional in &executable.optional_inserts {
        optional_concept_profiles.push(reserve_concept_step_profiles(
            pattern_profile,
            &optional.concept_instructions,
            &mut next_index,
        ));
        optional_connection_profiles.push(reserve_connection_step_profiles(
            pattern_profile,
            &optional.connection_instructions,
            &mut next_index,
        ));
    }
    (concept_profiles, connection_profiles, optional_concept_profiles, optional_connection_profiles)
}

fn reserve_concept_step_profiles(
    pattern_profile: &PatternProfile,
    instructions: &[ConceptInstruction],
    next_index: &mut usize,
) -> Vec<Arc<StepProfile>> {
    let mut profiles = Vec::with_capacity(instructions.len());
    for instruction in instructions {
        profiles.push(pattern_profile.extend_or_get_step(*next_index, || format!("{}", instruction)));
        *next_index += 1;
    }
    profiles
}

fn reserve_connection_step_profiles(
    pattern_profile: &PatternProfile,
    instructions: &[ConnectionInstruction],
    next_index: &mut usize,
) -> Vec<Arc<StepProfile>> {
    let mut profiles = Vec::with_capacity(instructions.len());
    for instruction in instructions {
        profiles.push(pattern_profile.extend_or_get_step(*next_index, || format!("{}", instruction)));
        *next_index += 1;
    }
    profiles
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_insert(
    executable: &InsertExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    concept_profiles: &[Arc<StepProfile>],
    connection_profiles: &[Arc<StepProfile>],
    optional_concept_profiles: &[Vec<Arc<StepProfile>>],
    optional_connection_profiles: &[Vec<Arc<StepProfile>>],
) -> Result<(), Box<WriteError>> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());
    execute_concept_instructions(
        &executable.concept_instructions,
        concept_profiles,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    execute_connection_instructions(
        &executable.connection_instructions,
        connection_profiles,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    for (i, optional) in executable.optional_inserts.iter().enumerate() {
        execute_optional_insert(
            optional,
            &optional_concept_profiles[i],
            &optional_connection_profiles[i],
            snapshot,
            thing_manager,
            parameters,
            row,
        )?;
    }
    Ok(())
}

fn execute_optional_insert(
    optional: &OptionalInsert,
    concept_profiles: &[Arc<StepProfile>],
    connection_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    for &input in &optional.required_input_variables {
        if row.len() <= input.as_usize() || row.get(input).is_none() {
            return Ok(());
        }
    }
    execute_concept_instructions(
        &optional.concept_instructions,
        concept_profiles,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    execute_connection_instructions(
        &optional.connection_instructions,
        connection_profiles,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    Ok(())
}

fn execute_concept_instructions(
    concept_instructions: &[ConceptInstruction],
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    debug_assert_eq!(concept_instructions.len(), step_profiles.len());
    for (instruction, step_profile) in concept_instructions.iter().zip(step_profiles) {
        let measurement = step_profile.start_measurement();
        match instruction {
            ConceptInstruction::PutAttribute(isa_attr) => {
                isa_attr.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
            ConceptInstruction::PutObject(isa_object) => {
                isa_object.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        }
        measurement.end(step_profile, 1, 1);
    }
    Ok(())
}

fn execute_connection_instructions(
    connection_instructions: &[ConnectionInstruction],
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    debug_assert_eq!(connection_instructions.len(), step_profiles.len());
    for (instruction, step_profile) in connection_instructions.iter().zip(step_profiles) {
        let measurement = step_profile.start_measurement();
        match instruction {
            ConnectionInstruction::Has(has) => {
                has.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
            ConnectionInstruction::Links(role_player) => {
                role_player.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        };
        measurement.end(step_profile, 1, 1);
    }
    Ok(())
}
