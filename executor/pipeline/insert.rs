/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Display, marker::PhantomData, sync::Arc};

use compiler::{
    VariablePosition,
    executable::insert::{
        VariableSource,
        executable::{ConditionalInsert, InsertExecutable},
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
        PipelineExecutionError, StageIterator, WrittenRowsIterator, required_inputs_satisfied,
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
        let (concept_profiles, connection_profiles) = build_step_profiles(&executable, &pattern_profile);

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

/// Build the step-profile vecs once at stage start. Each section (concept,
/// connection, and per-optional concept/connection) gets its own subpattern
/// under the stage's pattern profile, with step indices restarted at 0 inside
/// it — so the profile output groups instructions by section in the tree.
#[allow(clippy::type_complexity)]
pub(crate) fn build_step_profiles(
    executable: &InsertExecutable,
    pattern_profile: &PatternProfile,
) -> (Vec<Vec<Arc<StepProfile>>>, Vec<Vec<Arc<StepProfile>>>) {
    let mut next_subpattern: usize = 0;

    let mut concept_profiles = Vec::with_capacity(executable.inserts.len());
    let mut connection_profiles = Vec::with_capacity(executable.inserts.len());
    for (i, optional) in executable.inserts.iter().enumerate() {
        let concept_subpattern =
            pattern_profile.extend_or_get_subpattern(next_subpattern, || format!("Inserts {i} concept"));
        next_subpattern += 1;
        concept_profiles.push(reserve_step_profiles(&concept_subpattern, &optional.concept_instructions));

        let connection_subpattern =
            pattern_profile.extend_or_get_subpattern(next_subpattern, || format!("Inserts {i} connection"));
        next_subpattern += 1;
        connection_profiles.push(reserve_step_profiles(&connection_subpattern, &optional.connection_instructions));
    }
    (concept_profiles, connection_profiles)
}

fn reserve_step_profiles<I: Display>(sub_pattern: &PatternProfile, instructions: &[I]) -> Vec<Arc<StepProfile>> {
    instructions
        .iter()
        .enumerate()
        .map(|(i, instruction)| sub_pattern.extend_or_get_step(i, || format!("{}", instruction)))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_insert(
    executable: &InsertExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    concept_profiles: &[Vec<Arc<StepProfile>>],
    connection_profiles: &[Vec<Arc<StepProfile>>],
) -> Result<(), Box<WriteError>> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());
    debug_assert_eq!(executable.inserts.len(), concept_profiles.len());
    debug_assert_eq!(executable.inserts.len(), connection_profiles.len());
    // First all the concept inserts
    for (i, insert) in executable.inserts.iter().enumerate() {
        may_execute_concept_instructions(&insert, &concept_profiles[i], snapshot, thing_manager, parameters, row)?;
    }

    // Then all the connection inserts
    for (i, insert) in executable.inserts.iter().enumerate() {
        may_execute_connection_instructions(
            &insert,
            &connection_profiles[i],
            snapshot,
            thing_manager,
            parameters,
            row,
        )?;
    }
    Ok(())
}

fn may_execute_concept_instructions(
    insert: &ConditionalInsert,
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    if !required_inputs_satisfied(&insert.required_input_variables, row) {
        return Ok(());
    }

    debug_assert_eq!(insert.concept_instructions.len(), step_profiles.len());
    for (instruction, step_profile) in insert.concept_instructions.iter().zip(step_profiles) {
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

fn may_execute_connection_instructions(
    insert: &ConditionalInsert,
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    if !required_inputs_satisfied(&insert.required_input_variables, row) {
        return Ok(());
    }

    debug_assert_eq!(insert.connection_instructions.len(), step_profiles.len());
    for (instruction, step_profile) in insert.connection_instructions.iter().zip(step_profiles) {
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
