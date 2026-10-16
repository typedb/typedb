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
        PipelineExecutionError, RecursiveProfile, StageIterator, WrittenRowsIterator,
        stage::{ExecutionContext, StageAPI},
        write_condition_satisfied,
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

        let stage_profile = context.profile.profile_stage(|| String::from("Insert"), executable.executable_id);
        let mut next_subpattern: usize = 0;
        let pattern_profile = stage_profile.create_or_get_pattern(|| String::from("Insert pattern"));
        let recursive_profile =
            build_profiles_recursive(pattern_profile, &executable.root_insert, &mut next_subpattern);

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
                &recursive_profile,
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

pub(crate) fn build_profiles_recursive(
    pattern_profile: Arc<PatternProfile>,
    insert: &ConditionalInsert,
    next_subpattern: &mut usize,
) -> RecursiveProfile {
    let condition_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Condition pattern".to_owned());
    *next_subpattern += 1;
    let condition_profile = condition_subpattern.extend_or_get_step(0, || format!("Condition: {}", &insert.condition));

    let concept_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Concept instructions".to_owned());
    *next_subpattern += 1;
    let concept_instruction_profiles = reserve_step_profiles(&concept_subpattern, &insert.concept_instructions);

    let connection_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Connection instructions".to_owned());
    *next_subpattern += 1;
    let connection_instruction_profiles =
        reserve_step_profiles(&connection_subpattern, &insert.connection_instructions);

    let mut nested_profiles = Vec::with_capacity(insert.nested_inserts.len());
    for nested in &insert.nested_inserts {
        let subpattern_profile =
            pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Nested insert".to_owned());
        *next_subpattern += 1;
        nested_profiles.push(build_profiles_recursive(subpattern_profile, nested, next_subpattern))
    }
    RecursiveProfile {
        condition_profile,
        concept_instruction_profiles,
        connection_instruction_profiles,
        nested_profiles,
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
    root_profile: &RecursiveProfile,
) -> Result<(), Box<WriteError>> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());
    // First all the concept inserts
    may_execute_concept_instructions_recursive(
        &executable.root_insert,
        &root_profile,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;

    // Then all the connection inserts
    may_execute_connection_instructions_recursive(
        &executable.root_insert,
        &root_profile,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    Ok(())
}

fn may_execute_concept_instructions_recursive(
    insert: &ConditionalInsert,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, concept_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&insert.condition, row, snapshot, thing_manager, parameters, condition_profile)? {
        return Ok(());
    }

    debug_assert_eq!(insert.concept_instructions.len(), concept_instruction_profiles.len());
    for (instruction, step_profile) in insert.concept_instructions.iter().zip(concept_instruction_profiles.iter()) {
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

    for (nested, subprofile) in insert.nested_inserts.iter().zip(nested_profiles.iter()) {
        may_execute_concept_instructions_recursive(nested, subprofile, snapshot, thing_manager, parameters, row)?;
    }
    Ok(())
}

fn may_execute_connection_instructions_recursive(
    insert: &ConditionalInsert,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, connection_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&insert.condition, row, snapshot, thing_manager, parameters, &condition_profile)? {
        return Ok(());
    }

    debug_assert_eq!(insert.connection_instructions.len(), connection_instruction_profiles.len());
    for (instruction, step_profile) in insert.connection_instructions.iter().zip(connection_instruction_profiles.iter())
    {
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

    for (nested, subprofile) in insert.nested_inserts.iter().zip(nested_profiles.iter()) {
        may_execute_connection_instructions_recursive(nested, subprofile, snapshot, thing_manager, parameters, row)?;
    }

    Ok(())
}
