/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use compiler::executable::delete::{
    executable::{DeleteExecutable, OptionalDelete},
    instructions::ConnectionInstruction,
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use resource::{
    constants::traversal::CHECK_INTERRUPT_FREQUENCY_ROWS,
    profile::{PatternProfile, StepProfile},
};
use storage::snapshot::WritableSnapshot;

use crate::{
    ExecutionInterrupt,
    pipeline::{
        PipelineExecutionError, StageIterator, WrittenRowsIterator,
        stage::{ExecutionContext, StageAPI},
    },
    row::Row,
    write::{WriteError, write_instruction::AsWriteInstruction},
};

pub struct DeleteStageExecutor<InputIterator> {
    executable: Arc<DeleteExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> DeleteStageExecutor<InputIterator> {
    pub fn new(executable: Arc<DeleteExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for DeleteStageExecutor<InputIterator>
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
        // accumulate once, then we will operate in-place
        let mut batch = match input_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };

        // TODO: all write stages will have the same block below: we could merge them
        let profile = context.profile.profile_stage(|| String::from("Delete"), self.executable.executable_id);
        let pattern_profile = profile.create_or_get_pattern(|| String::from("Delete"));
        let (connection_profiles, optional_connection_profiles, concept_profiles) =
            build_delete_step_profiles(&self.executable, &pattern_profile);

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot = Arc::get_mut(&mut context.snapshot).unwrap();
        // First delete connections
        for index in 0..batch.len() {
            let mut row = batch.get_row_mut(index);

            if let Err(typedb_source) = execute_delete_connections(
                &self.executable.connection_instructions,
                &connection_profiles,
                snapshot,
                &context.thing_manager,
                &context.parameters,
                &mut row,
            ) {
                return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
            }

            for (i, optional) in self.executable.optional_deletes.iter().enumerate() {
                if let Err(typedb_source) = execute_optional_delete(
                    optional,
                    &optional_connection_profiles[i],
                    snapshot,
                    &context.thing_manager,
                    &context.parameters,
                    &mut row,
                ) {
                    return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
                }
            }

            if index % CHECK_INTERRUPT_FREQUENCY_ROWS == 0 {
                if let Some(interrupt) = interrupt.check() {
                    return Err((Box::new(PipelineExecutionError::Interrupted { interrupt }), context));
                }
            }
        }

        // Then delete concepts
        for index in 0..batch.len() {
            let mut row = batch.get_row_mut(index);
            if let Err(typedb_source) = execute_delete_concepts(
                &self.executable,
                &concept_profiles,
                snapshot,
                &context.thing_manager,
                &context.parameters,
                &mut row,
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

#[allow(clippy::type_complexity)]
fn build_delete_step_profiles(
    executable: &DeleteExecutable,
    pattern_profile: &PatternProfile,
) -> (Vec<Arc<StepProfile>>, Vec<Vec<Arc<StepProfile>>>, Vec<Arc<StepProfile>>) {
    let mut next_index = 0;
    let connection_profiles =
        reserve_connection_step_profiles(pattern_profile, &executable.connection_instructions, &mut next_index);
    let mut optional_connection_profiles = Vec::with_capacity(executable.optional_deletes.len());
    for optional in &executable.optional_deletes {
        optional_connection_profiles.push(reserve_connection_step_profiles(
            pattern_profile,
            &optional.connection_instructions,
            &mut next_index,
        ));
    }
    // Concept-deletion step profiles share slot indices with the connection profiles
    // (existing behaviour: `execute_delete_concepts` enumerates from 0). Allocating from
    // index 0 reuses the slots already populated above.
    let mut concept_index = 0;
    let mut concept_profiles = Vec::with_capacity(executable.concept_instructions.len());
    for instruction in &executable.concept_instructions {
        concept_profiles.push(pattern_profile.extend_or_get_step(concept_index, || format!("{}", instruction)));
        concept_index += 1;
    }
    (connection_profiles, optional_connection_profiles, concept_profiles)
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

fn execute_optional_delete(
    optional: &OptionalDelete,
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
    execute_delete_connections(
        &optional.connection_instructions,
        connection_profiles,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    Ok(())
}

pub fn execute_delete_connections(
    connection_instructions: &[ConnectionInstruction],
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    input_output_row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    debug_assert_eq!(connection_instructions.len(), step_profiles.len());
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for (instruction, step_profile) in connection_instructions.iter().zip(step_profiles) {
        let counters = step_profile.storage_counters();
        let measurement = step_profile.start_measurement();
        match instruction {
            ConnectionInstruction::Has(has) => {
                has.execute(snapshot, thing_manager, parameters, input_output_row, counters)?
            }
            ConnectionInstruction::Links(role_player) => {
                role_player.execute(snapshot, thing_manager, parameters, input_output_row, counters)?
            }
        }
        measurement.end(step_profile, 1, 1);
    }
    Ok(())
}

pub fn execute_delete_concepts(
    executable: &DeleteExecutable,
    step_profiles: &[Arc<StepProfile>],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    input_output_row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    debug_assert_eq!(executable.concept_instructions.len(), step_profiles.len());
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for (instruction, step_profile) in executable.concept_instructions.iter().zip(step_profiles) {
        let counters = step_profile.storage_counters();
        let measurement = step_profile.start_measurement();
        instruction.execute(snapshot, thing_manager, parameters, input_output_row, counters)?;
        measurement.end(step_profile, 1, 1);
    }
    Ok(())
}
