/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::delete::{
    executable::{DeleteExecutable, OptionalDelete},
    instructions::ConnectionInstruction,
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use resource::{constants::traversal::CHECK_INTERRUPT_FREQUENCY_ROWS, profile::StageProfile};
use storage::snapshot::WritableSnapshot;

use crate::{
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator, WrittenRowsIterator,
    },
    row::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
    ExecutionInterrupt,
};

pub struct DeleteStageExecutor<PreviousStage> {
    executable: Arc<DeleteExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> DeleteStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<DeleteExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for DeleteStageExecutor<PreviousStage>
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
        let (previous_iterator, mut context) = self.previous.into_iterator(interrupt.clone())?;
        // accumulate once, then we will operate in-place
        let mut batch = match previous_iterator.collect_owned() {
            Ok(batch) => batch,
            Err(err) => return Err((err, context)),
        };

        // TODO: all write stages will have the same block below: we could merge them
        let profile = context.profile.profile_stage(|| String::from("Delete"), self.executable.executable_id);

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot = Arc::get_mut(&mut context.snapshot).unwrap();
        // First delete connections
        for index in 0..batch.len() {
            let mut row = batch.get_row_mut(index);

            let mut profile_index = 0;

            if let Err(typedb_source) = execute_delete_connections(
                &self.executable.connection_instructions,
                snapshot,
                &context.thing_manager,
                &context.parameters,
                &mut row,
                &profile,
                &mut profile_index,
            ) {
                return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
            }

            for optional in &self.executable.optional_deletes {
                if let Err(typedb_source) = execute_optional_delete(
                    optional,
                    snapshot,
                    &context.thing_manager,
                    &context.parameters,
                    &mut row,
                    &profile,
                    &mut profile_index,
                ) {
                    return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
                }
            }

            if profile_index % CHECK_INTERRUPT_FREQUENCY_ROWS == 0 {
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
                snapshot,
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

fn execute_optional_delete(
    optional: &OptionalDelete,
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
    execute_delete_connections(
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

pub fn execute_delete_connections(
    connection_instructions: &[ConnectionInstruction],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    input_output_row: &mut Row<'_>,
    stage_profile: &StageProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for instruction in connection_instructions {
        let step_profile = stage_profile.extend_or_get(*profile_index, || format!("{}", instruction));
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
        measurement.end(&step_profile, 1, 1);
        *profile_index += 1;
    }
    Ok(())
}

pub fn execute_delete_concepts(
    executable: &DeleteExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    input_output_row: &mut Row<'_>,
    stage_profile: &StageProfile,
) -> Result<(), Box<WriteError>> {
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for (index, instruction) in executable.concept_instructions.iter().enumerate() {
        let step_profile = stage_profile.extend_or_get(index, || format!("{}", instruction));
        let counters = step_profile.storage_counters();
        let measurement = step_profile.start_measurement();
        instruction.execute(snapshot, thing_manager, parameters, input_output_row, counters)?;
        measurement.end(&step_profile, 1, 1);
    }
    Ok(())
}
