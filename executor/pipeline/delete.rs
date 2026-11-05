/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Display, marker::PhantomData, sync::Arc};

use compiler::executable::delete::{
    executable::{ConditionalDelete, DeleteExecutable},
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
        PipelineExecutionError, RecursiveProfile, StageIterator, WrittenRowsIterator,
        stage::{ExecutionContext, StageAPI},
        write_condition_satisfied,
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
        let mut next_subpattern = 0;
        let recursive_profile =
            build_profiles_recursive(pattern_profile, &self.executable.root_delete, &mut next_subpattern);

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot = Arc::get_mut(&mut context.snapshot).unwrap();
        // First delete connections from ALL rows
        for index in 0..batch.len() {
            let mut row = batch.get_row_mut(index);

            if let Err(typedb_source) = may_execute_delete_connections_recursive(
                &self.executable.root_delete,
                &recursive_profile,
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

        // Then delete concepts from ALL rows
        for index in 0..batch.len() {
            let mut row = batch.get_row_mut(index);

            if let Err(typedb_source) = may_execute_delete_concepts_recursive(
                &self.executable.root_delete,
                &recursive_profile,
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

fn reserve_step_profiles<I: Display>(sub_pattern: &PatternProfile, instructions: &[I]) -> Vec<Arc<StepProfile>> {
    instructions
        .iter()
        .enumerate()
        .map(|(i, instruction)| sub_pattern.extend_or_get_step(i, || format!("{}", instruction)))
        .collect()
}

pub fn may_execute_delete_connections_recursive(
    delete: &ConditionalDelete,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, connection_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&delete.condition, row, snapshot, thing_manager, parameters, condition_profile)? {
        return Ok(());
    }

    // Row multiplicity doesn't matter. You can't delete the same thing twice
    debug_assert_eq!(delete.connection_instructions.len(), connection_instruction_profiles.len());
    for (instruction, step_profile) in delete.connection_instructions.iter().zip(connection_instruction_profiles.iter())
    {
        let counters = step_profile.storage_counters();
        let measurement = step_profile.start_measurement();
        match instruction {
            ConnectionInstruction::Has(has) => has.execute(snapshot, thing_manager, parameters, row, counters)?,
            ConnectionInstruction::Links(role_player) => {
                role_player.execute(snapshot, thing_manager, parameters, row, counters)?
            }
        }
        measurement.end(step_profile, 1, 1);
    }
    for (nested, nested_profile) in delete.nested_deletes.iter().zip(nested_profiles.iter()) {
        may_execute_delete_connections_recursive(nested, nested_profile, snapshot, thing_manager, parameters, row)?;
    }
    Ok(())
}

pub fn may_execute_delete_concepts_recursive(
    delete: &ConditionalDelete,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, concept_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&delete.condition, row, snapshot, thing_manager, parameters, condition_profile)? {
        return Ok(());
    }

    // Row multiplicity doesn't matter. You can't delete the same thing twice
    debug_assert_eq!(delete.concept_instructions.len(), concept_instruction_profiles.len());
    for (instruction, step_profile) in delete.concept_instructions.iter().zip(concept_instruction_profiles.iter()) {
        let counters = step_profile.storage_counters();
        let measurement = step_profile.start_measurement();
        instruction.execute(snapshot, thing_manager, parameters, row, counters)?;
        measurement.end(step_profile, 1, 1);
    }
    for (nested, nested_profile) in delete.nested_deletes.iter().zip(nested_profiles.iter()) {
        may_execute_delete_concepts_recursive(nested, nested_profile, snapshot, thing_manager, parameters, row)?;
    }
    Ok(())
}

pub(crate) fn build_profiles_recursive(
    pattern_profile: Arc<PatternProfile>,
    delete: &ConditionalDelete,
    next_subpattern: &mut usize,
) -> RecursiveProfile {
    let condition_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Condition pattern".to_owned());
    *next_subpattern += 1;
    let condition_profile = condition_subpattern.extend_or_get_step(0, || format!("Condition: {}", &delete.condition));

    let concept_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Concept instructions".to_owned());
    *next_subpattern += 1;
    let concept_instruction_profiles = reserve_step_profiles(&concept_subpattern, &delete.concept_instructions);

    let connection_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Connection instructions".to_owned());
    *next_subpattern += 1;
    let connection_instruction_profiles =
        reserve_step_profiles(&connection_subpattern, &delete.connection_instructions);

    let mut nested_profiles = Vec::with_capacity(delete.nested_deletes.len());
    for nested in &delete.nested_deletes {
        let subpattern_profile =
            pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Nested delete".to_owned());
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
