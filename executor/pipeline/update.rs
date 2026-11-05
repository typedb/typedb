/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Display, marker::PhantomData, sync::Arc};

use compiler::{
    VariablePosition,
    executable::{
        insert::{VariableSource, instructions::ConceptInstruction},
        update::{
            executable::{ConditionalUpdate, UpdateExecutable},
            instructions::ConnectionInstruction,
        },
    },
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::{
    constants::traversal::CHECK_INTERRUPT_FREQUENCY_ROWS,
    profile::{PatternProfile, StepProfile},
};
use storage::snapshot::WritableSnapshot;

use crate::{
    ExecutionInterrupt,
    pipeline::{
        PipelineExecutionError, RecursiveProfile, WrittenRowsIterator,
        insert::prepare_output_rows,
        stage::{ExecutionContext, StageAPI, StageIterator},
        write_condition_satisfied,
    },
    row::Row,
    write::{WriteError, write_instruction::AsWriteInstruction},
};

pub struct UpdateStageExecutor<InputIterator> {
    executable: Arc<UpdateExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> UpdateStageExecutor<InputIterator> {
    pub fn new(executable: Arc<UpdateExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for UpdateStageExecutor<InputIterator>
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

        let profile = context.profile.profile_stage(|| String::from("Update"), executable.executable_id);
        let pattern_profile = profile.create_or_get_pattern(|| String::from("Update pattern"));
        let mut next_subpattern = 0;
        let recursive_profile =
            build_profiles_recursive(pattern_profile, &executable.root_update, &mut next_subpattern);

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

            if let Err(typedb_source) = execute_update(
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

fn reserve_step_profiles<I: Display>(sub_pattern: &PatternProfile, instructions: &[I]) -> Vec<Arc<StepProfile>> {
    instructions
        .iter()
        .enumerate()
        .map(|(i, instruction)| sub_pattern.extend_or_get_step(i, || format!("{}", instruction)))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn execute_update(
    executable: &UpdateExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    profile: &RecursiveProfile,
) -> Result<(), Box<WriteError>> {
    debug_assert!(row.get_multiplicity() == 1);
    debug_assert!(row.len() == executable.output_row_schema.len());

    // First the concept instructions
    may_execute_concept_instructions_recursive(
        &executable.root_update,
        profile,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    // Then the connection instructions
    may_execute_connection_instructions_recursive(
        &executable.root_update,
        profile,
        snapshot,
        thing_manager,
        parameters,
        row,
    )?;
    Ok(())
}

fn may_execute_concept_instructions_recursive(
    update: &ConditionalUpdate,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, concept_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&update.condition, row, snapshot, thing_manager, parameters, condition_profile)? {
        return Ok(());
    }

    debug_assert_eq!(update.concept_instructions.len(), concept_instruction_profiles.len());
    for (instruction, step_profile) in update.concept_instructions.iter().zip(concept_instruction_profiles.iter()) {
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

    for (nested, nested_profile) in update.nested_updates.iter().zip(nested_profiles.iter()) {
        may_execute_concept_instructions_recursive(nested, nested_profile, snapshot, thing_manager, parameters, row)?;
    }
    Ok(())
}

fn may_execute_connection_instructions_recursive(
    update: &ConditionalUpdate,
    profile: &RecursiveProfile,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
) -> Result<(), Box<WriteError>> {
    let RecursiveProfile { condition_profile, connection_instruction_profiles, nested_profiles, .. } = profile;
    if !write_condition_satisfied(&update.condition, row, snapshot, thing_manager, parameters, condition_profile)? {
        return Ok(());
    }

    debug_assert_eq!(update.connection_instructions.len(), connection_instruction_profiles.len());
    for (instruction, step_profile) in update.connection_instructions.iter().zip(connection_instruction_profiles.iter())
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
    for (nested, nested_profile) in update.nested_updates.iter().zip(nested_profiles.iter()) {
        may_execute_connection_instructions_recursive(
            nested,
            nested_profile,
            snapshot,
            thing_manager,
            parameters,
            row,
        )?;
    }
    Ok(())
}

pub(crate) fn build_profiles_recursive(
    pattern_profile: Arc<PatternProfile>,
    update: &ConditionalUpdate,
    next_subpattern: &mut usize,
) -> RecursiveProfile {
    let condition_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Condition pattern".to_owned());
    *next_subpattern += 1;
    let condition_profile = condition_subpattern.extend_or_get_step(0, || format!("Condition: {}", &update.condition));

    let concept_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Concept instructions".to_owned());
    *next_subpattern += 1;
    let concept_instruction_profiles = reserve_step_profiles(&concept_subpattern, &update.concept_instructions);

    let connection_subpattern =
        pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Connection instructions".to_owned());
    *next_subpattern += 1;
    let connection_instruction_profiles =
        reserve_step_profiles(&connection_subpattern, &update.connection_instructions);

    let mut nested_profiles = Vec::with_capacity(update.nested_updates.len());
    for nested in &update.nested_updates {
        let subpattern_profile =
            pattern_profile.extend_or_get_subpattern(*next_subpattern, || "Nested update".to_owned());
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
