/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use compiler::{
    VariablePosition,
    executable::{
        insert::{VariableSource, instructions::ConceptInstruction},
        update::{
            executable::{OptionalUpdate, UpdateExecutable},
            instructions::ConnectionInstruction,
        },
    },
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::{constants::traversal::CHECK_INTERRUPT_FREQUENCY_ROWS, profile::StageProfile};
use resource::profile::PatternProfile;
use storage::snapshot::WritableSnapshot;

use crate::{
    ExecutionInterrupt,
    pipeline::{
        PipelineExecutionError, WrittenRowsIterator,
        insert::prepare_output_rows,
        stage::{ExecutionContext, StageAPI, StageIterator},
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
                &pattern_profile,
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

fn execute_update(
    executable: &UpdateExecutable,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    pattern_profile: &PatternProfile,
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
        pattern_profile,
        &mut profile_index,
    )?;
    execute_connection_instructions(
        &executable.connection_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        pattern_profile,
        &mut profile_index,
    )?;
    for optional in &executable.optional_updates {
        execute_optional_update(optional, snapshot, thing_manager, parameters, row, pattern_profile, &mut profile_index)?;
    }
    Ok(())
}

fn execute_optional_update(
    optional: &OptionalUpdate,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    pattern_profile: &PatternProfile,
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
        pattern_profile,
        profile_index,
    )?;
    execute_connection_instructions(
        &optional.connection_instructions,
        snapshot,
        thing_manager,
        parameters,
        row,
        pattern_profile,
        profile_index,
    )?;
    Ok(())
}

fn execute_concept_instructions(
    concept_instructions: &[ConceptInstruction],
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    pattern_profile: &PatternProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    for instruction in concept_instructions {
        let step_profile = pattern_profile.extend_or_get_step(*profile_index, || format!("{}", instruction));
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

fn execute_connection_instructions(
    connection_instructions: &Vec<ConnectionInstruction>,
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    row: &mut Row<'_>,
    pattern_profile: &PatternProfile,
    profile_index: &mut usize,
) -> Result<(), Box<WriteError>> {
    for instruction in connection_instructions {
        let step_profile = pattern_profile.extend_or_get_step(*profile_index, || format!("{}", instruction));
        let measurement = step_profile.start_measurement();
        match instruction {
            ConnectionInstruction::Has(has) => {
                has.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
            ConnectionInstruction::Links(links) => {
                links.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        };
        measurement.end(&step_profile, 1, 1);
        *profile_index += 1;
    }
    Ok(())
}
