/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{
        insert::{instructions::ConceptInstruction, VariableSource},
        update::{
            executable::{OptionalUpdate, UpdateExecutable},
            instructions::ConnectionInstruction,
        },
    },
    VariablePosition,
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::{constants::traversal::CHECK_INTERRUPT_FREQUENCY_ROWS, profile::StageProfile};
use storage::snapshot::WritableSnapshot;

use crate::{
    pipeline::{
        insert::prepare_output_rows,
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, WrittenRowsIterator,
    },
    row::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
    ExecutionInterrupt,
};

pub struct UpdateStageExecutor<PreviousStage> {
    executable: Arc<UpdateExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> UpdateStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<UpdateExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for UpdateStageExecutor<PreviousStage>
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

        let profile = context.profile.profile_stage(|| String::from("Update"), executable.executable_id);

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
            match prepare_output_rows(executable.output_width() as u32, previous_iterator, &input_output_mapping) {
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

fn execute_update(
    executable: &UpdateExecutable,
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
    for optional in &executable.optional_updates {
        execute_optional_update(optional, snapshot, thing_manager, parameters, row, stage_profile, &mut profile_index)?;
    }
    Ok(())
}

fn execute_optional_update(
    optional: &OptionalUpdate,
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

fn execute_connection_instructions(
    connection_instructions: &Vec<ConnectionInstruction>,
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
            ConnectionInstruction::Links(links) => {
                links.execute(snapshot, thing_manager, parameters, row, step_profile.storage_counters())?;
            }
        };
        measurement.end(&step_profile, 1, 1);
        *profile_index += 1;
    }
    Ok(())
}
