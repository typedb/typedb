/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{
    executable::{function::ExecutableFunctionRegistry, insert::VariableSource, put::PutExecutable},
    VariablePosition,
};
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Batch,
    match_executor::MatchExecutor,
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, WrittenRowsIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct PutStageExecutor<PreviousStage> {
    executable: Arc<PutExecutable>,
    previous: PreviousStage,
    function_registry: Arc<ExecutableFunctionRegistry>,
}

impl<PreviousStage> PutStageExecutor<PreviousStage> {
    pub fn new(
        executable: Arc<PutExecutable>,
        previous: PreviousStage,
        function_registry: Arc<ExecutableFunctionRegistry>,
    ) -> Self {
        Self { executable, previous, function_registry }
    }

    pub(crate) fn output_width(&self) -> usize {
        self.executable.output_width()
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for PutStageExecutor<PreviousStage>
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
        let Self { previous: previous_stage, executable, function_registry } = self;

        let mut output_batch = Batch::new(executable.output_width() as u32, 10); // We don't know how many ahead of time
        let mut must_insert = Vec::new();

        let (mut previous_iterator, mut context) = previous_stage.into_iterator(interrupt.clone())?;
        let ExecutionContext { snapshot, thing_manager, profile, .. } = &context;
        let stage_profile = profile.profile_stage(|| String::from("Put"), executable.executable_id);
        let empty_output_row = vec![VariableValue::Empty; executable.output_width()];
        let input_output_mapping = executable
            .insert
            .output_row_schema
            .iter()
            .enumerate()
            .filter_map(|(i, entry_opt)| match entry_opt {
                Some((_, VariableSource::Input(pos))) => Some((*pos, VariablePosition::new(i as u32))),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        while let Some(input_result) = previous_iterator.next() {
            let input_row = match input_result {
                Ok(row) => row,
                Err(err) => return Err((err, context)),
            };

            let executor = MatchExecutor::new(
                &executable.match_,
                snapshot,
                thing_manager,
                input_row.clone(),
                function_registry.clone(),
                &context.profile,
            )
            .map_err(|err| Box::new(PipelineExecutionError::InitialisingMatchIterator { typedb_source: err }));
            let mut match_iterator = match executor {
                Ok(executor) => crate::pipeline::match_::unique_rows(crate::pipeline::match_::as_owned_rows(
                    executor.into_iterator(context.clone(), interrupt.clone()),
                ))
                .peekable(),
                Err(err) => return Err((err, context)),
            };
            // TODO: if the previous stage is not already in Collected format, this will end up temporarily allocating 2x
            //       the current memory. However, in the other case we don't know how many rows in the output batch to allocate ahead of time
            //       and require resizing. For now we take the simpler strategy that doesn't require resizing.
            let size_before = output_batch.len();
            while let Some(row_result) = match_iterator.next() {
                match row_result {
                    Ok(row) => {
                        output_batch.append(row);
                        must_insert.push(false);
                    }
                    Err(typedb_source) => {
                        return Err((Box::new(PipelineExecutionError::ReadPatternExecution { typedb_source }), context))
                    }
                }
            }
            if size_before == output_batch.len() {
                // copy out row multiplicity M, set it to 1, then append the row M times
                let multiplicity = input_row.multiplicity();
                for _ in 0..multiplicity {
                    output_batch.append(MaybeOwnedRow::new_borrowed(&empty_output_row, &1)); // Insert an empty row
                    output_batch.get_row_mut(output_batch.len() - 1).copy_mapped(
                        // Copy over input_row
                        input_row.as_reference(),
                        input_output_mapping.iter().map(|(s, d)| (s.clone(), d.clone())),
                    );
                    must_insert.push(true);
                }
            }
        }
        drop(previous_iterator);
        debug_assert_eq!(output_batch.len(), must_insert.len());
        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so we can get mut:
        let snapshot_mut = Arc::get_mut(&mut context.snapshot).unwrap();
        for index in 0..output_batch.len() {
            // TODO: parallelise -- though this requires our snapshots support parallel writes!
            let mut row = output_batch.get_row_mut(index);
            if must_insert[index] {
                if let Err(typedb_source) = crate::pipeline::insert::execute_insert(
                    &executable.insert,
                    snapshot_mut,
                    &context.thing_manager,
                    &context.parameters,
                    &mut row,
                    &stage_profile,
                ) {
                    return Err((Box::new(PipelineExecutionError::WriteError { typedb_source }), context));
                }
            }
            if index % 100 == 0 {
                if let Some(interrupt) = interrupt.check() {
                    return Err((Box::new(PipelineExecutionError::Interrupted { interrupt }), context));
                }
            }
        }

        Ok((WrittenRowsIterator::new(output_batch), context))
    }
}
