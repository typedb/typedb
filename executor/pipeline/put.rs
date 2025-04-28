/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{function::ExecutableFunctionRegistry, insert::VariableSource, put::PutExecutable},
    VariablePosition,
};
use resource::constants::traversal::{BATCH_DEFAULT_CAPACITY, CHECK_INTERRUPT_FREQUENCY_ROWS};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::Batch,
    error::ReadExecutionError,
    match_executor::MatchExecutor,
    pipeline::{
        stage::{ExecutionContext, StageAPI, StageIterator},
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
        let (previous_iterator, mut context) = previous_stage.into_iterator(interrupt.clone())?;
        let result =
            into_iterator_impl(&mut context, &mut interrupt, &executable, function_registry, previous_iterator);
        match result {
            Ok(written_rows_iterator) => Ok((written_rows_iterator, context)),
            Err(err) => Err((err, context)),
        }
    }
}

fn into_iterator_impl<Snapshot: WritableSnapshot + 'static>(
    context: &mut ExecutionContext<Snapshot>,
    interrupt: &mut ExecutionInterrupt,
    executable: &PutExecutable,
    function_registry: Arc<ExecutableFunctionRegistry>,
    mut previous_iterator: impl StageIterator,
) -> Result<WrittenRowsIterator, Box<PipelineExecutionError>> {
    let mut output_batch = Batch::new(executable.output_width() as u32, BATCH_DEFAULT_CAPACITY);
    let mut must_insert = Vec::new();
    let input_output_mapping = executable
        .insert
        .output_row_schema
        .iter()
        .enumerate()
        .filter_map(|(i, entry_opt)| match entry_opt {
            Some((_, VariableSource::Input(pos))) => Some((*pos, VariablePosition::new(i as u32))),
            _ => None,
        })
        .collect::<Vec<_>>();
    while let Some(input_row_result) = previous_iterator.next() {
        let input_row = input_row_result?;
        let size_before = output_batch.len();
        let mut match_iterator =
            match_iterator_for_row(context, interrupt, executable, function_registry.clone(), input_row.clone())?;
        match_iterator
            .try_for_each(|row_result| {
                output_batch.append_row(row_result?);
                must_insert.push(false);
                Ok(())
            })
            .map_err(|typedb_source| Box::new(PipelineExecutionError::ReadPatternExecution { typedb_source }))?;

        if size_before == output_batch.len() {
            must_insert.extend((0..input_row.multiplicity()).map(|_| true));
            crate::pipeline::insert::append_row_for_insert_mapped(
                &mut output_batch,
                input_row.as_reference(),
                &input_output_mapping,
            );
        }
    }
    drop(previous_iterator);
    debug_assert_eq!(output_batch.len(), must_insert.len());
    // once the previous iterator is complete, this must be the exclusive owner of Arc's, so we can get mut:
    perform_inserts(context, interrupt, executable, &mut output_batch, &must_insert)?;

    Ok(WrittenRowsIterator::new(output_batch))
}

fn match_iterator_for_row<Snapshot: ReadableSnapshot + 'static>(
    context: &ExecutionContext<Snapshot>,
    interrupt: &ExecutionInterrupt,
    put_executable: &PutExecutable,
    function_registry: Arc<ExecutableFunctionRegistry>,
    input_row: MaybeOwnedRow<'_>,
) -> Result<impl Iterator<Item = Result<MaybeOwnedRow<'static>, ReadExecutionError>>, Box<PipelineExecutionError>> {
    let executor = MatchExecutor::new(
        &put_executable.match_,
        &context.snapshot,
        &context.thing_manager,
        input_row,
        function_registry,
        &context.profile,
    )
    .map_err(|err| Box::new(PipelineExecutionError::InitialisingMatchIterator { typedb_source: err }))?;
    Ok(crate::pipeline::match_::unique_rows(crate::pipeline::match_::as_owned_rows(
        executor.into_iterator(context.clone(), interrupt.clone()),
    ))
    .peekable())
}

fn perform_inserts<Snapshot: WritableSnapshot>(
    context: &mut ExecutionContext<Snapshot>,
    interrupt: &mut ExecutionInterrupt,
    executable: &PutExecutable,
    output_batch: &mut Batch,
    must_insert: &[bool],
) -> Result<(), Box<PipelineExecutionError>> {
    let snapshot_mut = Arc::get_mut(&mut context.snapshot).unwrap();
    let stage_profile = context.profile.profile_stage(|| String::from("PutInsert"), executable.executable_id as _);
    for index in 0..output_batch.len() {
        // TODO: parallelise -- though this requires our snapshots support parallel writes!
        let mut row = output_batch.get_row_mut(index);
        if must_insert[index] {
            crate::pipeline::insert::execute_insert(
                &executable.insert,
                snapshot_mut,
                &context.thing_manager,
                &context.parameters,
                &mut row,
                &stage_profile,
            )
            .map_err(|typedb_source| Box::new(PipelineExecutionError::WriteError { typedb_source }))?;
        }
        if index % CHECK_INTERRUPT_FREQUENCY_ROWS == 0 {
            if let Some(interrupt) = interrupt.check() {
                return Err(Box::new(PipelineExecutionError::Interrupted { interrupt }));
            }
        }
    }
    Ok(())
}
