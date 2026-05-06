/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use compiler::executable::reduce::ReduceExecutable;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    ExecutionInterrupt,
    batch::Batch,
    pipeline::{
        PipelineExecutionError, WrittenRowsIterator,
        stage::{ExecutionContext, StageAPI, StageIterator},
    },
    reduce_executor::GroupedReducer,
};

pub struct ReduceStageExecutor<InputIterator> {
    executable: Arc<ReduceExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> ReduceStageExecutor<InputIterator> {
    pub fn new(executable: Arc<ReduceExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }
}

impl<Snapshot, InputIterator> StageAPI<Snapshot> for ReduceStageExecutor<InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type InputIterator = InputIterator;
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        _interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { executable, .. } = self;

        let stage_profile = context.profile.profile_stage(|| String::from("Reduce"), executable.executable_id);
        let pattern_profile = stage_profile.create_or_get_pattern(|| String::from("Reduce pattern"));
        let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Reduce execution"));
        let storage_counters = step_profile.storage_counters();

        // The reduce stage produces a single output batch from streaming input,
        // so we time the whole iterator drain as one batch and report the
        // count of input rows consumed.
        let measurement = step_profile.start_measurement();
        let (rows, input_row_count) = match reduce_iterator(&context, executable, input_iterator, &storage_counters) {
            Ok(out) => out,
            Err(err) => return Err((err, context)),
        };
        measurement.end(&step_profile, 1, input_row_count);
        Ok((WrittenRowsIterator::new(rows), context))
    }
}

fn reduce_iterator<Snapshot: ReadableSnapshot>(
    context: &ExecutionContext<Snapshot>,
    executable: Arc<ReduceExecutable>,
    iterator: impl StageIterator,
    storage_counters: &StorageCounters,
) -> Result<(Batch, u64), Box<PipelineExecutionError>> {
    let mut iterator = iterator;
    let mut grouped_reducer = GroupedReducer::new(executable.reduce_rows_executable.clone());
    let mut input_row_count: u64 = 0;
    while let Some(result) = iterator.next() {
        grouped_reducer.accept(&result?, context, storage_counters)?;
        input_row_count += 1;
    }
    Ok((grouped_reducer.finalise(), input_row_count))
}
