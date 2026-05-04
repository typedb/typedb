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

        let profile = context.profile.profile_stage(|| String::from("Reduce (not timed)"), executable.executable_id);
        let pattern_profile = profile.create_or_get_pattern(|| String::from("Reduce (not timed)"));
        let step_profile = pattern_profile.extend_or_get_step(0, || String::from("Reduction (not timed)"));

        let reducers_profile = context.profile.profile_stage(|| String::from("Reduce"), 0); // TODO executable id
        let reducers_pattern = reducers_profile.create_or_get_pattern(|| String::from("Reduce pattern"));
        let reducers_step = reducers_pattern.extend_or_get_step(0, || String::from("Reduce execution"));
        let storage_counters = reducers_step.storage_counters();

        let rows = match reduce_iterator(&context, executable, input_iterator, &storage_counters) {
            Ok(rows) => rows,
            Err(err) => return Err((err, context)),
        };
        let measurement = step_profile.start_measurement();
        measurement.end(&step_profile, 1, rows.len() as u64);
        Ok((WrittenRowsIterator::new(rows), context))
    }
}

fn reduce_iterator<Snapshot: ReadableSnapshot>(
    context: &ExecutionContext<Snapshot>,
    executable: Arc<ReduceExecutable>,
    iterator: impl StageIterator,
    storage_counters: &StorageCounters,
) -> Result<Batch, Box<PipelineExecutionError>> {
    let mut iterator = iterator;
    let mut grouped_reducer = GroupedReducer::new(executable.reduce_rows_executable.clone());
    while let Some(result) = iterator.next() {
        grouped_reducer.accept(&result?, context, storage_counters)?;
    }
    Ok(grouped_reducer.finalise())
}
