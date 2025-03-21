/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::reduce::ReduceExecutable;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{
        stage::{ExecutionContext, StageAPI, StageIterator},
        PipelineExecutionError, WrittenRowsIterator,
    },
    reduce_executor::GroupedReducer,
    ExecutionInterrupt,
};

pub struct ReduceStageExecutor<PreviousStage> {
    executable: Arc<ReduceExecutable>,
    previous: PreviousStage,
}

impl<PreviousStage> ReduceStageExecutor<PreviousStage> {
    pub fn new(executable: Arc<ReduceExecutable>, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for ReduceStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { previous, executable, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;

        let profile =
            context.profile.profile_stage(|| String::from("Reduce (not timed)"), executable.executable_id as i64);
        let step_profile = profile.extend_or_get(0, || String::from("Reduction (not timed)"));
        let rows = match reduce_iterator(&context, executable, previous_iterator) {
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
) -> Result<Batch, Box<PipelineExecutionError>> {
    let mut iterator = iterator;
    let mut grouped_reducer = GroupedReducer::new(executable.reduce_rows_executable.clone());
    while let Some(result) = iterator.next() {
        grouped_reducer.accept(&result?, context)?;
    }
    Ok(grouped_reducer.finalise())
}
