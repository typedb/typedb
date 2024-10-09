/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::MatchExecutable;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use lending_iterator::{adaptors::FlatMap, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    step_executors::StepExecutor,
    ExecutionInterrupt,
};

pub struct MatchExecutor {
    input: Option<MaybeOwnedRow<'static>>,
    step_executors: Vec<StepExecutor>,
    // modifiers: Modifier,
    output: Option<FixedBatch>,
}

impl MatchExecutor {
    pub fn new(
        match_executable: &MatchExecutable,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        input: MaybeOwnedRow<'_>,
    ) -> Result<Self, ConceptReadError> {
        let step_executors = match_executable
            .steps()
            .iter()
            .map(|step| StepExecutor::new(step, snapshot, thing_manager))
            .try_collect()?;

        Ok(Self {
            input: Some(input.into_owned()),
            step_executors,
            // modifiers:
            output: None,
        })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> PatternIterator<Snapshot> {
        PatternIterator::new(
            AsLendingIterator::new(BatchIterator::new(self, context, interrupt)).flat_map(FixedBatchRowIterator::new),
        )
    }

    fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let steps_len = self.step_executors.len();

        let (mut current_step, mut last_step_batch, mut direction) = if let Some(input) = self.input.take() {
            (0, Some(FixedBatch::from(input)), Direction::Forward)
        } else {
            (steps_len - 1, None, Direction::Backward)
        };

        loop {
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            match direction {
                Direction::Forward => {
                    if current_step >= steps_len {
                        return Ok(last_step_batch);
                    } else {
                        let executor = &mut self.step_executors[current_step];
                        let batch = executor.batch_from(last_step_batch.take().unwrap(), context, interrupt)?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                if current_step == 0 {
                                    return Ok(None);
                                } else {
                                    current_step -= 1;
                                }
                            }
                            Some(batch) => {
                                last_step_batch = Some(batch);
                                current_step += 1;
                            }
                        }
                    }
                }

                Direction::Backward => {
                    let batch = self.step_executors[current_step].batch_continue(context)?;
                    match batch {
                        None => {
                            if current_step == 0 {
                                return Ok(None);
                            } else {
                                current_step -= 1;
                            }
                        }
                        Some(batch) => {
                            direction = Direction::Forward;
                            last_step_batch = Some(batch);
                            current_step += 1;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Direction {
    Forward,
    Backward,
}

type PatternRowIterator<Snapshot> = FlatMap<
    AsLendingIterator<BatchIterator<Snapshot>>,
    FixedBatchRowIterator,
    fn(Result<FixedBatch, ReadExecutionError>) -> FixedBatchRowIterator,
>;

pub struct PatternIterator<Snapshot: ReadableSnapshot + 'static> {
    iterator: PatternRowIterator<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> PatternIterator<Snapshot> {
    fn new(iterator: PatternRowIterator<Snapshot>) -> Self {
        Self { iterator }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for PatternIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

pub(crate) struct BatchIterator<Snapshot> {
    executor: MatchExecutor,
    context: ExecutionContext<Snapshot>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(
        executor: MatchExecutor,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { executor, context, interrupt }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Iterator for BatchIterator<Snapshot> {
    type Item = Result<FixedBatch, ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.executor.compute_next_batch(&self.context, &mut self.interrupt);
        batch.transpose()
    }
}

// struct ResumeExecutor {
//     resume_points: Vec<SuspensionPoint>,
// }
