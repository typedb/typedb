/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use answer::{variable::Variable, variable_value::VariableValue};
use compiler::match_::{
    instructions::{CheckInstruction, ConstraintInstruction},
    planner::pattern_plan::{
        AssignmentProgram, CheckProgram, DisjunctionProgram, IntersectionProgram, MatchProgram, NegationProgram,
        OptionalProgram, Program, UnsortedJoinProgram,
    },
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::program::VariableRegistry;
use itertools::Itertools;
use lending_iterator::{adaptors::FlatMap, AsLendingIterator, LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    instruction::{iterator::TupleIterator, Checker, InstructionExecutor},
    pipeline::stage::ExecutionContext,
    row::{MaybeOwnedRow, Row},
    ExecutionInterrupt, SelectedPositions, VariablePosition,
};
use crate::program_executors::ProgramExecutor;

pub(crate) struct PatternExecutor {
    input: Option<MaybeOwnedRow<'static>>,
    program_executors: Vec<ProgramExecutor>,
    // modifiers: Modifier,
    output: Option<FixedBatch>,
}

impl PatternExecutor {
    pub(crate) fn new(
        match_program: &MatchProgram,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        input: MaybeOwnedRow<'_>,
    ) -> Result<Self, ConceptReadError> {
        let context = match_program.variable_registry();

        let program_executors = match_program.programs()
            .iter()
            .map(|program| ProgramExecutor::new(program, context, match_program.variable_positions(), snapshot, thing_manager))
            .try_collect()?;

        Ok(Self {
            input: Some(input.into_owned()),
            program_executors,
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
        let programs_len = self.program_executors.len();

        let (mut current_program, mut last_program_batch, mut direction) = if let Some(input) = self.input.take() {
            (0, Some(FixedBatch::from(input)), Direction::Forward)
        } else {
            (programs_len - 1, None, Direction::Backward)
        };

        loop {
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            match direction {
                Direction::Forward => {
                    if current_program >= programs_len {
                        return Ok(last_program_batch);
                    } else {
                        let executor = &mut self.program_executors[current_program];
                        let batch = executor.batch_from(last_program_batch.take().unwrap(), context, interrupt)?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                if current_program == 0 {
                                    return Ok(None);
                                } else {
                                    current_program -= 1;
                                }
                            }
                            Some(batch) => {
                                last_program_batch = Some(batch);
                                current_program += 1;
                            }
                        }
                    }
                }

                Direction::Backward => {
                    let batch = self.program_executors[current_program].batch_continue(context)?;
                    match batch {
                        None => {
                            if current_program == 0 {
                                return Ok(None);
                            } else {
                                current_program -= 1;
                            }
                        }
                        Some(batch) => {
                            direction = Direction::Forward;
                            last_program_batch = Some(batch);
                            current_program += 1;
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
    executor: PatternExecutor,
    context: ExecutionContext<Snapshot>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(
        executor: PatternExecutor,
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
