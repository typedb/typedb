/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::executable::match_::planner::match_executable::MatchExecutable;
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    pattern_executor::{MatchExecutor, PatternIterator},
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct MatchStageExecutor<PreviousStage> {
    executable: MatchExecutable,
    previous: PreviousStage,
}

impl<PreviousStage> MatchStageExecutor<PreviousStage> {
    pub fn new(executable: MatchExecutable, previous: PreviousStage) -> Self {
        Self { executable, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for MatchStageExecutor<PreviousStage>
where
    PreviousStage: StageAPI<Snapshot>,
    Snapshot: ReadableSnapshot + 'static,
{
    type OutputIterator = MatchStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { previous: previous_stage, executable, .. } = self;
        let (previous_iterator, context) = previous_stage.into_iterator(interrupt.clone())?;
        let iterator = previous_iterator;
        Ok((MatchStageIterator::new(iterator, executable, context.clone(), interrupt), context))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot + 'static, Iterator> {
    context: ExecutionContext<Snapshot>,
    executable: MatchExecutable,
    source_iterator: Iterator,
    current_iterator: Option<Peekable<PatternIterator<Snapshot>>>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot: ReadableSnapshot + 'static, Iterator> MatchStageIterator<Snapshot, Iterator> {
    fn new(
        iterator: Iterator,
        executable: MatchExecutable,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { context, executable, source_iterator: iterator, current_iterator: None, interrupt }
    }
}

impl<Snapshot, Iterator> LendingIterator for MatchStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.current_iterator.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            let ExecutionContext { snapshot, thing_manager, .. } = &self.context;

            let input_row = match self.source_iterator.next()? {
                Ok(row) => row,
                Err(err) => return Some(Err(err)),
            };

            let executor = MatchExecutor::new(&self.executable, snapshot, thing_manager, input_row)
                .map_err(|err| PipelineExecutionError::InitialisingMatchIterator { source: err });

            match executor {
                Ok(executor) => {
                    self.current_iterator =
                        Some(Peekable::new(executor.into_iterator(self.context.clone(), self.interrupt.clone())));
                }
                Err(err) => return Some(Err(err)),
            };
        }
        self.current_iterator.as_mut().unwrap().next().map(|result| {
            result.map_err(|err| PipelineExecutionError::ReadPatternExecution { typedb_source: err.clone() })
        })
    }
}

impl<Snapshot, Iterator> StageIterator for MatchStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIterator,
{
}
