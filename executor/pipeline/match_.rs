/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use compiler::{match_::planner::pattern_plan::MatchProgram, VariablePosition};
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    pattern_executor::{PatternExecutor, PatternIterator},
    pipeline::{
        stage::{StageAPI, StageContext},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct MatchStageExecutor<PreviousStage> {
    program: MatchProgram,
    previous: PreviousStage,
}

impl<PreviousStage> MatchStageExecutor<PreviousStage> {
    pub fn new(program: MatchProgram, previous: PreviousStage) -> Self {
        Self { program, previous }
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
    ) -> Result<(Self::OutputIterator, StageContext<Snapshot>), (PipelineExecutionError, StageContext<Snapshot>)> {
        let Self { previous: previous_stage, program, .. } = self;
        let (previous_iterator, context) = previous_stage.into_iterator(interrupt.clone())?;
        let iterator = previous_iterator;
        Ok((MatchStageIterator::new(iterator, program, context.clone(), interrupt), context))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot + 'static, Iterator> {
    context: StageContext<Snapshot>,
    program: MatchProgram,
    source_iterator: Iterator,
    current_iterator: Option<Peekable<PatternIterator<Snapshot>>>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot: ReadableSnapshot + 'static, Iterator> MatchStageIterator<Snapshot, Iterator> {
    fn new(
        iterator: Iterator,
        program: MatchProgram,
        context: StageContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { context, program, source_iterator: iterator, current_iterator: None, interrupt }
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
            let StageContext { snapshot, thing_manager, .. } = &self.context;
            // TODO: use the start to initialise the next iterator
            let _source_next = self.source_iterator.next()?;
            let iterator = PatternExecutor::new(&self.program, snapshot, thing_manager)
                .map_err(|err| PipelineExecutionError::InitialisingMatchIterator { source: err });
            match iterator {
                Ok(iterator) => {
                    self.current_iterator = Some(Peekable::new(iterator.into_iterator(
                        snapshot.clone(),
                        thing_manager.clone(),
                        self.interrupt.clone(),
                    )));
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
