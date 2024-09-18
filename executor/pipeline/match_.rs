/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use compiler::{match_::planner::pattern_plan::MatchProgram, VariablePosition};
use concept::thing::thing_manager::ThingManager;
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;
use tokio::sync::broadcast;

use crate::{
    pattern_executor::{PatternExecutor, PatternIterator},
    pipeline::{PipelineExecutionError, StageAPI, StageIterator},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct MatchStageExecutor<Snapshot: ReadableSnapshot + 'static, PreviousStage: StageAPI<Snapshot>> {
    program: MatchProgram,
    previous: PreviousStage,
    thing_manager: Arc<ThingManager>,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> MatchStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(program: MatchProgram, previous: PreviousStage, thing_manager: Arc<ThingManager>) -> Self {
        Self { program, previous, thing_manager, phantom: PhantomData::default() }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for MatchStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = MatchStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn into_iterator(
        mut self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        let Self { previous: previous_stage, program, .. } = self;
        let (previous_iterator, snapshot) = previous_stage.into_iterator(interrupt.clone())?;
        let iterator = previous_iterator;
        Ok((
            MatchStageIterator::new(iterator, program, snapshot.clone(), self.thing_manager.clone(), interrupt),
            snapshot,
        ))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot + 'static, Iterator> {
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
    program: MatchProgram,
    source_iterator: Iterator,
    current_iterator: Option<Peekable<PatternIterator<Snapshot>>>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot: ReadableSnapshot + 'static, Iterator: StageIterator> MatchStageIterator<Snapshot, Iterator> {
    fn new(
        iterator: Iterator,
        program: MatchProgram,
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { snapshot, program, thing_manager, source_iterator: iterator, current_iterator: None, interrupt }
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
            match self.source_iterator.next() {
                None => return None,
                Some(source_next) => {
                    // TODO: use the start to initialise the next iterator
                    let iterator = PatternExecutor::new(&self.program, &self.snapshot, &self.thing_manager)
                        .map_err(|err| PipelineExecutionError::InitialisingMatchIterator { source: err });
                    match iterator {
                        Ok(iterator) => {
                            self.current_iterator = Some(Peekable::new(iterator.into_iterator(
                                self.snapshot.clone(),
                                self.thing_manager.clone(),
                                self.interrupt.clone(),
                            )));
                        }
                        Err(err) => return Some(Err(err)),
                    };
                }
            }
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