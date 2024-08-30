/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use compiler::match_::planner::pattern_plan::MatchProgram;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::MaybeOwnedRow
    ,
    pipeline::{PipelineError, StageAPI, StageIterator},
};
use crate::pattern_executor::{PatternExecutor, PatternIterator};

pub struct MatchStageExecutor<Snapshot: ReadableSnapshot + 'static, PreviousStage: StageAPI<Snapshot>> {
    program: MatchProgram,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for MatchStageExecutor<Snapshot, PreviousStage>
    where
        Snapshot: ReadableSnapshot + 'static,
        PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = MatchStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn into_iterator(mut self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        let Self { previous: previous_stage, program, .. } = self;
        let (previous_iterator, snapshot, thing_manager) = previous_stage.into_iterator()?;
        let iterator = previous_iterator;
        Ok((MatchStageIterator::new(iterator, program, snapshot.clone(), thing_manager.clone()), snapshot, thing_manager))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot + 'static, Iterator> {
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
    program: MatchProgram,
    source_iterator: Iterator,
    current_iterator: Option<Peekable<PatternIterator<Snapshot>>>,
}

impl<Snapshot: ReadableSnapshot + 'static, Iterator: StageIterator> MatchStageIterator<Snapshot, Iterator> {
    fn new(iterator: Iterator, program: MatchProgram, snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self { snapshot, program, thing_manager, source_iterator: iterator, current_iterator: None }
    }

    fn create_next_current_iterator(&mut self) -> Result<(), PipelineError> {
        while self.current_iterator.is_none() {
            let start = self.source_iterator.next();
            match start {
                None => return Ok(()),
                Some(start) => {
                    let start = start?;
                    // TODO uses start to initialise new pattern iterator
                    let iterator = PatternExecutor::new(&self.program, self.snapshot.as_ref(), self.thing_manager.as_ref())
                        .map_err(|err| PipelineError::InitialisingMatchIterator(err))?;

                }
            }
        }

        Ok(())
    }
}

impl<Snapshot, Iterator> LendingIterator for MatchStageIterator<Snapshot, Iterator>
    where
        Snapshot: ReadableSnapshot + 'static,
        Iterator: StageIterator
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.current_iterator.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            match self.source_iterator.next() {
                None => return None,
                Some(source_next) => {
                    // TODO: use the start to initialise the next iterator
                    let iterator = PatternExecutor::new(&self.program, self.snapshot.as_ref(), self.thing_manager.as_ref())
                        .map_err(|err| PipelineError::InitialisingMatchIterator(err));
                    match iterator {
                        Ok(iterator) => {
                            self.current_iterator = Some(Peekable::new(
                                iterator
                                    .into_iterator(self.snapshot.clone(), self.thing_manager.clone())
                            ));
                        },
                        Err(err) => return Some(Err(err)),
                    };
                }
            }
        }
        self.current_iterator.as_mut().unwrap().next().map(|result| result.map_err(|err| PipelineError::ConceptRead(err.clone())))
    }
}

impl<Snapshot, Iterator> StageIterator for MatchStageIterator<Snapshot, Iterator>
    where
        Snapshot: ReadableSnapshot + 'static,
        Iterator: StageIterator
{}


//
// impl<Iterator> MatchStageIterator<Iterator>
// where
//     Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>,
// {
//     fn new(iterator: Iterator) -> Self {
//         Self { iterator }
//     }
// }
//
// impl<Iterator> LendingIterator for MatchStageIterator<Iterator>
//     where
//         Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>,
// {
//     type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;
//
//     fn next(&mut self) -> Option<Self::Item<'_>> {
//         self.iterator.next()
//     }
// }
//
// impl<Iterator> StageIterator for MatchStageIterator<Iterator> where
//     Iterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>>
// {}
