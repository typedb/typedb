/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};
use std::error::Error;
use std::fmt::{Display, Formatter};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{ReadSnapshot, ReadableSnapshot, WritableSnapshot, WriteSnapshot},
};

use crate::{
    batch::{Batch, BatchRowIterator, ImmutableRow},
    pattern_executor::MatchStage,
    write::{insert::InsertStage, WriteError},
};

pub enum PipelineContext<Snapshot: ReadableSnapshot> {
    Arced(Arc<Snapshot>, Arc<ThingManager>),
    Owned(Snapshot, ThingManager),
}

impl<Snapshot: ReadableSnapshot> PipelineContext<Snapshot> {
    pub(crate) fn borrow_parts(&self) -> (&Snapshot, &ThingManager) {
        match self {
            PipelineContext::Arced(snapshot, thing_manager) => (&snapshot, &thing_manager),
            PipelineContext::Owned(snapshot, thing_manager) => (&snapshot, &thing_manager),
        }
    }
}

impl<Snapshot: WritableSnapshot> PipelineContext<Snapshot> {
    pub fn borrow_parts_mut(&mut self) -> (&mut Snapshot, &mut ThingManager) {
        match self {
            PipelineContext::Arced(snapshot, thing_manager) => todo!("illegal"),
            PipelineContext::Owned(snapshot, thing_manager) => (snapshot, thing_manager),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PipelineError {
    ConceptRead(ConceptReadError),
    WriteError(WriteError),
}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for PipelineError {

}

pub trait PipelineStageAPI<Snapshot: ReadableSnapshot>:
    for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, PipelineError>>
{
    fn finalise(self) -> PipelineContext<Snapshot>;
}

pub enum ReadablePipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot>),
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for ReadablePipelineStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            ReadablePipelineStage::Initial(initial) => initial.next(),
            ReadablePipelineStage::Match(match_) => match_.next(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> PipelineStageAPI<Snapshot> for ReadablePipelineStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        // TODO: Ensure the stages are done somehow
        match self {
            ReadablePipelineStage::Match(match_) => match_.finalise(),
            ReadablePipelineStage::Initial(initial) => initial.finalise(),
        }
    }
}

pub enum WritablePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot>),
    Insert(InsertStage<Snapshot>),
}

impl<Snapshot: WritableSnapshot + 'static> LendingIterator for WritablePipelineStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WritablePipelineStage::Initial(initial) => initial.next(),
            WritablePipelineStage::Match(match_) => match_.next(),
            WritablePipelineStage::Insert(insert) => insert.next(),
        }
    }
}

impl<Snapshot: WritableSnapshot + 'static> PipelineStageAPI<Snapshot> for WritablePipelineStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        // TODO: Ensure the stages are done somehow
        match self {
            WritablePipelineStage::Match(match_) => match_.finalise(),
            WritablePipelineStage::Initial(initial) => initial.finalise(),
            WritablePipelineStage::Insert(insert) => insert.finalise(),
        }
    }
}

pub struct InitialStage<Snapshot: ReadableSnapshot + 'static> {
    context: PipelineContext<Snapshot>,
    only_entry: BatchRowIterator,
}

impl<Snapshot: ReadableSnapshot + 'static> InitialStage<Snapshot> {
    pub fn new(context: PipelineContext<Snapshot>) -> Self {
        Self { context, only_entry: BatchRowIterator::new(Ok(Batch::SINGLE_EMPTY_ROW)) }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for InitialStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;
    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.only_entry.next().map(|result| result.map_err(|source| PipelineError::ConceptRead(source.clone())))
    }
}

impl<Snapshot: ReadableSnapshot + 'static> PipelineStageAPI<Snapshot> for InitialStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        self.context
    }
}
