/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod accumulator;
pub mod common;
pub mod insert;
pub mod match_;

use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::Arc,
};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::{Batch, BatchRowIterator, ImmutableRow},
    pipeline::{common::InitialStage, insert::InsertStage, match_::MatchStage},
    write::WriteError,
};

pub enum PipelineContext<Snapshot: ReadableSnapshot> {
    Shared(Arc<Snapshot>, Arc<ThingManager>),
    Owned(Snapshot, ThingManager),
}

impl<Snapshot: ReadableSnapshot> PipelineContext<Snapshot> {
    pub(crate) fn borrow_parts(&self) -> (&Snapshot, &ThingManager) {
        match self {
            PipelineContext::Shared(snapshot, thing_manager) => (&snapshot, &thing_manager),
            PipelineContext::Owned(snapshot, thing_manager) => (&snapshot, &thing_manager),
        }
    }

    pub(crate) fn try_into_owned(self) -> Result<PipelineContext<Snapshot>, ()> {

        match self {
            PipelineContext::Owned(snapshot, thing_manager) => {
                Ok(PipelineContext::Owned(snapshot, thing_manager))
            }
            PipelineContext::Shared(mut shared_snapshot, mut shared_thing_manager) => {
                match (Arc::into_inner(shared_snapshot), Arc::into_inner(shared_thing_manager)) {
                    (Some(snapshot), Some(thing_manager)) => {
                        Ok(PipelineContext::Owned(snapshot, thing_manager))
                    }
                    (_, _) => {
                        Err(())
                    }
                }
            }
        }
    }
}

impl<Snapshot: WritableSnapshot> PipelineContext<Snapshot> {
    pub fn borrow_parts_mut(&mut self) -> (&mut Snapshot, &mut ThingManager) {
        match self {
            PipelineContext::Shared(snapshot, thing_manager) => todo!("illegal"),
            PipelineContext::Owned(snapshot, thing_manager) => (snapshot, thing_manager),
        }
    }
}

pub trait PipelineStageAPI<Snapshot: ReadableSnapshot>:
    for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, PipelineError>>
{
    fn try_finalise_and_get_owned_context(self) -> Result<PipelineContext<Snapshot>, PipelineError>;
    // fn try_get_shared_reference(self) -> Result<PipelineContext<Snapshot>, ()>;
    // fn try_finalise_and_drop_shared_context(mut self) -> Result<(), PipelineError>;
}

pub enum ReadablePipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot, ReadablePipelineStage<Snapshot>>),
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
    fn try_finalise_and_get_owned_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        // TODO: Ensure the stages are done somehow
        match self {
            ReadablePipelineStage::Match(match_) => match_.try_finalise_and_get_owned_context(),
            ReadablePipelineStage::Initial(initial) => initial.try_finalise_and_get_owned_context(),
        }
    }

    // fn try_get_shared_reference(self) -> Result<PipelineContext<Snapshot>, ()> {
    //     match self {
    //         ReadablePipelineStage::Match(match_) => match_.try_get_shared_reference(),
    //         ReadablePipelineStage::Initial(initial) => initial.try_get_shared_reference(),
    //     }
    // }
    //
    // fn try_finalise_and_drop_shared_context(self) -> Result<(), PipelineError> {
    //     match self {
    //         ReadablePipelineStage::Match(match_) => match_.try_finalise_and_drop_shared_context(),
    //         ReadablePipelineStage::Initial(initial) => initial.try_finalise_and_drop_shared_context(),
    //     }
    // }
}

pub enum WritablePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot, WritablePipelineStage<Snapshot>>),
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
    fn try_finalise_and_get_owned_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        // TODO: Ensure the stages are done somehow
        match self {
            WritablePipelineStage::Match(match_) => match_.try_finalise_and_get_owned_context(),
            WritablePipelineStage::Initial(initial) => initial.try_finalise_and_get_owned_context(),
            WritablePipelineStage::Insert(insert) => insert.try_finalise_and_get_owned_context(),
        }
    }
}

// Errors
#[derive(Debug, Clone)]
pub enum PipelineError {
    ConceptRead(ConceptReadError),
    WriteError(WriteError),
    FinalisedUnconsumedStage,
    CouldNotGetOwnedContext,
}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for PipelineError {}
