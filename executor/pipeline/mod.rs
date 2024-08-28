/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod accumulator;
pub mod common;
mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod stage_wrappers;

use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::Arc,
};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::ImmutableRow,
    pipeline::{initial::InitialStage, insert::InsertStage, match_::MatchStage},
    write::WriteError,
};

pub trait UninitialisedStageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    type IteratingStage;
    fn initialise_and_into_iterator(self) -> Result<Self::IteratingStage, PipelineError>;
}

pub trait IteratingStageAPI<Snapshot: ReadableSnapshot>:
    for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, PipelineError>>
{
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError>;
    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError>;
}

pub trait PipelineStageAPI<Snapshot: ReadableSnapshot>: IteratingStageAPI<Snapshot>
where
    Snapshot: ReadableSnapshot + 'static,
{
    fn initialise(&mut self) -> Result<(), PipelineError>;
}

pub enum PipelineContext<Snapshot: ReadableSnapshot> {
    Shared(Arc<Snapshot>, Arc<ThingManager>),
    Owned(Snapshot, ThingManager),
    __Transient,
}

impl<Snapshot: ReadableSnapshot> PipelineContext<Snapshot> {
    pub(crate) fn borrow_parts(&self) -> (&Snapshot, &ThingManager) {
        match self {
            PipelineContext::Shared(snapshot, thing_manager) => (&snapshot, &thing_manager),
            PipelineContext::Owned(snapshot, thing_manager) => (&snapshot, &thing_manager),
            PipelineContext::__Transient => unreachable!(),
        }
    }

    // TODO: This doesn't fail. Shall we change it to get_shared?
    pub(crate) fn try_get_shared(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        if let PipelineContext::Shared(snapshot, thing_manager) = self {
            Ok(PipelineContext::Shared(snapshot.clone(), thing_manager.clone()))
        } else {
            let mut tmp = PipelineContext::__Transient;
            std::mem::swap(self, &mut tmp);
            let PipelineContext::Owned(snapshot, thing_manager) = tmp else { unreachable!() };
            let arc_snapshot = Arc::new(snapshot);
            let arc_thing_manager = Arc::new(thing_manager);
            *self = PipelineContext::Shared(arc_snapshot.clone(), arc_thing_manager.clone());
            Ok(PipelineContext::Shared(arc_snapshot.clone(), arc_thing_manager.clone()))
        }
    }

    pub(crate) fn try_into_owned(self) -> Result<PipelineContext<Snapshot>, ()> {
        match self {
            PipelineContext::Owned(snapshot, thing_manager) => Ok(PipelineContext::Owned(snapshot, thing_manager)),
            PipelineContext::Shared(mut shared_snapshot, mut shared_thing_manager) => {
                match (Arc::into_inner(shared_snapshot), Arc::into_inner(shared_thing_manager)) {
                    (Some(snapshot), Some(thing_manager)) => Ok(PipelineContext::Owned(snapshot, thing_manager)),
                    (_, _) => Err(()),
                }
            }
            PipelineContext::__Transient => unreachable!(),
        }
    }
}

impl<Snapshot: WritableSnapshot> PipelineContext<Snapshot> {
    pub fn borrow_parts_mut(&mut self) -> (&mut Snapshot, &mut ThingManager) {
        match self {
            PipelineContext::Shared(snapshot, thing_manager) => todo!("illegal"),
            PipelineContext::Owned(snapshot, thing_manager) => (snapshot, thing_manager),
            PipelineContext::__Transient => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PipelineError {
    ConceptRead(ConceptReadError),
    WriteError(WriteError),
    FinalisedUnconsumedStage,
    CouldNotGetOwnedContextFromShared,
    IllegalState,
}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for PipelineError {}
