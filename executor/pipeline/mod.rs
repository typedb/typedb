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

use std::{error::Error, fmt, sync::Arc};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{batch::ImmutableRow, write::WriteError};

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

pub struct SharedPipelineContext<Snapshot> {
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
}

impl<Snapshot> SharedPipelineContext<Snapshot> {
    fn new(snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self { snapshot, thing_manager }
    }
}

pub struct OwnedPipelineContext<Snapshot> {
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
}

impl<Snapshot> OwnedPipelineContext<Snapshot> {
    fn new(snapshot: Snapshot, thing_manager: ThingManager) -> Self {
        Self { snapshot: Arc::new(snapshot), thing_manager: Arc::new(thing_manager) }
    }
}

pub enum PipelineContext<Snapshot> {
    Shared(SharedPipelineContext<Snapshot>),
    Owned(OwnedPipelineContext<Snapshot>),
}

impl<Snapshot> PipelineContext<Snapshot> {
    pub fn shared(snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self::Shared(SharedPipelineContext::new(snapshot, thing_manager))
    }

    pub fn owned(snapshot: Snapshot, thing_manager: ThingManager) -> Self {
        Self::Owned(OwnedPipelineContext::new(snapshot, thing_manager))
    }

    pub(crate) fn borrow_parts(&self) -> (&Arc<Snapshot>, &Arc<ThingManager>) {
        match self {
            PipelineContext::Shared(SharedPipelineContext { snapshot, thing_manager }) => (snapshot, thing_manager),
            PipelineContext::Owned(OwnedPipelineContext { snapshot, thing_manager }) => (snapshot, thing_manager),
        }
    }

    pub fn into_owned_parts(self) -> (Snapshot, ThingManager) {
        match self {
            PipelineContext::Shared(SharedPipelineContext { snapshot, thing_manager }) => unreachable!(),
            PipelineContext::Owned(OwnedPipelineContext { snapshot, thing_manager }) => {
                (Arc::into_inner(snapshot).unwrap(), Arc::into_inner(thing_manager).unwrap())
            }
        }
    }

    // TODO: This doesn't fail. Shall we change it to get_shared?
    pub(crate) fn try_get_shared(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            PipelineContext::Shared(SharedPipelineContext { snapshot, thing_manager }) => {
                Ok(PipelineContext::shared(snapshot.clone(), thing_manager.clone()))
            }
            PipelineContext::Owned(OwnedPipelineContext { snapshot, thing_manager }) => {
                let snapshot = snapshot.clone();
                let thing_manager = thing_manager.clone();
                *self = PipelineContext::shared(snapshot.clone(), thing_manager.clone());
                Ok(PipelineContext::shared(snapshot, thing_manager))
            }
        }
    }

    pub(crate) fn try_into_owned(self) -> Result<PipelineContext<Snapshot>, ()> {
        match self {
            PipelineContext::Owned(..) => Ok(self),
            PipelineContext::Shared(SharedPipelineContext { snapshot, thing_manager }) => {
                match (Arc::into_inner(snapshot), Arc::into_inner(thing_manager)) {
                    (Some(snapshot), Some(thing_manager)) => Ok(Self::owned(snapshot, thing_manager)),
                    (_, _) => Err(()),
                }
            }
        }
    }
}

impl<Snapshot: WritableSnapshot> PipelineContext<Snapshot> {
    pub fn borrow_parts_mut(&mut self) -> (&mut Snapshot, &mut ThingManager) {
        match self {
            PipelineContext::Shared(_) => todo!("illegal"),
            PipelineContext::Owned(OwnedPipelineContext { snapshot, thing_manager }) => {
                (Arc::get_mut(snapshot).unwrap(), Arc::get_mut(thing_manager).unwrap())
            }
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

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for PipelineError {}
