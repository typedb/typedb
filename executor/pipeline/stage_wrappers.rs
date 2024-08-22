/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::ImmutableRow,
    pipeline::{
        initial::InitialStage, insert::InsertStage, match_::MatchStage, IteratingStageAPI, PipelineContext,
        PipelineError, PipelineStageAPI,
    },
};

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
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self {
            ReadablePipelineStage::Match(match_) => match_.initialise(),
            ReadablePipelineStage::Initial(initial) => initial.initialise(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> IteratingStageAPI<Snapshot> for ReadablePipelineStage<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            ReadablePipelineStage::Match(match_) => match_.try_get_shared_context(),
            ReadablePipelineStage::Initial(initial) => initial.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        // TODO: Ensure the stages are done somehow
        match self {
            ReadablePipelineStage::Match(match_) => match_.finalise_and_into_context(),
            ReadablePipelineStage::Initial(initial) => initial.finalise_and_into_context(),
        }
    }
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
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self {
            WritablePipelineStage::Match(match_) => match_.initialise(),
            WritablePipelineStage::Initial(initial) => initial.initialise(),
            WritablePipelineStage::Insert(insert) => insert.initialise(),
        }
    }
}
impl<Snapshot: WritableSnapshot + 'static> IteratingStageAPI<Snapshot> for WritablePipelineStage<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            WritablePipelineStage::Match(match_) => match_.try_get_shared_context(),
            WritablePipelineStage::Initial(initial) => initial.try_get_shared_context(),
            WritablePipelineStage::Insert(insert) => insert.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            WritablePipelineStage::Match(match_) => match_.finalise_and_into_context(),
            WritablePipelineStage::Initial(initial) => initial.finalise_and_into_context(),
            WritablePipelineStage::Insert(insert) => insert.finalise_and_into_context(),
        }
    }
}
