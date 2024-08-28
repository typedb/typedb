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

pub enum ReadPipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot, ReadPipelineStage<Snapshot>>),
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for ReadPipelineStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            ReadPipelineStage::Initial(initial) => initial.next(),
            ReadPipelineStage::Match(match_) => match_.next(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> PipelineStageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self {
            ReadPipelineStage::Match(match_) => match_.initialise(),
            ReadPipelineStage::Initial(initial) => initial.initialise(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> IteratingStageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            ReadPipelineStage::Match(match_) => match_.try_get_shared_context(),
            ReadPipelineStage::Initial(initial) => initial.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        // TODO: Ensure the stages are done somehow
        match self {
            ReadPipelineStage::Match(match_) => match_.finalise_and_into_context(),
            ReadPipelineStage::Initial(initial) => initial.finalise_and_into_context(),
        }
    }
}

pub enum WritePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(MatchStage<Snapshot, WritePipelineStage<Snapshot>>),
    Insert(InsertStage<Snapshot>),
}

impl<Snapshot: WritableSnapshot + 'static> LendingIterator for WritePipelineStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WritePipelineStage::Initial(initial) => initial.next(),
            WritePipelineStage::Match(match_) => match_.next(),
            WritePipelineStage::Insert(insert) => insert.next(),
        }
    }
}

impl<Snapshot: WritableSnapshot + 'static> PipelineStageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    fn initialise(&mut self) -> Result<(), PipelineError> {
        match self {
            WritePipelineStage::Match(match_) => match_.initialise(),
            WritePipelineStage::Initial(initial) => initial.initialise(),
            WritePipelineStage::Insert(insert) => insert.initialise(),
        }
    }
}
impl<Snapshot: WritableSnapshot + 'static> IteratingStageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            WritePipelineStage::Match(match_) => match_.try_get_shared_context(),
            WritePipelineStage::Initial(initial) => initial.try_get_shared_context(),
            WritePipelineStage::Insert(insert) => insert.try_get_shared_context(),
        }
    }

    fn finalise_and_into_context(self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        match self {
            WritePipelineStage::Match(match_) => match_.finalise_and_into_context(),
            WritePipelineStage::Initial(initial) => initial.finalise_and_into_context(),
            WritePipelineStage::Insert(insert) => insert.finalise_and_into_context(),
        }
    }
}
