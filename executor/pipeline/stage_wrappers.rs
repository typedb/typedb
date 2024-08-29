/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::MaybeOwnedRow,
    pipeline::{
        initial::InitialStage, insert::InsertStageExecutor, match_::MatchStageExecutor,
        PipelineError,
    },
};
use crate::pipeline::initial::InitialIterator;
use crate::pipeline::match_::MatchStageIterator;
use crate::pipeline::{StageAPI, StageIterator, WrittenRowsIterator};

pub enum ReadPipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(Box<MatchStageExecutor<Snapshot, ReadPipelineStage<Snapshot>>>),
}

pub enum ReadStageIterator {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<ReadStageIterator>>),
}

impl<Snapshot: ReadableSnapshot + 'static> StageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    type OutputIterator = ReadStageIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        match self {
            ReadPipelineStage::Initial(stage) => {
                let (iterator, snapshot, thing_manager) = stage.into_iterator()?;
                Ok((ReadStageIterator::Initial(iterator), snapshot, thing_manager))
            }
            ReadPipelineStage::Match(stage) => {
                let (iterator, snapshot, thing_manager) = stage.into_iterator()?;
                Ok((ReadStageIterator::Match(Box::new(iterator)), snapshot, thing_manager))
            }
        }
    }
}

impl LendingIterator for ReadStageIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.next(),
            ReadStageIterator::Match(iterator) => iterator.next()
        }
    }
}

impl StageIterator for ReadStageIterator {
    fn collect_owned(self) -> Result<Vec<MaybeOwnedRow<'static>>, PipelineError> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.collect_owned(),
            ReadStageIterator::Match(iterator) => iterator.collect_owned(),
        }
    }
}

pub enum WritePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(Box<MatchStageExecutor<Snapshot, WritePipelineStage<Snapshot>>>),
    Insert(Box<InsertStageExecutor<Snapshot, WritePipelineStage<Snapshot>>>),
}

impl<Snapshot: WritableSnapshot + 'static> StageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    type OutputIterator = WriteStageIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        match self {
            WritePipelineStage::Initial(stage) => {
                let (iterator, snapshot, thing_manager) = stage.into_iterator()?;
                Ok((WriteStageIterator::Initial(iterator), snapshot, thing_manager))
            }
            WritePipelineStage::Match(stage) => {
                let (iterator, snapshot, thing_manager) = stage.into_iterator()?;
                Ok((WriteStageIterator::Match(Box::new(iterator)), snapshot, thing_manager))
            }
            WritePipelineStage::Insert(stage) => {
                let (iterator, snapshot, thing_manager) = stage.into_iterator()?;
                Ok((WriteStageIterator::Write(iterator), snapshot, thing_manager))
            }
        }
    }
}

pub enum WriteStageIterator {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<WriteStageIterator>>),
    Write(WrittenRowsIterator),
}

impl LendingIterator for WriteStageIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.next(),
            WriteStageIterator::Match(iterator) => iterator.next(),
            WriteStageIterator::Write(iterator) => iterator.next(),
        }
    }
}

impl StageIterator for WriteStageIterator {
    fn collect_owned(self) -> Result<Vec<MaybeOwnedRow<'static>>, PipelineError> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.collect_owned(),
            WriteStageIterator::Match(iterator) => iterator.collect_owned(),
            WriteStageIterator::Write(iterator) => iterator.collect_owned(),
        }
    }
}
