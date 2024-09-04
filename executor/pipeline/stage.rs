/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::sync::Arc;

use compiler::VariablePosition;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::Batch,
    pipeline::{
        delete::DeleteStageExecutor,
        initial::{InitialIterator, InitialStage},
        insert::InsertStageExecutor,
        match_::{MatchStageExecutor, MatchStageIterator},
        PipelineExecutionError, StageAPI, StageIterator, WrittenRowsIterator,
    },
    row::MaybeOwnedRow,
};

pub enum ReadPipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(Box<MatchStageExecutor<Snapshot, ReadPipelineStage<Snapshot>>>),
}

pub enum ReadStageIterator<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
}

impl<Snapshot: ReadableSnapshot + 'static> StageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    type OutputIterator = ReadStageIterator<Snapshot>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        match self {
            Self::Initial(stage) => stage.named_selected_outputs(),
            Self::Match(stage) => stage.named_selected_outputs(),
        }
    }

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        match self {
            ReadPipelineStage::Initial(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((ReadStageIterator::Initial(iterator), snapshot))
            }
            ReadPipelineStage::Match(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((ReadStageIterator::Match(Box::new(iterator)), snapshot))
            }
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for ReadStageIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.next(),
            ReadStageIterator::Match(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> StageIterator for ReadStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
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
    Delete(Box<DeleteStageExecutor<Snapshot, WritePipelineStage<Snapshot>>>),
}

impl<Snapshot: WritableSnapshot + 'static> StageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    type OutputIterator = WriteStageIterator<Snapshot>;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String> {
        match self {
            WritePipelineStage::Initial(stage) => stage.named_selected_outputs(),
            WritePipelineStage::Match(stage) => stage.named_selected_outputs(),
            WritePipelineStage::Insert(stage) => stage.named_selected_outputs(),
            WritePipelineStage::Delete(stage) => stage.named_selected_outputs(),
        }
    }

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)> {
        match self {
            WritePipelineStage::Initial(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((WriteStageIterator::Initial(iterator), snapshot))
            }
            WritePipelineStage::Match(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((WriteStageIterator::Match(Box::new(iterator)), snapshot))
            }
            WritePipelineStage::Insert(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((WriteStageIterator::Write(iterator), snapshot))
            }
            WritePipelineStage::Delete(stage) => {
                let (iterator, snapshot) = stage.into_iterator()?;
                Ok((WriteStageIterator::Write(iterator), snapshot))
            }
        }
    }
}

pub enum WriteStageIterator<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
    Write(WrittenRowsIterator),
}

impl<Snapshot: WritableSnapshot + 'static> LendingIterator for WriteStageIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.next(),
            WriteStageIterator::Match(iterator) => iterator.next(),
            WriteStageIterator::Write(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: WritableSnapshot + 'static> StageIterator for WriteStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.collect_owned(),
            WriteStageIterator::Match(iterator) => iterator.collect_owned(),
            WriteStageIterator::Write(iterator) => iterator.collect_owned(),
        }
    }
}
