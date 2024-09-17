/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::VariablePosition;
use concept::thing::thing_manager::ThingManager;
use ir::program::ParameterRegistry;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::Batch,
    pipeline::{
        delete::DeleteStageExecutor,
        initial::{InitialIterator, InitialStage},
        insert::InsertStageExecutor,
        match_::{MatchStageExecutor, MatchStageIterator},
        modifiers::{
            LimitStageExecutor, LimitStageIterator, OffsetStageExecutor, OffsetStageIterator, SelectStageExecutor,
            SelectStageIterator, SortStageExecutor, SortStageIterator,
        },
        PipelineExecutionError, WrittenRowsIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

#[derive(Debug)]
pub struct StageContext<Snapshot> {
    pub snapshot: Arc<Snapshot>,
    pub thing_manager: Arc<ThingManager>,
    pub parameters: Arc<ParameterRegistry>,
}

impl<Snapshot> Clone for StageContext<Snapshot> {
    fn clone(&self) -> Self {
        let Self { snapshot, thing_manager, parameters } = self;
        Self { snapshot: snapshot.clone(), thing_manager: thing_manager.clone(), parameters: parameters.clone() }
    }
}

pub trait StageAPI<Snapshot> {
    type OutputIterator: StageIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, StageContext<Snapshot>), (PipelineExecutionError, StageContext<Snapshot>)>;
}

pub trait StageIterator:
    for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>> + Sized
{
    fn collect_owned(mut self) -> Result<Batch, PipelineExecutionError> {
        // specific iterators can optimise this by not iterating + collecting!
        let first = self.next();
        let mut batch = match first {
            None => return Ok(Batch::new(0, 1)),
            Some(row) => {
                let row = row?;
                let mut batch = Batch::new(row.len() as u32, 10);
                batch.append(row);
                batch
            }
        };
        while let Some(row) = self.next() {
            let row = row?;
            batch.append(row);
        }
        Ok(batch)
    }
}

pub enum ReadPipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(Box<MatchStageExecutor<ReadPipelineStage<Snapshot>>>),
    Sort(Box<SortStageExecutor<ReadPipelineStage<Snapshot>>>),
    Limit(Box<LimitStageExecutor<ReadPipelineStage<Snapshot>>>),
    Offset(Box<OffsetStageExecutor<ReadPipelineStage<Snapshot>>>),
    Select(Box<SelectStageExecutor<ReadPipelineStage<Snapshot>>>),
}

pub enum ReadStageIterator<Snapshot: ReadableSnapshot + 'static> {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
    Sort(SortStageIterator<Snapshot>),
    Limit(Box<LimitStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
    Offset(Box<OffsetStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
    Select(Box<SelectStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
}

impl<Snapshot: ReadableSnapshot + 'static> StageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    type OutputIterator = ReadStageIterator<Snapshot>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, StageContext<Snapshot>), (PipelineExecutionError, StageContext<Snapshot>)> {
        match self {
            ReadPipelineStage::Initial(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Initial(iterator), snapshot))
            }
            ReadPipelineStage::Match(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Match(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Sort(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Sort(iterator), snapshot))
            }
            ReadPipelineStage::Offset(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Offset(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Limit(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Limit(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Select(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Select(Box::new(iterator)), snapshot))
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
            ReadStageIterator::Sort(iterator) => iterator.next(),
            ReadStageIterator::Offset(iterator) => iterator.next(),
            ReadStageIterator::Limit(iterator) => iterator.next(),
            ReadStageIterator::Select(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> StageIterator for ReadStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.collect_owned(),
            ReadStageIterator::Match(iterator) => iterator.collect_owned(),
            ReadStageIterator::Sort(iterator) => iterator.collect_owned(),
            ReadStageIterator::Offset(iterator) => iterator.collect_owned(),
            ReadStageIterator::Limit(iterator) => iterator.collect_owned(),
            ReadStageIterator::Select(iterator) => iterator.collect_owned(),
        }
    }
}

pub enum WritePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialStage<Snapshot>),
    Match(Box<MatchStageExecutor<WritePipelineStage<Snapshot>>>),
    Insert(Box<InsertStageExecutor<WritePipelineStage<Snapshot>>>),
    Delete(Box<DeleteStageExecutor<WritePipelineStage<Snapshot>>>),
    Sort(Box<SortStageExecutor<WritePipelineStage<Snapshot>>>),
    Limit(Box<LimitStageExecutor<WritePipelineStage<Snapshot>>>),
    Offset(Box<OffsetStageExecutor<WritePipelineStage<Snapshot>>>),
    Select(Box<SelectStageExecutor<WritePipelineStage<Snapshot>>>),
}

impl<Snapshot: WritableSnapshot + 'static> StageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    type OutputIterator = WriteStageIterator<Snapshot>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, StageContext<Snapshot>), (PipelineExecutionError, StageContext<Snapshot>)> {
        match self {
            WritePipelineStage::Initial(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Initial(iterator), context))
            }
            WritePipelineStage::Match(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Match(Box::new(iterator)), context))
            }
            WritePipelineStage::Insert(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Write(iterator), context))
            }
            WritePipelineStage::Delete(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Write(iterator), context))
            }
            WritePipelineStage::Sort(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Sort(iterator), snapshot))
            }
            WritePipelineStage::Limit(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Limit(Box::new(iterator)), snapshot))
            }
            WritePipelineStage::Offset(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Offset(Box::new(iterator)), snapshot))
            }
            WritePipelineStage::Select(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Select(Box::new(iterator)), snapshot))
            }
        }
    }
}

pub enum WriteStageIterator<Snapshot: WritableSnapshot + 'static> {
    Initial(InitialIterator),
    Match(Box<MatchStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
    Write(WrittenRowsIterator),
    Sort(SortStageIterator<Snapshot>),
    Limit(Box<LimitStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
    Offset(Box<OffsetStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
    Select(Box<SelectStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
}

impl<Snapshot: WritableSnapshot + 'static> LendingIterator for WriteStageIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.next(),
            WriteStageIterator::Match(iterator) => iterator.next(),
            WriteStageIterator::Write(iterator) => iterator.next(),
            WriteStageIterator::Sort(iterator) => iterator.next(),
            WriteStageIterator::Limit(iterator) => iterator.next(),
            WriteStageIterator::Offset(iterator) => iterator.next(),
            WriteStageIterator::Select(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: WritableSnapshot + 'static> StageIterator for WriteStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.collect_owned(),
            WriteStageIterator::Match(iterator) => iterator.collect_owned(),
            WriteStageIterator::Write(iterator) => iterator.collect_owned(),
            WriteStageIterator::Sort(iterator) => iterator.collect_owned(),
            WriteStageIterator::Limit(iterator) => iterator.collect_owned(),
            WriteStageIterator::Offset(iterator) => iterator.collect_owned(),
            WriteStageIterator::Select(iterator) => iterator.collect_owned(),
        }
    }
}
