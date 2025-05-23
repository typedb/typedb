/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use ir::pipeline::ParameterRegistry;
use lending_iterator::LendingIterator;
use resource::{constants::traversal::BATCH_DEFAULT_CAPACITY, profile::QueryProfile};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::Batch,
    pipeline::{
        delete::DeleteStageExecutor,
        initial::{InitialIterator, InitialStage},
        insert::InsertStageExecutor,
        match_::{MatchStageExecutor, MatchStageIterator},
        modifiers::{
            DistinctStageExecutor, DistinctStageIterator, LimitStageExecutor, LimitStageIterator, OffsetStageExecutor,
            OffsetStageIterator, RequireStageExecutor, RequireStageIterator, SelectStageExecutor, SelectStageIterator,
            SortStageExecutor, SortStageIterator,
        },
        put::PutStageExecutor,
        reduce::ReduceStageExecutor,
        update::UpdateStageExecutor,
        PipelineExecutionError, WrittenRowsIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

#[derive(Debug)]
pub struct ExecutionContext<Snapshot> {
    pub snapshot: Arc<Snapshot>,
    pub thing_manager: Arc<ThingManager>,
    pub parameters: Arc<ParameterRegistry>,
    pub profile: Arc<QueryProfile>,
}

impl<Snapshot> ExecutionContext<Snapshot> {
    pub fn new(snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>, parameters: Arc<ParameterRegistry>) -> Self {
        Self::new_with_profile(snapshot, thing_manager, parameters, Arc::new(QueryProfile::new(false)))
    }

    pub fn new_with_profile(
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
        parameters: Arc<ParameterRegistry>,
        query_profile: Arc<QueryProfile>,
    ) -> Self {
        Self { snapshot, thing_manager, parameters, profile: query_profile }
    }

    pub(crate) fn clone_with_replaced_parameters(&self, parameters: Arc<ParameterRegistry>) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            thing_manager: self.thing_manager.clone(),
            parameters,
            profile: self.profile.clone(),
        }
    }

    pub(crate) fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snapshot
    }

    pub(crate) fn thing_manager(&self) -> &Arc<ThingManager> {
        &self.thing_manager
    }

    pub(crate) fn type_manager(&self) -> &TypeManager {
        self.thing_manager.type_manager()
    }

    pub(crate) fn parameters(&self) -> &ParameterRegistry {
        &self.parameters
    }
}

impl<Snapshot> Clone for ExecutionContext<Snapshot> {
    fn clone(&self) -> Self {
        let Self { snapshot, thing_manager, parameters, profile } = self;
        Self {
            snapshot: snapshot.clone(),
            thing_manager: thing_manager.clone(),
            parameters: parameters.clone(),
            profile: profile.clone(),
        }
    }
}

pub trait StageAPI<Snapshot> {
    type OutputIterator: StageIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    >;
}

pub trait StageIterator:
    for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>> + Sized
{
    fn collect_owned(mut self) -> Result<Batch, Box<PipelineExecutionError>> {
        // specific iterators can optimise this by not iterating + collecting!
        let first = self.next();
        let mut batch = match first {
            None => {
                return Ok(Batch::new(0, 0));
            }
            Some(row) => {
                let row = row?;
                let mut batch = Batch::new(row.len() as u32, BATCH_DEFAULT_CAPACITY);
                batch.append_row(row);
                batch
            }
        };
        while let Some(row) = self.next() {
            let row = row?;
            batch.append_row(row);
        }
        Ok(batch)
    }

    fn multiplicity_sum_if_collected(&self) -> Option<usize> {
        None
    }
}

pub enum ReadPipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Initial(Box<InitialStage<Snapshot>>),
    Match(Box<MatchStageExecutor<ReadPipelineStage<Snapshot>>>),
    Select(Box<SelectStageExecutor<ReadPipelineStage<Snapshot>>>),
    Sort(Box<SortStageExecutor<ReadPipelineStage<Snapshot>>>),
    Distinct(Box<DistinctStageExecutor<ReadPipelineStage<Snapshot>>>),
    Limit(Box<LimitStageExecutor<ReadPipelineStage<Snapshot>>>),
    Offset(Box<OffsetStageExecutor<ReadPipelineStage<Snapshot>>>),
    Require(Box<RequireStageExecutor<ReadPipelineStage<Snapshot>>>),
    Reduce(Box<ReduceStageExecutor<ReadPipelineStage<Snapshot>>>),
}

pub enum ReadStageIterator<Snapshot: ReadableSnapshot + 'static> {
    Initial(Box<InitialIterator>),
    Match(Box<MatchStageIterator<Snapshot, ReadStageIterator<Snapshot>>>),
    Sort(SortStageIterator),
    Distinct(Box<DistinctStageIterator<ReadStageIterator<Snapshot>>>),
    Limit(Box<LimitStageIterator<ReadStageIterator<Snapshot>>>),
    Offset(Box<OffsetStageIterator<ReadStageIterator<Snapshot>>>),
    Select(Box<SelectStageIterator<ReadStageIterator<Snapshot>>>),
    Require(Box<RequireStageIterator<ReadStageIterator<Snapshot>>>),
    Reduce(Box<WrittenRowsIterator>), // TODO: ReduceStageIterator<ReadStageIterator<Snapshot>>>
}

impl<Snapshot: ReadableSnapshot + 'static> StageAPI<Snapshot> for ReadPipelineStage<Snapshot> {
    type OutputIterator = ReadStageIterator<Snapshot>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self {
            ReadPipelineStage::Initial(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Initial(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Match(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Match(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Sort(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Sort(iterator), snapshot))
            }
            ReadPipelineStage::Distinct(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Distinct(Box::new(iterator)), snapshot))
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
            ReadPipelineStage::Require(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Require(Box::new(iterator)), snapshot))
            }
            ReadPipelineStage::Reduce(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((ReadStageIterator::Reduce(Box::new(iterator)), snapshot))
            }
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for ReadStageIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.next(),
            ReadStageIterator::Match(iterator) => iterator.next(),
            ReadStageIterator::Sort(iterator) => iterator.next(),
            ReadStageIterator::Distinct(iterator) => iterator.next(),
            ReadStageIterator::Offset(iterator) => iterator.next(),
            ReadStageIterator::Limit(iterator) => iterator.next(),
            ReadStageIterator::Select(iterator) => iterator.next(),
            ReadStageIterator::Require(iterator) => iterator.next(),
            ReadStageIterator::Reduce(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> StageIterator for ReadStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, Box<PipelineExecutionError>> {
        match self {
            ReadStageIterator::Initial(iterator) => iterator.collect_owned(),
            ReadStageIterator::Match(iterator) => iterator.collect_owned(),
            ReadStageIterator::Sort(iterator) => iterator.collect_owned(),
            ReadStageIterator::Distinct(iterator) => iterator.collect_owned(),
            ReadStageIterator::Offset(iterator) => iterator.collect_owned(),
            ReadStageIterator::Limit(iterator) => iterator.collect_owned(),
            ReadStageIterator::Select(iterator) => iterator.collect_owned(),
            ReadStageIterator::Require(iterator) => iterator.collect_owned(),
            ReadStageIterator::Reduce(iterator) => iterator.collect_owned(),
        }
    }
}

pub enum WritePipelineStage<Snapshot: WritableSnapshot + 'static> {
    Initial(Box<InitialStage<Snapshot>>),
    Match(Box<MatchStageExecutor<WritePipelineStage<Snapshot>>>),
    Insert(Box<InsertStageExecutor<WritePipelineStage<Snapshot>>>),
    Update(Box<UpdateStageExecutor<WritePipelineStage<Snapshot>>>),
    Put(Box<PutStageExecutor<WritePipelineStage<Snapshot>>>),
    Delete(Box<DeleteStageExecutor<WritePipelineStage<Snapshot>>>),
    Sort(Box<SortStageExecutor<WritePipelineStage<Snapshot>>>),
    Limit(Box<LimitStageExecutor<WritePipelineStage<Snapshot>>>),
    Offset(Box<OffsetStageExecutor<WritePipelineStage<Snapshot>>>),
    Select(Box<SelectStageExecutor<WritePipelineStage<Snapshot>>>),
    Require(Box<RequireStageExecutor<WritePipelineStage<Snapshot>>>),
    Distinct(Box<DistinctStageExecutor<WritePipelineStage<Snapshot>>>),
    Reduce(Box<ReduceStageExecutor<WritePipelineStage<Snapshot>>>),
}

impl<Snapshot: WritableSnapshot + 'static> StageAPI<Snapshot> for WritePipelineStage<Snapshot> {
    type OutputIterator = WriteStageIterator<Snapshot>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self {
            WritePipelineStage::Initial(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Initial(Box::new(iterator)), context))
            }
            WritePipelineStage::Match(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Match(Box::new(iterator)), context))
            }
            WritePipelineStage::Insert(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Write(iterator), context))
            }
            WritePipelineStage::Update(stage) => {
                let (iterator, context) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Write(iterator), context))
            }
            WritePipelineStage::Put(stage) => {
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
            WritePipelineStage::Distinct(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Distinct(Box::new(iterator)), snapshot))
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
            WritePipelineStage::Require(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Require(Box::new(iterator)), snapshot))
            }
            WritePipelineStage::Reduce(stage) => {
                let (iterator, snapshot) = stage.into_iterator(interrupt)?;
                Ok((WriteStageIterator::Reduce(iterator), snapshot))
            }
        }
    }
}

pub enum WriteStageIterator<Snapshot: WritableSnapshot + 'static> {
    Initial(Box<InitialIterator>),
    Match(Box<MatchStageIterator<Snapshot, WriteStageIterator<Snapshot>>>),
    Write(WrittenRowsIterator),
    Sort(SortStageIterator),
    Distinct(Box<DistinctStageIterator<WriteStageIterator<Snapshot>>>),
    Limit(Box<LimitStageIterator<WriteStageIterator<Snapshot>>>),
    Offset(Box<OffsetStageIterator<WriteStageIterator<Snapshot>>>),
    Select(Box<SelectStageIterator<WriteStageIterator<Snapshot>>>),
    Require(Box<RequireStageIterator<WriteStageIterator<Snapshot>>>),
    Reduce(WrittenRowsIterator),
}

impl<Snapshot: WritableSnapshot + 'static> LendingIterator for WriteStageIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.next(),
            WriteStageIterator::Match(iterator) => iterator.next(),
            WriteStageIterator::Write(iterator) => iterator.next(),
            WriteStageIterator::Sort(iterator) => iterator.next(),
            WriteStageIterator::Distinct(iterator) => iterator.next(),
            WriteStageIterator::Limit(iterator) => iterator.next(),
            WriteStageIterator::Offset(iterator) => iterator.next(),
            WriteStageIterator::Select(iterator) => iterator.next(),
            WriteStageIterator::Require(iterator) => iterator.next(),
            WriteStageIterator::Reduce(iterator) => iterator.next(),
        }
    }
}

impl<Snapshot: WritableSnapshot + 'static> StageIterator for WriteStageIterator<Snapshot> {
    fn collect_owned(self) -> Result<Batch, Box<PipelineExecutionError>> {
        match self {
            WriteStageIterator::Initial(iterator) => iterator.collect_owned(),
            WriteStageIterator::Match(iterator) => iterator.collect_owned(),
            WriteStageIterator::Write(iterator) => iterator.collect_owned(),
            WriteStageIterator::Sort(iterator) => iterator.collect_owned(),
            WriteStageIterator::Distinct(iterator) => iterator.collect_owned(),
            WriteStageIterator::Limit(iterator) => iterator.collect_owned(),
            WriteStageIterator::Offset(iterator) => iterator.collect_owned(),
            WriteStageIterator::Select(iterator) => iterator.collect_owned(),
            WriteStageIterator::Require(iterator) => iterator.collect_owned(),
            WriteStageIterator::Reduce(iterator) => iterator.collect_owned(),
        }
    }
}
