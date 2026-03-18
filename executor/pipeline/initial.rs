/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use lending_iterator::LendingIterator;
use std::marker::PhantomData;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    pipeline::{
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow
    ,
};

pub struct InitialStage {
    initial_batch: FixedBatch,
}

impl InitialStage {
    pub fn new_empty() -> Self {
        Self { initial_batch: FixedBatch::SINGLE_EMPTY_ROW }
    }

    pub fn new_with(initial_row: MaybeOwnedRow<'_>) -> Self {
        let batch = FixedBatch::from(initial_row);
        Self { initial_batch: batch, }
    }

    pub(crate) fn into_iterator(
        self,
    ) -> InitialIterator {
        InitialIterator::new(self.initial_batch)
    }
}

pub struct InitialIterator {
    iterator: Box<FixedBatchRowIterator>,
    index: u32,
}

impl InitialIterator {
    pub(crate) fn new(batch: FixedBatch) -> Self {
        Self { iterator: Box::new(FixedBatchRowIterator::new(Ok(batch))), index: 0 }
    }
}

impl LendingIterator for InitialIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next().map(|result| {
            result.map_err(|err| Box::new(PipelineExecutionError::ReadPatternExecution { typedb_source: err.clone() }))
        })
    }
}

impl StageIterator for InitialIterator {}

// Dummy to allow InputIterator to conform to the stage API - would be better if InputIterator isn't a stage at all?
pub struct EmptyIterator {}

impl LendingIterator for EmptyIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        None
    }
}

impl StageIterator for EmptyIterator { }