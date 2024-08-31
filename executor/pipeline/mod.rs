/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::Arc,
};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{batch::Batch, row::MaybeOwnedRow, write::WriteError};

pub mod accumulator;
pub mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod stage;

pub trait StageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    type OutputIterator: StageIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError>;
}

pub trait StageIterator: for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>> + Sized {
    fn collect_owned(mut self) -> Result<Batch, PipelineError> {
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

#[derive(Debug, Clone)]
pub enum PipelineError {
    ConceptRead(ConceptReadError),
    InitialisingMatchIterator(ConceptReadError),
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

// Can be used as normal lending iterator, or optimally collect into owned using `collect_owned()`
pub struct WrittenRowsIterator {
    rows: Batch,
    index: usize,
}

impl WrittenRowsIterator {
    pub(crate) fn new(rows: Batch) -> Self {
        Self { rows, index: 0 }
    }
}

impl LendingIterator for WrittenRowsIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let index = self.index;
        if index < self.rows.len() {
            self.index += 1;
            Some(Ok(self.rows.get_row(index)))
        } else {
            return None;
        }
    }
}

impl StageIterator for WrittenRowsIterator {
    fn collect_owned(self) -> Result<Batch, PipelineError> {
        debug_assert!(self.index == 0, "Truncating start of rows is not implemented");
        Ok(self.rows)
    }
}
