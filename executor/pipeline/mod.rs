/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt::{Display, Formatter}, sync::Arc, vec};
use itertools::Itertools;

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::MaybeOwnedRow,
    write::WriteError,
};

pub mod accumulator;
mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod stage;

pub trait StageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    type OutputIterator: StageIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError>;
}

pub trait StageIterator: for<'a> LendingIterator<Item<'a>=Result<MaybeOwnedRow<'a>, PipelineError>> {
    fn collect_owned(self) -> Result<Vec<MaybeOwnedRow<'static>>, PipelineError> {
        // specific iterators can optimise this by not iterating + collecting!
        let rows: Vec<MaybeOwnedRow<'static>> = self
            .map_static(|result| result.map(|row| row.into_owned()))
            .try_collect()?;
        Ok(rows)
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
struct WrittenRowsIterator {
    rows: Vec<MaybeOwnedRow<'static>>,
    index: usize,
}

impl WrittenRowsIterator {
    pub(crate) fn new(rows: Vec<MaybeOwnedRow<'static>>) -> Self {
        Self { rows, index: 0 }
    }
}

impl LendingIterator for WrittenRowsIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let index = self.index;
        if index >= self.rows.len() {
            return None
        } else {
            self.index += 1;
            let row = &self.rows[index];
            Some(Ok(row.as_reference()))
        }
    }
}

impl StageIterator for WrittenRowsIterator {
    fn collect_owned(self) -> Result<Vec<MaybeOwnedRow<'static>>, PipelineError> {
        debug_assert!(self.index == 0, "Truncating start of rows is not implemented");
        Ok(self.rows)
    }
}
