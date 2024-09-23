/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use error::typedb_error;
use lending_iterator::LendingIterator;

use crate::{
    batch::Batch, error::ReadExecutionError, pipeline::stage::StageIterator, row::MaybeOwnedRow, write::WriteError,
};

pub mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod reduce;
pub mod stage;


typedb_error!(
    pub PipelineExecutionError(component = "Pipeline execution", prefix = "PEX") {
        // TODO: migrate to `typedb_error` once they are typedb errors
        Interrupted(1, "Query was interrupted."),
        ConceptRead(2, "Error reading concept.", ( source: ConceptReadError )),
        InitialisingMatchIterator(3, "Error initialising Match clause iterator.", ( source: ConceptReadError )),
        WriteError(4, "Error executing write operation.", ( typedb_source: WriteError )),
        ReadPatternExecution(5, "Error executing a read pattern.", ( typedb_source : ReadExecutionError )),
    }
);

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
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let index = self.index;
        if index < self.rows.len() {
            self.index += 1;
            Some(Ok(self.rows.get_row(index)))
        } else {
            None
        }
    }
}

impl StageIterator for WrittenRowsIterator {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
        debug_assert!(self.index == 0, "Truncating start of rows is not implemented");
        Ok(self.rows)
    }
}
