/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use compiler::VariablePosition;
use concept::error::ConceptReadError;
use error::typedb_error;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{batch::Batch, error::ReadExecutionError, ExecutionInterrupt, pipeline::stage::StageIterator, row::MaybeOwnedRow, write::WriteError};
use crate::pipeline::fetch::FetchStageExecutor;
use crate::pipeline::stage::{ExecutionContext, StageAPI};
use crate::{
    InterruptType,
};

pub mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod reduce;
pub mod stage;
pub mod fetch;

pub enum Pipeline<Snapshot: ReadableSnapshot, Nonterminals: StageAPI<Snapshot>> {
    Unfetched(Nonterminals, HashMap<String, VariablePosition>),
    Fetched(Nonterminals, FetchStageExecutor<Snapshot>),
}

impl<Snapshot: ReadableSnapshot + 'static, Nonterminals: StageAPI<Snapshot>> Pipeline<Snapshot, Nonterminals> {
    fn new_unfetched(nonterminals: Nonterminals, variable_positions: HashMap<String, VariablePosition>) -> Self {
        Self::Unfetched(nonterminals, variable_positions)
    }

    fn new_fetched(nonterminals: Nonterminals, terminal: FetchStageExecutor<Snapshot>) -> Self {
        Self::Fetched(nonterminals, terminal)
    }

    pub fn rows_positions(&self) -> Option<&HashMap<String, VariablePosition>> {
        match self {
            Self::Unfetched(_, positions) => Some(positions),
            Self::Fetched(_, _) => None,
        }
    }

    pub fn into_rows_iterator(
        self,
        execution_interrupt: ExecutionInterrupt
    ) -> Result<(Nonterminals::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)> {
        match self {
            Self::Unfetched(nonterminals, _) => {
                nonterminals.into_iterator(execution_interrupt)
            }
            Self::Fetched(nonterminals, _) => {
                let (_, context) = nonterminals.into_iterator(execution_interrupt)?;
                Err((PipelineExecutionError::FetchUsedAsRows {}, context))
            }
        }
    }

    pub fn into_maps_iterator(
        self,
        execution_interrupt: ExecutionInterrupt
    ) -> Result<((), ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)> {
        match self {
            Self::Unfetched(nonterminals, _) => {
                let (_, context) = nonterminals.into_iterator(execution_interrupt)?;
                Err((PipelineExecutionError::FetchUsedAsRows {}, context))
            }
            Self::Fetched(nonterminals, terminal) => {
                let (rows_iterator, context) = nonterminals.into_iterator(execution_interrupt)?;
                todo!()
            }
        }
    }
}

typedb_error!(
    pub PipelineExecutionError(component = "Pipeline execution", prefix = "PEX") {
        // TODO: migrate to `typedb_error` once they are typedb errors
        Interrupted(1, "Execution interrupted by to a concurrent {interrupt}.", interrupt: InterruptType),
        FetchUsedAsRows(2, "Cannot use a Fetch query to return ConceptRows"),
        RowsUsedAsFetch(3, "Cannot use query returning ConceptRows as a Fetch query."),
        ConceptRead(4, "Error reading concept.", ( source: ConceptReadError )),
        InitialisingMatchIterator(5, "Error initialising Match clause iterator.", ( source: ConceptReadError )),
        WriteError(6, "Error executing write operation.", ( typedb_source: WriteError )),
        ReadPatternExecution(7, "Error executing a read pattern.", ( typedb_source : ReadExecutionError )),
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
