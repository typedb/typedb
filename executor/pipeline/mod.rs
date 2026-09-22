/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use compiler::{
    ExecutorVariable,
    executable::{WritePatternCondition, match_::instructions::CheckInstruction},
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use error::typedb_error;
use ir::pipeline::ParameterRegistry;
use lending_iterator::LendingIterator;
use resource::profile::{StepProfile, StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    InterruptType,
    batch::Batch,
    error::ReadExecutionError,
    instruction::checker::Checker,
    pipeline::{fetch::FetchExecutionError, stage::StageIterator},
    row::{MaybeOwnedRow, Row},
    write::WriteError,
};

pub mod delete;
pub mod fetch;
mod given;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod pipeline;
pub mod put;
pub mod reduce;
pub mod stage;
pub mod update;

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
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

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

fn write_condition_satisfied(
    condition: &WritePatternCondition,
    row: &Row<'_>,
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    profile: &StepProfile,
) -> Result<bool, Box<WriteError>> {
    let row = MaybeOwnedRow::new_from_row(row);
    Checker::filter(&*condition, &row, snapshot, thing_manager, parameters, profile.storage_counters())
        .map_err(|typedb_source| Box::new(WriteError::ConceptRead { typedb_source }))
}

impl StageIterator for WrittenRowsIterator {
    fn collect_owned(self) -> Result<Batch, Box<PipelineExecutionError>> {
        debug_assert!(self.index == 0, "Truncating start of rows is not implemented");
        Ok(self.rows)
    }

    fn multiplicity_sum_if_collected(&self) -> Option<usize> {
        Some(self.rows.get_multiplicities().iter().sum::<u64>() as usize)
    }
}

typedb_error! {
    pub PipelineExecutionError(component = "Pipeline execution", prefix = "PEX") {
        // TODO: migrate to `typedb_error` once they are typedb errors
        Interrupted(1, "Execution interrupted by a concurrent {interrupt}.", interrupt: InterruptType),
        FetchUsedAsRows(2, "Cannot use a Fetch query to return ConceptRows"),
        RowsUsedAsFetch(3, "Cannot use query returning ConceptRows as a Fetch query."),
        ConceptRead(4, "Error reading concept.", typedb_source: Box<ConceptReadError>),
        InitialisingMatchIterator(5, "Error initialising Match clause iterator.", typedb_source: Box<ConceptReadError>),
        WriteError(6, "Error executing write operation.", typedb_source: Box<WriteError>),
        ReadPatternExecution(7, "Error executing a read pattern.", typedb_source: ReadExecutionError),
        FetchError(8, "Error executing fetch operation.", typedb_source: FetchExecutionError),
        GivenValueDidNotSatisfyDeclaredType(
            9,
            "The given value at row '{row_index}' and column '{column_index}' does not not satisfy the declared type",
            row_index: usize,
            column_index: usize,
        ),
        GivenValueDidNotSatisfyDeclaredOptionality(
            10,
            "The given value at row '{row_index}' and column '{column_index}' was None, but the variable was not declared optional.",
            row_index: usize,
            column_index: usize,
        ),
        GivenConceptDoesNotExist(
            11,
            "The given instance at row '{row_index}' and column '{column_index}' does not exist in the database",
            row_index: usize,
            column_index: usize,
        ),
    }
}
