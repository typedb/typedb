/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::{
    function::ExecutableFunctionRegistry, match_::planner::conjunction_executable::ConjunctionExecutable,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pipeline::QueryContext;
use lending_iterator::{AsLendingIterator, LendingIterator, adaptors::FlatMap};
use resource::profile::QueryProfile;
use storage::snapshot::ReadableSnapshot;

use crate::{
    ExecutionInterrupt,
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{
        create_pattern_executor_for_conjunction, pattern_executor::PatternExecutor, tabled_functions::TabledFunctions,
    },
    row::MaybeOwnedRow,
};

pub struct MatchExecutor {
    entry: PatternExecutor,
    input: Option<MaybeOwnedRow<'static>>,
    tabled_functions: TabledFunctions,
}

impl MatchExecutor {
    pub fn new(
        conjunction_executable: &ConjunctionExecutable,
        execution_context: &ExecutionContext<impl ReadableSnapshot>,
        input: MaybeOwnedRow<'_>,
        function_registry: Arc<ExecutableFunctionRegistry>,
        profile: &Arc<QueryProfile>,
    ) -> Result<Self, Box<ConceptReadError>> {
        let stage_profile = profile.profile_stage(|| String::from("Match"), conjunction_executable.executable_id());
        Ok(Self {
            entry: create_pattern_executor_for_conjunction(
                execution_context,
                &function_registry,
                conjunction_executable,
                stage_profile,
            )?,
            tabled_functions: TabledFunctions::new(function_registry, profile.clone()),
            input: Some(input.into_owned()),
        })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        execution_context: ExecutionContext<Snapshot>,
        query_context: Arc<QueryContext>,
        interrupt: ExecutionInterrupt,
    ) -> PatternIterator<Snapshot> {
        PatternIterator::new(
            AsLendingIterator::new(BatchIterator::new(self, execution_context, query_context, interrupt)).flat_map(FixedBatchRowIterator::new),
        )
    }

    pub(super) fn compute_next_batch(
        &mut self,
        execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        query_context: &QueryContext,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, Box<ReadExecutionError>> {
        if let Some(input) = self.input.take() {
            self.entry.prepare(FixedBatch::from(input.into_owned()));
        }
        self.entry
            .compute_next_batch(execution_context, &query_context.parameters, interrupt, &mut self.tabled_functions)
            .map_err(|err| Box::new(err))
    }
}

pub(crate) struct BatchIterator<Snapshot> {
    executor: MatchExecutor,
    execution_context: ExecutionContext<Snapshot>,
    query_context: Arc<QueryContext>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(
        executor: MatchExecutor,
        execution_context: ExecutionContext<Snapshot>,
        query_context: Arc<QueryContext>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { executor, execution_context, query_context, interrupt }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Iterator for BatchIterator<Snapshot> {
    type Item = Result<FixedBatch, Box<ReadExecutionError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.executor.compute_next_batch(&self.execution_context, &self.query_context, &mut self.interrupt);
        batch.transpose()
    }
}

// Wrappers around
type PatternRowIterator<Snapshot> = FlatMap<
    AsLendingIterator<BatchIterator<Snapshot>>,
    FixedBatchRowIterator,
    fn(Result<FixedBatch, Box<ReadExecutionError>>) -> FixedBatchRowIterator,
>;

pub struct PatternIterator<Snapshot: ReadableSnapshot + 'static> {
    iterator: PatternRowIterator<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> PatternIterator<Snapshot> {
    fn new(iterator: PatternRowIterator<Snapshot>) -> Self {
        Self { iterator }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for PatternIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}
