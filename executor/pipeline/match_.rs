/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{iter::Peekable, sync::Arc};

use compiler::executable::{function::ExecutableFunctionRegistry, match_::planner::match_executable::MatchExecutable};
use itertools::{Itertools, UniqueBy};
use lending_iterator::{adaptors::Map, IntoIter, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ReadExecutionError,
    match_executor::{MatchExecutor, PatternIterator},
    pipeline::{
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError, StageIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct MatchStageExecutor<PreviousStage> {
    executable: Arc<MatchExecutable>,
    previous: PreviousStage,
    function_registry: Arc<ExecutableFunctionRegistry>,
}

impl<PreviousStage> MatchStageExecutor<PreviousStage> {
    pub fn new(
        executable: Arc<MatchExecutable>,
        previous: PreviousStage,
        function_registry: Arc<ExecutableFunctionRegistry>,
    ) -> Self {
        Self { executable, previous, function_registry }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for MatchStageExecutor<PreviousStage>
where
    PreviousStage: StageAPI<Snapshot>,
    Snapshot: ReadableSnapshot + 'static,
{
    type OutputIterator = MatchStageIterator<Snapshot, PreviousStage::OutputIterator>;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let Self { previous: previous_stage, executable, function_registry, .. } = self;
        let (previous_iterator, context) = previous_stage.into_iterator(interrupt.clone())?;
        let iterator = previous_iterator;
        Ok((MatchStageIterator::new(iterator, executable, function_registry, context.clone(), interrupt), context))
    }
}

pub struct MatchStageIterator<Snapshot: ReadableSnapshot + 'static, Iterator> {
    context: ExecutionContext<Snapshot>,
    executable: Arc<MatchExecutable>,
    function_registry: Arc<ExecutableFunctionRegistry>,
    source_iterator: Iterator,
    current_iterator: Option<Peekable<UniqueRows<AsOwnedRows<PatternIterator<Snapshot>>>>>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot: ReadableSnapshot + 'static, Iterator> MatchStageIterator<Snapshot, Iterator> {
    fn new(
        iterator: Iterator,
        executable: Arc<MatchExecutable>,
        function_registry: Arc<ExecutableFunctionRegistry>,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { context, executable, function_registry, source_iterator: iterator, current_iterator: None, interrupt }
    }
}

impl<Snapshot, Iterator> LendingIterator for MatchStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while !self.current_iterator.as_mut().is_some_and(|iter| iter.peek().is_some()) {
            let ExecutionContext { snapshot, thing_manager, profile, .. } = &self.context;

            let input_row = match self.source_iterator.next()? {
                Ok(row) => row,
                Err(err) => return Some(Err(err)),
            };

            let executor = MatchExecutor::new(
                &self.executable,
                snapshot,
                thing_manager,
                input_row,
                self.function_registry.clone(),
                profile,
            )
            .map_err(|err| Box::new(PipelineExecutionError::InitialisingMatchIterator { typedb_source: err }));

            match executor {
                Ok(executor) => {
                    self.current_iterator = Some(
                        unique_rows(as_owned_rows(
                            executor.into_iterator(self.context.clone(), self.interrupt.clone()),
                        ))
                        .peekable(),
                    );
                }
                Err(err) => return Some(Err(err)),
            };
        }
        self.current_iterator.as_mut().unwrap().next().map(|result| {
            result.map_err(|err| Box::new(PipelineExecutionError::ReadPatternExecution { typedb_source: err.clone() }))
        })
    }
}

impl<Snapshot, Iterator> StageIterator for MatchStageIterator<Snapshot, Iterator>
where
    Snapshot: ReadableSnapshot + 'static,
    Iterator: StageIterator,
{
}

type AsOwnedRows<I> = IntoIter<
    Map<
        I,
        fn(Result<MaybeOwnedRow<'_>, &ReadExecutionError>) -> Result<MaybeOwnedRow<'static>, ReadExecutionError>,
        lending_iterator::higher_order::AdHocHkt<Result<MaybeOwnedRow<'static>, ReadExecutionError>>,
    >,
>;
fn as_owned_rows<I>(iter: I) -> AsOwnedRows<I>
where
    I: for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>>,
{
    iter.map_static(
        (|row| match row {
            Ok(row) => Ok(row.into_owned()),
            Err(err) => Err(err.clone()),
        })
            as fn(Result<MaybeOwnedRow<'_>, &ReadExecutionError>) -> Result<MaybeOwnedRow<'static>, ReadExecutionError>,
    )
    .into_iter()
}

type UniqueRows<I> = UniqueBy<
    I,
    Result<MaybeOwnedRow<'static>, ()>,
    fn(&Result<MaybeOwnedRow<'static>, ReadExecutionError>) -> Result<MaybeOwnedRow<'static>, ()>,
>;

fn unique_rows<I>(iter: I) -> UniqueRows<I>
where
    I: Iterator<Item = Result<MaybeOwnedRow<'static>, ReadExecutionError>>,
{
    iter.unique_by(
        (|item| match item {
            Ok(row) => Ok(row.clone()),
            Err(_) => Err(()),
        }) as fn(&Result<MaybeOwnedRow<'static>, ReadExecutionError>) -> Result<MaybeOwnedRow<'static>, ()>,
    )
}
