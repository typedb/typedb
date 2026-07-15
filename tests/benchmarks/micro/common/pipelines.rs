/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use executor::{
    ExecutionInterrupt,
    pipeline::{
        PipelineExecutionError,
        pipeline::Pipeline,
        stage::{ExecutionContext, ReadPipelineStage, WritePipelineStage},
    },
};
use query::{error::QueryError, given_rows::GivenRowsSimple};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    AnswerConsumer,
    transaction::{PartialTx, UnifiedTransactionView, WriteTransactionView},
    utils::pack_result,
};

pub(super) fn execute_read_query_in<TX: UnifiedTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<(AC::Output, TX), (Box<QueryError>, TX)> {
    let (snapshot, partial_tx) = tx.into_parts();
    let prepare_result = prepare_read_pipeline(snapshot, partial_tx, query, given_rows);
    let ReadPipelineWrapper { pipeline, partial_tx, .. } = prepare_result.map_err(to_err_and_tx)?;
    let interrupt = ExecutionInterrupt::new_uninterruptible();
    if pipeline.has_fetch() {
        let result = pipeline
            .into_documents_iterator(interrupt)
            .and_then(|(mut iter, context)| pack_result(AC::consume_docs(&mut iter), context));
        to_result_with_tx(partial_tx, query, result)
    } else {
        let result = pipeline
            .into_rows_iterator(interrupt)
            .and_then(|(mut iter, context)| pack_result(AC::consume_rows(&mut iter), context));
        to_result_with_tx(partial_tx, query, result)
    }
}

pub(super) fn execute_write_query_in<TX: UnifiedTransactionView + WriteTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<(AC::Output, TX), (Box<QueryError>, TX)> {
    let (arc_snapshot, partial_tx) = tx.into_parts();
    let snapshot = Arc::into_inner(arc_snapshot).expect("Expected exclusive ownership");
    let prepare_result = prepare_write_pipeline(snapshot, partial_tx, query, given_rows);
    let WritePipelineWrapper { pipeline, partial_tx, .. } = prepare_result.map_err(to_err_and_tx)?;
    let interrupt = ExecutionInterrupt::new_uninterruptible();
    if pipeline.has_fetch() {
        let result = pipeline
            .into_documents_iterator(interrupt)
            .and_then(|(mut iter, context)| pack_result(AC::consume_docs(&mut iter), context));
        to_result_with_tx(partial_tx, query, result)
    } else {
        let result = pipeline
            .into_rows_iterator(interrupt)
            .and_then(|(mut iter, context)| pack_result(AC::consume_rows(&mut iter), context));
        to_result_with_tx(partial_tx, query, result)
    }
}

struct ReadPipelineWrapper<Snapshot: ReadableSnapshot + 'static> {
    snapshot: Arc<Snapshot>,
    partial_tx: PartialTx,
    pipeline: Pipeline<Snapshot, ReadPipelineStage<Snapshot>>,
}

struct WritePipelineWrapper<Snapshot: WritableSnapshot + 'static> {
    partial_tx: PartialTx,
    pipeline: Pipeline<Snapshot, WritePipelineStage<Snapshot>>,
}

fn prepare_read_pipeline<Snapshot: ReadableSnapshot>(
    snapshot: Arc<Snapshot>,
    partial_tx: PartialTx,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<ReadPipelineWrapper<Snapshot>, ErrorWrapper<Snapshot, Box<QueryError>>> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let prepare_result = partial_tx.query_manager.prepare_read_pipeline(
        snapshot.clone(),
        &partial_tx.type_manager,
        partial_tx.thing_manager.clone(),
        partial_tx.function_manager.clone(),
        &parsed,
        given_rows,
        &query,
    );
    match prepare_result {
        Ok(pipeline) => Ok(ReadPipelineWrapper { snapshot, partial_tx, pipeline }),
        Err(err) => Err(ErrorWrapper::new(snapshot, partial_tx, err)),
    }
}

fn prepare_write_pipeline<Snapshot: WritableSnapshot>(
    snapshot: Snapshot,
    partial_tx: PartialTx,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<WritePipelineWrapper<Snapshot>, ErrorWrapper<Snapshot, Box<QueryError>>> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let prepare_result = partial_tx.query_manager.prepare_write_pipeline(
        snapshot,
        &partial_tx.type_manager,
        partial_tx.thing_manager.clone(),
        partial_tx.function_manager.clone(),
        &parsed,
        given_rows,
        &query,
    );
    match prepare_result {
        Ok(pipeline) => Ok(WritePipelineWrapper { partial_tx, pipeline }),
        Err((snapshot, err)) => Err(ErrorWrapper::new(Arc::new(snapshot), partial_tx, err)),
    }
}

// Local util structs & functions
struct ErrorWrapper<Snapshot, Err> {
    snapshot: Arc<Snapshot>,
    partial_tx: PartialTx,
    err: Err,
}

impl<Snapshot: ReadableSnapshot, Err> ErrorWrapper<Snapshot, Err> {
    fn new(snapshot: Arc<Snapshot>, partial_tx: PartialTx, err: Err) -> Self {
        Self { snapshot, partial_tx, err }
    }
}

fn to_err_and_tx<TX: UnifiedTransactionView, Err>(error_wrapper: ErrorWrapper<TX::Snapshot, Err>) -> (Err, TX) {
    (error_wrapper.err, TX::reconstruct(error_wrapper.snapshot, error_wrapper.partial_tx))
}

fn to_result_with_tx<T, TX: UnifiedTransactionView>(
    partial_tx: PartialTx,
    source_query: &str,
    result: Result<(T, ExecutionContext<TX::Snapshot>), (Box<PipelineExecutionError>, ExecutionContext<TX::Snapshot>)>,
) -> Result<(T, TX), (Box<QueryError>, TX)> {
    match result {
        Ok((consumed, context)) => Ok((consumed, TX::reconstruct(context.snapshot, partial_tx))),
        Err((err, context)) => Err((
            Box::new(QueryError::ReadPipelineExecution { source_query: source_query.to_owned(), typedb_source: err }),
            TX::reconstruct(context.snapshot, partial_tx),
        )),
    }
}
