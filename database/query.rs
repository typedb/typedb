/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{sync::Arc, time::Instant};

use compiler::{query_structure::QueryStructure, VariablePosition};
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::stage::{ExecutionContext, StageIterator},
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use ir::pipeline::ParameterRegistry;
use itertools::{Either, Itertools};
use options::QueryOptions;
use query::{error::QueryError, query_manager::QueryManager};
use storage::{durability_client::WALClient, snapshot::WritableSnapshot};
use tracing::{event, Level};
use typeql::query::SchemaQuery;

use crate::{
    transaction::{TransactionSchema, TransactionWrite},
    with_transaction_parts,
};

pub type StreamQueryOutputDescriptor = Vec<(String, VariablePosition)>;
pub type WriteQueryBatchAnswer = (StreamQueryOutputDescriptor, Batch, Option<QueryStructure>);
pub type WriteQueryDocumentsAnswer = (Arc<ParameterRegistry>, Vec<ConceptDocument>);
pub type WriteQueryResult = Result<WriteQueryAnswer, Box<QueryError>>;

#[derive(Debug)]
pub struct WriteQueryAnswer {
    pub query_options: QueryOptions,
    pub answer: Either<WriteQueryBatchAnswer, WriteQueryDocumentsAnswer>,
}

impl WriteQueryAnswer {
    fn new_batch(query_options: QueryOptions, answer: WriteQueryBatchAnswer) -> Self {
        Self { query_options, answer: Either::Left(answer) }
    }

    fn new_documents(query_options: QueryOptions, answer: WriteQueryDocumentsAnswer) -> Self {
        Self { query_options, answer: Either::Right(answer) }
    }
}

pub fn execute_schema_query(
    transaction: TransactionSchema<WALClient>,
    query: SchemaQuery,
    source_query: String,
) -> (TransactionSchema<WALClient>, Result<(), Box<QueryError>>) {
    with_transaction_parts!(
        TransactionSchema,
        transaction,
        |inner_snapshot, type_manager, thing_manager, function_manager, query_manager| {
            query_manager.execute_schema(
                &mut inner_snapshot,
                &type_manager,
                &thing_manager,
                &function_manager,
                query,
                &source_query,
            )
        }
    )
}

pub fn execute_write_query_in_schema(
    transaction: TransactionSchema<WALClient>,
    query_options: QueryOptions,
    pipeline: typeql::query::Pipeline,
    source_query: String,
    interrupt: ExecutionInterrupt,
) -> (TransactionSchema<WALClient>, WriteQueryResult) {
    let TransactionSchema {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = transaction;

    let (snapshot, result) = execute_write_query_in(
        snapshot.into_inner(),
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &query_manager,
        query_options,
        &pipeline,
        &source_query,
        interrupt,
    );

    let transaction = TransactionSchema::from_parts(
        Arc::new(snapshot),
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );

    (transaction, result)
}

pub fn execute_write_query_in_write(
    transaction: TransactionWrite<WALClient>,
    query_options: QueryOptions,
    pipeline: typeql::query::Pipeline,
    source_query: String,
    interrupt: ExecutionInterrupt,
) -> (TransactionWrite<WALClient>, WriteQueryResult) {
    let TransactionWrite {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = transaction;

    let (snapshot, result) = execute_write_query_in(
        snapshot.into_inner(),
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &query_manager,
        query_options,
        &pipeline,
        &source_query,
        interrupt,
    );

    let transaction = TransactionWrite::from_parts(
        Arc::new(snapshot),
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );

    (transaction, result)
}

pub(crate) fn execute_write_query_in<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_manager: &QueryManager,
    query_options: QueryOptions,
    pipeline: &typeql::query::Pipeline,
    source_query: &str,
    interrupt: ExecutionInterrupt,
) -> (Snapshot, WriteQueryResult) {
    let start_time = Instant::now();
    let result = query_manager.prepare_write_pipeline(
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        pipeline,
        source_query,
    );
    let pipeline = match result {
        Ok(pipeline) => pipeline,
        Err((snapshot, err)) => return (snapshot, Err(err)),
    };

    if pipeline.has_fetch() {
        let (iterator, parameters, snapshot, query_profile) = match pipeline.into_documents_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, profile, parameters, .. })) => {
                (iterator, parameters, snapshot, profile)
            }
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(Box::new(QueryError::WritePipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    })),
                );
            }
        };

        let mut documents = Vec::new();
        for next in iterator {
            match next {
                Ok(document) => documents.push(document),
                Err(typedb_source) => {
                    return (
                        Arc::into_inner(snapshot).unwrap(),
                        Err(Box::new(QueryError::WritePipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source,
                        })),
                    )
                }
            }
        }
        if query_profile.is_enabled() {
            let micros = Instant::now().duration_since(start_time).as_micros();
            event!(
                Level::INFO,
                "Write query done (excluding network request time) in {} micros.\n{}",
                micros,
                query_profile
            );
        }
        (
            Arc::into_inner(snapshot).unwrap(),
            Ok(WriteQueryAnswer::new_documents(query_options, (parameters, documents))),
        )
    } else {
        let named_outputs = pipeline.rows_positions().unwrap();
        let query_structure = pipeline.query_structure().cloned();
        let query_output_descriptor: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();
        let (iterator, snapshot, query_profile) = match pipeline.into_rows_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, profile, .. })) => (iterator, snapshot, profile),
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(Box::new(QueryError::WritePipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    })),
                );
            }
        };

        let result = match iterator.collect_owned() {
            Ok(batch) => (
                Arc::into_inner(snapshot).unwrap(),
                Ok(WriteQueryAnswer::new_batch(query_options, (query_output_descriptor, batch, query_structure))),
            ),
            Err(err) => (
                Arc::into_inner(snapshot).unwrap(),
                Err(Box::new(QueryError::WritePipelineExecution {
                    source_query: source_query.to_string(),
                    typedb_source: err,
                })),
            ),
        };

        if query_profile.is_enabled() {
            let micros = Instant::now().duration_since(start_time).as_micros();
            event!(
                Level::INFO,
                "Write query done (excluding network request time) in {} micros.\n{}",
                micros,
                query_profile
            );
        }
        result
    }
}
