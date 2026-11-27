/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(dead_code)]

use std::{fmt, sync::Arc};

use concept::{
    thing::thing_manager::ThingManager,
    type_::{TypeAPI, type_manager::TypeManager},
};
use database::{
    Database,
    query::{execute_schema_query, execute_write_query_in_schema, execute_write_query_in_write},
    transaction::{CommitIntent, DataCommitError, TransactionRead, TransactionSchema, TransactionWrite},
};
use encoding::{
    graph::{
        Typed,
        type_::vertex::{TypeID, TypeVertexEncoding},
    },
    value::{label::Label, value::Value},
};
use executor::{ExecutionInterrupt, pipeline::stage::StageIterator};
use function::function_manager::FunctionManager;
use options::{QueryOptions, TransactionOptions};
use query::{given_rows::GivenRowsSimple, query_manager::QueryManager};
use resource::profile::StorageCounters;
use storage::{
    StorageCommitError,
    durability_client::WALClient,
    isolation_manager::IsolationConflict,
    snapshot::{ReadSnapshot, ReadableSnapshot, SchemaSnapshot, SnapshotError, WriteSnapshot},
};
use test_utils::{TempDir, create_tmp_storage_dir};
use test_utils_database::{create_db, open_db};

pub const SCHEMA: &str = r#"define
    attribute name, value string;
    attribute embedding, value vector(3, "float32") @index(hnsw:cosine);
    entity item owns name, owns embedding @card(0..);
"#;

pub const A: &str = "[1.0, 0.0, 0.0]";
pub const B: &str = "[0.0, 1.0, 0.0]";
pub const C: &str = "[0.0, 0.0, 1.0]";

pub const ALL: &str = "match $e isa embedding;";
pub const OWNERSHIPS: &str = "match $i isa item, has embedding $e;";

pub fn insert(vector: &str) -> String {
    format!(r#"insert $i isa item, has embedding vector({vector}, "float32");"#)
}

pub fn insert_named(name: &str, vector: &str) -> String {
    format!(r#"insert $i isa item, has name "{name}", has embedding vector({vector}, "float32");"#)
}

pub fn attach(name: &str, vector: &str) -> String {
    format!(r#"match $i isa item, has name "{name}"; insert $i has embedding vector({vector}, "float32");"#)
}

pub fn detach(name: &str) -> String {
    format!(r#"match $i isa item, has name "{name}", has embedding $e; delete has $e of $i;"#)
}

pub fn delete(vector: &str) -> String {
    format!(r#"match $e isa embedding; $e == vector({vector}, "float32"); delete $e;"#)
}

pub fn equal_to(vector: &str) -> String {
    format!(r#"match $e isa embedding; $e == vector({vector}, "float32");"#)
}

pub fn nearest_to(vector: &str) -> String {
    format!(r#"match let $e, $_ in cosine_similarity_search(embedding, vector({vector}, "float32"), -1.0);"#)
}

pub fn create() -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_storage_dir();
    let (_, database) = create_db(&tmp_dir, "vector-isolation");
    (tmp_dir, database)
}

pub fn setup() -> (TempDir, Arc<Database<WALClient>>) {
    let (tmp_dir, database) = create();
    commit_schema(database.clone(), SCHEMA);
    (tmp_dir, database)
}

pub fn reopen(tmp_dir: &TempDir, database: Arc<Database<WALClient>>) -> Arc<Database<WALClient>> {
    drop(database);
    let (_, database) = open_db(tmp_dir, "vector-isolation");
    database
}

pub fn open_read(database: &Arc<Database<WALClient>>) -> TransactionRead<WALClient> {
    TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap()
}

pub fn open_write(database: &Arc<Database<WALClient>>) -> TransactionWrite<WALClient> {
    TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap()
}

pub fn open_schema(database: &Arc<Database<WALClient>>) -> TransactionSchema<WALClient> {
    TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap()
}

pub fn run_write(tx: TransactionWrite<WALClient>, query: &str) -> TransactionWrite<WALClient> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        Arc::new(parsed),
        None::<GivenRowsSimple>,
        query.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap_or_else(|err| panic!("write query failed: {query:?}: {err:?}"));
    tx
}

pub fn run_write_in_schema(tx: TransactionSchema<WALClient>, query: &str) -> TransactionSchema<WALClient> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let (tx, result) = execute_write_query_in_schema(
        tx,
        QueryOptions::default_grpc(),
        Arc::new(parsed),
        None::<GivenRowsSimple>,
        query.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap_or_else(|err| panic!("write query failed: {query:?}: {err:?}"));
    tx
}

pub fn run_schema(tx: TransactionSchema<WALClient>, query: &str) -> TransactionSchema<WALClient> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_schema();
    let (tx, result) = execute_schema_query(tx, parsed, query.to_string());
    result.unwrap_or_else(|err| panic!("schema query failed: {query:?}: {err:?}"));
    tx
}

pub fn try_commit(tx: TransactionWrite<WALClient>) -> Result<(), DataCommitError> {
    let (mut profile, intent) = tx.finalise();
    intent.and_then(|intent| intent.commit(profile.commit_profile()))
}

pub fn commit_schema_transaction(tx: TransactionSchema<WALClient>) {
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

pub fn commit_write(database: Arc<Database<WALClient>>, query: &str) {
    try_commit(run_write(open_write(&database), query)).unwrap();
}

pub fn commit_schema(database: Arc<Database<WALClient>>, query: &str) {
    commit_schema_transaction(run_schema(open_schema(&database), query));
}

pub fn isolation_conflict(result: Result<(), DataCommitError>) -> IsolationConflict {
    match result {
        Err(DataCommitError::SnapshotError {
            typedb_source: SnapshotError::Commit { typedb_source: StorageCommitError::Isolation { conflict, .. } },
        }) => conflict,
        other => panic!("expected an isolation conflict, got: {other:?}"),
    }
}

pub fn checkpoint(database: &Arc<Database<WALClient>>) {
    database.checkpoint().unwrap();
}

pub fn embedding_type_id(database: &Arc<Database<WALClient>>) -> TypeID {
    let reader = open_read(database);
    let attribute_type =
        reader.type_manager.get_attribute_type(reader.snapshot.as_ref(), &Label::build("embedding", None)).unwrap();
    attribute_type.expect("the embedding type exists").vertex().type_id_()
}

pub fn set_aside_vector_count(database: &Arc<Database<WALClient>>) -> usize {
    database.vector_index().latest_version().get_deleted_attributes(embedding_type_id(database)).len()
}

pub trait Transaction {
    type Snapshot: ReadableSnapshot + fmt::Debug + 'static;

    fn snapshot(&self) -> &Arc<Self::Snapshot>;
    fn query_manager(&self) -> &QueryManager;
    fn type_manager(&self) -> &TypeManager;
    fn thing_manager(&self) -> &Arc<ThingManager>;
    fn function_manager(&self) -> &Arc<FunctionManager>;
}

macro_rules! impl_transaction {
    ($transaction:ident, $snapshot:ident) => {
        impl Transaction for $transaction<WALClient> {
            type Snapshot = $snapshot<WALClient>;

            fn snapshot(&self) -> &Arc<Self::Snapshot> {
                &self.snapshot
            }

            fn query_manager(&self) -> &QueryManager {
                &self.query_manager
            }

            fn type_manager(&self) -> &TypeManager {
                &self.type_manager
            }

            fn thing_manager(&self) -> &Arc<ThingManager> {
                &self.thing_manager
            }

            fn function_manager(&self) -> &Arc<FunctionManager> {
                &self.function_manager
            }
        }
    };
}

impl_transaction!(TransactionRead, ReadSnapshot);
impl_transaction!(TransactionWrite, WriteSnapshot);
impl_transaction!(TransactionSchema, SchemaSnapshot);

pub fn count(tx: &impl Transaction, query: &str) -> usize {
    try_count(tx, query).unwrap_or_else(|| panic!("read query failed to compile: {query:?}"))
}

pub fn try_count(tx: &impl Transaction, query: &str) -> Option<usize> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = tx
        .query_manager()
        .prepare_read_pipeline(
            tx.snapshot().clone(),
            tx.type_manager(),
            tx.thing_manager().clone(),
            tx.function_manager().clone(),
            &parsed,
            None::<GivenRowsSimple>,
            query,
            None,
        )
        .ok()?;
    let (rows, _context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    Some(rows.collect_owned().unwrap().len())
}

pub fn values(tx: &impl Transaction) -> Option<Vec<Vec<f32>>> {
    let snapshot = tx.snapshot().as_ref();
    let attribute_type = tx.type_manager().get_attribute_type(snapshot, &Label::build("embedding", None)).unwrap()?;
    let mut attributes =
        tx.thing_manager().get_attributes_in(snapshot, attribute_type, StorageCounters::DISABLED).unwrap();
    let mut values = Vec::new();
    while let Some(attribute) = attributes.next() {
        let attribute = attribute.unwrap();
        let value = attribute.get_value(snapshot, tx.thing_manager(), StorageCounters::DISABLED).unwrap();
        let Value::Vector(vector) = value else { panic!("expected a vector value") };
        values.push(vector.elements().to_vec());
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Some(values)
}
