/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Vector attribute values live only in the usearch-backed VectorStore (RocksDB keeps just the
//! existence key). These tests cover the two recovery paths after a restart:
//! WAL-tail replay through the commit observer, and loading the checkpoint extension.

use std::sync::Arc;

use database::{
    Database,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, TransactionRead, TransactionSchema, TransactionWrite},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use encoding::{
    graph::{
        Typed,
        type_::vertex::{TypeID, TypeVertexEncoding},
    },
    value::label::Label,
};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use query::given_rows::GivenRowsSimple;
use resource::profile::CommitProfile;
use storage::durability_client::WALClient;
use test_utils::create_tmp_storage_dir;
use test_utils_storage::create_rocks_resources;

fn open_db(path: &std::path::Path) -> Arc<Database<WALClient>> {
    let diagnostics_manager = Arc::new(DiagnosticsManager::new_disabled());
    let resources = create_rocks_resources();
    Arc::new(Database::<WALClient>::open(path, &diagnostics_manager, &resources).expect("database open"))
}

fn define_schema(db: Arc<Database<WALClient>>) {
    let schema = r#"define
        attribute embedding, value vector(3, "float32");
        entity item owns embedding @card(0..);
    "#;
    let tx = TransactionSchema::open(db, TransactionOptions::default()).expect("schema txn");
    let query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let (tx, result) = execute_schema_query(tx, query, schema.to_string());
    result.expect("schema query");
    let (_profile, intent) = tx.finalise();
    intent.expect("schema commit intent").commit(&mut CommitProfile::DISABLED).expect("schema commit");
}

fn insert_vectors(db: Arc<Database<WALClient>>, query: &str) {
    let tx = TransactionWrite::open(db, TransactionOptions::default()).expect("write txn");
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        pipeline,
        None::<GivenRowsSimple>,
        query.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.expect("insert query");
    let (_profile, intent) = tx.finalise();
    intent.expect("commit intent").commit(&mut CommitProfile::DISABLED).expect("data commit");
}

fn lookup_embedding_type_id(db: &Arc<Database<WALClient>>) -> TypeID {
    let tx = TransactionRead::open(db.clone(), TransactionOptions::default()).expect("read txn");
    let attribute_type = tx
        .type_manager
        .get_attribute_type(tx.snapshot.as_ref(), &Label::build("embedding", None))
        .unwrap()
        .expect("embedding type");
    let type_id = attribute_type.vertex().type_id_();
    tx.close();
    type_id
}

#[test]
fn vectors_survive_restart_via_wal_replay_and_checkpoint() {
    let tmp = create_tmp_storage_dir();
    let db_path = tmp.join("vector-durability");

    let type_id;
    {
        let db = open_db(&db_path);
        define_schema(db.clone());
        insert_vectors(
            db.clone(),
            r#"insert
                $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");
                $b isa item, has embedding vector([0.0, 1.0, 0.0], "float32");
                $c isa item, has embedding vector([0.0, 0.0, 1.0], "float32");
            "#,
        );
        type_id = lookup_embedding_type_id(&db);
        assert_eq!(db.vector_store().indexed_vector_count(type_id), 3);
        drop(db);
    }

    // restart 1: no checkpoint contains the vectors yet -> they are recovered by replaying the
    // WAL through the commit observer during storage recovery. Database::load then writes a
    // fresh checkpoint (including the vector store extension) because the WAL is ahead of it.
    {
        let db = open_db(&db_path);
        assert_eq!(
            db.vector_store().indexed_vector_count(type_id),
            3,
            "vectors must be rebuilt from the WAL tail on restart"
        );
        drop(db);
    }

    // restart 2: the previous load wrote a checkpoint with the vector store extension -> this
    // load restores the store from the checkpoint file (plus an empty WAL tail).
    {
        let db = open_db(&db_path);
        let store = db.vector_store();
        assert_eq!(store.indexed_vector_count(type_id), 3, "vectors must be restored from the checkpoint extension");

        // the values themselves are intact and searchable
        let results = store.search(type_id, &[1.0, 0.0, 0.0], 3);
        let mut found: Vec<Vec<f32>> = results
            .iter()
            .filter_map(|(id, _)| store.get_by_key(type_id, id.as_vector_index_key()))
            .collect();
        found.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(found, vec![vec![0.0, 0.0, 1.0], vec![0.0, 1.0, 0.0], vec![1.0, 0.0, 0.0]]);
    }
}
