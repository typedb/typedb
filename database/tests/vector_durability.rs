/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
use options::{MvccCleanupStrategy, QueryOptions, TransactionOptions};
use query::given_rows::GivenRowsSimple;
use resource::profile::CommitProfile;
use storage::durability_client::WALClient;
use test_utils::create_tmp_storage_dir;
use test_utils_storage::create_rocks_resources;

fn open_db(path: &std::path::Path) -> Arc<Database<WALClient>> {
    let diagnostics_manager = Arc::new(DiagnosticsManager::new_disabled());
    let resources = create_rocks_resources();
    Arc::new(
        Database::<WALClient>::open(path, &diagnostics_manager, &resources, MvccCleanupStrategy::Disabled)
            .expect("database open"),
    )
}

fn define_schema(db: Arc<Database<WALClient>>) {
    run_schema_query(
        db,
        r#"define
            attribute embedding, value vector(3, "float32") @index(hnsw:cosine);
            entity item owns embedding @card(0..);
        "#,
    );
}

fn run_schema_query(db: Arc<Database<WALClient>>, schema: &str) {
    let tx = TransactionSchema::open(db, TransactionOptions::default()).expect("schema txn");
    let query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let (tx, result) = execute_schema_query(tx, query, schema.to_string());
    result.expect("schema query");
    let (_profile, intent) = tx.finalise();
    intent.expect("schema commit intent").commit(&mut CommitProfile::disabled()).expect("schema commit");
}

fn insert_vectors(db: Arc<Database<WALClient>>, query: &str) {
    let tx = TransactionWrite::open(db, TransactionOptions::default()).expect("write txn");
    let pipeline = std::sync::Arc::new(typeql::parse_query(query).unwrap().into_structure().into_pipeline());
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
    intent.expect("commit intent").commit(&mut CommitProfile::disabled()).expect("data commit");
}

fn lookup_embedding_type_id(db: &Arc<Database<WALClient>>) -> TypeID {
    lookup_attribute_type_id(db, "embedding")
}

fn lookup_attribute_type_id(db: &Arc<Database<WALClient>>, label: &str) -> TypeID {
    let tx = TransactionRead::open(db.clone(), TransactionOptions::default()).expect("read txn");
    let attribute_type = tx
        .type_manager
        .get_attribute_type(tx.snapshot.as_ref(), &Label::build(label, None))
        .unwrap()
        .expect("attribute type");
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
        assert_eq!(db.vector_index().indexed_vector_count(type_id), 3);
        drop(db);
    }

    {
        let db = open_db(&db_path);
        assert_eq!(
            db.vector_index().indexed_vector_count(type_id),
            3,
            "vectors must be rebuilt from the WAL tail on restart"
        );
        drop(db);
    }

    {
        let db = open_db(&db_path);
        let store = db.vector_index();
        assert_eq!(store.indexed_vector_count(type_id), 3, "vectors must be restored from the checkpoint extension");

        let results = store.search(type_id, &[1.0, 0.0, 0.0], 3);
        let mut found: Vec<Vec<f32>> = results.iter().filter_map(|(vertex, _)| store.get_vector(*vertex)).collect();
        found.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(found, vec![vec![0.0, 0.0, 1.0], vec![0.0, 1.0, 0.0], vec![1.0, 0.0, 0.0]]);
    }
}

#[test]
fn checkpoint_missing_the_vector_index_extension_fails_to_open_when_vectors_exist() {
    let tmp = create_tmp_storage_dir();
    let db_path = tmp.join("vector-missing-extension");
    {
        let db = open_db(&db_path);
        define_schema(db.clone());
        insert_vectors(db.clone(), r#"insert $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");"#);
        drop(db);
    }
    {
        let db = open_db(&db_path);
        drop(db);
    }

    let mut removed = 0;
    for entry in std::fs::read_dir(db_path.join("checkpoint")).expect("checkpoint dir") {
        let extension_dir = entry.expect("checkpoint entry").path().join("VECTOR_INDEX");
        if extension_dir.exists() {
            std::fs::remove_dir_all(&extension_dir).expect("remove extension");
            removed += 1;
        }
    }
    assert!(removed > 0, "expected at least one checkpoint with the vector index extension");

    let diagnostics_manager = Arc::new(DiagnosticsManager::new_disabled());
    let resources = create_rocks_resources();
    let result = Database::<WALClient>::open(&db_path, &diagnostics_manager, &resources, MvccCleanupStrategy::Disabled);
    let error = format!("{:?}", result.err().expect("open should fail without the vector index extension"));
    assert!(error.contains("vector values that are not in the vector index"), "unexpected error: {error}");
}

#[test]
fn undefining_a_vector_type_drops_its_index_so_the_type_id_is_reusable() {
    let tmp = create_tmp_storage_dir();
    let db = open_db(&tmp.join("vector-undefine"));
    define_schema(db.clone());
    insert_vectors(db.clone(), r#"insert $a isa item, has embedding vector([1.0, 0.0, 0.0], "float32");"#);
    let old_type_id = lookup_embedding_type_id(&db);
    assert_eq!(db.vector_index().indexed_vector_count(old_type_id), 1);

    insert_vectors(db.clone(), "match $x isa item; delete $x;");
    insert_vectors(db.clone(), "match $e isa embedding; delete $e;");
    run_schema_query(db.clone(), "undefine embedding;");

    assert_eq!(db.vector_index().indexed_vector_count(old_type_id), 0);

    run_schema_query(
        db.clone(),
        r#"define attribute embedding2, value vector(4, "float32") @index(hnsw:cosine); entity item owns embedding2 @card(0..);"#,
    );
    insert_vectors(db.clone(), r#"insert $a isa item, has embedding2 vector([0.0, 1.0, 0.0, 0.0], "float32");"#);
    assert_eq!(db.vector_index().indexed_vector_count(lookup_attribute_type_id(&db, "embedding2")), 1);
}
