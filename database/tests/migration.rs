/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
    migration::{
        Checksums,
        database_exporter::DatabaseExporter,
        database_import_handler::{
            DatabaseImportHandler, ImportHandlerError, open_import_schema_transaction, open_import_write_transaction,
        },
        database_importer::{DatabaseImportError, DatabaseImporter, ImporterHandle},
        item::MigrationItem,
    },
    transaction::{
        CommitIntent, DataCommitIntent, SchemaCommitIntent, TransactionError, TransactionRead, TransactionSchema,
        TransactionWrite,
    },
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use encoding::value::{label::Label, value::Value};
use options::{MvccCleanupStrategy, TransactionOptions, byte_size::ByteSize};
use resource::profile::CommitProfile;
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_dir, init_logging};

const SCHEMA: &str = r#"define
  attribute name, value string;
  entity person, owns name, plays friendship:friend;
  relation friendship, relates friend @card(0..2);
"#;

#[derive(Debug)]
struct TestImportHandler {
    database_manager: Arc<DatabaseManager>,
    staged_database: Arc<Database<WALClient>>,
}

impl DatabaseImportHandler for TestImportHandler {
    fn database_name(&self) -> &str {
        self.staged_database.name()
    }

    fn open_schema(&self) -> Result<TransactionSchema<WALClient>, TransactionError> {
        open_import_schema_transaction(&self.staged_database)
    }

    fn open_write(&self) -> Result<TransactionWrite<WALClient>, TransactionError> {
        open_import_write_transaction(&self.staged_database)
    }

    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), ImportHandlerError> {
        intent.commit(&mut CommitProfile::disabled()).map_err(|error| Arc::new(error) as _)
    }

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), ImportHandlerError> {
        intent.commit(&mut CommitProfile::disabled()).map_err(|error| Arc::new(error) as _)
    }

    fn finalise(self: Box<Self>) -> Result<(), ImportHandlerError> {
        let Self { database_manager, staged_database } = *self;
        let name = staged_database.name().to_owned();
        drop(staged_database);
        database_manager.finalise_imported_database(&name).map_err(|error| Arc::new(error) as _)
    }
}

fn manager(data_dir: &TempDir) -> Arc<DatabaseManager> {
    let diagnostics = Arc::new(DiagnosticsManager::new_disabled());
    DatabaseManager::new(
        data_dir.as_ref(),
        diagnostics,
        ByteSize::mb(64),
        ByteSize::mb(64),
        ImportOwnership::Exclusive,
        MvccCleanupStrategy::Disabled,
    )
    .expect("DatabaseManager::new")
}

fn importer(database_manager: &Arc<DatabaseManager>, name: &str) -> ImporterHandle {
    let staged_database = database_manager.prepare_imported_database(name.to_owned()).expect("prepare");
    let handler = TestImportHandler { database_manager: database_manager.clone(), staged_database };
    DatabaseImporter::new(Box::new(handler), database_manager.import_directory().to_owned()).start(10)
}

fn read_transaction(database_manager: &Arc<DatabaseManager>, name: &str) -> TransactionRead<WALClient> {
    let database = database_manager.database(name).expect("imported database is served");
    TransactionRead::open(database, TransactionOptions::default()).expect("open read")
}

fn export(transaction: &TransactionRead<WALClient>, name: &str) -> Vec<MigrationItem> {
    let mut exporter = DatabaseExporter::new(transaction, "3.0.0-test", name).expect("exporter");
    let mut items = Vec::new();
    while let Some(item) = exporter.next_item().expect("next item") {
        items.push(item);
    }
    items
}

fn source_items() -> Vec<MigrationItem> {
    let alice = "alice".to_owned();
    let bob = "bob".to_owned();
    vec![
        MigrationItem::Schema(SCHEMA.to_owned()),
        MigrationItem::Attribute {
            id: alice.clone(),
            label: Label::build("name", None),
            value: Value::String("Alice".into()),
        },
        MigrationItem::Attribute {
            id: bob.clone(),
            label: Label::build("name", None),
            value: Value::String("Bob".into()),
        },
        MigrationItem::Entity {
            id: "p1".to_owned(),
            label: Label::build("person", None),
            owned_attributes: vec![alice],
        },
        MigrationItem::Entity { id: "p2".to_owned(), label: Label::build("person", None), owned_attributes: vec![bob] },
        MigrationItem::Relation {
            id: "f1".to_owned(),
            label: Label::build("friendship", None),
            owned_attributes: vec![],
            related_role_players: vec![(
                Label::build_scoped("friend", "friendship", None),
                vec!["p1".to_owned(), "p2".to_owned()],
            )],
        },
        MigrationItem::Checksums(Checksums {
            entity_count: 2,
            attribute_count: 2,
            relation_count: 1,
            role_count: 2,
            ownership_count: 2,
        }),
    ]
}

fn empty_checksums() -> MigrationItem {
    MigrationItem::Checksums(Checksums::new())
}

async fn import_all(database_manager: &Arc<DatabaseManager>, name: &str, items: Vec<MigrationItem>) {
    let mut importer = importer(database_manager, name);
    importer.send_batch(items.into_iter().map(Ok::<_, DatabaseImportError>)).await.expect("send");
    importer.finalize().await.expect("import");
}

#[tokio::test]
async fn a_database_without_a_schema_or_data_round_trips() {
    init_logging();
    let data_dir = create_tmp_dir("migration_empty");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "blank", vec![MigrationItem::Schema(String::new()), empty_checksums()]).await;

    let items = export(&read_transaction(&database_manager, "blank"), "blank");
    assert!(
        matches!(
            items.as_slice(),
            [MigrationItem::Schema(_), MigrationItem::Header { .. }, MigrationItem::Checksums(_)]
        ),
        "{items:?}"
    );
    import_all(&database_manager, "blank-copy", items).await;
    assert!(database_manager.database("blank-copy").is_some());
}

#[tokio::test]
async fn an_exported_stream_opens_with_the_schema_and_closes_with_the_checksums() {
    init_logging();
    let data_dir = create_tmp_dir("migration_stream_shape");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "source", source_items()).await;

    let transaction = read_transaction(&database_manager, "source");
    let items = export(&transaction, "source");

    match &items[0] {
        MigrationItem::Schema(schema) => assert!(schema.contains("entity person"), "unexpected schema: {schema}"),
        other => panic!("the stream must open with the schema, not {other:?}"),
    }
    match &items[1] {
        MigrationItem::Header { typedb_version, original_database } => {
            assert_eq!(typedb_version, "3.0.0-test");
            assert_eq!(original_database, "source");
        }
        other => panic!("the schema must be followed by the header, not {other:?}"),
    }
    match items.last().expect("a non-empty stream") {
        MigrationItem::Checksums(checksums) => {
            assert_eq!(checksums.entity_count, 2);
            assert_eq!(checksums.attribute_count, 2);
            assert_eq!(checksums.relation_count, 1);
            assert_eq!(checksums.role_count, 2);
            assert_eq!(checksums.ownership_count, 2);
        }
        other => panic!("the stream must close with the checksums, not {other:?}"),
    }
}

#[tokio::test]
async fn an_exported_stream_is_importable_as_it_comes() {
    init_logging();
    let data_dir = create_tmp_dir("migration_round_trip");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "source", source_items()).await;

    let source_items = {
        let transaction = read_transaction(&database_manager, "source");
        export(&transaction, "source")
    };
    import_all(&database_manager, "target", source_items).await;

    let transaction = read_transaction(&database_manager, "target");
    let copied_items = export(&transaction, "target");
    let (MigrationItem::Checksums(copied), MigrationItem::Checksums(original)) =
        (copied_items.last().unwrap(), export(&read_transaction(&database_manager, "source"), "source").pop().unwrap())
    else {
        panic!("both streams must close with the checksums")
    };
    assert_eq!(copied.entity_count, original.entity_count);
    assert_eq!(copied.attribute_count, original.attribute_count);
    assert_eq!(copied.relation_count, original.relation_count);
    assert_eq!(copied.role_count, original.role_count);
    assert_eq!(copied.ownership_count, original.ownership_count);
}

#[tokio::test]
async fn an_out_of_order_stream_is_rejected() {
    init_logging();
    let data_dir = create_tmp_dir("migration_stream_order");
    let database_manager = manager(&data_dir);

    let mut early = importer(&database_manager, "early");
    early
        .send(MigrationItem::Header { typedb_version: "3.0.0-test".to_owned(), original_database: "source".to_owned() })
        .await
        .expect("send");
    let result = early.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::ItemBeforeSchema { .. })), "{result:?}");

    let mut twice = importer(&database_manager, "twice");
    twice.send(MigrationItem::Schema(SCHEMA.to_owned())).await.expect("first schema");
    twice.send(MigrationItem::Schema(SCHEMA.to_owned())).await.expect("second schema");
    let result = twice.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::SchemaAlreadyImported { .. })), "{result:?}");

    let mut late = importer(&database_manager, "late");
    for item in source_items() {
        late.send(item).await.expect("send");
    }
    late.send(MigrationItem::Entity {
        id: "p3".to_owned(),
        label: Label::build("person", None),
        owned_attributes: vec![],
    })
    .await
    .expect("send");
    let result = late.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::ItemAfterChecksums { .. })), "{result:?}");

    let mut duplicate = importer(&database_manager, "duplicate");
    for item in [MigrationItem::Schema(SCHEMA.to_owned()), empty_checksums(), empty_checksums()] {
        duplicate.send(item).await.expect("send");
    }
    let result = duplicate.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::DuplicateClientChecksums { .. })), "{result:?}");
}

#[tokio::test]
async fn an_item_that_fails_to_decode_fails_the_import() {
    init_logging();
    let data_dir = create_tmp_dir("migration_decode_failure");
    let database_manager = manager(&data_dir);

    let mut importer = importer(&database_manager, "undecodable");
    importer.send(MigrationItem::Schema(SCHEMA.to_owned())).await.expect("send");
    // Any TypeDB error stands in for the server's item decoding error.
    let batch = [Ok(empty_checksums()), Err(DatabaseImportError::ImporterStopped {})];
    importer.send_batch(batch).await.expect("send");
    let result = importer.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::ItemDecode { .. })), "{result:?}");
}

#[tokio::test]
async fn an_aborted_import_stops_the_importer() {
    init_logging();
    let data_dir = create_tmp_dir("migration_abort");
    let database_manager = manager(&data_dir);

    let mut aborted = importer(&database_manager, "aborted");
    aborted.send(MigrationItem::Schema(SCHEMA.to_owned())).await.expect("send");
    let result = aborted.abort().await;
    assert!(matches!(result, Err(DatabaseImportError::Interrupted { .. })), "{result:?}");
}
