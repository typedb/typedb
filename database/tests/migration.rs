/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concurrency::{TokioTaskSpawner, TokioTaskTracker};
use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
    migration::{
        Checksums, Counts, MigrationOperationError,
        database_exporter::{DatabaseExporter, ExportMessage},
        database_importer::{DatabaseImportError, DatabaseImporter, DatabaseImporterHandle},
        importing_database_handle::{
            ImportingDatabaseHandle, open_import_schema_transaction, open_import_write_transaction,
        },
        item::{AttributeItem, EntityItem, MigrationItem, RelationItem},
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
use tokio::sync::watch;

const SCHEMA: &str = r#"define
  attribute name, value string;
  entity person, owns name, plays friendship:friend;
  relation friendship, relates friend @card(0..2);
"#;

#[derive(Debug)]
struct TestImportingDatabaseHandle {
    database_manager: Arc<DatabaseManager>,
    staged_database: Arc<Database<WALClient>>,
}

impl ImportingDatabaseHandle for TestImportingDatabaseHandle {
    fn database_name(&self) -> &str {
        self.staged_database.name()
    }

    fn open_schema(&self) -> Result<TransactionSchema<WALClient>, TransactionError> {
        open_import_schema_transaction(&self.staged_database)
    }

    fn open_write(&self) -> Result<TransactionWrite<WALClient>, TransactionError> {
        open_import_write_transaction(&self.staged_database)
    }

    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), MigrationOperationError> {
        intent.commit(&mut CommitProfile::disabled()).map_err(|error| Arc::new(error) as _)
    }

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), MigrationOperationError> {
        intent.commit(&mut CommitProfile::disabled()).map_err(|error| Arc::new(error) as _)
    }

    fn try_unwrap(self: Arc<Self>) -> Result<Box<dyn ImportingDatabaseHandle>, Arc<dyn ImportingDatabaseHandle>> {
        Arc::try_unwrap(self).map(|this| Box::new(this) as _).map_err(|this| this as _)
    }

    fn finalise(self: Box<Self>) -> Result<(), MigrationOperationError> {
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

const BUFFER_SIZE: usize = 10;

fn importing_database(database_manager: &Arc<DatabaseManager>, name: &str) -> Arc<dyn ImportingDatabaseHandle> {
    let staged_database = database_manager.prepare_imported_database(name.to_owned()).expect("prepare");
    Arc::new(TestImportingDatabaseHandle { database_manager: database_manager.clone(), staged_database })
}

// Tests that don't exercise shutdown use an untracked spawner: without a shutdown sender the tracker is closed, but
// it still runs the tasks spawned through it.
fn task_spawner() -> TokioTaskSpawner {
    let (_, shutdown_receiver) = watch::channel(());
    TokioTaskTracker::new(shutdown_receiver).get_spawner()
}

fn importer(database_manager: &Arc<DatabaseManager>, name: &str) -> DatabaseImporterHandle {
    importer_with_spawner(database_manager, name, &task_spawner())
}

fn importer_with_spawner(
    database_manager: &Arc<DatabaseManager>,
    name: &str,
    task_spawner: &TokioTaskSpawner,
) -> DatabaseImporterHandle {
    let scratch_directory = database_manager.import_directory().to_owned();
    DatabaseImporter::start(importing_database(database_manager, name), scratch_directory, BUFFER_SIZE, task_spawner)
}

async fn importer_with_schema(
    database_manager: &Arc<DatabaseManager>,
    name: &str,
    schema: &str,
) -> DatabaseImporterHandle {
    let mut importer = importer(database_manager, name);
    importer.send_schema(schema.to_owned()).await.expect("schema");
    importer
}

fn read_transaction(database_manager: &Arc<DatabaseManager>, name: &str) -> TransactionRead<WALClient> {
    let database = database_manager.database(name).expect("imported database is served");
    TransactionRead::open(database, TransactionOptions::default()).expect("open read")
}

fn export_messages(transaction: &TransactionRead<WALClient>, name: &str) -> Vec<ExportMessage> {
    let mut exporter = DatabaseExporter::new(transaction, "3.0.0-test", name).expect("exporter");
    let mut messages = Vec::new();
    while let Some(message) = exporter.next_item().expect("next item") {
        messages.push(message);
    }
    messages
}

/// Splits an exported stream into its schema, which must come first, and its items.
fn export(transaction: &TransactionRead<WALClient>, name: &str) -> (String, Vec<MigrationItem>) {
    let mut messages = export_messages(transaction, name).into_iter();
    let Some(ExportMessage::Schema(schema)) = messages.next() else { panic!("the stream must open with the schema") };
    let items = messages
        .map(|message| match message {
            ExportMessage::Item(item) => item,
            ExportMessage::Schema(_) => panic!("the stream must carry exactly one schema"),
        })
        .collect();
    (schema, items)
}

fn source_items() -> Vec<MigrationItem> {
    let alice = "alice".to_owned();
    let bob = "bob".to_owned();
    vec![
        MigrationItem::Attribute(AttributeItem {
            id: alice.clone(),
            label: Label::build("name", None),
            value: Value::String("Alice".into()),
        }),
        MigrationItem::Attribute(AttributeItem {
            id: bob.clone(),
            label: Label::build("name", None),
            value: Value::String("Bob".into()),
        }),
        MigrationItem::Entity(EntityItem {
            id: "p1".to_owned(),
            label: Label::build("person", None),
            owned_attributes: vec![alice],
        }),
        MigrationItem::Entity(EntityItem {
            id: "p2".to_owned(),
            label: Label::build("person", None),
            owned_attributes: vec![bob],
        }),
        MigrationItem::Relation(RelationItem {
            id: "f1".to_owned(),
            label: Label::build("friendship", None),
            owned_attributes: vec![],
            related_role_players: vec![(
                Label::build_scoped("friend", "friendship", None),
                vec!["p1".to_owned(), "p2".to_owned()],
            )],
        }),
        MigrationItem::Checksums(Checksums::from_counts(Counts {
            entity_count: 2,
            attribute_count: 2,
            relation_count: 1,
            role_count: 2,
            ownership_count: 2,
        })),
    ]
}

fn empty_checksums() -> MigrationItem {
    MigrationItem::Checksums(Checksums::new())
}

async fn import_all(mut importer: DatabaseImporterHandle, items: Vec<MigrationItem>) {
    importer.send_batch(items.into_iter().map(Ok::<_, DatabaseImportError>)).await.expect("send");
    importer.finalize().await.expect("import");
}

#[tokio::test]
async fn a_database_without_a_schema_or_data_round_trips() {
    init_logging();
    let data_dir = create_tmp_dir("migration_empty");
    let database_manager = manager(&data_dir);
    import_all(importer_with_schema(&database_manager, "blank", "").await, vec![empty_checksums()]).await;

    let (schema, items) = export(&read_transaction(&database_manager, "blank"), "blank");
    assert!(matches!(items.as_slice(), [MigrationItem::Header { .. }, MigrationItem::Checksums(_)]), "{items:?}");
    import_all(importer_with_schema(&database_manager, "blank-copy", &schema).await, items).await;
    assert!(database_manager.database("blank-copy").is_some());
}

#[tokio::test]
async fn an_exported_stream_opens_with_the_schema_and_closes_with_the_checksums() {
    init_logging();
    let data_dir = create_tmp_dir("migration_stream_shape");
    let database_manager = manager(&data_dir);
    import_all(importer_with_schema(&database_manager, "source", SCHEMA).await, source_items()).await;

    let transaction = read_transaction(&database_manager, "source");
    let (schema, items) = export(&transaction, "source");

    assert!(schema.contains("entity person"), "unexpected schema: {schema}");
    match &items[0] {
        MigrationItem::Header { typedb_version, original_database } => {
            assert_eq!(typedb_version, "3.0.0-test");
            assert_eq!(original_database, "source");
        }
        other => panic!("the schema must be followed by the header, not {other:?}"),
    }
    match items.last().expect("a non-empty stream") {
        MigrationItem::Checksums(checksums) => {
            assert_eq!(
                checksums.counts(),
                Counts { entity_count: 2, attribute_count: 2, relation_count: 1, role_count: 2, ownership_count: 2 }
            );
        }
        other => panic!("the stream must close with the checksums, not {other:?}"),
    }
}

#[tokio::test]
async fn an_exported_stream_is_importable_as_it_comes() {
    init_logging();
    let data_dir = create_tmp_dir("migration_round_trip");
    let database_manager = manager(&data_dir);
    import_all(importer_with_schema(&database_manager, "source", SCHEMA).await, source_items()).await;

    let (source_schema, source_items) = {
        let transaction = read_transaction(&database_manager, "source");
        export(&transaction, "source")
    };
    import_all(importer_with_schema(&database_manager, "target", &source_schema).await, source_items).await;

    let transaction = read_transaction(&database_manager, "target");
    let (_, copied_items) = export(&transaction, "target");
    let (MigrationItem::Checksums(copied), MigrationItem::Checksums(original)) = (
        copied_items.last().unwrap(),
        export(&read_transaction(&database_manager, "source"), "source").1.pop().unwrap(),
    ) else {
        panic!("both streams must close with the checksums")
    };
    assert_eq!(copied.counts(), original.counts());
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
    twice.send_schema(SCHEMA.to_owned()).await.expect("first schema");
    twice.send_schema(SCHEMA.to_owned()).await.expect("second schema");
    let result = twice.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::SchemaAlreadyImported { .. })), "{result:?}");

    let mut late = importer_with_schema(&database_manager, "late", SCHEMA).await;
    for item in source_items() {
        late.send(item).await.expect("send");
    }
    late.send(MigrationItem::Entity(EntityItem {
        id: "p3".to_owned(),
        label: Label::build("person", None),
        owned_attributes: vec![],
    }))
    .await
    .expect("send");
    let result = late.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::ItemAfterChecksums { .. })), "{result:?}");

    let mut duplicate = importer_with_schema(&database_manager, "duplicate", SCHEMA).await;
    for item in [empty_checksums(), empty_checksums()] {
        duplicate.send(item).await.expect("send");
    }
    let result = duplicate.finalize().await;
    assert!(matches!(result, Err(DatabaseImportError::DuplicateClientChecksums { .. })), "{result:?}");
}

#[tokio::test]
async fn a_relation_whose_players_arrive_later_survives_batch_commits() {
    init_logging();
    let data_dir = create_tmp_dir("migration_pending_players");
    let database_manager = manager(&data_dir);
    let person = |id: String| {
        MigrationItem::Entity(EntityItem { id, label: Label::build("person", None), owned_attributes: vec![] })
    };
    let max_writes_per_commit = 2;
    let padding = max_writes_per_commit * 2;

    let mut items = vec![MigrationItem::Relation(RelationItem {
        id: "f1".to_owned(),
        label: Label::build("friendship", None),
        owned_attributes: vec![],
        related_role_players: vec![(Label::build_scoped("friend", "friendship", None), vec!["p1".to_owned()])],
    })];
    items.extend((0..padding).map(|i| person(format!("pad{i}"))));
    items.push(person("p1".to_owned()));
    items.push(MigrationItem::Checksums(Checksums::from_counts(Counts {
        entity_count: (padding + 1) as i64,
        relation_count: 1,
        role_count: 1,
        ..Counts::default()
    })));
    let mut importer = DatabaseImporter::start_with_max_writes_per_commit(
        importing_database(&database_manager, "pending"),
        database_manager.import_directory().to_owned(),
        BUFFER_SIZE,
        &task_spawner(),
        max_writes_per_commit,
    );
    importer.send_schema(SCHEMA.to_owned()).await.expect("schema");
    import_all(importer, items).await;

    let transaction = read_transaction(&database_manager, "pending");
    let relations: Vec<MigrationItem> = export(&transaction, "pending")
        .1
        .into_iter()
        .filter(|item| matches!(item, MigrationItem::Relation(_)))
        .collect();
    let [MigrationItem::Relation(RelationItem { related_role_players, .. })] = relations.as_slice() else {
        panic!("one relation expected, got {relations:?}")
    };
    assert_eq!(related_role_players.iter().map(|(_, players)| players.len()).sum::<usize>(), 1);
}

#[tokio::test]
async fn a_relation_without_role_players_is_rejected() {
    init_logging();
    let data_dir = create_tmp_dir("migration_playerless_relation");
    let database_manager = manager(&data_dir);

    let no_roles = vec![];
    let empty_role = vec![(Label::build_scoped("friend", "friendship", None), vec![])];
    for (name, related_role_players) in [("no-roles", no_roles), ("empty-role", empty_role)] {
        let mut importer = importer_with_schema(&database_manager, name, SCHEMA).await;
        importer
            .send(MigrationItem::Relation(RelationItem {
                id: "f1".to_owned(),
                label: Label::build("friendship", None),
                owned_attributes: vec![],
                related_role_players,
            }))
            .await
            .expect("send");
        let result = importer.finalize().await;
        assert!(matches!(result, Err(DatabaseImportError::RelationWithoutRolePlayers { .. })), "{result:?}");
    }
}

#[tokio::test]
async fn an_item_that_fails_to_decode_fails_the_import() {
    init_logging();
    let data_dir = create_tmp_dir("migration_decode_failure");
    let database_manager = manager(&data_dir);

    let mut importer = importer_with_schema(&database_manager, "undecodable", SCHEMA).await;
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

    let aborted = importer_with_schema(&database_manager, "aborted", SCHEMA).await;
    let result = aborted.abort().await;
    assert!(matches!(result, Err(DatabaseImportError::Interrupted { .. })), "{result:?}");
}

#[tokio::test]
async fn shutdown_waits_for_an_importer_whose_handle_was_dropped() {
    init_logging();
    let data_dir = create_tmp_dir("migration_dropped_handle");
    let database_manager = manager(&data_dir);
    let (shutdown_sender, shutdown_receiver) = watch::channel(());
    let task_tracker = TokioTaskTracker::new(shutdown_receiver);

    let mut dropped = importer_with_spawner(&database_manager, "dropped", &task_tracker.get_spawner());
    dropped.send_schema(SCHEMA.to_owned()).await.expect("schema");
    drop(dropped);
    shutdown_sender.send(()).expect("shutdown");
    task_tracker.join().await;

    // The importer has stopped and released the staged database, which can now be discarded.
    database_manager.discard_imported_database("dropped").expect("discard");
}
