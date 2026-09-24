/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
    migration::{
        Checksums,
        database_exporter::DatabaseExporter,
        database_import_handler::{
            DatabaseImportHandler, ImportHandlerError, open_import_schema_transaction, open_import_write_transaction,
        },
        database_importer::{DatabaseImportError, DatabaseImporter},
        item::MigrationMessage,
    },
    transaction::{
        CommitIntent, DataCommitIntent, SchemaCommitIntent, TransactionError, TransactionRead, TransactionSchema,
        TransactionWrite,
    },
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use encoding::value::{label::Label, value::Value};
use executor::ExecutionInterrupt;
use options::{MvccCleanupStrategy, TransactionOptions, byte_size::ByteSize};
use resource::profile::CommitProfile;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tokio::sync::mpsc::error::TrySendError;
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_dir, init_logging};
use tokio::sync::mpsc::Sender;

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

fn importer_sender(
    database_manager: &Arc<DatabaseManager>,
    name: &str,
) -> (Sender<MigrationMessage>, JoinHandle<Result<u64, DatabaseImportError>>) {
    let staged_database = database_manager.prepare_imported_database(name.to_owned()).expect("prepare");
    let handler = TestImportHandler { database_manager: database_manager.clone(), staged_database };
    let (sender, receiver) = tokio::sync::mpsc::channel(10);
    let importer = DatabaseImporter::new(
        Box::new(handler),
        database_manager.import_directory().to_owned(),
        ExecutionInterrupt::new_uninterruptible(),
        receiver,
    );
    let join_handle = thread::spawn(move || importer.listen());
    (sender, join_handle)
}

fn read_transaction(database_manager: &Arc<DatabaseManager>, name: &str) -> TransactionRead<WALClient> {
    let database = database_manager.database(name).expect("imported database is served");
    TransactionRead::open(database, TransactionOptions::default()).expect("open read")
}

fn export(transaction: &TransactionRead<WALClient>, name: &str) -> Vec<MigrationMessage> {
    let mut exporter = DatabaseExporter::new(transaction, "3.0.0-test", name).expect("exporter");
    let mut items = Vec::new();
    while let Some(item) = exporter.next_item().expect("next item") {
        items.push(item);
    }
    items
}

fn source_items() -> Vec<MigrationMessage> {
    let alice = "alice".to_owned();
    let bob = "bob".to_owned();
    vec![
        MigrationMessage::Schema(SCHEMA.to_owned()),
        MigrationMessage::Attribute {
            id: alice.clone(),
            label: Label::build("name", None),
            value: Value::String("Alice".into()),
        },
        MigrationMessage::Attribute {
            id: bob.clone(),
            label: Label::build("name", None),
            value: Value::String("Bob".into()),
        },
        MigrationMessage::Entity {
            id: "p1".to_owned(),
            label: Label::build("person", None),
            owned_attributes: vec![alice],
        },
        MigrationMessage::Entity { id: "p2".to_owned(), label: Label::build("person", None), owned_attributes: vec![bob] },
        MigrationMessage::Relation {
            id: "f1".to_owned(),
            label: Label::build("friendship", None),
            owned_attributes: vec![],
            related_role_players: vec![(
                Label::build_scoped("friend", "friendship", None),
                vec!["p1".to_owned(), "p2".to_owned()],
            )],
        },
        MigrationMessage::Checksums(Checksums {
            entity_count: 2,
            attribute_count: 2,
            relation_count: 1,
            role_count: 2,
            ownership_count: 2,
        }),
    ]
}

fn empty_checksums() -> MigrationMessage {
    MigrationMessage::Checksums(Checksums::new())
}

fn import_all(database_manager: &Arc<DatabaseManager>, name: &str, items: Vec<MigrationMessage>) {
    let (sender, task) = importer_sender(database_manager, name);
    for item in items {
        sender.try_send(item).expect("apply");
    }
    sender.try_send(MigrationMessage::Finalize).expect("finalize message");
    task.join().unwrap().unwrap();
}

#[test]
fn a_database_without_a_schema_or_data_round_trips() {
    init_logging();
    let data_dir = create_tmp_dir("migration_empty");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "blank", vec![MigrationMessage::Schema(String::new()), empty_checksums()]);

    let items = export(&read_transaction(&database_manager, "blank"), "blank");
    assert!(
        matches!(
            items.as_slice(),
            [MigrationMessage::Schema(_), MigrationMessage::Header { .. }, MigrationMessage::Checksums(_)]
        ),
        "{items:?}"
    );
    import_all(&database_manager, "blank-copy", items);
    assert!(database_manager.database("blank-copy").is_some());
}

#[test]
fn an_exported_stream_opens_with_the_schema_and_closes_with_the_checksums() {
    init_logging();
    let data_dir = create_tmp_dir("migration_stream_shape");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "source", source_items());

    let transaction = read_transaction(&database_manager, "source");
    let items = export(&transaction, "source");

    match &items[0] {
        MigrationMessage::Schema(schema) => assert!(schema.contains("entity person"), "unexpected schema: {schema}"),
        other => panic!("the stream must open with the schema, not {other:?}"),
    }
    match &items[1] {
        MigrationMessage::Header { typedb_version, original_database } => {
            assert_eq!(typedb_version, "3.0.0-test");
            assert_eq!(original_database, "source");
        }
        other => panic!("the schema must be followed by the header, not {other:?}"),
    }
    match items.last().expect("a non-empty stream") {
        MigrationMessage::Checksums(checksums) => {
            assert_eq!(checksums.entity_count, 2);
            assert_eq!(checksums.attribute_count, 2);
            assert_eq!(checksums.relation_count, 1);
            assert_eq!(checksums.role_count, 2);
            assert_eq!(checksums.ownership_count, 2);
        }
        other => panic!("the stream must close with the checksums, not {other:?}"),
    }
}

#[test]
fn an_exported_stream_is_importable_as_it_comes() {
    init_logging();
    let data_dir = create_tmp_dir("migration_round_trip");
    let database_manager = manager(&data_dir);
    import_all(&database_manager, "source", source_items());

    let source_items = {
        let transaction = read_transaction(&database_manager, "source");
        export(&transaction, "source")
    };
    import_all(&database_manager, "target", source_items);

    let transaction = read_transaction(&database_manager, "target");
    let copied_items = export(&transaction, "target");
    let (MigrationMessage::Checksums(copied), MigrationMessage::Checksums(original)) =
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

#[test]
fn an_out_of_order_stream_is_rejected() {
    init_logging();
    let data_dir = create_tmp_dir("migration_stream_order");
    let database_manager = manager(&data_dir);

    let (sender, task) = importer_sender(&database_manager, "early");
    sender.try_send(MigrationMessage::Header {
        typedb_version: "3.0.0-test".to_owned(),
        original_database: "source".to_owned(),
    }).expect("Message failed to send");
    let result = task.join().expect("Task failed to finish");
    assert!(matches!(result, Err(DatabaseImportError::ItemBeforeSchema { .. })), "{result:?}");

    let (twice, twice_task) = importer_sender(&database_manager, "twice");
    twice.try_send(MigrationMessage::Schema(SCHEMA.to_owned())).expect("Messaged failed");
    let twice_result = twice_task.join().expect("Task failed to finish");
    assert!(matches!(result, Err(DatabaseImportError::SchemaAlreadyImported { .. })), "{twice_result:?}");

    let (late, late_task) = importer_sender(&database_manager, "late");
    for item in source_items() {
        late.try_send(item).expect("Messaged failed");
    }
    late.try_send(MigrationMessage::Entity {
        id: "p3".to_owned(),
        label: Label::build("person", None),
        owned_attributes: vec![],
    }).expect("Message failed");
    let result= late_task.join().expect("Task failed to finish");
    assert!(matches!(result, Err(DatabaseImportError::ItemAfterChecksums { .. })), "{result:?}");
    let result = late.try_send(MigrationMessage::Checksums(Checksums::new()));
    assert!(matches!(result, Err(TrySendError::Closed(_))), "{result:?}");
}
