/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use concept::thing::thing_manager::ConceptPersistenceGuarantees;
use concurrency::{TokioTaskSpawner, TokioTaskTracker};
use database::{
    Database,
    database_manager::{DatabaseManager, ImportOwnership},
    migration::{
        Counts, MigrationOperationError,
        database_exporter::{DatabaseExporter, ExportMessage},
        database_importer::{DatabaseImportError, DatabaseImporter, DatabaseImporterHandle, ImporterConfiguration},
        importing_database_handle::{
            ImportingDatabaseHandle, open_import_schema_transaction, open_import_write_transaction,
        },
        item::{
            AttributeItem, AttributeReferencedByValue, EntityItem, Guarantees, ItemOrder, MigrationItem,
            OwnedAttribute, Player, RelationItem, Role,
        },
    },
    transaction::{
        CommitIntent, DataCommitIntent, SchemaCommitIntent, TransactionError, TransactionRead, TransactionSchema,
        TransactionWrite,
    },
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use options::{MvccCleanupStrategy, TransactionOptions, byte_size::ByteSize};
use prost::Message;
use resource::{constants::server::DISTRIBUTION_INFO, profile::CommitProfile};
use server::service::migration::item::{EncodedItem, decode_item, encode_item};
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_dir, init_logging};
use tokio::sync::watch;
use typedb_protocol::migration::Item;

const PREVIOUS_VERSION: &str = "3_13";
const CURRENT_VERSION: &str = "3_14";

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

    fn open_write(
        &self,
        concept_persistence_guarantees: Option<Box<dyn ConceptPersistenceGuarantees>>,
    ) -> Result<TransactionWrite<WALClient>, TransactionError> {
        open_import_write_transaction(&self.staged_database, concept_persistence_guarantees)
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

fn task_spawner() -> TokioTaskSpawner {
    let (_, shutdown_receiver) = watch::channel(());
    TokioTaskTracker::new(shutdown_receiver).get_spawner()
}

fn importer(database_manager: &Arc<DatabaseManager>, name: &str) -> DatabaseImporterHandle {
    let staged_database = database_manager.prepare_imported_database(name.to_owned()).expect("prepare");
    let handle = TestImportingDatabaseHandle { database_manager: database_manager.clone(), staged_database };
    let configuration = ImporterConfiguration::new(database_manager.import_directory().to_owned());
    DatabaseImporter::start(Arc::new(handle), configuration, &task_spawner())
}

fn fixtures_directory() -> PathBuf {
    let in_crate = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    if in_crate.is_dir() { in_crate } else { PathBuf::from("server/tests/fixtures") }
}

fn fixture_paths(version: &str) -> (PathBuf, PathBuf) {
    let directory = fixtures_directory();
    (directory.join(format!("schema_v_{version}.tql")), directory.join(format!("data_v_{version}.typedb")))
}

fn read_fixture(version: &str) -> (String, Vec<Item>) {
    let (schema_path, data_path) = fixture_paths(version);
    let schema = fs::read_to_string(&schema_path).unwrap_or_else(|err| panic!("{}: {err}", schema_path.display()));
    let data = fs::read(&data_path).unwrap_or_else(|err| panic!("{}: {err}", data_path.display()));
    (schema, decode_stream(&data))
}

fn decode_stream(mut bytes: &[u8]) -> Vec<Item> {
    let mut items = Vec::new();
    while !bytes.is_empty() {
        items.push(Item::decode_length_delimited(&mut bytes).expect("length-delimited item"));
    }
    items
}

fn encode_stream(items: Vec<MigrationItem>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for item in items {
        match encode_item(ExportMessage::Item(item)) {
            EncodedItem::Item(item) => item.encode_length_delimited(&mut bytes).expect("encode item"),
            EncodedItem::Schema(_) => unreachable!("an item never encodes as the schema"),
        }
    }
    bytes
}

fn decode_items(items: &[Item]) -> Vec<MigrationItem> {
    items.iter().cloned().map(|item| decode_item(item).expect("decoded item")).collect()
}

async fn import(
    database_manager: &Arc<DatabaseManager>,
    name: &str,
    schema: &str,
    items: Vec<Item>,
) -> Result<(), DatabaseImportError> {
    let mut importer = importer(database_manager, name);
    importer.send_schema(schema.to_owned()).await?;
    importer.send_batch(items.into_iter().map(decode_item)).await?;
    importer.finalize().await.map(|_| ())
}

fn export(database_manager: &Arc<DatabaseManager>, name: &str) -> (String, Vec<MigrationItem>) {
    let database = database_manager.database(name).expect("imported database is served");
    let transaction = TransactionRead::open(database, TransactionOptions::default()).expect("open read");
    let mut exporter = DatabaseExporter::new(&transaction, DISTRIBUTION_INFO.version, name).expect("exporter");
    let mut schema = None;
    let mut items = Vec::new();
    while let Some(message) = exporter.next_item().expect("next item") {
        match message {
            ExportMessage::Schema(exported) => assert!(schema.replace(exported).is_none(), "one schema"),
            ExportMessage::Item(item) => items.push(item),
        }
    }
    (schema.expect("the stream opens with the schema"), items)
}

#[derive(Debug, PartialEq, Eq)]
struct ExportContentDescription {
    header: Option<(String, Guarantees)>,
    attributes: Vec<String>,
    entities: Vec<String>,
    relations: Vec<String>,
    checksums: Option<Counts>,
}

fn export_description(items: &[MigrationItem]) -> ExportContentDescription {
    let mut header = None;
    let mut checksums = None;
    let mut attributes = HashMap::new();
    let mut entities = HashMap::new();
    let mut relations = HashMap::new();
    for item in items {
        match item {
            MigrationItem::Header { typedb_version, guarantees, .. } => {
                assert!(header.replace((typedb_version.clone(), *guarantees)).is_none(), "one header")
            }
            MigrationItem::Checksums(received) => {
                assert!(checksums.replace(received.counts()).is_none(), "one checksums")
            }
            MigrationItem::Attribute(AttributeItem { globally_unique_id, label, value }) => {
                let description = format!("{label} {value:?}");
                assert!(
                    attributes.insert(globally_unique_id.as_str(), description).is_none(),
                    "duplicate {globally_unique_id}"
                )
            }
            MigrationItem::Entity(entity) => {
                assert!(entities.insert(entity.globally_unique_id.as_str(), entity).is_none())
            }
            MigrationItem::Relation(relation) => {
                assert!(relations.insert(relation.globally_unique_id.as_str(), relation).is_none())
            }
        }
    }

    let owned = |owned_attributes: &[OwnedAttribute]| -> Vec<String> {
        let mut owned: Vec<String> = owned_attributes
            .iter()
            .map(|OwnedAttribute { globally_unique_id, attribute_by_value }| match attribute_by_value {
                Some(AttributeReferencedByValue { label, value }) => format!("{label} {value:?}"),
                None => attributes
                    .get(globally_unique_id.as_str())
                    .unwrap_or_else(|| panic!("dangling attribute {globally_unique_id}"))
                    .clone(),
            })
            .collect();
        owned.sort();
        owned
    };
    let entity = |entity: &EntityItem| format!("{} {:?}", entity.label, owned(&entity.attributes));
    fn relation(
        item: &RelationItem,
        entities: &HashMap<&str, &EntityItem>,
        relations: &HashMap<&str, &RelationItem>,
        entity: &dyn Fn(&EntityItem) -> String,
        owned: &dyn Fn(&[OwnedAttribute]) -> Vec<String>,
    ) -> String {
        let mut roles: Vec<String> = item
            .roles
            .iter()
            .map(|Role { label, players }| {
                let mut players: Vec<String> = players
                    .iter()
                    .map(|Player { globally_unique_id: id, .. }| {
                        match (entities.get(id.as_str()), relations.get(id.as_str())) {
                            (Some(player), None) => entity(player),
                            (None, Some(player)) => {
                                format!("({})", relation(player, entities, relations, entity, owned))
                            }
                            _ => panic!("dangling or ambiguous player {id}"),
                        }
                    })
                    .collect();
                players.sort();
                format!("{label} {players:?}")
            })
            .collect();
        roles.sort();
        format!("{} {:?} {roles:?}", item.label, owned(&item.attributes))
    }

    let mut attribute_values: Vec<String> = attributes.values().cloned().collect();
    let mut entity_values: Vec<String> = entities.values().map(|item| entity(item)).collect();
    let mut relation_values: Vec<String> =
        relations.values().map(|item| relation(item, &entities, &relations, &entity, &owned)).collect();
    attribute_values.sort();
    entity_values.sort();
    relation_values.sort();
    ExportContentDescription {
        header,
        attributes: attribute_values,
        entities: entity_values,
        relations: relation_values,
        checksums,
    }
}

fn description_without_header(export_content_description: ExportContentDescription) -> ExportContentDescription {
    ExportContentDescription { header: None, ..export_content_description }
}

async fn import_round_trips(version: &str, expected_guarantees: Guarantees) {
    init_logging();
    let data_dir = create_tmp_dir(&format!("migration_fixture_{version}"));
    let database_manager = manager(&data_dir);
    let (schema, items) = read_fixture(version);
    let fixture = export_description(&decode_items(&items));
    let (_, guarantees) = fixture.header.clone().expect("the fixture carries a header");
    assert_eq!(guarantees, expected_guarantees);

    import(&database_manager, "imported", &schema, items).await.expect("import");

    let (_, exported) = export(&database_manager, "imported");
    let exported = export_description(&exported);
    assert_eq!(
        exported.header.as_ref().map(|(_, guarantees)| guarantees.item_order),
        Some(ItemOrder::AttributesEntitiesRelations)
    );
    assert_eq!(description_without_header(exported), description_without_header(fixture));
}

#[tokio::test]
async fn an_export_of_the_previous_version_imports_and_round_trips() {
    import_round_trips(PREVIOUS_VERSION, Guarantees::default()).await;
}

#[tokio::test]
async fn an_export_of_the_current_version_imports_and_round_trips() {
    let guarantees = Guarantees {
        item_order: ItemOrder::AttributesEntitiesRelations,
        attributes_unique: true,
        label_scoped_ids_present: true,
        owned_inlined_attributes_referenced_by_value: true,
    };
    import_round_trips(CURRENT_VERSION, guarantees).await;
}

#[tokio::test]
async fn the_fixtures_of_both_versions_describe_the_same_database() {
    let (_, previous) = read_fixture(PREVIOUS_VERSION);
    let (_, current) = read_fixture(CURRENT_VERSION);
    assert_eq!(
        description_without_header(export_description(&decode_items(&current))),
        description_without_header(export_description(&decode_items(&previous)))
    );
}

/// Regenerates the current version's fixture from the previous version's: `cargo test -p server --test
/// test_migration regenerate_the_current_version_fixture -- --ignored`. Run it after a change to the stream format, and review the
/// resulting diff.
#[tokio::test]
#[ignore]
async fn regenerate_the_current_version_fixture() {
    init_logging();
    let data_dir = create_tmp_dir("migration_fixture_regenerate");
    let database_manager = manager(&data_dir);
    let (schema, items) = read_fixture(PREVIOUS_VERSION);
    import(&database_manager, "fixture", &schema, items).await.expect("import");
    let (schema, items) = export(&database_manager, "fixture");
    let (schema_path, data_path) = fixture_paths(CURRENT_VERSION);
    fs::write(&schema_path, schema).expect("write schema");
    fs::write(&data_path, encode_stream(items)).expect("write data");
}
