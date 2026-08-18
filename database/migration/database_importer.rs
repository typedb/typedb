/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
};

use bytes::{Bytes, byte_array::ByteArray};
use cache::{CacheError, SpilloverCache};
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        ThingAPI,
        attribute::Attribute,
        entity::Entity,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::ThingManager,
    },
    type_::{
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationIndependent, AnnotationKey},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::Constraint,
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
    },
};
use encoding::value::{label::Label, value::Value};
use error::typedb_error;
use executor::ExecutionInterrupt;
use query::error::QueryError;
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::StorageCounters};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use typeql::{parse_query, query::SchemaQuery};

use crate::{
    migration::{
        Checksums,
        database_import_handler::{DatabaseImportHandler, ImportHandlerError},
    },
    query::execute_schema_query,
    transaction::{TransactionError, TransactionSchema, TransactionWrite},
    with_transaction_parts,
};

macro_rules! is_specializing_fn {
    (
        $fn_name:ident,
        $capability_ty:ty,
        $get_cardinality_method:ident
    ) => {
        fn $fn_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            capability: $capability_ty,
        ) -> Result<bool, DatabaseImportError> {
            let object_type = capability.object();
            let interface_type = capability.interface();
            let cardinalities = object_type
                .$get_cardinality_method(snapshot, type_manager, interface_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            // If this capability is affected by multiple cardinality constraints from the same interface type, then
            // the object type has multiple ownerships of this interface type: some inherited and one declared (specializing)
            let same_interface_type_count = cardinalities
                .into_iter()
                .filter(|constraint| constraint.source().interface() == interface_type)
                .count();
            Ok(same_interface_type_count > 1)
        }
    };
}

macro_rules! for_item_in_write_transaction {
    (
        $self:ident, |$inner_snapshot:ident, $type_manager:ident, $thing_manager:ident| $expr:expr
    ) => {{
        let transaction = $self.get_data_transaction()?;
        let (transaction, result) = with_transaction_parts!(
            TransactionWrite,
            transaction,
            |$inner_snapshot, $type_manager, $thing_manager, _fm, _qm| { $expr }
        );

        $self.data_transaction = Some(transaction);
        result?;
        $self.count_item();
        if $self.transaction_item_count() % DatabaseImporter::COMMIT_BATCH_SIZE == 0 {
            let transaction = $self.data_transaction.take().unwrap();
            $self.commit_write_transaction(transaction)?;
        }
        Ok(())
    }};
}

#[derive(Debug)]
struct SchemaInfo {
    temporarily_independent_attribute_types: HashSet<AttributeType>,
    temporarily_non_independent_attribute_types: HashSet<AttributeType>,
    temporarily_independent_relation_types: HashSet<RelationType>,
    original_keys: HashSet<Owns>,
    original_cardinalities_owns: HashMap<Owns, Option<AnnotationCardinality>>,
    original_cardinalities_plays: HashMap<Plays, Option<AnnotationCardinality>>,
    original_cardinalities_relates: HashMap<Relates, Option<AnnotationCardinality>>,
    temporarily_removed_specializing_owns: HashMap<Owns, (HashSet<OwnsAnnotation>, Ordering)>,
    temporarily_removed_specializing_plays: HashMap<Plays, HashSet<PlaysAnnotation>>,
}

impl SchemaInfo {
    fn new() -> Self {
        Self {
            temporarily_independent_attribute_types: HashSet::new(),
            temporarily_non_independent_attribute_types: HashSet::new(),
            temporarily_independent_relation_types: HashSet::new(),
            original_keys: HashSet::new(),
            original_cardinalities_owns: HashMap::new(),
            original_cardinalities_plays: HashMap::new(),
            original_cardinalities_relates: HashMap::new(),
            temporarily_removed_specializing_owns: HashMap::new(),
            temporarily_removed_specializing_plays: HashMap::new(),
        }
    }
}

type IID = ByteArray<BUFFER_KEY_INLINE>;

#[derive(Debug)]
struct DataInfo {
    objects: ObjectsInfo,
    attributes: AttributesInfo,
    checksums: Checksums,
    expected_checksums: Option<Checksums>,
}

impl DataInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            objects: ObjectsInfo::new(&cache_directory, database_name),
            attributes: AttributesInfo::new(cache_directory, database_name),
            checksums: Checksums::new(),
            expected_checksums: None,
        }
    }

    fn record_entity(&mut self, original_id: String, entity: Entity) -> Result<(), DatabaseImportError> {
        self.checksums.entity_count += 1;
        self.objects.instance_id_mapping.record(original_id, entity.into_object())
    }

    fn record_relation(&mut self, original_id: String, relation: Relation) -> Result<(), DatabaseImportError> {
        self.checksums.relation_count += 1;
        self.objects.instance_id_mapping.record(original_id, relation.into_object())
    }

    fn record_attribute(&mut self, original_id: String, attribute: Attribute) -> Result<(), DatabaseImportError> {
        self.checksums.attribute_count += 1;
        self.attributes.instance_id_mapping.record(original_id, attribute)
    }

    fn record_ownership(&mut self) {
        self.checksums.ownership_count += 1;
    }

    fn record_role(&mut self) {
        self.checksums.role_count += 1;
    }

    fn record_expected_checksums(&mut self, checksums: Checksums) -> Result<(), DatabaseImportError> {
        if self.expected_checksums.is_some() {
            return Err(DatabaseImportError::DuplicateClientChecksums {});
        }
        self.expected_checksums = Some(checksums);
        Ok(())
    }

    fn verify_checksums(&self) -> Result<(), DatabaseImportError> {
        let self_checksums = &self.checksums;
        match &self.expected_checksums {
            Some(client_checksums) => {
                if self_checksums == client_checksums {
                    Ok(())
                } else {
                    let details = format!("client expected {client_checksums}, but recorded {self_checksums}");
                    Err(DatabaseImportError::InvalidChecksumsOnDone { details })
                }
            }
            None => Err(DatabaseImportError::NoClientChecksumsOnDone {}),
        }
    }
}

#[derive(Debug)]
struct InstanceIDMapping<T: ThingAPI> {
    original_to_buffered_ids: SpilloverCache<IID>,
    _phantom_data: PhantomData<T>,
}

impl<T: ThingAPI> InstanceIDMapping<T> {
    const CACHE_SPILLOVER_THRESHOLD: usize = 300_000;

    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            original_to_buffered_ids: SpilloverCache::new(
                cache_directory,
                Some(database_name),
                Self::CACHE_SPILLOVER_THRESHOLD,
            ),
            _phantom_data: PhantomData,
        }
    }

    fn get_by_original_id(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        id: &str,
    ) -> Result<Option<T>, DatabaseImportError> {
        let buffered_id_opt =
            self.original_to_buffered_ids.get(id).map_err(|source| DatabaseImportError::CacheError { source })?;
        let Some(buffered_id) = buffered_id_opt else {
            return Ok(None);
        };
        thing_manager
            .get_instance::<T>(snapshot, &Bytes::Array(buffered_id), StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })
    }

    fn record(&mut self, original_id: String, instance: T) -> Result<(), DatabaseImportError> {
        self.original_to_buffered_ids
            .insert(original_id, instance.iid().into_array())
            .map_err(|source| DatabaseImportError::CacheError { source })
    }
}

#[derive(Debug)]
struct ObjectsInfo {
    pub instance_id_mapping: InstanceIDMapping<Object>,
    // TODO: Should be a SpilloverCache
    pub awaited_for_roles: HashMap<String, HashSet<(RoleType, Relation)>>,
}

impl ObjectsInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            awaited_for_roles: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct AttributesInfo {
    pub instance_id_mapping: InstanceIDMapping<Attribute>,
    // TODO: Should be a SpilloverCache
    pub awaited_for_ownerships: HashMap<String, HashSet<Object>>,
}

impl AttributesInfo {
    fn new(cache_directory: &PathBuf, database_name: &str) -> Self {
        Self {
            instance_id_mapping: InstanceIDMapping::new(cache_directory, database_name),
            awaited_for_ownerships: HashMap::new(),
        }
    }
}

pub struct DatabaseImporter {
    import_handler: Option<Box<dyn DatabaseImportHandler>>,
    database_name: String,
    schema_info: SchemaInfo,
    data_info: DataInfo,
    data_transaction: Option<TransactionWrite<WALClient>>,
    transaction_item_count: u64,
    total_item_count: u64,
    interrupt: ExecutionInterrupt,
}

impl std::fmt::Debug for DatabaseImporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseImporter").field("database_name", &self.database_name).finish_non_exhaustive()
    }
}

impl DatabaseImporter {
    const COMMIT_BATCH_SIZE: u64 = 10_000;

    pub fn new(
        import_handler: Box<dyn DatabaseImportHandler>,
        scratch_directory: PathBuf,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        let database_name = import_handler.database_name().to_owned();
        let data_info = DataInfo::new(&scratch_directory, &database_name);
        Self {
            import_handler: Some(import_handler),
            database_name,
            schema_info: SchemaInfo::new(),
            data_info,
            data_transaction: None,
            transaction_item_count: 0,
            total_item_count: 0,
            interrupt,
        }
    }

    fn check_interrupt(&mut self) -> Result<(), DatabaseImportError> {
        match self.interrupt.check() {
            Some(_) => Err(DatabaseImportError::Interrupted {}),
            None => Ok(()),
        }
    }

    pub fn import_schema(&mut self, schema: String) -> Result<(), DatabaseImportError> {
        if schema.trim().is_empty() {
            return Ok(());
        }
        self.submit_original_schema(schema)?;
        self.relax_schema()
    }

    pub fn import_attribute(
        &mut self,
        id: String,
        label: Label,
        value: Value<'static>,
    ) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let attribute_type = type_manager
                .get_attribute_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownAttributeType { label })?;

            debug_assert!(attribute_type.is_independent(&snapshot, &type_manager).unwrap());
            let attribute = thing_manager
                .create_attribute(&mut snapshot, attribute_type, value)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.fulfill_awaiting_ownerships(&mut snapshot, &thing_manager, &id, &attribute)?;

            self.data_info.record_attribute(id, attribute)
        })
    }

    pub fn import_entity(
        &mut self,
        id: String,
        label: Label,
        owned_attributes: Vec<String>,
    ) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let entity_type = type_manager
                .get_entity_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownEntityType { label })?;

            let entity = thing_manager
                .create_entity(&mut snapshot, entity_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.process_owned_attributes(&mut snapshot, &thing_manager, entity.into_object(), owned_attributes)?;
            self.fulfill_awaiting_roles(&mut snapshot, &thing_manager, &id, entity.into_object())?;

            self.data_info.record_entity(id, entity)
        })
    }

    pub fn import_relation(
        &mut self,
        id: String,
        label: Label,
        owned_attributes: Vec<String>,
        related_role_players: Vec<(Label, Vec<String>)>,
    ) -> Result<(), DatabaseImportError> {
        for_item_in_write_transaction!(self, |snapshot, type_manager, thing_manager| {
            let relation_type = type_manager
                .get_relation_type(&snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRelationType { label })?;

            debug_assert!(type_manager.get_is_relation_type_independent(&snapshot, relation_type).unwrap());

            let relation = thing_manager
                .create_relation(&mut snapshot, relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;

            self.process_owned_attributes(&mut snapshot, &thing_manager, relation.into_object(), owned_attributes)?;
            self.process_related_roles(&mut snapshot, &type_manager, &thing_manager, relation, related_role_players)?;
            self.fulfill_awaiting_roles(&mut snapshot, &thing_manager, &id, relation.into_object())?;

            self.data_info.record_relation(id, relation)
        })
    }

    pub fn record_expected_checksums(&mut self, checksums: Checksums) -> Result<(), DatabaseImportError> {
        self.data_info.record_expected_checksums(checksums)
    }

    pub fn import_done(mut self) -> Result<(), DatabaseImportError> {
        self.check_interrupt()?;
        if let Some(data_transaction) = self.data_transaction.take() {
            self.commit_write_transaction(data_transaction)?;
        }

        self.check_interrupt()?;
        self.validate_imported_data()?;
        self.check_interrupt()?;
        self.restore_relaxed_schema()?;

        let Some(import_handler) = self.import_handler.take() else {
            return Err(DatabaseImportError::DoubleFinalisation { name: self.database_name().to_string() });
        };
        import_handler.finalise().map_err(|typedb_source| DatabaseImportError::FinalisationFailed { typedb_source })
    }

    fn process_owned_attributes(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        owned_attribute_ids: Vec<String>,
    ) -> Result<(), DatabaseImportError> {
        for id in owned_attribute_ids {
            match self.data_info.attributes.instance_id_mapping.get_by_original_id(snapshot, thing_manager, &id)? {
                Some(attribute) => {
                    object
                        .set_has_unordered(snapshot, thing_manager, &attribute, StorageCounters::DISABLED)
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    self.data_info.record_ownership();
                }
                None => {
                    self.data_info.attributes.awaited_for_ownerships.entry(id).or_insert(HashSet::new()).insert(object);
                }
            }
        }
        Ok(())
    }

    fn fulfill_awaiting_ownerships(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        original_id: &str,
        attribute: &Attribute,
    ) -> Result<(), DatabaseImportError> {
        if let Some(awaiting_objects) = self.data_info.attributes.awaited_for_ownerships.remove(original_id) {
            for object in awaiting_objects {
                object
                    .set_has_unordered(snapshot, thing_manager, attribute, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_ownership();
            }
        }
        Ok(())
    }

    fn process_related_roles(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation: Relation,
        related_roles: Vec<(Label, Vec<String>)>,
    ) -> Result<(), DatabaseImportError> {
        for (label, player_ids) in related_roles {
            let role_type = type_manager
                .get_role_type(snapshot, &label)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .ok_or_else(|| DatabaseImportError::UnknownRoleType { label })?;

            for id in player_ids {
                match self.data_info.objects.instance_id_mapping.get_by_original_id(snapshot, thing_manager, &id)? {
                    Some(player) => {
                        relation
                            .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                        self.data_info.record_role();
                    }
                    None => {
                        self.data_info
                            .objects
                            .awaited_for_roles
                            .entry(id)
                            .or_insert(HashSet::new())
                            .insert((role_type, relation));
                    }
                }
            }
        }
        Ok(())
    }

    fn fulfill_awaiting_roles(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        original_id: &str,
        player: Object,
    ) -> Result<(), DatabaseImportError> {
        if let Some(awaiting_relations) = self.data_info.objects.awaited_for_roles.remove(original_id) {
            for (role_type, relation) in awaiting_relations {
                relation
                    .add_player(snapshot, thing_manager, role_type, player, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.data_info.record_role();
            }
        }
        Ok(())
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn transaction_item_count(&self) -> u64 {
        self.transaction_item_count
    }

    pub fn total_item_count(&self) -> u64 {
        self.total_item_count
    }

    fn submit_original_schema(&self, schema: String) -> Result<(), DatabaseImportError> {
        let parsed = parse_query(schema.as_ref())
            .map_err(|typedb_source| DatabaseImportError::SchemaQueryParseFailed { typedb_source })?;
        match parsed.into_structure() {
            typeql::query::QueryStructure::Schema(schema_query) => match &schema_query {
                SchemaQuery::Define(_) => {
                    let transaction = self.open_schema_transaction()?;
                    let (transaction, query_result) = execute_schema_query(transaction, schema_query, schema);
                    query_result.map_err(|typedb_source| DatabaseImportError::SchemaQueryFailed { typedb_source })?;
                    self.commit_schema_transaction(transaction, |typedb_source| {
                        DatabaseImportError::ProvidedSchemaCommitFailed { typedb_source }
                    })
                }
                _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
            },
            _ => Err(DatabaseImportError::InvalidSchemaDefineQuery {}),
        }
    }

    fn relax_schema(&mut self) -> Result<(), DatabaseImportError> {
        let transaction = self.open_schema_transaction()?;
        let (transaction, ()) = with_transaction_parts!(
            TransactionSchema,
            transaction,
            |inner_snapshot, type_manager, thing_manager, _fm, _qm| {
                self.make_attribute_types_independent(&mut inner_snapshot, &type_manager, &thing_manager)?;
                self.make_relation_types_independent(&mut inner_snapshot, &type_manager)?;
                self.relax_capabilities_and_cardinalities(&mut inner_snapshot, &type_manager, &thing_manager)?;
            }
        );

        self.commit_schema_transaction(transaction, |typedb_source| {
            DatabaseImportError::PreparationSchemaCommitFailed { typedb_source }
        })
    }

    fn restore_relaxed_schema(&self) -> Result<(), DatabaseImportError> {
        let transaction = self.open_schema_transaction()?;
        let (transaction, ()) = with_transaction_parts!(
            TransactionSchema,
            transaction,
            |inner_snapshot, type_manager, thing_manager, _fm, _qm| {
                self.restore_independent_attribute_types(&mut inner_snapshot, &type_manager, &thing_manager)?;
                self.restore_independent_relation_types(&mut inner_snapshot, &type_manager)?;
                self.restore_capabilities_and_cardinalities(&mut inner_snapshot, &type_manager, &thing_manager)?;
            }
        );

        self.commit_schema_transaction(transaction, |typedb_source| {
            DatabaseImportError::FinalizationSchemaCommitFailed { typedb_source }
        })
    }

    // The @independent constraint is inherited by all subtypes, and a subtype cannot redeclare
    // an inherited constraint. To make every attribute type independent, the annotation is
    // declared on root types only, and existing subtype declarations are temporarily undeclared
    // so they do not become redundant
    fn make_attribute_types_independent(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let attribute_types = type_manager
            .get_attribute_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for attribute_type in attribute_types {
            let is_root = attribute_type
                .get_supertype(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .is_none();
            let annotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);
            let is_declared_independent = attribute_type
                .get_annotations_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                .contains(&annotation);
            if is_root && !is_declared_independent {
                attribute_type
                    .set_annotation(snapshot, type_manager, thing_manager, annotation, StorageCounters::DISABLED)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.schema_info.temporarily_independent_attribute_types.insert(attribute_type);
            } else if !is_root && is_declared_independent {
                attribute_type
                    .unset_annotation(snapshot, type_manager, AnnotationCategory::Independent)
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                self.schema_info.temporarily_non_independent_attribute_types.insert(attribute_type);
            }
        }
        Ok(())
    }

    fn make_relation_types_independent(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), DatabaseImportError> {
        let relation_types = type_manager
            .get_relation_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for relation_type in relation_types {
            type_manager
                .set_relation_type_independent(snapshot, relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            self.schema_info.temporarily_independent_relation_types.insert(relation_type);
        }
        Ok(())
    }

    fn relax_capabilities_and_cardinalities(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let schema_info = &mut self.schema_info;
        let object_types = type_manager
            .get_object_types(snapshot)
            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
        for object_type in object_types {
            let all_owns = object_type
                .get_owns_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for owns in all_owns.iter() {
                if Self::is_specializing_owns(snapshot, type_manager, *owns)? {
                    let ordering = owns
                        .get_ordering(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    let annotations = owns
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    owns.owner()
                        .unset_owns(snapshot, type_manager, thing_manager, owns.attribute())
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    schema_info.temporarily_removed_specializing_owns.insert(*owns, (annotations.clone(), ordering));
                    continue;
                }

                let original_cardinality = owns
                    .get_cardinality(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                if original_cardinality != AnnotationCardinality::unchecked() {
                    match owns
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .iter()
                        .find(|annotation| {
                            matches!(annotation, OwnsAnnotation::Cardinality(_))
                                || matches!(annotation, OwnsAnnotation::Key(_))
                        }) {
                        Some(annotation) => match annotation {
                            OwnsAnnotation::Cardinality(cardinality) => {
                                schema_info.original_cardinalities_owns.insert(*owns, Some(*cardinality));
                            }
                            OwnsAnnotation::Key(_) => {
                                owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Key)
                                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                                schema_info.original_keys.insert(*owns);
                            }
                            _ => unreachable!("Expected a key or a cardinality annotation"),
                        },
                        None => {
                            schema_info.original_cardinalities_owns.insert(*owns, None);
                        }
                    }
                    owns.set_annotation(
                        snapshot,
                        type_manager,
                        thing_manager,
                        OwnsAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                    )
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                }
            }

            let all_plays = object_type
                .get_plays_declared(snapshot, type_manager)
                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
            for plays in all_plays.iter() {
                if Self::is_specializing_plays(snapshot, type_manager, *plays)? {
                    let annotations = plays
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    plays
                        .player()
                        .unset_plays(snapshot, type_manager, thing_manager, plays.role())
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                    schema_info.temporarily_removed_specializing_plays.insert(*plays, annotations.clone());
                    continue;
                }

                let original_cardinality = plays
                    .get_cardinality(snapshot, type_manager)
                    .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                if original_cardinality != AnnotationCardinality::unchecked() {
                    match plays
                        .get_annotations_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                        .iter()
                        .find(|annotation| matches!(annotation, PlaysAnnotation::Cardinality(_)))
                    {
                        Some(annotation) => match annotation {
                            PlaysAnnotation::Cardinality(cardinality) => {
                                schema_info.original_cardinalities_plays.insert(*plays, Some(*cardinality));
                            }
                            _ => unreachable!("Expected a cardinality annotation"),
                        },
                        None => {
                            schema_info.original_cardinalities_plays.insert(*plays, None);
                        }
                    }
                    plays
                        .set_annotation(
                            snapshot,
                            type_manager,
                            thing_manager,
                            PlaysAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                        )
                        .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                }
            }

            match object_type {
                ObjectType::Entity(_) => {}
                ObjectType::Relation(relation_type) => {
                    let all_relates = relation_type
                        .get_relates_declared(snapshot, type_manager)
                        .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                    for relates in all_relates.iter() {
                        // all relates are exclusive for each role type

                        let original_cardinality = relates
                            .get_cardinality(snapshot, type_manager)
                            .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?;
                        if original_cardinality != AnnotationCardinality::unchecked() {
                            match relates
                                .get_annotations_declared(snapshot, type_manager)
                                .map_err(|typedb_source| DatabaseImportError::ConceptRead { typedb_source })?
                                .iter()
                                .find(|annotation| matches!(annotation, RelatesAnnotation::Cardinality(_)))
                            {
                                Some(annotation) => match annotation {
                                    RelatesAnnotation::Cardinality(cardinality) => {
                                        schema_info
                                            .original_cardinalities_relates
                                            .insert(*relates, Some(cardinality.clone()));
                                    }
                                    _ => unreachable!("Expected a cardinality annotation"),
                                },
                                None => {
                                    schema_info.original_cardinalities_relates.insert(*relates, None);
                                }
                            }
                            relates
                                .set_annotation(
                                    snapshot,
                                    type_manager,
                                    thing_manager,
                                    RelatesAnnotation::Cardinality(AnnotationCardinality::unchecked()),
                                )
                                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    is_specializing_fn!(is_specializing_owns, Owns, get_owned_attribute_type_constraints_cardinality);

    is_specializing_fn!(is_specializing_plays, Plays, get_played_role_type_constraints_cardinality);

    fn restore_independent_attribute_types(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        for attribute_type in &self.schema_info.temporarily_independent_attribute_types {
            attribute_type
                .unset_annotation(snapshot, type_manager, AnnotationCategory::Independent)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        for attribute_type in &self.schema_info.temporarily_non_independent_attribute_types {
            let annotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);
            attribute_type
                .set_annotation(snapshot, type_manager, thing_manager, annotation, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn restore_independent_relation_types(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), DatabaseImportError> {
        for relation_type in &self.schema_info.temporarily_independent_relation_types {
            type_manager
                .unset_relation_type_independent(snapshot, *relation_type)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }
        Ok(())
    }

    fn restore_capabilities_and_cardinalities(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), DatabaseImportError> {
        let schema_info = &self.schema_info;
        for (owns, (annotations, ordering)) in &schema_info.temporarily_removed_specializing_owns {
            owns.owner()
                .set_owns(snapshot, type_manager, thing_manager, owns.attribute(), *ordering, StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            for annotation in annotations {
                owns.set_annotation(snapshot, type_manager, thing_manager, annotation.clone())
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }
        }

        for (plays, annotations) in &schema_info.temporarily_removed_specializing_plays {
            plays
                .player()
                .set_plays(snapshot, type_manager, thing_manager, plays.role(), StorageCounters::DISABLED)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            for annotation in annotations {
                plays
                    .set_annotation(snapshot, type_manager, thing_manager, annotation.clone())
                    .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            }
        }

        for (owns, annotation) in &schema_info.original_cardinalities_owns {
            match annotation {
                Some(cardinality) => owns.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    OwnsAnnotation::Cardinality(cardinality.clone()),
                ),
                None => owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality),
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for owns in &schema_info.original_keys {
            owns.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality)
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
            owns.set_annotation(snapshot, type_manager, thing_manager, OwnsAnnotation::Key(AnnotationKey))
                .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for (plays, annotation) in &schema_info.original_cardinalities_plays {
            match annotation {
                Some(cardinality) => plays.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    PlaysAnnotation::Cardinality(cardinality.clone()),
                ),
                None => plays.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality),
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        for (relates, annotation) in &schema_info.original_cardinalities_relates {
            match annotation {
                Some(cardinality) => relates.set_annotation(
                    snapshot,
                    type_manager,
                    thing_manager,
                    RelatesAnnotation::Cardinality(cardinality.clone()),
                ),
                None => {
                    relates.unset_annotation(snapshot, type_manager, thing_manager, AnnotationCategory::Cardinality)
                }
            }
            .map_err(|typedb_source| DatabaseImportError::ConceptWrite { typedb_source })?;
        }

        Ok(())
    }

    fn validate_imported_data(&self) -> Result<(), DatabaseImportError> {
        if !self.data_info.objects.awaited_for_roles.is_empty() {
            return Err(DatabaseImportError::IncompleteRolesOnDone {
                count: self.data_info.objects.awaited_for_roles.len(),
            });
        }

        if !self.data_info.attributes.awaited_for_ownerships.is_empty() {
            return Err(DatabaseImportError::IncompleteOwnershipsOnDone {
                count: self.data_info.attributes.awaited_for_ownerships.len(),
            });
        }

        self.data_info.verify_checksums()
    }

    fn count_item(&mut self) {
        self.transaction_item_count += 1;
        self.total_item_count += 1;
    }

    fn get_data_transaction(&mut self) -> Result<TransactionWrite<WALClient>, DatabaseImportError> {
        match self.data_transaction.take() {
            Some(transaction) => Ok(transaction),
            None => self
                .import_handler()?
                .open_write()
                .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source }),
        }
    }

    fn open_schema_transaction(&self) -> Result<TransactionSchema<WALClient>, DatabaseImportError> {
        self.import_handler()?
            .open_schema()
            .map_err(|typedb_source| DatabaseImportError::TransactionFailed { typedb_source })
    }

    fn commit_schema_transaction(
        &self,
        transaction: TransactionSchema<WALClient>,
        map_err: impl Fn(ImportHandlerError) -> DatabaseImportError,
    ) -> Result<(), DatabaseImportError> {
        let (_, finalise_result) = transaction.finalise();
        let intent = finalise_result.map_err(|source| map_err(Arc::new(source)))?;
        self.import_handler()?.commit_schema(intent).map_err(map_err)
    }

    fn commit_write_transaction(&self, transaction: TransactionWrite<WALClient>) -> Result<(), DatabaseImportError> {
        let map_err = |typedb_source| DatabaseImportError::DataCommitFailed { typedb_source };
        let (_, finalise_result) = transaction.finalise();
        let intent = finalise_result.map_err(|source| map_err(Arc::new(source)))?;
        self.import_handler()?.commit_data(intent).map_err(map_err)
    }

    fn import_handler(&self) -> Result<&dyn DatabaseImportHandler, DatabaseImportError> {
        self.import_handler.as_deref().ok_or(DatabaseImportError::AccessAfterFinalisation {})
    }
}

impl Drop for DatabaseImporter {
    fn drop(&mut self) {
        if let Some(data_transaction) = self.data_transaction.take() {
            assert!(
                self.import_handler.is_some(),
                "Unexpected open import transaction while the import is still not done"
            );
            data_transaction.close();
        }
    }
}

typedb_error! {
    pub DatabaseImportError(component = "Database import", prefix = "DBI") {
        TransactionFailed(1, "Import transaction failed.", typedb_source: TransactionError),
        ConceptRead(2, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        ConceptWrite(3, "Error writing concepts.", typedb_source: Box<ConceptWriteError>),
        DataCommitFailed(6, "Import data transaction commit failed.", typedb_source: ImportHandlerError),
        ProvidedSchemaCommitFailed(7, "Imported schema cannot be committed due to errors.", typedb_source: ImportHandlerError),
        PreparationSchemaCommitFailed(8, "Import schema transaction commit failed on preparation. It is a sign of a bug.", typedb_source: ImportHandlerError),
        FinalizationSchemaCommitFailed(9, "Import schema transaction commit failed on finalization. It is a sign of a bug.", typedb_source: ImportHandlerError),
        SchemaQueryParseFailed(10, "Import schema query parsing failed.", typedb_source: typeql::Error),
        SchemaQueryFailed(11, "Import schema query failed.", typedb_source: Box<QueryError>),
        InvalidSchemaDefineQuery(12, "Import schema query is not a valid define query."),
        DuplicateClientChecksums(13, "Checksums received multiple times. It is a sign of a corrupted file or a client bug."),
        UnknownAttributeType(14, "Cannot process an attribute: attribute type '{label}' does not exist in the schema.", label: Label),
        UnknownEntityType(15, "Cannot process an entity: entity type '{label}' does not exist in the schema.", label: Label),
        UnknownRelationType(16, "Cannot process a relation: relation type '{label}' does not exist in the schema.", label: Label),
        UnknownRoleType(17, "Cannot process a role player: role type '{label}' does not exist in the schema.", label: Label),
        NoClientChecksumsOnDone(18, "Cannot verify the imported database as there are no checksums received from the client. It is a sign of a corrupted file or a client bug."),
        InvalidChecksumsOnDone(19, "Invalid imported database with a checksums mismatch: {details}.", details: String),
        IncompleteOwnershipsOnDone(20, "Invalid imported database with {count} unknown owned attributes. It is a sign of a corrupted file or a client bug.", count: usize),
        IncompleteRolesOnDone(21, "Invalid imported database with {count} unknown role players. It is a sign of a corrupted file or a client bug.", count: usize),
        DoubleFinalisation(22, "Error finalizing import for database '{name}': it was already finalized. It is a sign of a corrupted file or a client bug.", name: String),
        AccessAfterFinalisation(24, "Tried to modify the imported database's state after finalization. It is a sign of a client bug."),
        CacheError(25, "Error writing import data.", source: CacheError),
        FinalisationFailed(26, "Import finalisation failed.", typedb_source: ImportHandlerError),
        Interrupted(27, "The import was interrupted by a close request or server shutdown before completion. The import is aborted and can be retried."),
    }
}
