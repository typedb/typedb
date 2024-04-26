/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    sync::Arc,
};

use bytes::Bytes;
use durability::SequenceNumber;
use encoding::{AsBytes, graph::{
    type_::{
        vertex::{
            build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
            build_vertex_role_type_prefix, new_vertex_attribute_type, new_vertex_entity_type,
            new_vertex_relation_type, new_vertex_role_type, TypeVertex,
        },
    },
    Typed,
}, layout::prefix::Prefix, Prefixed, value::{
    label::Label,
    value_type::ValueType,
}};
use encoding::graph::type_::edge::TypeEdge;
use storage::{MVCCStorage, ReadSnapshotOpenError, snapshot::ReadableSnapshot};
use storage::key_range::KeyRange;

use crate::type_::{attribute_type::{AttributeType, AttributeTypeAnnotation}, entity_type::{EntityType, EntityTypeAnnotation}, object_type::ObjectType, Ordering, owns::Owns, plays::Plays, relates::Relates, relation_type::{RelationType, RelationTypeAnnotation}, role_type::{RoleType, RoleTypeAnnotation}, TypeAPI};
use crate::type_::owns::OwnsAnnotation;
use crate::type_::storage_source::StorageTypeManagerSource;
use crate::type_::type_manager::{ReadableType, TypeManager};

// TODO: could/should we slab allocate the schema cache?
pub struct TypeCache {
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityTypeCache>]>,
    relation_types: Box<[Option<RelationTypeCache>]>,
    role_types: Box<[Option<RoleTypeCache>]>,
    attribute_types: Box<[Option<AttributeTypeCache>]>,

    owns: HashMap<Owns<'static>, OwnsCache>,

    entity_types_index_label: HashMap<Label<'static>, EntityType<'static>>,
    relation_types_index_label: HashMap<Label<'static>, RelationType<'static>>,
    role_types_index_label: HashMap<Label<'static>, RoleType<'static>>,
    attribute_types_index_label: HashMap<Label<'static>, AttributeType<'static>>,
}

#[derive(Debug)]
struct TypeAPICache<T: TypeAPI<'static> + ReadableType<'static, 'static>> {
    type_: T,
    label: Label<'static>,
    is_root: bool,
    annotations_declared: HashSet<T::AnnotationType>,
    // TODO: Should these all be sets instead of vec?
    supertype: Option<T::SelfWithLifetime>, // TODO: use smallvec if we want to have some inline - benchmark.
    supertypes: Vec<T::SelfWithLifetime>,  // TODO: use smallvec if we want to have some inline - benchmark.
    subtypes_declared: Vec<T::SelfWithLifetime>, // TODO: benchmark smallvec.
    subtypes_transitive: Vec<T::SelfWithLifetime>, // TODO: benchmark smallvec
}

impl<T> TypeAPICache<T> where T: TypeAPI<'static> + ReadableType<'static, 'static> {
    fn build_for<Snapshot: ReadableSnapshot>(snapshot: &Snapshot, type_ : T) -> TypeAPICache<T> {
        let label = StorageTypeManagerSource::storage_get_label(snapshot, type_.clone()).unwrap().unwrap();
        let is_root = TypeManager::<Snapshot>::check_type_is_root(&label, T::ROOT_KIND);
        let annotations_declared = StorageTypeManagerSource::storage_get_type_annotations(snapshot, type_.clone()).unwrap().into_iter()
            .map(|annotation| T::AnnotationType::from(annotation))
            .collect::<HashSet<T::AnnotationType>>();
        let supertype = StorageTypeManagerSource::storage_get_supertype(snapshot, type_.clone()).unwrap();
        let supertypes = StorageTypeManagerSource::storage_get_supertypes_transitive(snapshot, type_.clone()).unwrap();
        let subtypes_declared = StorageTypeManagerSource::storage_get_subtypes(snapshot, type_.clone()).unwrap();
        let subtypes_transitive = StorageTypeManagerSource::storage_get_subtypes_transitive(snapshot, type_.clone()).unwrap();
        Self {
            type_,
            label,
            is_root,
            annotations_declared,
            supertype,
            supertypes,
            subtypes_declared,
            subtypes_transitive
        }
    }
}

#[derive(Debug)]
struct EntityTypeCache {
    type_api_cache_ : TypeAPICache<EntityType<'static>>,
    owns_declared: HashSet<Owns<'static>>,
    plays_declared: HashSet<Plays<'static>>,
    // ...
}

#[derive(Debug)]
struct RelationTypeCache {
    type_api_cache_ : TypeAPICache<RelationType<'static>>,
    relates_declared: HashSet<Relates<'static>>,
    owns_declared: HashSet<Owns<'static>>,
    plays_declared: HashSet<Plays<'static>>,
}

#[derive(Debug)]
struct RoleTypeCache {
    type_api_cache_ : TypeAPICache<RoleType<'static>>,
    ordering: Ordering,
    relates_declared: Relates<'static>,
}

#[derive(Debug)]
struct AttributeTypeCache {
    type_api_cache_ :  TypeAPICache<AttributeType<'static>>,
    value_type: Option<ValueType>,
    // owners: HashSet<Owns<'static>>
}

#[derive(Debug)]
struct OwnsCache {
    ordering: Ordering,
    annotations_declared: HashSet<OwnsAnnotation>,
}

impl TypeCache {

    // If creation becomes slow, We should restore pre-fetching of the schema
    //  with a single pass on disk (as it was in 1f339733feaf4542e47ff604462f107d2ade1f1a)
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, TypeCacheCreateError> {
        use TypeCacheCreateError::SnapshotOpen;
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot =
            storage.open_snapshot_read_at(open_sequence_number).map_err(|error| SnapshotOpen { source: error })?;

        let entity_type_caches = Self::create_entity_caches(&snapshot);
        let entity_type_index_labels = entity_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.type_api_cache_.label.clone(), cache.type_api_cache_.type_.clone())))
            .collect();

        let relation_type_caches = Self::create_relation_caches(&snapshot);
        let relation_type_index_labels = relation_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.type_api_cache_.label.clone(), cache.type_api_cache_.type_.clone())))
            .collect();

        let role_type_caches = Self::create_role_caches(&snapshot);
        let role_type_index_labels = role_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.type_api_cache_.label.clone(), cache.type_api_cache_.type_.clone())))
            .collect();

        let attribute_type_caches = Self::create_attribute_caches(&snapshot);
        let attribute_type_index_labels = attribute_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.type_api_cache_.label.clone(), cache.type_api_cache_.type_.clone())))
            .collect();

        Ok(TypeCache {
            open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            role_types: role_type_caches,
            attribute_types: attribute_type_caches,
            owns: Self::create_owns_caches(&snapshot),

            entity_types_index_label: entity_type_index_labels,
            relation_types_index_label: relation_type_index_labels,
            role_types_index_label: role_type_index_labels,
            attribute_types_index_label: attribute_type_index_labels,
        })
    }

    fn create_entity_caches(
        snapshot: &impl ReadableSnapshot
    ) -> Box<[Option<EntityTypeCache>]> {
        let entities = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_entity_type_prefix(), Prefix::VertexEntityType.fixed_width_keys()))
            .collect_cloned_hashset(|key, _| {
                EntityType::new(new_vertex_entity_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_entity_id = entities.iter().map(|e| e.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_entity_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();

        for entity in entities.into_iter() {
            let cache = EntityTypeCache {
                type_api_cache_:  TypeAPICache::build_for(snapshot, entity.clone()),
                owns_declared: StorageTypeManagerSource::storage_get_owns(snapshot, entity.clone()).unwrap(),
                plays_declared: StorageTypeManagerSource::storage_get_plays(snapshot, entity.clone()).unwrap(),
            };
            caches[entity.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_relation_caches(
        snapshot: &impl ReadableSnapshot
    ) -> Box<[Option<RelationTypeCache>]> {
        let relations = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_relation_type_prefix(), Prefix::VertexRelationType.fixed_width_keys()))
            .collect_cloned_hashset(|key, _| {
                RelationType::new(new_vertex_relation_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_relation_id = relations.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_relation_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for relation in relations.into_iter() {
            let cache = RelationTypeCache {
                type_api_cache_:  TypeAPICache::build_for(snapshot, relation.clone()),
                relates_declared: StorageTypeManagerSource::storage_get_relates(snapshot, relation.clone()).unwrap(),
                owns_declared : StorageTypeManagerSource::storage_get_owns(snapshot, relation.clone()).unwrap(),
                plays_declared : StorageTypeManagerSource::storage_get_plays(snapshot, relation.clone()).unwrap()
            };
            caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_role_caches(
        snapshot: &impl ReadableSnapshot,
    ) -> Box<[Option<RoleTypeCache>]> {
        let roles = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_role_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                RoleType::new(new_vertex_role_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_role_id = roles.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_role_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for role in roles.into_iter() {
            let ordering = StorageTypeManagerSource::storage_get_type_ordering(snapshot, role.clone()).unwrap();
            let cache = RoleTypeCache {
                type_api_cache_:  TypeAPICache::build_for(snapshot, role.clone()),
                ordering,
                relates_declared: StorageTypeManagerSource::storage_get_relations(snapshot, role.clone()).unwrap()
            };
            caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_attribute_caches(
        snapshot: &impl ReadableSnapshot,
    ) -> Box<[Option<AttributeTypeCache>]> {
        let attributes = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_attribute_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                AttributeType::new(new_vertex_attribute_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_attribute_id = attributes.iter().map(|a| a.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_attribute_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for attribute in attributes {
            let cache = AttributeTypeCache {
                type_api_cache_:  TypeAPICache::build_for(snapshot, attribute.clone()),
                value_type: StorageTypeManagerSource::storage_get_value_type(snapshot, attribute.clone()).unwrap(),
            };
            caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_owns_caches(
        snapshot: &impl ReadableSnapshot,
    ) -> HashMap<Owns<'static>, OwnsCache> {
        snapshot
            .iterate_range(KeyRange::new_within(TypeEdge::build_prefix(Prefix::EdgeOwnsReverse), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashmap(|key, _| {
                let edge = TypeEdge::new(Bytes::Reference(key.byte_ref()));
                let attribute = AttributeType::new(edge.from().into_owned());
                let owner = ObjectType::new(edge.to().into_owned());
                let owns = Owns::new(owner, attribute);
                (
                    owns.clone(),
                    OwnsCache {
                        ordering: StorageTypeManagerSource::storage_get_type_edge_ordering(snapshot, owns.clone()).unwrap(),
                        annotations_declared: StorageTypeManagerSource::storage_get_type_edge_annotations(snapshot, owns.clone()).unwrap()
                            .into_iter()
                            .map(|annotation| OwnsAnnotation::from(annotation))
                            .collect(),
                    }
                )
            })
            .unwrap()
    }

    pub(crate) fn get_entity_type(&self, label: &Label<'_>) -> Option<EntityType<'static>> {
        self.entity_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_relation_type(&self, label: &Label<'_>) -> Option<RelationType<'static>> {
        self.relation_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_role_type(&self, label: &Label<'_>) -> Option<RoleType<'static>> {
        self.role_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_attribute_type(&self, label: &Label<'_>) -> Option<AttributeType<'static>> {
        self.attribute_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_entity_type_supertype(&self, entity_type: EntityType<'static>) -> Option<EntityType<'static>> {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.supertype.clone()
    }

    pub(crate) fn get_relation_type_supertype(
        &self,
        relation_type: RelationType<'static>,
    ) -> Option<RelationType<'static>> {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.supertype.clone()
    }

    pub(crate) fn get_role_type_supertype(&self, role_type: RoleType<'static>) -> Option<RoleType<'static>> {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.supertype.clone()
    }

    pub(crate) fn get_attribute_type_supertype(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> Option<AttributeType<'static>> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.supertype.clone()
    }

    pub(crate) fn get_entity_type_supertypes(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.supertypes
    }

    pub(crate) fn get_relation_type_supertypes(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.supertypes
    }

    pub(crate) fn get_role_type_supertypes(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.supertypes
    }

    pub(crate) fn get_attribute_type_supertypes(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.supertypes
    }


    pub(crate) fn get_entity_type_subtypes(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.subtypes_declared
    }

    pub(crate) fn get_relation_type_subtypes(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.subtypes_declared
    }

    pub(crate) fn get_role_type_subtypes(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.subtypes_declared
    }

    pub(crate) fn get_attribute_type_subtypes(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.subtypes_declared
    }


    pub(crate) fn get_entity_type_subtypes_transitive(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.subtypes_transitive
    }

    pub(crate) fn get_relation_type_subtypes_transitive(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.subtypes_transitive
    }

    pub(crate) fn get_role_type_subtypes_transitive(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.subtypes_transitive
    }

    pub(crate) fn get_attribute_type_subtypes_transitive(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.subtypes_transitive
    }

    pub(crate) fn get_entity_type_label(&self, entity_type: EntityType<'static>) -> &Label<'static> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.label
    }

    pub(crate) fn get_relation_type_label(&self, relation_type: RelationType<'static>) -> &Label<'static> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.label
    }

    pub(crate) fn get_role_type_label(&self, role_type: RoleType<'static>) -> &Label<'static> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.label
    }

    pub(crate) fn get_attribute_type_label(&self, attribute_type: AttributeType<'static>) -> &Label<'static> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.label
    }

    pub(crate) fn get_entity_type_is_root(&self, entity_type: EntityType<'static>) -> bool {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.is_root
    }

    pub(crate) fn get_relation_type_is_root(&self, relation_type: RelationType<'static>) -> bool {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.is_root
    }

    pub(crate) fn get_role_type_is_root(&self, role_type: RoleType<'static>) -> bool {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.is_root
    }

    pub(crate) fn get_attribute_type_is_root(&self, attribute_type: AttributeType<'static>) -> bool {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.is_root
    }

    pub(crate) fn get_role_type_ordering(&self, role_type: RoleType<'static>) -> Ordering {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().ordering
    }

    pub(crate) fn get_entity_type_owns(&self, entity_type: EntityType<'static>) -> &HashSet<Owns<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().owns_declared
    }

    pub(crate) fn get_relation_type_owns(&self, relation_type: RelationType<'static>) -> &HashSet<Owns<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().owns_declared
    }

    pub(crate) fn get_relation_type_relates(&self, relation_type: RelationType<'static>) -> &HashSet<Relates<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().relates_declared
    }

    pub(crate) fn get_entity_type_plays(&self, entity_type: EntityType<'static>) -> &HashSet<Plays<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().plays_declared
    }

    pub(crate) fn get_relation_type_plays(&self, relation_type: RelationType<'static>) -> &HashSet<Plays<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().plays_declared
    }

    pub(crate) fn get_attribute_type_value_type(&self, attribute_type: AttributeType<'static>) -> Option<ValueType> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().value_type
    }

    pub(crate) fn get_entity_type_annotations(
        &self,
        entity_type: EntityType<'static>,
    ) -> &HashSet<EntityTypeAnnotation> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().type_api_cache_.annotations_declared
    }

    pub(crate) fn get_relation_type_annotations(
        &self,
        relation_type: RelationType<'static>,
    ) -> &HashSet<RelationTypeAnnotation> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().type_api_cache_.annotations_declared
    }

    pub(crate) fn get_role_type_annotations(&self, role_type: RoleType<'static>) -> &HashSet<RoleTypeAnnotation> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().type_api_cache_.annotations_declared
    }

    pub(crate) fn get_attribute_type_annotations(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &HashSet<AttributeTypeAnnotation> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().type_api_cache_.annotations_declared
    }

    pub(crate) fn get_owns_annotations<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<OwnsAnnotation> {
        &self.owns.get(&owns).unwrap().annotations_declared
    }

    pub(crate) fn get_owns_ordering<'c>(&'c self, owns: Owns<'c>) -> Ordering {
        self.owns.get(&owns).unwrap().ordering
    }

    fn get_entity_type_cache<'c>(
        entity_type_caches: &'c [Option<EntityTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c EntityTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), Prefix::VertexEntityType);
        let as_u16 = type_vertex.type_id_().as_u16();
        entity_type_caches[as_u16 as usize].as_ref()
    }

    fn get_relation_type_cache<'c>(
        relation_type_caches: &'c [Option<RelationTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c RelationTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), Prefix::VertexRelationType);
        let as_u16 = type_vertex.type_id_().as_u16();
        relation_type_caches[as_u16 as usize].as_ref()
    }

    fn get_role_type_cache<'c>(
        role_type_caches: &'c [Option<RoleTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c RoleTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), Prefix::VertexRoleType);
        let as_u16 = type_vertex.type_id_().as_u16();
        role_type_caches[as_u16 as usize].as_ref()
    }

    fn get_attribute_type_cache<'c>(
        attribute_type_caches: &'c [Option<AttributeTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c AttributeTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), Prefix::VertexAttributeType);
        let as_u16 = type_vertex.type_id_().as_u16();
        attribute_type_caches[as_u16 as usize].as_ref()
    }
}

#[derive(Debug)]
pub enum TypeCacheCreateError {
    SnapshotOpen { source: ReadSnapshotOpenError },
}

impl fmt::Display for TypeCacheCreateError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotOpen { .. } => todo!(),
        }
    }
}

impl Error for TypeCacheCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotOpen { source } => Some(source),
        }
    }
}
