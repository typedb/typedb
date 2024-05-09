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
use encoding::{graph::{
    type_::{
        edge::TypeEdge,
        vertex::{
            build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
            build_vertex_role_type_prefix, new_vertex_attribute_type, new_vertex_entity_type,
            new_vertex_relation_type, new_vertex_role_type, TypeVertex,
        },
    },
    Typed,
}, layout::prefix::Prefix, value::{label::Label, value_type::ValueType}, Prefixed};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot, MVCCStorage, ReadSnapshotOpenError};

use crate::type_::{attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, owns::{Owns, OwnsAnnotation}, plays::Plays, relates::Relates, relation_type::RelationType, role_type::RoleType, type_manager::{ReadableType, TypeManager}, type_reader::TypeReader, Ordering, TypeAPI, OwnerAPI, PlayerAPI, attribute_type};
use crate::type_::type_manager::KindAPI;

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
struct CommonTypeCache<T: KindAPI<'static>> {
    type_: T,
    label: Label<'static>,
    is_root: bool,
    annotations_declared: HashSet<T::AnnotationType>,
    // TODO: Should these all be sets instead of vec?
    supertype: Option<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    supertypes: Vec<T>,   // TODO: use smallvec if we want to have some inline - benchmark.
    subtypes_declared: Vec<T>, // TODO: benchmark smallvec.
    subtypes_transitive: Vec<T>, // TODO: benchmark smallvec
}

#[derive(Debug)]
pub struct OwnsPlaysCache {
    owns_declared: HashSet<Owns<'static>>,
    plays_declared: HashSet<Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct EntityTypeCache {
    common_type_cache: CommonTypeCache<EntityType<'static>>,
    owns_plays_cache: OwnsPlaysCache,
    // ...
}

#[derive(Debug)]
pub(crate) struct RelationTypeCache {
    common_type_cache: CommonTypeCache<RelationType<'static>>,
    relates_declared: HashSet<Relates<'static>>,
    owns_plays_cache: OwnsPlaysCache,
}

#[derive(Debug)]
pub(crate) struct RoleTypeCache {
    common_type_cache: CommonTypeCache<RoleType<'static>>,
    ordering: Ordering,
    relates_declared: Relates<'static>,
}

#[derive(Debug)]
pub(crate) struct AttributeTypeCache {
    common_type_cache: CommonTypeCache<AttributeType<'static>>,
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
            .filter_map(|entry| {
                entry.as_ref().map(|cache| (cache.common_type_cache.label.clone(), cache.common_type_cache.type_.clone()))
            })
            .collect();

        let relation_type_caches = Self::create_relation_caches(&snapshot);
        let relation_type_index_labels = relation_type_caches
            .iter()
            .filter_map(|entry| {
                entry
                    .as_ref()
                    .map(|cache| (cache.common_type_cache.label.clone(), cache.common_type_cache.type_.clone()))
            })
            .collect();

        let role_type_caches = Self::create_role_caches(&snapshot);
        let role_type_index_labels = role_type_caches
            .iter()
            .filter_map(|entry| {
                entry.as_ref().map(|cache| (cache.common_type_cache.label.clone(), cache.common_type_cache.type_.clone()))
            })
            .collect();

        let attribute_type_caches = Self::create_attribute_caches(&snapshot);
        let attribute_type_index_labels = attribute_type_caches
            .iter()
            .filter_map(|entry| {
                entry.as_ref().map(|cache| (cache.common_type_cache.label.clone(), cache.common_type_cache.type_.clone()))
            })
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

    fn build_common_cache<Snapshot, T>(snapshot: &Snapshot, type_: T) -> CommonTypeCache<T>
    where
        Snapshot: ReadableSnapshot,
        T: KindAPI<'static> + ReadableType<Output<'static>=T>,
    {
        let label = TypeReader::get_label(snapshot, type_.clone()).unwrap().unwrap();
        let is_root = TypeManager::<Snapshot>::check_type_is_root(&label, T::ROOT_KIND);
        let annotations_declared = TypeReader::get_type_annotations(snapshot, type_.clone())
            .unwrap()
            .into_iter()
            .map(|annotation| T::AnnotationType::from(annotation))
            .collect::<HashSet<T::AnnotationType>>();
        let supertype = TypeReader::get_supertype(snapshot, type_.clone()).unwrap();
        let supertypes = TypeReader::get_supertypes_transitive(snapshot, type_.clone()).unwrap();
        let subtypes_declared = TypeReader::get_subtypes(snapshot, type_.clone()).unwrap();
        let subtypes_transitive = TypeReader::get_subtypes_transitive(snapshot, type_.clone()).unwrap();
        CommonTypeCache {
            type_,
            label,
            is_root,
            annotations_declared,
            supertype,
            supertypes,
            subtypes_declared,
            subtypes_transitive,
        }
    }
    fn build_owns_plays_cache<Snapshot, T>(snapshot: &Snapshot, type_: T) -> OwnsPlaysCache
        where
            Snapshot: ReadableSnapshot,
            T: KindAPI<'static> + OwnerAPI<'static> + PlayerAPI<'static> + ReadableType<Output<'static>=T>,
    {
        OwnsPlaysCache {
            owns_declared: TypeReader::get_owns(snapshot, type_.clone()).unwrap(),
            plays_declared: TypeReader::get_plays(snapshot, type_.clone()).unwrap(),
        }
    }

    fn create_entity_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<EntityTypeCache>]> {
        let entities = snapshot
            .iterate_range(KeyRange::new_within(
                build_vertex_entity_type_prefix(),
                Prefix::VertexEntityType.fixed_width_keys(),
            ))
            .collect_cloned_hashset(|key, _| {
                EntityType::new(new_vertex_entity_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_entity_id = entities.iter().map(|e| e.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_entity_id).map(|_| None).collect::<Box<[_]>>();

        for entity in entities.into_iter() {
            let cache = EntityTypeCache {
                common_type_cache: Self::build_common_cache(snapshot, entity.clone()),
                owns_plays_cache : Self::build_owns_plays_cache(snapshot, entity.clone()),
            };
            caches[entity.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_relation_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<RelationTypeCache>]> {
        let relations = snapshot
            .iterate_range(KeyRange::new_within(
                build_vertex_relation_type_prefix(),
                Prefix::VertexRelationType.fixed_width_keys(),
            ))
            .collect_cloned_hashset(|key, _| {
                RelationType::new(new_vertex_relation_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_relation_id = relations.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_relation_id).map(|_| None).collect::<Box<[_]>>();
        for relation in relations.into_iter() {
            let cache = RelationTypeCache {
                common_type_cache: Self::build_common_cache(snapshot, relation.clone()),
                relates_declared: TypeReader::get_relates(snapshot, relation.clone()).unwrap(),
                owns_plays_cache: Self::build_owns_plays_cache(snapshot, relation.clone()),
            };
            caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_role_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<RoleTypeCache>]> {
        let roles = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_role_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                RoleType::new(new_vertex_role_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_role_id = roles.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_role_id).map(|_| None).collect::<Box<[_]>>();
        for role in roles.into_iter() {
            let ordering = TypeReader::get_type_ordering(snapshot, role.clone()).unwrap();
            let cache = RoleTypeCache {
                common_type_cache: Self::build_common_cache(snapshot, role.clone()),
                ordering,
                relates_declared: TypeReader::get_relations(snapshot, role.clone()).unwrap(),
            };
            caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_attribute_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<AttributeTypeCache>]> {
        let attributes = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_attribute_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                AttributeType::new(new_vertex_attribute_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_attribute_id = attributes.iter().map(|a| a.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_attribute_id).map(|_| None).collect::<Box<[_]>>();
        for attribute in attributes {
            let cache = AttributeTypeCache {
                common_type_cache: Self::build_common_cache(snapshot, attribute.clone()),
                value_type: TypeReader::get_value_type(snapshot, attribute.clone()).unwrap(),
            };
            caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }

    fn create_owns_caches(snapshot: &impl ReadableSnapshot) -> HashMap<Owns<'static>, OwnsCache> {
        snapshot
            .iterate_range(KeyRange::new_within(
                TypeEdge::build_prefix(Prefix::EdgeOwnsReverse),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_hashmap(|key, _| {
                let edge = TypeEdge::new(Bytes::Reference(key.byte_ref()));
                let attribute = AttributeType::new(edge.from().into_owned());
                let owner = ObjectType::new(edge.to().into_owned());
                let owns = Owns::new(owner, attribute);
                (
                    owns.clone(),
                    OwnsCache {
                        ordering: TypeReader::get_type_edge_ordering(snapshot, owns.clone()).unwrap(),
                        annotations_declared: TypeReader::get_type_edge_annotations(snapshot, owns.clone())
                            .unwrap()
                            .into_iter()
                            .map(|annotation| OwnsAnnotation::from(annotation))
                            .collect(),
                    },
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

    pub(crate) fn get_supertype<'a, 'this, T, CACHE>(&'this self, type_: T) -> Option<T::SelfStatic>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        // TODO: Why does this not return &Option<EntityType<'static>> ?
        Some(T::get_cache(self, type_).common_type_cache().supertype.as_ref()?.clone())
    }

    pub(crate) fn get_supertypes<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        &T::get_cache(self, type_).common_type_cache().supertypes
    }
    pub(crate) fn get_subtypes<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        &T::get_cache(self, type_).common_type_cache().subtypes_declared
    }

    pub(crate) fn get_subtypes_transitive<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        &T::get_cache(self, type_).common_type_cache().subtypes_transitive
    }

    pub(crate) fn get_label<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Label<'static>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE : HasCommonTypeCache<T::SelfStatic> + 'this
    {
        &T::get_cache(self, type_).common_type_cache().label
    }

    pub(crate) fn is_root<'a, 'this, T, CACHE>(&'this self, type_: T) -> bool
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        T::get_cache(self, type_).common_type_cache().is_root
    }

    pub(crate) fn get_annotations<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<<<T as KindAPI<'a>>::SelfStatic as KindAPI<'static>>::AnnotationType>
        where T: KindAPI<'a> + CacheGetter<CacheType=CACHE>,
              CACHE: HasCommonTypeCache<T::SelfStatic> + 'this
    {
        &T::get_cache(self, type_).common_type_cache().annotations_declared
    }

    pub(crate) fn get_owns<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<Owns<'static>>
        where T:  OwnerAPI<'static> + PlayerAPI<'static> + CacheGetter<CacheType=CACHE>,
              CACHE: HasOwnsPlaysCache + 'this
    {
        &T::get_cache(self, type_).owns_plays_cache().owns_declared
    }

    pub(crate) fn get_role_type_ordering(&self, role_type: RoleType<'static>) -> Ordering {
        RoleType::get_cache(&self, role_type).ordering
    }

    pub(crate) fn get_relation_type_relates(&self, relation_type: RelationType<'static>) -> &HashSet<Relates<'static>> {
        &RelationType::get_cache(self, relation_type).relates_declared
        // &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().relates_declared
    }

    pub(crate) fn get_plays<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<Plays<'static>>
        where T:  OwnerAPI<'static> + PlayerAPI<'static> + CacheGetter<CacheType=CACHE>,
              CACHE: HasOwnsPlaysCache + 'this
    {
        &T::get_cache(self, type_).owns_plays_cache().plays_declared
    }

    pub(crate) fn get_attribute_type_value_type(&self, attribute_type: AttributeType<'static>) -> Option<ValueType> {
        AttributeType::get_cache(&self, attribute_type).value_type
    }

    pub(crate) fn get_owns_annotations<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<OwnsAnnotation> {
        &self.owns.get(&owns).unwrap().annotations_declared
    }

    pub(crate) fn get_owns_ordering<'c>(&'c self, owns: Owns<'c>) -> Ordering {
        self.owns.get(&owns).unwrap().ordering
    }
}

pub(crate) trait CacheGetter {
    type CacheType;
    fn get_cache<'cache>(type_cache: &'cache TypeCache, type_: Self) -> &'cache Self::CacheType;
}

macro_rules! impl_cache_getter {
    ($cache_type: ty, $inner_type: ident, $member_name: ident) => {
        impl<'a> CacheGetter for $inner_type<'a> {
            type CacheType = $cache_type;
            fn get_cache<'cache>(type_cache: &'cache TypeCache, type_: $inner_type<'a>) -> &'cache Self::CacheType {
                let as_u16 = type_.vertex().type_id_().as_u16();
                type_cache.$member_name[as_u16 as usize].as_ref().unwrap()
            }
        }
    };
}

impl_cache_getter!(EntityTypeCache, EntityType, entity_types);
impl_cache_getter!(AttributeTypeCache, AttributeType, attribute_types);
impl_cache_getter!(RelationTypeCache, RelationType, relation_types);
impl_cache_getter!(RoleTypeCache, RoleType, role_types);

pub trait HasCommonTypeCache<T: KindAPI<'static>> {
    fn common_type_cache(&self) -> &CommonTypeCache<T>;
}

macro_rules! impl_has_common_type_cache {
    ($cache_type: ty, $inner_type: ty) => {
        impl HasCommonTypeCache<$inner_type> for $cache_type {
            fn common_type_cache(&self) -> &CommonTypeCache<$inner_type> {
                &self.common_type_cache
            }
        }
    };
}
impl_has_common_type_cache!(EntityTypeCache, EntityType<'static>);
impl_has_common_type_cache!(AttributeTypeCache, AttributeType<'static>);
impl_has_common_type_cache!(RelationTypeCache, RelationType<'static>);
impl_has_common_type_cache!(RoleTypeCache, RoleType<'static>);

pub trait HasOwnsPlaysCache {
    fn owns_plays_cache(&self) -> &OwnsPlaysCache;
}
macro_rules! impl_has_owns_plays_cache {
    ($cache_type: ty, $inner_type: ty) => {
        impl HasOwnsPlaysCache for $cache_type {
            fn owns_plays_cache(&self) -> &OwnsPlaysCache {
                &self.owns_plays_cache
            }
        }
    };
}
impl_has_owns_plays_cache!(EntityTypeCache, EntityType<'static>);
impl_has_owns_plays_cache!(RelationTypeCache, RelationType<'static>);


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
