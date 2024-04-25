/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fmt,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use durability::SequenceNumber;
use encoding::{
    graph::{
        type_::{
            edge::{
                build_edge_owns_prefix_prefix, build_edge_plays_prefix_prefix, build_edge_relates_prefix_prefix,
                build_edge_relates_reverse_prefix_prefix, build_edge_sub_prefix_prefix, new_edge_owns, new_edge_plays,
                new_edge_relates, new_edge_relates_reverse, new_edge_sub,
            },
            Kind,
            property::{build_property_type_label, build_property_type_value_type, TypeVertexProperty},
            vertex::{
                build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
                build_vertex_role_type_prefix, new_vertex_attribute_type, new_vertex_entity_type,
                new_vertex_relation_type, new_vertex_role_type, TypeVertex,
            },
        },
        Typed,
    },
    layout::{infix::Infix, prefix::Prefix},
    Prefixed,
    value::{
        label::Label,
        string::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
};
use encoding::graph::type_::edge::TypeEdge;
use encoding::graph::type_::property::{build_property_type_edge_ordering, build_property_type_ordering, TypeEdgeProperty};
use resource::constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_VALUE_INLINE};
use storage::{MVCCStorage, ReadSnapshotOpenError, snapshot::ReadableSnapshot};
use storage::key_range::KeyRange;

use crate::type_::{annotation::{Annotation, AnnotationAbstract, AnnotationDistinct, AnnotationIndependent}, attribute_type::{AttributeType, AttributeTypeAnnotation}, deserialise_annotation_cardinality, deserialise_ordering, entity_type::{EntityType, EntityTypeAnnotation}, IntoCanonicalTypeEdge, object_type::ObjectType, Ordering, owns::Owns, plays::Plays, relates::Relates, relation_type::{RelationType, RelationTypeAnnotation}, role_type::{RoleType, RoleTypeAnnotation}, TypeAPI};
use crate::type_::owns::OwnsAnnotation;

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
struct EntityTypeCache {
    type_: EntityType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations_declared: HashSet<EntityTypeAnnotation>,

    // TODO: Should these all be sets instead of vec?
    supertype: Option<EntityType<'static>>,
    supertypes: Vec<EntityType<'static>>, // TODO: use smallvec if we want to have some inline - benchmark.

    // subtypes_declared: Vec<AttributeType<'static>>, // TODO: benchmark smallvec.
    // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    owns_declared: HashSet<Owns<'static>>,

    plays_declared: HashSet<Plays<'static>>,
    // ...
}

#[derive(Debug)]
struct RelationTypeCache {
    type_: RelationType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations_declared: HashSet<RelationTypeAnnotation>,

    supertype: Option<RelationType<'static>>,
    supertypes: Vec<RelationType<'static>>, // TODO: benchmark smallvec

    // subtypes_declared: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    // subtypes: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    relates_declared: HashSet<Relates<'static>>,
    owns_declared: HashSet<Owns<'static>>,

    plays_declared: HashSet<Plays<'static>>,
}

#[derive(Debug)]
struct RoleTypeCache {
    type_: RoleType<'static>,
    label: Label<'static>,
    is_root: bool,
    ordering: Ordering,
    annotations_declared: HashSet<RoleTypeAnnotation>,
    relates_declared: Relates<'static>,

    supertype: Option<RoleType<'static>>,
    supertypes: Vec<RoleType<'static>>, // TODO: benchmark smallvec

    // subtypes_declared: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    // subtypes: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
}

#[derive(Debug)]
struct AttributeTypeCache {
    type_: AttributeType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations_declared: HashSet<AttributeTypeAnnotation>,
    value_type: Option<ValueType>,

    supertype: Option<AttributeType<'static>>,
    supertypes: Vec<AttributeType<'static>>, // TODO: benchmark smallvec

    // subtypes_declared: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    // subtypes: Vec<AttributeType<'static>>, // TODO: benchmark smallvec

    // owners: HashSet<Owns<'static>>
}

#[derive(Debug)]
struct OwnsCache {
    ordering: Ordering,
    annotations_declared: HashSet<OwnsAnnotation>,
}

impl TypeCache {
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, TypeCacheCreateError> {
        use TypeCacheCreateError::SnapshotOpen;
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot =
            storage.open_snapshot_read_at(open_sequence_number).map_err(|error| SnapshotOpen { source: error })?;
        let vertex_properties = snapshot
            .iterate_range(KeyRange::new_within(TypeVertexProperty::build_prefix(), TypeVertexProperty::FIXED_WIDTH_ENCODING))
            .collect_cloned_bmap(|key, value| {
                (TypeVertexProperty::new(Bytes::Array(ByteArray::from(key.byte_ref()))), ByteArray::from(value))
            })
            .unwrap();

        let entity_type_caches = Self::create_entity_caches(&snapshot, &vertex_properties);
        let entity_type_index_labels = entity_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let relation_type_caches = Self::create_relation_caches(&snapshot, &vertex_properties);
        let relation_type_index_labels = relation_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let role_type_caches = Self::create_role_caches(&snapshot, &vertex_properties);
        let role_type_index_labels = role_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let attribute_type_caches = Self::create_attribute_caches(&snapshot, &vertex_properties);
        let attribute_type_index_labels = attribute_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let edge_properties = snapshot
            .iterate_range(KeyRange::new_within(TypeEdgeProperty::build_prefix(), TypeEdgeProperty::FIXED_WIDTH_ENCODING))
            .collect_cloned_bmap(|key, value| {
                (TypeEdgeProperty::new(Bytes::Array(ByteArray::from(key.byte_ref()))), ByteArray::from(value))
            })
            .unwrap();

        Ok(TypeCache {
            open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            role_types: role_type_caches,
            attribute_types: attribute_type_caches,
            owns: Self::create_owns_caches(&snapshot, &edge_properties),

            entity_types_index_label: entity_type_index_labels,
            relation_types_index_label: relation_type_index_labels,
            role_types_index_label: role_type_index_labels,
            attribute_types_index_label: attribute_type_index_labels,
        })
    }

    fn create_entity_caches(
        snapshot: &impl ReadableSnapshot,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<EntityTypeCache>]> {
        let entities = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_entity_type_prefix(), Prefix::VertexEntityType.fixed_width_keys()))
            .collect_cloned_hashset(|key, _| {
                EntityType::new(new_vertex_entity_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_entity_id = entities.iter().map(|e| e.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_entity_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        let supertypes = Self::fetch_supertypes(snapshot, Prefix::VertexEntityType, EntityType::new);
        let owns = Self::fetch_owns(snapshot, Prefix::VertexEntityType, |v| ObjectType::Entity(EntityType::new(v)));
        let plays = Self::fetch_plays(snapshot, Prefix::VertexEntityType, |v| ObjectType::Entity(EntityType::new(v)));
        for entity in entities.into_iter() {
            let object = ObjectType::Entity(entity.clone());
            let label = Self::read_type_label(vertex_properties, entity.vertex().into_owned());
            let is_root = label == Kind::Entity.root_label();
            let cache = EntityTypeCache {
                type_: entity.clone(),
                label,
                is_root,
                annotations_declared: Self::read_entity_annotations(vertex_properties, entity.clone()),
                supertype: supertypes.get(&entity).cloned(),
                supertypes: Vec::new(),
                owns_declared: owns.iter().filter(|owns| owns.owner() == object).cloned().collect(),
                plays_declared: plays.iter().filter(|plays| plays.player() == object).cloned().collect(),
            };
            caches[entity.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        Self::set_entity_supertypes_transitive(&mut caches);
        caches
    }

    fn read_entity_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        entity_type: EntityType<'static>,
    ) -> HashSet<EntityTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_vertex_annotations(vertex_properties, entity_type.into_vertex()).into_iter() {
            annotations.insert(EntityTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn set_entity_supertypes_transitive(entity_type_caches: &mut [Option<EntityTypeCache>]) {
        for index in 0..entity_type_caches.len() {
            if entity_type_caches[index].is_none() {
                continue;
            }
            let mut supertype = entity_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache =
                    Self::get_entity_type_cache(entity_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().cloned();
                entity_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_relation_caches(
        snapshot: &impl ReadableSnapshot,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<RelationTypeCache>]> {
        let relations = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_relation_type_prefix(), Prefix::VertexRelationType.fixed_width_keys()))
            .collect_cloned_hashset(|key, _| {
                RelationType::new(new_vertex_relation_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_relation_id = relations.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_relation_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        let supertypes = Self::fetch_supertypes(snapshot, Prefix::VertexRelationType, RelationType::new);
        let relates = snapshot
            .iterate_range(KeyRange::new_within(build_edge_relates_prefix_prefix(Prefix::VertexRelationType), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|k, _| {
                let edge = new_edge_relates(Bytes::Reference(k.byte_ref()));
                (RelationType::new(edge.from().into_owned()), RoleType::new(edge.to().into_owned()))
            })
            .unwrap();
        let owns =
            Self::fetch_owns(snapshot, Prefix::VertexRelationType, |v| ObjectType::Relation(RelationType::new(v)));
        let plays =
            Self::fetch_plays(snapshot, Prefix::VertexRelationType, |v| ObjectType::Relation(RelationType::new(v)));
        for relation in relations.into_iter() {
            let object = ObjectType::Relation(relation.clone());
            let label = Self::read_type_label(vertex_properties, relation.vertex().into_owned());
            let is_root = label == Kind::Relation.root_label();
            let relates_declared: HashSet<Relates<'static>> = relates
                .iter()
                .filter(|(rel, _)| rel == &relation)
                .map(|(relation, role)| Relates::new(relation.clone(), role.clone()))
                .collect();

            let cache = RelationTypeCache {
                type_: relation.clone(),
                label,
                is_root,
                annotations_declared: Self::read_relation_annotations(vertex_properties, relation.clone()),
                supertype: supertypes.get(&relation).cloned(),
                supertypes: Vec::new(),
                relates_declared: relates_declared,
                owns_declared: owns.iter().filter(|owns| owns.owner() == object).cloned().collect(),
                plays_declared: plays.iter().filter(|plays| plays.player() == object).cloned().collect(),
            };
            caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        Self::set_relation_supertypes_transitive(&mut caches);
        caches
    }

    fn read_relation_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        relation_type: RelationType<'static>,
    ) -> HashSet<RelationTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_vertex_annotations(vertex_properties, relation_type.into_vertex()).into_iter() {
            annotations.insert(RelationTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn set_relation_supertypes_transitive(relation_type_caches: &mut Box<[Option<RelationTypeCache>]>) {
        for index in 0..relation_type_caches.len() {
            if relation_type_caches[index].is_none() {
                continue;
            }
            let mut supertype = relation_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache =
                    Self::get_relation_type_cache(relation_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().cloned();
                relation_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_role_caches(
        snapshot: &impl ReadableSnapshot,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<RoleTypeCache>]> {
        let roles = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_role_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                RoleType::new(new_vertex_role_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_role_id = roles.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_role_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        let supertypes = Self::fetch_supertypes(snapshot, Prefix::VertexRoleType, RoleType::new);
        let relates = snapshot
            .iterate_range(KeyRange::new_within(build_edge_relates_reverse_prefix_prefix(Prefix::VertexRoleType), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|k, _| {
                let edge = new_edge_relates_reverse(Bytes::Reference(k.byte_ref()));
                Relates::new(RelationType::new(edge.to().into_owned()), RoleType::new(edge.from().into_owned()))
            })
            .unwrap();
        for role in roles.into_iter() {
            let label = Self::read_type_label(vertex_properties, role.vertex().into_owned());
            let is_root = label == Kind::Role.root_label();
            let ordering = Self::read_role_ordering(vertex_properties, role.clone());
            let annotations = Self::read_role_annotations(vertex_properties, role.clone());
            let cache = RoleTypeCache {
                type_: role.clone(),
                label,
                is_root,
                ordering,
                annotations_declared: annotations,
                relates_declared: relates.iter().find(|relates| relates.role() == role).unwrap().clone(),
                supertype: supertypes.get(&role).cloned(),
                supertypes: Vec::new(),
            };
            caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        Self::set_role_supertypes_transitive(&mut caches);
        caches
    }

    fn read_role_ordering(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        role_type: RoleType<'_>,
    ) -> Ordering {
        vertex_properties
            .get(&build_property_type_ordering(role_type.into_vertex()))
            .map(|bytes| {
                deserialise_ordering(ByteReference::from(bytes))
            })
            .unwrap()
    }

    fn read_role_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        role_type: RoleType<'static>,
    ) -> HashSet<RoleTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_vertex_annotations(vertex_properties, role_type.into_vertex()).into_iter() {
            annotations.insert(RoleTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn set_role_supertypes_transitive(role_type_caches: &mut Box<[Option<RoleTypeCache>]>) {
        for index in 0..role_type_caches.len() {
            if role_type_caches[index].is_none() {
                continue;
            }
            let mut supertype = role_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache =
                    Self::get_role_type_cache(role_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().cloned();
                role_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_attribute_caches(
        snapshot: &impl ReadableSnapshot,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<AttributeTypeCache>]> {
        let attributes = snapshot
            .iterate_range(KeyRange::new_within(build_vertex_attribute_type_prefix(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                AttributeType::new(new_vertex_attribute_type(Bytes::Reference(key.byte_ref())).into_owned())
            })
            .unwrap();
        let max_attribute_id = attributes.iter().map(|a| a.vertex().type_id_().as_u16()).max().unwrap();
        let mut caches = (0..=max_attribute_id).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        let supertypes = Self::fetch_supertypes(snapshot, Prefix::VertexAttributeType, AttributeType::new);
        for attribute in attributes {
            let label = Self::read_type_label(vertex_properties, attribute.vertex().into_owned());
            let is_root = label == Kind::Attribute.root_label();
            let cache = AttributeTypeCache {
                type_: attribute.clone(),
                label,
                is_root,
                annotations_declared: Self::read_attribute_annotations(vertex_properties, attribute.clone()),
                value_type: Self::read_value_type(vertex_properties, attribute.vertex().into_owned()),
                supertype: supertypes.get(&attribute).cloned(),
                supertypes: Vec::new(),
            };
            caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        Self::set_attribute_supertypes_transitive(&mut caches);
        caches
    }

    fn read_attribute_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        attribute_type: AttributeType<'static>,
    ) -> HashSet<AttributeTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_vertex_annotations(vertex_properties, attribute_type.into_vertex()).into_iter() {
            annotations.insert(AttributeTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn set_attribute_supertypes_transitive(attribute_type_caches: &mut Box<[Option<AttributeTypeCache>]>) {
        for index in 0..attribute_type_caches.len() {
            if attribute_type_caches[index].is_none() {
                continue;
            }
            let mut supertype = attribute_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache =
                    Self::get_attribute_type_cache(attribute_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().cloned();
                attribute_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_owns_caches(
        snapshot: &impl ReadableSnapshot,
        edge_properties: &BTreeMap<TypeEdgeProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
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
                        ordering: Self::read_edge_ordering(edge_properties, owns.clone().into_type_edge()),
                        annotations_declared: Self::read_edge_annotations(edge_properties, owns.into_type_edge())
                            .into_iter()
                            .map(|annotation| OwnsAnnotation::from(annotation))
                            .collect(),
                    }
                )
            })
            .unwrap()
    }

    fn fetch_owns<F>(snapshot: &impl ReadableSnapshot, prefix: Prefix, from_reader: F) -> Vec<Owns<'static>>
        where
            F: Fn(TypeVertex<'static>) -> ObjectType<'static>,
    {
        snapshot
            .iterate_range(KeyRange::new_within(build_edge_owns_prefix_prefix(prefix), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|key, _| {
                let edge = new_edge_owns(Bytes::Reference(key.byte_ref()));
                Owns::new(from_reader(edge.from().into_owned()), AttributeType::new(edge.to().into_owned()))
            })
            .unwrap()
    }

    fn fetch_plays<F>(snapshot: &impl ReadableSnapshot, prefix: Prefix, from_constructor: F) -> Vec<Plays<'static>>
        where
            F: Fn(TypeVertex<'static>) -> ObjectType<'static>,
    {
        snapshot
            .iterate_range(KeyRange::new_within(build_edge_plays_prefix_prefix(prefix), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|key, _| {
                let edge = new_edge_plays(Bytes::Reference(key.byte_ref()));
                Plays::new(from_constructor(edge.from().into_owned()), RoleType::new(edge.to().into_owned()))
            })
            .unwrap()
    }

    fn fetch_supertypes<F, T: Ord>(
        snapshot: &impl ReadableSnapshot,
        prefix: Prefix,
        type_constructor: F,
    ) -> BTreeMap<T, T>
        where
            F: Fn(TypeVertex<'static>) -> T,
    {
        snapshot
            .iterate_range(KeyRange::new_within(build_edge_sub_prefix_prefix(prefix), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_bmap(|key, _| {
                let edge = new_edge_sub(Bytes::Reference(key.byte_ref()));
                (type_constructor(edge.from().into_owned()), type_constructor(edge.to().into_owned()))
            })
            .unwrap()
    }

    fn read_type_label(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Label<'static> {
        vertex_properties
            .get(&build_property_type_label(type_vertex))
            .map(|bytes| {
                Label::parse_from(StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(
                    ByteReference::from(bytes),
                )))
            })
            .unwrap()
    }

    fn read_value_type(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Option<ValueType> {
        vertex_properties
            .get(&build_property_type_value_type(type_vertex))
            .map(|bytes| ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap())))
    }

    fn read_vertex_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<Annotation> {
        vertex_properties
            .iter()
            .filter_map(|(property, value)| {
                if property.type_vertex() != type_vertex {
                    None
                } else {
                    // WARNING: do _not_ remove the explicit enumeration, as this will help us catch when future annotations are added
                    match property.infix() {
                        Infix::PropertyAnnotationAbstract => Some(Annotation::Abstract(AnnotationAbstract::new())),
                        Infix::PropertyAnnotationDistinct => Some(Annotation::Distinct(AnnotationDistinct::new())),
                        Infix::PropertyAnnotationIndependent => {
                            Some(Annotation::Independent(AnnotationIndependent::new()))
                        }
                        Infix::PropertyAnnotationCardinality => {
                            Some(Annotation::Cardinality(deserialise_annotation_cardinality(ByteReference::from(value))))
                        }
                        Infix::PropertyLabel
                        | Infix::PropertyOrdering
                        | Infix::PropertyValueType
                        | Infix::PropertyHasOrder
                        | Infix::PropertyRolePlayerOrder => None,
                        Infix::_PropertyAnnotationLast => {
                            unreachable!("Received unexpected marker annotation")
                        }
                    }
                }
            })
            .collect()
    }

    fn read_edge_ordering(
        edge_properties: &BTreeMap<TypeEdgeProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_edge: TypeEdge<'static>,
    ) -> Ordering {
        let ordering = edge_properties.get(&build_property_type_edge_ordering(type_edge)).unwrap();
        deserialise_ordering(ByteReference::from(ordering))
    }

    fn read_edge_annotations(
        edge_properties: &BTreeMap<TypeEdgeProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_edge: TypeEdge<'static>,
    ) -> HashSet<Annotation> {
        edge_properties
            .iter()
            .filter_map(|(property, value)| {
                if property.type_edge() != type_edge {
                    None
                } else {
                    // WARNING: do _not_ remove the explicit enumeration, as this will help us catch when future annotations are added
                    match property.infix() {
                        Infix::PropertyAnnotationAbstract => Some(Annotation::Abstract(AnnotationAbstract::new())),
                        Infix::PropertyAnnotationDistinct => Some(Annotation::Distinct(AnnotationDistinct::new())),
                        Infix::PropertyAnnotationIndependent => {
                            Some(Annotation::Independent(AnnotationIndependent::new()))
                        }
                        Infix::PropertyAnnotationCardinality => {
                            Some(Annotation::Cardinality(deserialise_annotation_cardinality(ByteReference::from(value))))
                        }
                        Infix::PropertyLabel
                        | Infix::PropertyOrdering
                        | Infix::PropertyValueType
                        | Infix::PropertyHasOrder
                        | Infix::PropertyRolePlayerOrder => None,
                        Infix::_PropertyAnnotationLast => {
                            unreachable!("Received unexpected marker annotation")
                        }
                    }
                }
            })
            .collect()
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
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_relation_type_supertype(
        &self,
        relation_type: RelationType<'static>,
    ) -> Option<RelationType<'static>> {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_role_type_supertype(&self, role_type: RoleType<'static>) -> Option<RoleType<'static>> {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_attribute_type_supertype(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> Option<AttributeType<'static>> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_entity_type_supertypes(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_relation_type_supertypes(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_role_type_supertypes(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_attribute_type_supertypes(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_entity_type_label(&self, entity_type: EntityType<'static>) -> &Label<'static> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_relation_type_label(&self, relation_type: RelationType<'static>) -> &Label<'static> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_role_type_label(&self, role_type: RoleType<'static>) -> &Label<'static> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_attribute_type_label(&self, attribute_type: AttributeType<'static>) -> &Label<'static> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_entity_type_is_root(&self, entity_type: EntityType<'static>) -> bool {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_relation_type_is_root(&self, relation_type: RelationType<'static>) -> bool {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_role_type_is_root(&self, role_type: RoleType<'static>) -> bool {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_attribute_type_is_root(&self, attribute_type: AttributeType<'static>) -> bool {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().is_root
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
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().annotations_declared
    }

    pub(crate) fn get_relation_type_annotations(
        &self,
        relation_type: RelationType<'static>,
    ) -> &HashSet<RelationTypeAnnotation> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().annotations_declared
    }

    pub(crate) fn get_role_type_annotations(&self, role_type: RoleType<'static>) -> &HashSet<RoleTypeAnnotation> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().annotations_declared
    }

    pub(crate) fn get_attribute_type_annotations(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &HashSet<AttributeTypeAnnotation> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().annotations_declared
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
