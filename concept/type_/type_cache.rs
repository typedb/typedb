/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::{BTreeMap, Bound, HashMap, HashSet};

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use durability::SequenceNumber;
use encoding::{
    graph::{
        type_::{
            edge::{
                build_edge_owns_prefix, build_edge_relates_prefix, build_edge_relates_reverse_prefix,
                build_edge_sub_prefix, new_edge_owns, new_edge_relates, new_edge_relates_reverse, new_edge_sub,
            },
            property::{build_property_type_label, build_property_type_value_type, TypeVertexProperty},
            vertex::{
                build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
                build_vertex_role_type_prefix, is_vertex_attribute_type, is_vertex_entity_type,
                is_vertex_relation_type, is_vertex_role_type, new_vertex_attribute_type, new_vertex_entity_type,
                new_vertex_relation_type, new_vertex_role_type, TypeVertex,
            },
            Root,
        },
        Typed,
    },
    layout::{infix::InfixType, prefix::PrefixType},
    value::{
        label::Label,
        string::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
    Prefixed,
};
use encoding::graph::type_::edge::{build_edge_plays_prefix, new_edge_plays};
use primitive::prefix_range::PrefixRange;
use resource::constants::{
    encoding::LABEL_SCOPED_NAME_STRING_INLINE,
    snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
};
use storage::{snapshot::snapshot::ReadSnapshot, MVCCStorage};

use crate::type_::{annotation::AnnotationAbstract, attribute_type::{AttributeType, AttributeTypeAnnotation}, AttributeTypeAPI, entity_type::{EntityType, EntityTypeAnnotation}, EntityTypeAPI, object_type::ObjectType, owns::Owns, relation_type::{RelationType, RelationTypeAnnotation}, RelationTypeAPI, RoleTypeAPI, TypeAPI};
use crate::type_::annotation::Annotation;
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::role_type::{RoleType, RoleTypeAnnotation};

// TODO: could/should we slab allocate the schema cache?
pub struct TypeCache {
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityTypeCache>]>,
    relation_types: Box<[Option<RelationTypeCache>]>,
    role_types: Box<[Option<RoleTypeCache>]>,
    attribute_types: Box<[Option<AttributeTypeCache>]>,

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
    annotations: HashSet<EntityTypeAnnotation>,

    // TODO: Should these all be sets instead of vec?
    supertype: Option<EntityType<'static>>,
    supertypes: Vec<EntityType<'static>>, // TODO: use smallvec if we want to have some inline - benchmark.

    // subtypes_direct: Vec<AttributeType<'static>>, // TODO: benchmark smallvec.
    // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    owns_direct: HashSet<Owns<'static>>,

    plays_direct: HashSet<Plays<'static>>,

    // ...
}

#[derive(Debug)]
struct RelationTypeCache {
    type_: RelationType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations: HashSet<RelationTypeAnnotation>,

    supertype: Option<RelationType<'static>>,
    supertypes: Vec<RelationType<'static>>, // TODO: benchmark smallvec

    // subtypes_direct: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec

    relates_direct: HashSet<Relates<'static>>,
    owns_direct: HashSet<Owns<'static>>,

    plays_direct: HashSet<Plays<'static>>,
}

#[derive(Debug)]
struct RoleTypeCache {
    type_: RoleType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations: HashSet<RoleTypeAnnotation>,
    relates: Relates<'static>,

    supertype: Option<RoleType<'static>>,
    supertypes: Vec<RoleType<'static>>, // TODO: benchmark smallvec

                                        // subtypes_direct: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
                                        // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
}

#[derive(Debug)]
struct AttributeTypeCache {
    type_: AttributeType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations: HashSet<AttributeTypeAnnotation>,
    value_type: Option<ValueType>,

    supertype: Option<AttributeType<'static>>,
    supertypes: Vec<AttributeType<'static>>, // TODO: benchmark smallvec

                                             // subtypes_direct: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
                                             // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
}

impl TypeCache {
    pub fn new<D>(storage: &MVCCStorage<D>, open_sequence_number: SequenceNumber) -> Self {
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot = storage.open_snapshot_read_at(open_sequence_number);
        let vertex_properties = snapshot
            .iterate_range(PrefixRange::new_within(TypeVertexProperty::build_prefix()))
            .collect_cloned_bmap(|key, value| {
                (
                    TypeVertexProperty::new(Bytes::Array(ByteArray::from(key.byte_ref()))),
                    ByteArray::from(value),
                )
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

        let attribute_type_caches = Self::create_attribute_caches(snapshot, &vertex_properties);
        let attribute_type_index_labels = attribute_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        TypeCache {
            open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            role_types: role_type_caches,
            attribute_types: attribute_type_caches,

            entity_types_index_label: entity_type_index_labels,
            relation_types_index_label: relation_type_index_labels,
            role_types_index_label: role_type_index_labels,
            attribute_types_index_label: attribute_type_index_labels,
        }
    }

    fn create_entity_caches<D>(
        snapshot: &ReadSnapshot<'_, D>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<EntityTypeCache>]> {
        let entity_data = snapshot
            .iterate_range(PrefixRange::new_within(build_vertex_entity_type_prefix()))
            .collect_cloned_bmap(|key, value| (ByteArray::from(key.byte_ref()), ByteArray::from(value)))
            .unwrap();
        let max_entity_id = entity_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_entity_type(Bytes::Reference(ByteReference::from(key))) {
                    Some(new_vertex_entity_type(Bytes::Reference(ByteReference::from(key))).type_id().as_u16())
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        Self::build_entity_caches(&entity_data, vertex_properties, max_entity_id)
    }

    fn build_entity_caches(
        entity_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_entity: u16,
    ) -> Box<[Option<EntityTypeCache>]> {
        let mut caches: Box<[Option<EntityTypeCache>]> =
            (0..=max_entity).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in entity_data.iter() {
            if is_vertex_entity_type(Bytes::Reference(ByteReference::from(key))) {
                let entity_type: EntityType<'static> =
                    EntityType::new(new_vertex_entity_type(Bytes::Array(key.clone())));
                let type_index = Typed::type_id(entity_type.vertex()).as_u16();

                let label = Self::read_type_label(vertex_properties, entity_type.vertex().clone());
                let is_root = label == Root::Entity.label();
                let supertype =
                    Self::read_supertype_vertex(entity_data, entity_type.vertex().clone()).map(EntityType::new);
                let annotations = Self::read_entity_annotations(vertex_properties, entity_type.clone());
                let owns_direct = Self::read_owns_attribute_vertexes(entity_data, entity_type.vertex().clone())
                    .into_iter()
                    .map(|v| Owns::new(ObjectType::Entity(entity_type.clone()), AttributeType::new(v)))
                    .collect();
                let plays_direct = Self::read_plays_role_vertexes(entity_data, entity_type.vertex().clone())
                    .into_iter()
                    .map(|v| Plays::new(ObjectType::Entity(entity_type.clone()), RoleType::new(v)))
                    .collect();
                let cache = EntityTypeCache {
                    type_: entity_type,
                    label,
                    is_root,
                    annotations,
                    supertype,
                    supertypes: Vec::new(),
                    owns_direct,
                    plays_direct,
                };
                let i = type_index as usize;
                caches[i] = Some(cache);
            }
        }
        Self::set_entity_supertypes_transitive(&mut caches);
        caches
    }

    fn read_entity_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        entity_type: EntityType<'static>,
    ) -> HashSet<EntityTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_annotations(vertex_properties, entity_type.into_vertex()).into_iter() {
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

    fn create_relation_caches<D>(
        snapshot: &ReadSnapshot<'_, D>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<RelationTypeCache>]> {
        let relation_data = snapshot
            .iterate_range(PrefixRange::new_within(build_vertex_relation_type_prefix()))
            .collect_cloned_bmap(|key, value| (ByteArray::from(key.byte_ref()), ByteArray::from(value)))
            .unwrap();
        let max_relation_id = relation_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_relation_type(Bytes::Reference(ByteReference::from(key))) {
                    Some(
                        new_vertex_relation_type(Bytes::Reference(ByteReference::from(key)))
                            .type_id()
                            .as_u16(),
                    )
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        Self::build_relation_caches(&relation_data, vertex_properties, max_relation_id)
    }

    fn build_relation_caches(
        relation_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_relation: u16,
    ) -> Box<[Option<RelationTypeCache>]> {
        let mut caches: Box<[Option<RelationTypeCache>]> =
            (0..=max_relation).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in relation_data.iter() {
            if is_vertex_relation_type(Bytes::Reference(ByteReference::from(key))) {
                let relation_type = RelationType::new(new_vertex_relation_type(Bytes::Array(key.clone())));
                let type_index = Typed::type_id(relation_type.vertex()).as_u16();

                let label = Self::read_type_label(vertex_properties, relation_type.vertex().clone());
                let is_root = label == Root::Relation.label();
                let supertype =
                    Self::read_supertype_vertex(relation_data, relation_type.vertex().clone()).map(RelationType::new);
                let annotations = Self::read_relation_annotations(vertex_properties, relation_type.clone());
                let relates_direct =
                    Self::read_relates_vertexes(relation_data, relation_type.vertex().clone())
                        .into_iter()
                        .map(|v| Relates::new(relation_type.clone(), RoleType::new(v)))
                        .collect();let owns_direct = Self::read_owns_attribute_vertexes(relation_data, relation_type.vertex().clone())
                    .into_iter()
                    .map(|v| Owns::new(ObjectType::Relation(relation_type.clone()), AttributeType::new(v)))
                    .collect();
                let plays_direct = Self::read_plays_role_vertexes(relation_data, relation_type.clone().into_vertex())
                    .into_iter()
                    .map(|v| Plays::new(ObjectType::Relation(relation_type.clone()), RoleType::new(v)))
                    .collect();
                let cache = RelationTypeCache {
                    type_: relation_type,
                    label,
                    is_root,
                    annotations,
                    supertype,
                    supertypes: Vec::new(),
                    relates_direct,
                    owns_direct,
                    plays_direct,
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_relation_supertypes_transitive(&mut caches);
        caches
    }

    fn read_relation_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        relation_type: RelationType<'static>,
    ) -> HashSet<RelationTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_annotations(vertex_properties, relation_type.into_vertex()).into_iter() {
            annotations.insert(RelationTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn read_relates_vertexes(
        types_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<TypeVertex<'static>> {
        let edge_prefix = build_edge_relates_prefix(type_vertex).into_owned_array();
        types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()))
            .map(|(key, _)| new_edge_relates(Bytes::Reference(ByteReference::from(key))).to().into_owned())
            .collect()
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

    fn create_role_caches<D>(
        snapshot: &ReadSnapshot<'_, D>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<RoleTypeCache>]> {
        let role_data = snapshot
            .iterate_range(PrefixRange::new_within(build_vertex_role_type_prefix()))
            .collect_cloned_bmap(|key, value| (ByteArray::from(key.byte_ref()), ByteArray::from(value)))
            .unwrap();
        let max_role_id = role_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_role_type(Bytes::Reference(ByteReference::from(key))) {
                    Some(new_vertex_role_type(Bytes::Reference(ByteReference::from(key))).type_id().as_u16())
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        Self::build_role_caches(&role_data, vertex_properties, max_role_id)
    }

    fn build_role_caches(
        role_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_role: u16,
    ) -> Box<[Option<RoleTypeCache>]> {
        let mut caches: Box<[Option<RoleTypeCache>]> =
            (0..=max_role).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in role_data.iter() {
            if is_vertex_role_type(Bytes::Reference(ByteReference::from(key))) {
                let role_type = RoleType::new(new_vertex_role_type(Bytes::Array(key.clone())));
                let type_index = Typed::type_id(role_type.vertex()).as_u16();

                let label = Self::read_type_label(vertex_properties, role_type.vertex().clone());
                let is_root = label == Root::Role.label();
                let supertype = Self::read_supertype_vertex(role_data, role_type.vertex().clone()).map(RoleType::new);
                let relates = Relates::new(
                    RelationType::new(Self::read_role_relater(role_data, role_type.vertex().clone())),
                    role_type.clone(),
                );
                let annotations = Self::read_role_annotations(vertex_properties, role_type.clone());
                let cache = RoleTypeCache {
                    type_: role_type,
                    label,
                    is_root,
                    annotations,
                    relates,
                    supertype,
                    supertypes: Vec::new(),
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_role_supertypes_transitive(&mut caches);
        caches
    }

    fn read_role_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        role_type: RoleType<'static>,
    ) -> HashSet<RoleTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_annotations(vertex_properties, role_type.into_vertex()).into_iter() {
            annotations.insert(RoleTypeAnnotation::from(annotation));
        }
        annotations
    }

    fn read_role_relater(
        types_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        role_type: TypeVertex<'static>,
    ) -> TypeVertex<'static> {
        let prefix = build_edge_relates_reverse_prefix(role_type);
        let relater: Vec<TypeVertex<'static>> = types_data
            .range::<[u8], _>((Bound::Included(prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(prefix.bytes()))
            .map(|(key, _)| {
                new_edge_relates_reverse(Bytes::Reference(ByteReference::from(key))).to().into_owned()
            })
            .collect();
        debug_assert_eq!(relater.len(), 1);
        relater.into_iter().next().unwrap()
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

    fn create_attribute_caches<D>(
        snapshot: ReadSnapshot<'_, D>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
    ) -> Box<[Option<AttributeTypeCache>]> {
        let attribute_data = snapshot
            .iterate_range(PrefixRange::new_within(build_vertex_attribute_type_prefix()))
            .collect_cloned_bmap(|key, value| (ByteArray::from(key.byte_ref()), ByteArray::from(value)))
            .unwrap();
        let max_attribute_id = attribute_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_attribute_type(Bytes::Reference(ByteReference::from(key))) {
                    Some(
                        new_vertex_attribute_type(Bytes::Reference(ByteReference::from(key)))
                            .type_id()
                            .as_u16(),
                    )
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        Self::build_attribute_caches(&attribute_data, vertex_properties, max_attribute_id)
    }

    fn build_attribute_caches(
        attribute_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_attribute: u16,
    ) -> Box<[Option<AttributeTypeCache>]> {
        let mut caches: Box<[Option<AttributeTypeCache>]> =
            (0..=max_attribute).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in attribute_data.iter() {
            if is_vertex_attribute_type(Bytes::Reference(ByteReference::from(key))) {
                let attribute_type = AttributeType::new(new_vertex_attribute_type(Bytes::Array(key.clone())));
                let type_index = Typed::type_id(attribute_type.vertex()).as_u16();

                let label = Self::read_type_label(vertex_properties, attribute_type.vertex().clone());
                let is_root = label == Root::Attribute.label();
                let annotations = Self::read_attribute_annotations(vertex_properties, attribute_type.clone());
                let value_type = Self::read_value_type(vertex_properties, attribute_type.vertex().clone());
                let supertype = Self::read_supertype_vertex(attribute_data, attribute_type.vertex().clone())
                    .map(AttributeType::new);
                let cache = AttributeTypeCache {
                    type_: attribute_type,
                    label,
                    is_root,
                    annotations,
                    value_type,
                    supertype,
                    supertypes: Vec::new(),
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_attribute_supertypes_transitive(&mut caches);
        caches
    }

    fn read_attribute_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        attribute_type: AttributeType<'static>,
    ) -> HashSet<AttributeTypeAnnotation> {
        let mut annotations = HashSet::new();
        for annotation in Self::read_annotations(vertex_properties, attribute_type.into_vertex()).into_iter() {
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

    fn read_annotations(
        vertex_properties: &BTreeMap<TypeVertexProperty<'_>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<Annotation> {
        vertex_properties
            .iter()
            .filter_map(|(property, _value)| {
                if property.type_vertex() != type_vertex {
                    None
                } else {
                    // WARNING: do _not_ remove the explicit enumeration, as this will help us catch when future annotations are added
                    match property.infix() {
                        InfixType::PropertyAnnotationAbstract => Some(Annotation::Abstract(AnnotationAbstract::new())),
                        | InfixType::EdgeSub
                        | InfixType::EdgeSubReverse
                        | InfixType::EdgeOwns
                        | InfixType::EdgeOwnsReverse
                        | InfixType::EdgePlays
                        | InfixType::EdgePlaysReverse
                        | InfixType::EdgeRelates
                        | InfixType::EdgeRelatesReverse
                        | InfixType::EdgeHas
                        | InfixType::EdgeHasReverse
                        | InfixType::PropertyLabel
                        | InfixType::PropertyValueType => None,
                    }
                }
            })
            .collect()
    }

    fn read_supertype_vertex(
        types_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Option<TypeVertex<'static>> {
        let edge_prefix = build_edge_sub_prefix(type_vertex).into_owned_array();
        let mut edges = types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()));
        let supertype = edges
            .next()
            .map(|(key, _)| new_edge_sub(Bytes::Reference(ByteReference::from(key))).to().into_owned());
        debug_assert!(edges.next().is_none());
        supertype
    }

    fn read_owns_attribute_vertexes(
        types_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<TypeVertex<'static>> {
        let edge_prefix = build_edge_owns_prefix(type_vertex).into_owned_array();
        types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()))
            .map(|(key, _)| new_edge_owns(Bytes::Reference(ByteReference::from(key))).to().into_owned())
            .collect()
    }

    fn read_plays_role_vertexes(
        types_data: &BTreeMap<ByteArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<TypeVertex<'static>> {
        let edge_prefix = build_edge_plays_prefix(type_vertex).into_owned_array();
        types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()))
            .map(|(key, _)| new_edge_plays(Bytes::Reference(ByteReference::from(key))).to().into_owned())
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

    pub(crate) fn get_entity_type_supertype(
        &self,
        entity_type: impl EntityTypeAPI<'static>,
    ) -> Option<EntityType<'static>> {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_relation_type_supertype(
        &self,
        relation_type: impl RelationTypeAPI<'static>,
    ) -> Option<RelationType<'static>> {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_role_type_supertype(&self, role_type: impl RoleTypeAPI<'static>) -> Option<RoleType<'static>> {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_attribute_type_supertype(
        &self,
        attribute_type: impl AttributeTypeAPI<'static>,
    ) -> Option<AttributeType<'static>> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().supertype.clone()
    }

    pub(crate) fn get_entity_type_supertypes<'a>(
        &self,
        entity_type: impl EntityTypeAPI<'a>,
    ) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_relation_type_supertypes(
        &self,
        relation_type: impl RelationTypeAPI<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_role_type_supertypes(&self, role_type: impl RoleTypeAPI<'static>) -> &Vec<RoleType<'static>> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_attribute_type_supertypes(
        &self,
        attribute_type: impl AttributeTypeAPI<'static>,
    ) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_entity_type_label(&self, entity_type: impl EntityTypeAPI<'static>) -> &Label<'static> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_relation_type_label(&self, relation_type: impl RelationTypeAPI<'static>) -> &Label<'static> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_role_type_label(&self, role_type: impl RoleTypeAPI<'static>) -> &Label<'static> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_attribute_type_label(&self, attribute_type: impl AttributeTypeAPI<'static>) -> &Label<'static> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_entity_type_is_root(&self, entity_type: impl EntityTypeAPI<'static>) -> bool {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_relation_type_is_root(&self, relation_type: impl RelationTypeAPI<'static>) -> bool {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_role_type_is_root(&self, role_type: impl RoleTypeAPI<'static>) -> bool {
        Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_attribute_type_is_root(&self, attribute_type: impl AttributeTypeAPI<'static>) -> bool {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_entity_type_owns(&self, entity_type: EntityType<'static>) -> &HashSet<Owns<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().owns_direct
    }

    pub(crate) fn get_relation_type_owns(&self, relation_type: RelationType<'static>) -> &HashSet<Owns<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().owns_direct
    }

    pub(crate) fn get_relation_type_relates(&self, relation_type: RelationType<'static>) -> &HashSet<Relates<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().relates_direct
    }

    pub(crate) fn get_entity_type_plays(&self, entity_type: EntityType<'static>) -> &HashSet<Plays<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().plays_direct
    }

    pub(crate) fn get_relation_type_plays(&self, relation_type: RelationType<'static>) -> &HashSet<Plays<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().plays_direct
    }

    pub(crate) fn get_attribute_type_value_type(&self, attribute_type: AttributeType<'static>) -> Option<ValueType> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().value_type
    }

    pub(crate) fn get_entity_type_annotations(
        &self,
        entity_type: impl EntityTypeAPI<'static>,
    ) -> &HashSet<EntityTypeAnnotation> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().annotations
    }

    pub(crate) fn get_relation_type_annotations(
        &self,
        relation_type: impl RelationTypeAPI<'static>,
    ) -> &HashSet<RelationTypeAnnotation> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().annotations
    }

    pub(crate) fn get_role_type_annotations(
        &self,
        role_type: impl RoleTypeAPI<'static>,
    ) -> &HashSet<RoleTypeAnnotation> {
        &Self::get_role_type_cache(&self.role_types, role_type.into_vertex()).unwrap().annotations
    }

    pub(crate) fn get_attribute_type_annotations(
        &self,
        attribute_type: impl AttributeTypeAPI<'static>,
    ) -> &HashSet<AttributeTypeAnnotation> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().annotations
    }

    fn get_entity_type_cache<'c>(
        entity_type_caches: &'c [Option<EntityTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c EntityTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexEntityType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        entity_type_caches[as_u16 as usize].as_ref()
    }

    fn get_relation_type_cache<'c>(
        relation_type_caches: &'c [Option<RelationTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c RelationTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRelationType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        relation_type_caches[as_u16 as usize].as_ref()
    }

    fn get_role_type_cache<'c>(
        role_type_caches: &'c [Option<RoleTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c RoleTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRoleType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        role_type_caches[as_u16 as usize].as_ref()
    }

    fn get_attribute_type_cache<'c>(
        attribute_type_caches: &'c [Option<AttributeTypeCache>],
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c AttributeTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexAttributeType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        attribute_type_caches[as_u16 as usize].as_ref()
    }
}
