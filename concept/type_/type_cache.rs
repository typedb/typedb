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

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use durability::SequenceNumber;
use encoding::{
    graph::{
        type_::{
            edge::{build_edge_owns_prefix, build_edge_sub_prefix, new_edge_owns, new_edge_sub},
            property::{
                build_property_type_annotation_abstract, build_property_type_label, build_property_type_value_type,
                TypeVertexProperty,
            },
            vertex::{
                build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
                is_vertex_attribute_type, is_vertex_entity_type, is_vertex_relation_type, new_vertex_attribute_type,
                new_vertex_entity_type, new_vertex_relation_type, TypeVertex,
            },
            Root,
        },
        Typed,
    },
    layout::prefix::PrefixType,
    property::{
        label::Label,
        string::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
    Keyable, Prefixed,
};
use resource::constants::{
    encoding::LABEL_SCOPED_NAME_STRING_INLINE,
    snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
};
use storage::{key_value::StorageKeyArray, MVCCStorage};

use crate::type_::{
    annotation::AnnotationAbstract,
    attribute_type::{AttributeType, AttributeTypeAnnotation},
    entity_type::{EntityType, EntityTypeAnnotation},
    object_type::ObjectType,
    owns::Owns,
    relation_type::{RelationType, RelationTypeAnnotation},
    AttributeTypeAPI, EntityTypeAPI, RelationTypeAPI, TypeAPI,
};

// TODO: could/should we slab allocate the schema cache?
pub struct TypeCache {
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityTypeCache>]>,
    relation_types: Box<[Option<RelationTypeCache>]>,
    attribute_types: Box<[Option<AttributeTypeCache>]>,

    entity_types_index_label: HashMap<Label<'static>, EntityType<'static>>,
    relation_types_index_label: HashMap<Label<'static>, RelationType<'static>>,
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
    // owns_direct

    // ...
}

struct RelationTypeCache {
    type_: RelationType<'static>,
    label: Label<'static>,
    is_root: bool,
    annotations: HashSet<RelationTypeAnnotation>,

    supertype: Option<RelationType<'static>>,
    supertypes: Vec<RelationType<'static>>, // TODO: benchmark smallvec

    // subtypes_direct: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    // subtypes_transitive: Vec<AttributeType<'static>>, // TODO: benchmark smallvec
    owns_direct: HashSet<Owns<'static>>,
}

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
    pub fn new(storage: &MVCCStorage, open_sequence_number: SequenceNumber) -> Self {
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot = storage.open_snapshot_read_at(open_sequence_number);

        let type_vertex_properties = snapshot
            .iterate_prefix(TypeVertexProperty::build_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>()
            .unwrap();

        let entity_types_data = snapshot
            .iterate_prefix(build_vertex_entity_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>()
            .unwrap();
        let max_entity_id = entity_types_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                    Some(
                        new_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
                            .type_id()
                            .as_u16(),
                    )
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        let entity_type_caches = Self::create_entity_caches(&entity_types_data, &type_vertex_properties, max_entity_id);
        let entity_type_index_labels = entity_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let relation_types_data = snapshot
            .iterate_prefix(build_vertex_relation_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>()
            .unwrap();
        let max_relation_id = relation_types_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                    Some(
                        new_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
                            .type_id()
                            .as_u16(),
                    )
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        let relation_type_caches =
            Self::create_relation_caches(&relation_types_data, &type_vertex_properties, max_relation_id);
        let relation_type_index_labels = relation_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        let attribute_types_data = snapshot
            .iterate_prefix(build_vertex_attribute_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>()
            .unwrap();
        let max_attribute_id = attribute_types_data
            .iter()
            .filter_map(|(key, _)| {
                if is_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                    Some(
                        new_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
                            .type_id()
                            .as_u16(),
                    )
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);
        let attribute_type_caches =
            Self::create_attribute_caches(&attribute_types_data, &type_vertex_properties, max_attribute_id);
        let attribute_type_index_labels = attribute_type_caches
            .iter()
            .filter_map(|entry| entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone())))
            .collect();

        TypeCache {
            open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            attribute_types: attribute_type_caches,

            entity_types_index_label: entity_type_index_labels,
            relation_types_index_label: relation_type_index_labels,
            attribute_types_index_label: attribute_type_index_labels,
        }
    }

    fn create_entity_caches(
        entity_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_entity: u16,
    ) -> Box<[Option<EntityTypeCache>]> {
        let mut caches: Box<[Option<EntityTypeCache>]> =
            (0..=max_entity).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in entity_types_data.iter() {
            if is_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let entity_type: EntityType<'static> =
                    EntityType::new(new_vertex_entity_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(entity_type.vertex()).as_u16();

                let label = Self::read_type_label(type_vertex_properties, entity_type.vertex().clone());
                let is_root = label == Root::Entity.label();
                let supertype =
                    Self::read_supertype_vertex(entity_types_data, entity_type.vertex().clone()).map(EntityType::new);
                let annotations = Self::read_entity_annotations(type_vertex_properties, entity_type.clone());
                let owns_direct = Self::read_owns_attribute_vertexes(entity_types_data, entity_type.vertex().clone())
                    .into_iter()
                    .map(|v| Owns::new(ObjectType::Entity(entity_type.clone()), AttributeType::new(v)))
                    .collect();
                let cache = EntityTypeCache {
                    type_: entity_type,
                    label,
                    is_root,
                    annotations,
                    supertype,
                    supertypes: Vec::new(),
                    owns_direct: owns_direct,
                };
                let i = type_index as usize;
                caches[i] = Some(cache);
            }
        }
        Self::set_entity_supertypes_transitive(&mut caches);
        caches
    }

    fn read_entity_annotations(
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        entity_type: EntityType<'static>,
    ) -> HashSet<EntityTypeAnnotation> {
        let mut annotations = HashSet::new();
        Self::read_annotation_abstract(type_vertex_properties, entity_type.into_vertex())
            .map(|a| annotations.insert(EntityTypeAnnotation::from(a)));
        annotations
    }

    fn set_entity_supertypes_transitive(entity_type_caches: &mut Box<[Option<EntityTypeCache>]>) {
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
        relation_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_relation: u16,
    ) -> Box<[Option<RelationTypeCache>]> {
        let mut caches: Box<[Option<RelationTypeCache>]> =
            (0..=max_relation).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in relation_types_data.iter() {
            if is_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let relation_type =
                    RelationType::new(new_vertex_relation_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(relation_type.vertex()).as_u16();

                let label = Self::read_type_label(type_vertex_properties, relation_type.vertex().clone());
                let is_root = label == Root::Relation.label();
                let supertype = Self::read_supertype_vertex(relation_types_data, relation_type.vertex().clone())
                    .map(RelationType::new);
                let annotations = Self::read_relation_annotations(type_vertex_properties, relation_type.clone());
                let owns_direct =
                    Self::read_owns_attribute_vertexes(relation_types_data, relation_type.vertex().clone())
                        .into_iter()
                        .map(|v| Owns::new(ObjectType::Relation(relation_type.clone()), AttributeType::new(v)))
                        .collect();
                let cache = RelationTypeCache {
                    type_: relation_type,
                    label,
                    is_root,
                    annotations,
                    supertype,
                    supertypes: Vec::new(),
                    owns_direct: owns_direct,
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_relation_supertypes_transitive(&mut caches);
        caches
    }

    fn read_relation_annotations(
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        relation_type: RelationType<'static>,
    ) -> HashSet<RelationTypeAnnotation> {
        let mut annotations = HashSet::new();
        Self::read_annotation_abstract(type_vertex_properties, relation_type.into_vertex())
            .map(|a| annotations.insert(RelationTypeAnnotation::from(a)));
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

    fn create_attribute_caches(
        attribute_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        max_attribute: u16,
    ) -> Box<[Option<AttributeTypeCache>]> {
        let mut caches: Box<[Option<AttributeTypeCache>]> =
            (0..=max_attribute).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in attribute_types_data.iter() {
            if is_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let attribute_type =
                    AttributeType::new(new_vertex_attribute_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(attribute_type.vertex()).as_u16();

                let label = Self::read_type_label(type_vertex_properties, attribute_type.vertex().clone());
                let is_root = label == Root::Attribute.label();
                let annotations = Self::read_attribute_annotations(type_vertex_properties, attribute_type.clone());
                let value_type = Self::read_value_type(type_vertex_properties, attribute_type.vertex().clone());
                let supertype = Self::read_supertype_vertex(attribute_types_data, attribute_type.vertex().clone())
                    .map(|v| AttributeType::new(v));
                let cache = AttributeTypeCache {
                    type_: attribute_type,
                    label: label,
                    is_root: is_root,
                    annotations: annotations,
                    value_type: value_type,
                    supertype: supertype,
                    supertypes: Vec::new(),
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_attribute_supertypes_transitive(&mut caches);
        caches
    }

    fn read_attribute_annotations(
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        attribute_type: AttributeType<'static>,
    ) -> HashSet<AttributeTypeAnnotation> {
        let mut annotations = HashSet::new();
        Self::read_annotation_abstract(type_vertex_properties, attribute_type.into_vertex())
            .map(|a| annotations.insert(AttributeTypeAnnotation::from(a)));
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
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Label<'static> {
        let property = build_property_type_label(type_vertex);
        type_vertex_properties
            .get(&property.into_storage_key().into_owned_array())
            .map(|bytes| {
                Label::parse_from(StringBytes::new(ByteArrayOrRef::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(
                    ByteReference::from(bytes),
                )))
            })
            .unwrap()
    }

    fn read_value_type(
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Option<ValueType> {
        let property = build_property_type_value_type(type_vertex);
        type_vertex_properties
            .get(&property.into_storage_key().into_owned_array())
            .map(|bytes| ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap())))
    }

    fn read_annotation_abstract(
        type_vertex_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Option<AnnotationAbstract> {
        let property = build_property_type_annotation_abstract(type_vertex);
        type_vertex_properties
            .get(&property.into_storage_key().into_owned_array())
            .map(|_bytes| AnnotationAbstract::new())
    }

    fn read_supertype_vertex(
        types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Option<TypeVertex<'static>> {
        let edge_prefix = build_edge_sub_prefix(type_vertex).into_owned_array();
        let mut edges = types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()));
        let supertype = edges.next().map(|(key, _)| {
            new_edge_sub(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))).to().into_owned()
        });
        debug_assert!(edges.next().is_none());
        supertype
    }

    fn read_owns_attribute_vertexes(
        types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
        type_vertex: TypeVertex<'static>,
    ) -> Vec<TypeVertex<'static>> {
        let edge_prefix = build_edge_owns_prefix(type_vertex).into_owned_array();
        types_data
            .range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()))
            .map(|(key, _)| new_edge_owns(ByteArrayOrRef::Array(key.byte_array().clone())).to().into_owned())
            .collect()
    }

    pub(crate) fn get_entity_type(&self, label: &Label<'_>) -> Option<EntityType<'static>> {
        self.entity_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_relation_type(&self, label: &Label<'_>) -> Option<RelationType<'static>> {
        self.relation_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_attribute_type(&self, label: &Label<'_>) -> Option<AttributeType<'static>> {
        self.attribute_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_entity_type_supertype(
        &self,
        entity_type: impl EntityTypeAPI<'static>,
    ) -> Option<EntityType<'static>> {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertype.as_ref().cloned()
    }

    pub(crate) fn get_relation_type_supertype(
        &self,
        relation_type: impl RelationTypeAPI<'static>,
    ) -> Option<RelationType<'static>> {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex())
            .unwrap()
            .supertype
            .as_ref()
            .cloned()
    }

    pub(crate) fn get_attribute_type_supertype(
        &self,
        attribute_type: impl AttributeTypeAPI<'static>,
    ) -> Option<AttributeType<'static>> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex())
            .unwrap()
            .supertype
            .as_ref()
            .cloned()
    }

    pub(crate) fn get_entity_type_supertypes(
        &self,
        entity_type: impl EntityTypeAPI<'static>,
    ) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().supertypes
    }

    pub(crate) fn get_relation_type_supertypes(
        &self,
        relation_type: impl RelationTypeAPI<'static>,
    ) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().supertypes
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

    pub(crate) fn get_attribute_type_label(&self, attribute_type: impl AttributeTypeAPI<'static>) -> &Label<'static> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().label
    }

    pub(crate) fn get_entity_type_is_root(&self, entity_type: impl EntityTypeAPI<'static>) -> bool {
        Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_relation_type_is_root(&self, relation_type: impl RelationTypeAPI<'static>) -> bool {
        Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_attribute_type_is_root(&self, attribute_type: impl AttributeTypeAPI<'static>) -> bool {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().is_root
    }

    pub(crate) fn get_entity_type_owns<'this>(
        &'this self,
        entity_type: EntityType<'static>,
    ) -> &HashSet<Owns<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.into_vertex()).unwrap().owns_direct
    }

    pub(crate) fn get_relation_type_owns<'this>(
        &'this self,
        relation_type: RelationType<'static>,
    ) -> &HashSet<Owns<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.into_vertex()).unwrap().owns_direct
    }

    pub(crate) fn get_attribute_type_value_type(&self, attribute_type: AttributeType<'static>) -> Option<ValueType> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().value_type.clone()
    }

    pub(crate) fn get_entity_type_annotations<'this>(
        &'this self,
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

    pub(crate) fn get_attribute_type_annotations(
        &self,
        attribute_type: impl AttributeTypeAPI<'static>,
    ) -> &HashSet<AttributeTypeAnnotation> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.into_vertex()).unwrap().annotations
    }

    fn get_entity_type_cache<'c>(
        entity_type_caches: &'c Box<[Option<EntityTypeCache>]>,
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c EntityTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexEntityType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        entity_type_caches[as_u16 as usize].as_ref()
    }

    fn get_relation_type_cache<'c>(
        relation_type_caches: &'c Box<[Option<RelationTypeCache>]>,
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c RelationTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRelationType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        relation_type_caches[as_u16 as usize].as_ref()
    }

    fn get_attribute_type_cache<'c>(
        attribute_type_caches: &'c Box<[Option<AttributeTypeCache>]>,
        type_vertex: TypeVertex<'_>,
    ) -> Option<&'c AttributeTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexAttributeType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        attribute_type_caches[as_u16 as usize].as_ref()
    }
}
