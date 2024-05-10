/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use encoding::{
    graph::{
        type_::{
            edge::TypeEdge,
            vertex::{
                build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix,
                build_vertex_role_type_prefix, new_vertex_attribute_type, new_vertex_entity_type,
                new_vertex_relation_type, new_vertex_role_type, TypeVertex,
            },
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::label::Label,
};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::type_::{
    attribute_type::AttributeType,
    entity_type::EntityType,
    object_type::ObjectType,
    owns::{Owns, OwnsAnnotation},
    relation_type::RelationType,
    role_type::RoleType,
    type_cache::{
        selection::HasCommonTypeCache, AttributeTypeCache, CommonTypeCache, EntityTypeCache, OwnerPlayerCache,
        OwnsCache, RelationTypeCache, RoleTypeCache,
    },
    type_manager::{KindAPI, ReadableType, TypeManager},
    type_reader::TypeReader,
    OwnerAPI, PlayerAPI, TypeAPI,
};

pub(super) fn build_label_to_type_index<T: KindAPI<'static>, CACHE: HasCommonTypeCache<T>>(
    type_cache_array: &Box<[Option<CACHE>]>,
) -> HashMap<Label<'static>, T> {
    type_cache_array
        .iter()
        .filter_map(|entry| {
            entry
                .as_ref()
                .map(|cache| (cache.common_type_cache().label.clone(), cache.common_type_cache().type_.clone()))
        })
        .collect()
}

pub(super) fn create_entity_type_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<EntityTypeCache>]> {
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
            common_type_cache: create_common_type_cache(snapshot, entity.clone()),
            owner_player_cache: create_owner_player_cache(snapshot, entity.clone()),
        };
        caches[entity.vertex().type_id_().as_u16() as usize] = Some(cache);
    }
    caches
}
pub(super) fn create_relation_type_cache(snapshot: &impl ReadableSnapshot) -> Box<[Option<RelationTypeCache>]> {
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
            common_type_cache: create_common_type_cache(snapshot, relation.clone()),
            owner_player_cache: create_owner_player_cache(snapshot, relation.clone()),
            relates_declared: TypeReader::get_relates(snapshot, relation.clone()).unwrap(),
        };
        caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
    }
    caches
}

pub(super) fn create_attribute_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<AttributeTypeCache>]> {
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
            common_type_cache: create_common_type_cache(snapshot, attribute.clone()),
            value_type: TypeReader::get_value_type(snapshot, attribute.clone()).unwrap(),
        };
        caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
    }
    caches
}

pub(super) fn create_role_caches(snapshot: &impl ReadableSnapshot) -> Box<[Option<RoleTypeCache>]> {
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
            common_type_cache: create_common_type_cache(snapshot, role.clone()),
            ordering,
            relates_declared: TypeReader::get_relations(snapshot, role.clone()).unwrap(),
        };
        caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
    }
    caches
}

pub(super) fn create_owns_caches(snapshot: &impl ReadableSnapshot) -> HashMap<Owns<'static>, OwnsCache> {
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

fn create_common_type_cache<'a, Snapshot, T>(snapshot: &Snapshot, type_: T) -> CommonTypeCache<T>
where
    Snapshot: ReadableSnapshot,
    T: KindAPI<'static> + ReadableType<Output<'static> = T>,
{
    let label = TypeReader::get_label(snapshot, type_.clone()).unwrap().unwrap();
    let is_root = TypeManager::<Snapshot>::check_type_is_root(&label, T::ROOT_KIND);
    let annotations_declared = TypeReader::get_type_annotations(snapshot, type_.clone()).unwrap();
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

fn create_owner_player_cache<'a, Snapshot, T>(snapshot: &Snapshot, type_: T) -> OwnerPlayerCache
where
    Snapshot: ReadableSnapshot,
    T: KindAPI<'static> + OwnerAPI<'static> + PlayerAPI<'static> + ReadableType<Output<'static> = T>,
{
    OwnerPlayerCache {
        owns_declared: TypeReader::get_owns(snapshot, type_.clone()).unwrap(),
        plays_declared: TypeReader::get_plays(snapshot, type_.clone()).unwrap(),
    }
}
