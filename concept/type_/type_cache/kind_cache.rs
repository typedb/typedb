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
    value::{label::Label, value_type::ValueType},
};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::type_::{
    attribute_type::AttributeType,
    entity_type::EntityType,
    object_type::ObjectType,
    owns::{Owns, OwnsAnnotation},
    plays::Plays,
    relates::Relates,
    relation_type::RelationType,
    role_type::RoleType,
    type_manager::{KindAPI, ReadableType, TypeManager},
    type_reader::TypeReader,
    Ordering, OwnerAPI, PlayerAPI, TypeAPI,
};

#[derive(Debug)]
pub(crate) struct EntityTypeCache {
    pub(super) common_type_cache: CommonTypeCache<EntityType<'static>>,
    pub(super) owner_player_cache: OwnerPlayerCache,
    // ...
}

#[derive(Debug)]
pub(crate) struct RelationTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RelationType<'static>>,
    pub(super) relates_declared: HashSet<Relates<'static>>,
    pub(super) relates_transitive: HashMap<RoleType<'static>, Relates<'static>>,
    pub(super) owner_player_cache: OwnerPlayerCache,
}

#[derive(Debug)]
pub(crate) struct RoleTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RoleType<'static>>,
    pub(super) ordering: Ordering,
    pub(super) relates_declared: Relates<'static>,
}

#[derive(Debug)]
pub(crate) struct AttributeTypeCache {
    pub(super) common_type_cache: CommonTypeCache<AttributeType<'static>>,
    pub(super) value_type: Option<ValueType>,
    // owners: HashSet<Owns<'static>>
}

#[derive(Debug)]
pub(crate) struct OwnsCache {
    pub(super) ordering: Ordering,
    pub(super) overrides: Option<Owns<'static>>,
    pub(super) annotations_declared: HashSet<OwnsAnnotation>,
}

#[derive(Debug)]
pub(crate) struct PlaysCache {
    pub(super) overrides: Option<Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct CommonTypeCache<T: KindAPI<'static>> {
    pub(super) type_: T,
    pub(super) label: Label<'static>,
    pub(super) is_root: bool,
    pub(super) annotations_declared: HashSet<T::AnnotationType>,
    // TODO: Should these all be sets instead of vec?
    pub(super) supertype: Option<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) supertypes: Vec<T>,   // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) subtypes_declared: Vec<T>, // TODO: benchmark smallvec.
    pub(super) subtypes_transitive: Vec<T>, // TODO: benchmark smallvec
}

#[derive(Debug)]
pub struct OwnerPlayerCache {
    pub(super) owns_declared: HashSet<Owns<'static>>,
    pub(super) owns_transitive: HashMap<AttributeType<'static>, Owns<'static>>,
    pub(super) plays_declared: HashSet<Plays<'static>>,
    pub(super) plays_transitive: HashMap<RoleType<'static>, Plays<'static>>,
}

impl EntityTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<EntityTypeCache>]> {
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
                common_type_cache: CommonTypeCache::create(snapshot, entity.clone()),
                owner_player_cache: OwnerPlayerCache::create(snapshot, entity.clone()),
            };
            caches[entity.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl RelationTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<RelationTypeCache>]> {
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
                common_type_cache: CommonTypeCache::create(snapshot, relation.clone()),
                owner_player_cache: OwnerPlayerCache::create(snapshot, relation.clone()),
                relates_declared: TypeReader::get_relates(snapshot, relation.clone()).unwrap(),
                relates_transitive: TypeReader::get_relates_transitive(snapshot, relation.clone()).unwrap(),
            };
            caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl AttributeTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<AttributeTypeCache>]> {
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
                common_type_cache: CommonTypeCache::create(snapshot, attribute.clone()),
                value_type: TypeReader::get_value_type(snapshot, attribute.clone()).unwrap(),
            };
            caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}
impl RoleTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<RoleTypeCache>]> {
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
                common_type_cache: CommonTypeCache::create(snapshot, role.clone()),
                ordering,
                relates_declared: TypeReader::get_relation(snapshot, role.clone()).unwrap(),
            };
            caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl OwnsCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Owns<'static>, OwnsCache> {
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
                        overrides: TypeReader::get_owns_override(snapshot, owns.clone()).unwrap(),
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
}

impl PlaysCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Plays<'static>, PlaysCache> {
        snapshot
            .iterate_range(KeyRange::new_within(
                TypeEdge::build_prefix(Prefix::EdgePlays),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_hashmap(|key, _| {
                let edge = TypeEdge::new(Bytes::Reference(key.byte_ref()));
                let player = ObjectType::new(edge.from().into_owned());
                let role = RoleType::new(edge.to().into_owned());
                let plays = Plays::new(player, role);
                (
                    plays.clone(),
                    PlaysCache { overrides: TypeReader::get_plays_override(snapshot, plays.clone()).unwrap() },
                )
            })
            .unwrap()
    }
}

impl<T: KindAPI<'static> + ReadableType<ReadOutput<'static> = T>> CommonTypeCache<T> {
    fn create<Snapshot>(snapshot: &Snapshot, type_: T) -> CommonTypeCache<T>
    where
        Snapshot: ReadableSnapshot,
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
}
impl OwnerPlayerCache {
    fn create<'a, Snapshot, T>(snapshot: &Snapshot, type_: T) -> OwnerPlayerCache
    where
        Snapshot: ReadableSnapshot,
        T: KindAPI<'static> + OwnerAPI<'static> + PlayerAPI<'static> + ReadableType<ReadOutput<'static> = T>,
    {
        OwnerPlayerCache {
            owns_declared: TypeReader::get_owns(snapshot, type_.clone()).unwrap(),
            owns_transitive: TypeReader::get_owns_transitive(snapshot, type_.clone()).unwrap(),
            plays_declared: TypeReader::get_plays(snapshot, type_.clone()).unwrap(),
            plays_transitive: TypeReader::get_plays_transitive(snapshot, type_.clone()).unwrap(),
        }
    }
}
