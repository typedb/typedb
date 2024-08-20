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
            vertex::{PrefixedTypeVertexEncoding, TypeVertex},
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
};
use encoding::graph::type_::vertex::TypeVertexEncoding;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::type_::{
    attribute_type::AttributeType,
    entity_type::EntityType,
    object_type::ObjectType,
    owns::{Owns, OwnsAnnotation},
    plays::{Plays, PlaysAnnotation},
    relates::{Relates, RelatesAnnotation},
    relation_type::RelationType,
    role_type::RoleType,
    type_manager::type_reader::TypeReader,
    Capability, KindAPI, ObjectTypeAPI, Ordering, PlayerAPI, TypeAPI,
};

#[derive(Debug)]
pub(crate) struct EntityTypeCache {
    pub(super) common_type_cache: CommonTypeCache<EntityType<'static>>,
    pub(super) object_cache: ObjectCache,
}

#[derive(Debug)]
pub(crate) struct RelationTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RelationType<'static>>,
    pub(super) relates_declared: HashSet<Relates<'static>>,
    pub(super) relates: HashSet<Relates<'static>>,
    pub(super) object_cache: ObjectCache,
}

#[derive(Debug)]
pub(crate) struct RoleTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RoleType<'static>>,
    pub(super) ordering: Ordering,
    pub(super) relates_declared: Relates<'static>,
    pub(super) relates: HashSet<Relates<'static>>,
    pub(super) plays_declared: HashSet<Plays<'static>>,
    pub(super) plays: HashMap<ObjectType<'static>, Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct AttributeTypeCache {
    pub(super) common_type_cache: CommonTypeCache<AttributeType<'static>>,
    pub(super) value_type_declared: Option<ValueType>,
    pub(super) value_type: Option<(ValueType, AttributeType<'static>)>,
    pub(super) owns_declared: HashSet<Owns<'static>>,
    pub(super) owns: HashMap<ObjectType<'static>, Owns<'static>>,
}

#[derive(Debug)]
pub(crate) struct OwnsCache {
    pub(super) ordering: Ordering,
    pub(super) overrides: Option<Owns<'static>>,
    pub(super) overriding: HashSet<Owns<'static>>,
    pub(super) overriding_transitive: HashSet<Owns<'static>>,
    pub(super) annotations_declared: HashSet<OwnsAnnotation>,
    pub(super) annotations: HashMap<OwnsAnnotation, Owns<'static>>,
}

#[derive(Debug)]
pub(crate) struct PlaysCache {
    pub(super) overrides: Option<Plays<'static>>,
    pub(super) overriding: HashSet<Plays<'static>>,
    pub(super) overriding_transitive: HashSet<Plays<'static>>,
    pub(super) annotations_declared: HashSet<PlaysAnnotation>,
    pub(super) annotations: HashMap<PlaysAnnotation, Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct RelatesCache {
    pub(super) overrides: Option<Relates<'static>>,
    pub(super) overriding: HashSet<Relates<'static>>,
    pub(super) overriding_transitive: HashSet<Relates<'static>>,
    pub(super) annotations_declared: HashSet<RelatesAnnotation>,
    pub(super) annotations: HashMap<RelatesAnnotation, Relates<'static>>,
}

#[derive(Debug)]
pub(crate) struct CommonTypeCache<T: KindAPI<'static>> {
    pub(super) type_: T,
    pub(super) label: Label<'static>,
    pub(super) annotations_declared: HashSet<T::AnnotationType>,
    pub(super) annotations: HashMap<T::AnnotationType, T>,
    // TODO: Should these all be sets instead of vec?
    pub(super) supertype: Option<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) supertypes: Vec<T>,   // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) subtypes: Vec<T>,     // TODO: benchmark smallvec.
    pub(super) subtypes_transitive: Vec<T>, // TODO: benchmark smallvec
}

#[derive(Debug)]
pub struct ObjectCache {
    pub(super) owns_declared: HashSet<Owns<'static>>,
    pub(super) owns: HashSet<Owns<'static>>,
    pub(super) owns_overrides: HashMap<Owns<'static>, Owns<'static>>,
    pub(super) plays_declared: HashSet<Plays<'static>>,
    pub(super) plays: HashSet<Plays<'static>>,
    pub(super) plays_overrides: HashMap<Plays<'static>, Plays<'static>>,
}

impl EntityTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<EntityTypeCache>]> {
        let entities = snapshot
            .iterate_range(KeyRange::new_within(EntityType::prefix_for_kind(), EntityType::PREFIX.fixed_width_keys()))
            .collect_cloned_hashset(|key, _| EntityType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_entity_id = entities.iter().map(|e| e.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_entity_id).map(|_| None).collect::<Box<[_]>>();

        for entity in entities.into_iter() {
            let cache = EntityTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, entity.clone()),
                object_cache: ObjectCache::create(snapshot, entity.clone()),
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
                RelationType::prefix_for_kind(),
                Prefix::VertexRelationType.fixed_width_keys(),
            ))
            .collect_cloned_hashset(|key, _| RelationType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_relation_id = relations.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_relation_id).map(|_| None).collect::<Box<[_]>>();
        for relation in relations.into_iter() {
            let cache = RelationTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, relation.clone()),
                object_cache: ObjectCache::create(snapshot, relation.clone()),
                relates_declared: TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation.clone())
                    .unwrap(),
                relates: TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation.clone()).unwrap(),
            };
            caches[relation.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl AttributeTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<AttributeTypeCache>]> {
        let attributes = snapshot
            .iterate_range(KeyRange::new_within(AttributeType::prefix_for_kind(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| AttributeType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_attribute_id = attributes.iter().map(|a| a.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_attribute_id).map(|_| None).collect::<Box<[_]>>();
        for attribute in attributes {
            let cache = AttributeTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, attribute.clone()),
                value_type_declared: TypeReader::get_value_type_declared(snapshot, attribute.clone()).unwrap(),
                value_type: TypeReader::get_value_type(snapshot, attribute.clone()).unwrap(),
                owns_declared: TypeReader::get_capabilities_for_interface_declared::<Owns<'static>>(
                    snapshot,
                    attribute.clone(),
                )
                .unwrap(),
                owns: TypeReader::get_capabilities_for_interface::<Owns<'static>>(snapshot, attribute.clone()).unwrap(),
            };
            caches[attribute.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl RoleTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<RoleTypeCache>]> {
        let roles = snapshot
            .iterate_range(KeyRange::new_within(RoleType::prefix_for_kind(), TypeVertex::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| RoleType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_role_id = roles.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_role_id).map(|_| None).collect::<Box<[_]>>();
        for role in roles.into_iter() {
            let ordering = TypeReader::get_type_ordering(snapshot, role.clone()).unwrap();
            let cache = RoleTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, role.clone()),
                ordering,
                relates_declared: TypeReader::get_role_type_relates_declared(snapshot, role.clone()).unwrap(),
                relates: TypeReader::get_role_type_relates(snapshot, role.clone()).unwrap(),
                plays_declared: TypeReader::get_capabilities_for_interface_declared::<Plays<'static>>(
                    snapshot,
                    role.clone(),
                )
                .unwrap(),
                plays: TypeReader::get_capabilities_for_interface::<Plays<'static>>(snapshot, role.clone()).unwrap(),
            };
            caches[role.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl OwnsCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Owns<'static>, OwnsCache> {
        let mut map = HashMap::new();
        let mut it = snapshot.iterate_range(KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeOwnsReverse),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));
        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let attribute = AttributeType::new(edge.from().into_owned());
            let owner = ObjectType::new(edge.to().into_owned());
            let owns = Owns::new(owner, attribute);
            let cache = OwnsCache {
                ordering: TypeReader::get_type_edge_ordering(snapshot, owns.clone()).unwrap(),
                overrides: TypeReader::get_capability_override(snapshot, owns.clone()).unwrap(),
                overriding: TypeReader::get_overriding_capabilities(snapshot, owns.clone()).unwrap(),
                overriding_transitive: TypeReader::get_overriding_capabilities_transitive(snapshot, owns.clone())
                    .unwrap(),
                annotations_declared: TypeReader::get_type_edge_annotations_declared(snapshot, owns.clone())
                    .unwrap()
                    .into_iter()
                    .map(|annotation| OwnsAnnotation::try_from(annotation).unwrap())
                    .collect(),
                annotations: TypeReader::get_type_edge_annotations(snapshot, owns.clone())
                    .unwrap()
                    .into_iter()
                    .map(|(annotation, owns)| (OwnsAnnotation::try_from(annotation).unwrap(), owns))
                    .collect(),
            };
            map.insert(owns.clone(), cache);
        }
        map
    }
}

impl PlaysCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Plays<'static>, PlaysCache> {
        let mut map = HashMap::new();
        let mut it = snapshot.iterate_range(KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgePlays),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));

        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let player = ObjectType::new(edge.from().into_owned());
            let role = RoleType::new(edge.to().into_owned());
            let plays = Plays::new(player, role);
            let cache = PlaysCache {
                overrides: TypeReader::get_capability_override(snapshot, plays.clone()).unwrap(),
                overriding: TypeReader::get_overriding_capabilities(snapshot, plays.clone()).unwrap(),
                overriding_transitive: TypeReader::get_overriding_capabilities_transitive(snapshot, plays.clone())
                    .unwrap(),
                annotations_declared: TypeReader::get_type_edge_annotations_declared(snapshot, plays.clone())
                    .unwrap()
                    .into_iter()
                    .map(|annotation| PlaysAnnotation::try_from(annotation).unwrap())
                    .collect(),
                annotations: TypeReader::get_type_edge_annotations(snapshot, plays.clone())
                    .unwrap()
                    .into_iter()
                    .map(|(annotation, plays)| (PlaysAnnotation::try_from(annotation).unwrap(), plays))
                    .collect(),
            };
            map.insert(plays.clone(), cache);
        }
        map
    }
}

impl RelatesCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Relates<'static>, RelatesCache> {
        let mut map = HashMap::new();
        let mut it = snapshot.iterate_range(KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeRelates),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));

        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let relation = RelationType::new(edge.from().into_owned());
            let role = RoleType::new(edge.to().into_owned());
            let relates = Relates::new(relation, role);
            let cache = RelatesCache {
                overrides: TypeReader::get_capability_override(snapshot, relates.clone()).unwrap(),
                overriding: TypeReader::get_overriding_capabilities(snapshot, relates.clone()).unwrap(),
                overriding_transitive: TypeReader::get_overriding_capabilities_transitive(snapshot, relates.clone())
                    .unwrap(),
                annotations_declared: TypeReader::get_type_edge_annotations_declared(snapshot, relates.clone())
                    .unwrap()
                    .into_iter()
                    .map(|annotation| RelatesAnnotation::try_from(annotation).unwrap())
                    .collect(),
                annotations: TypeReader::get_type_edge_annotations(snapshot, relates.clone())
                    .unwrap()
                    .into_iter()
                    .map(|(annotation, relates)| (RelatesAnnotation::try_from(annotation).unwrap(), relates))
                    .collect(),
            };
            map.insert(relates.clone(), cache);
        }
        map
    }
}

impl<T: KindAPI<'static, SelfStatic = T>> CommonTypeCache<T> {
    fn create<Snapshot>(snapshot: &Snapshot, type_: T) -> CommonTypeCache<T>
    where
        Snapshot: ReadableSnapshot,
    {
        let label = TypeReader::get_label(snapshot, type_.clone()).unwrap().unwrap();
        let annotations_declared = TypeReader::get_type_annotations_declared(snapshot, type_.clone()).unwrap();
        let annotations = TypeReader::get_type_annotations(snapshot, type_.clone()).unwrap();
        let supertype = TypeReader::get_supertype(snapshot, type_.clone()).unwrap();
        let supertypes = TypeReader::get_supertypes_transitive(snapshot, type_.clone()).unwrap();
        let subtypes = TypeReader::get_subtypes(snapshot, type_.clone()).unwrap();
        let subtypes_transitive = TypeReader::get_subtypes_transitive(snapshot, type_.clone()).unwrap();
        CommonTypeCache {
            type_,
            label,
            annotations_declared,
            annotations,
            supertype,
            supertypes,
            subtypes,
            subtypes_transitive,
        }
    }
}

impl ObjectCache {
    fn create<Snapshot, T>(snapshot: &Snapshot, type_: T) -> ObjectCache
    where
        Snapshot: ReadableSnapshot,
        T: KindAPI<'static> + ObjectTypeAPI<'static> + PlayerAPI<'static>,
    {
        let object_type = type_.into_owned_object_type();
        ObjectCache {
            owns_declared: TypeReader::get_capabilities_declared::<Owns<'static>>(snapshot, object_type.clone())
                .unwrap(),
            owns: TypeReader::get_capabilities::<Owns<'static>>(snapshot, object_type.clone()).unwrap(),
            owns_overrides: TypeReader::get_object_capabilities_overrides::<Owns<'static>>(
                snapshot,
                object_type.clone(),
            )
            .unwrap(),
            plays_declared: TypeReader::get_capabilities_declared::<Plays<'static>>(snapshot, object_type.clone())
                .unwrap(),
            plays: TypeReader::get_capabilities::<Plays<'static>>(snapshot, object_type.clone()).unwrap(),
            plays_overrides: TypeReader::get_object_capabilities_overrides::<Plays<'static>>(
                snapshot,
                object_type.clone(),
            )
            .unwrap(),
        }
    }
}
