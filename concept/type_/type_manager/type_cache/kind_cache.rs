/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use encoding::{
    graph::{
        type_::{
            edge::TypeEdge,
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
};
use lending_iterator::LendingIterator;
use storage::{
    key_range::{KeyRange, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::type_::{
    attribute_type::AttributeType,
    constraint::{CapabilityConstraint, TypeConstraint},
    entity_type::EntityType,
    object_type::ObjectType,
    owns::Owns,
    plays::Plays,
    relates::Relates,
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
    pub(super) relates_root: HashSet<Relates<'static>>,
    pub(super) relates_declared: HashSet<Relates<'static>>,
    pub(super) relates: HashSet<Relates<'static>>,
    pub(super) relates_with_specialised: HashSet<Relates<'static>>,
    pub(super) related_role_type_constraints:
        HashMap<RoleType<'static>, HashSet<CapabilityConstraint<Relates<'static>>>>,
    pub(super) object_cache: ObjectCache,
}

#[derive(Debug)]
pub(crate) struct RoleTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RoleType<'static>>,
    pub(super) ordering: Ordering,
    pub(super) relates_root: Relates<'static>,
    pub(super) relates: HashSet<Relates<'static>>,
    pub(super) relation_types: HashMap<RelationType<'static>, Relates<'static>>,
    pub(super) plays: HashSet<Plays<'static>>,
    pub(super) player_types: HashMap<ObjectType<'static>, Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct AttributeTypeCache {
    pub(super) common_type_cache: CommonTypeCache<AttributeType<'static>>,
    pub(super) value_type_declared: Option<ValueType>,
    pub(super) value_type: Option<(ValueType, AttributeType<'static>)>,
    pub(super) owns: HashSet<Owns<'static>>,
    pub(super) owner_types: HashMap<ObjectType<'static>, Owns<'static>>,
}

#[derive(Debug)]
pub(crate) struct OwnsCache {
    pub(super) ordering: Ordering,
    pub(super) common_capability_cache: CommonCapabilityCache<Owns<'static>>,
}

#[derive(Debug)]
pub(crate) struct PlaysCache {
    pub(super) common_capability_cache: CommonCapabilityCache<Plays<'static>>,
}

#[derive(Debug)]
pub(crate) struct RelatesCache {
    pub(super) is_specialising: bool,
    pub(super) common_capability_cache: CommonCapabilityCache<Relates<'static>>,
}

#[derive(Debug)]
pub(crate) struct CommonTypeCache<T: KindAPI<'static>> {
    pub(super) type_: T,
    pub(super) label: Arc<Label<'static>>,
    pub(super) annotations_declared: HashSet<T::AnnotationType>,
    pub(super) constraints: HashSet<TypeConstraint<T>>,
    // TODO: Should these all be sets instead of vec?
    pub(super) supertype: Option<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) supertypes_transitive: Vec<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) subtypes: HashSet<T>,
    pub(super) subtypes_transitive: Vec<T>, // TODO: benchmark smallvec
}

#[derive(Debug)]
pub(crate) struct CommonCapabilityCache<CAP: Capability<'static>> {
    pub(super) capability: CAP,
    pub(super) annotations_declared: HashSet<CAP::AnnotationType>,
    pub(super) constraints: HashSet<CapabilityConstraint<CAP>>,
}

#[derive(Debug)]
pub struct ObjectCache {
    pub(super) owns_declared: HashSet<Owns<'static>>,
    pub(super) owns: HashSet<Owns<'static>>,
    pub(super) owns_with_specialised: HashSet<Owns<'static>>,
    pub(super) owned_attribute_type_constraints:
        HashMap<AttributeType<'static>, HashSet<CapabilityConstraint<Owns<'static>>>>,
    pub(super) plays_declared: HashSet<Plays<'static>>,
    pub(super) plays: HashSet<Plays<'static>>,
    pub(super) plays_with_specialised: HashSet<Plays<'static>>,
    pub(super) played_role_type_constraints: HashMap<RoleType<'static>, HashSet<CapabilityConstraint<Plays<'static>>>>,
}

impl EntityTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<EntityTypeCache>]> {
        let entities = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(EntityType::prefix_for_kind()),
                EntityType::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_hashset(|key, _| EntityType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_entity_id = entities.iter().map(|e| e.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_entity_id).map(|_| None).collect::<Box<[_]>>();

        for entity_type in entities.into_iter() {
            let cache = EntityTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, entity_type.clone()),
                object_cache: ObjectCache::create(snapshot, entity_type.clone()),
            };
            caches[entity_type.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl RelationTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<RelationTypeCache>]> {
        let relations = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(RelationType::prefix_for_kind()),
                Prefix::VertexRelationType.fixed_width_keys(),
            ))
            .collect_cloned_hashset(|key, _| RelationType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_relation_id = relations.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_relation_id).map(|_| None).collect::<Box<[_]>>();
        for relation_type in relations.into_iter() {
            let common_type_cache = CommonTypeCache::create(snapshot, relation_type.clone());
            let object_cache = ObjectCache::create(snapshot, relation_type.clone());
            let relates_root = TypeReader::get_relation_type_relates_root(snapshot, relation_type.clone()).unwrap();
            let relates_declared =
                TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_type.clone()).unwrap();
            let relates =
                TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone(), false).unwrap();
            let relates_with_specialised =
                TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone(), true).unwrap();
            let related_role_type_constraints =
                TypeReader::get_type_capabilities_constraints::<Relates<'static>>(snapshot, relation_type.clone())
                    .unwrap();
            let cache = RelationTypeCache {
                common_type_cache,
                relates_root,
                relates_declared,
                relates,
                relates_with_specialised,
                related_role_type_constraints,
                object_cache,
            };
            caches[relation_type.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl AttributeTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<AttributeTypeCache>]> {
        let attributes = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(AttributeType::prefix_for_kind()),
                TypeVertex::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_hashset(|key, _| AttributeType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_attribute_id = attributes.iter().map(|a| a.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_attribute_id).map(|_| None).collect::<Box<[_]>>();
        for attribute_type in attributes {
            let cache = AttributeTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, attribute_type.clone()),
                value_type_declared: TypeReader::get_value_type_declared(snapshot, attribute_type.clone()).unwrap(),
                value_type: TypeReader::get_value_type(snapshot, attribute_type.clone()).unwrap(),
                owns: TypeReader::get_capabilities_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())
                    .unwrap(),
                owner_types: TypeReader::get_object_types_with_capabilities_for_interface::<Owns<'static>>(
                    snapshot,
                    attribute_type.clone(),
                )
                .unwrap(),
            };
            caches[attribute_type.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl RoleTypeCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> Box<[Option<RoleTypeCache>]> {
        let roles = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(RoleType::prefix_for_kind()),
                TypeVertex::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_hashset(|key, _| RoleType::read_from(Bytes::Reference(key.byte_ref()).into_owned()))
            .unwrap();
        let max_role_id = roles.iter().map(|r| r.vertex().type_id_().as_u16()).max().unwrap_or(0);
        let mut caches = (0..=max_role_id).map(|_| None).collect::<Box<[_]>>();
        for role_type in roles.into_iter() {
            let ordering = TypeReader::get_type_ordering(snapshot, role_type.clone()).unwrap();
            let cache = RoleTypeCache {
                common_type_cache: CommonTypeCache::create(snapshot, role_type.clone()),
                ordering,
                relates_root: TypeReader::get_role_type_relates_root(snapshot, role_type.clone()).unwrap(),
                relates: TypeReader::get_capabilities_for_interface::<Relates<'static>>(snapshot, role_type.clone())
                    .unwrap(),
                relation_types: TypeReader::get_object_types_with_capabilities_for_interface::<Relates<'static>>(
                    snapshot,
                    role_type.clone(),
                )
                .unwrap(),
                plays: TypeReader::get_capabilities_for_interface::<Plays<'static>>(snapshot, role_type.clone())
                    .unwrap(),
                player_types: TypeReader::get_object_types_with_capabilities_for_interface::<Plays<'static>>(
                    snapshot,
                    role_type.clone(),
                )
                .unwrap(),
            };
            caches[role_type.vertex().type_id_().as_u16() as usize] = Some(cache);
        }
        caches
    }
}

impl OwnsCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Owns<'static>, OwnsCache> {
        let mut map = HashMap::new();
        let mut it = snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(TypeEdge::build_prefix(Prefix::EdgeOwnsReverse)),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));
        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let attribute = AttributeType::new(edge.from().into_owned());
            let owner = ObjectType::new(edge.to().into_owned());
            let owns = Owns::new(owner, attribute);
            let cache = OwnsCache {
                ordering: TypeReader::get_capability_ordering(snapshot, owns.clone()).unwrap(),
                common_capability_cache: CommonCapabilityCache::create(snapshot, owns.clone()),
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
            RangeStart::Inclusive(TypeEdge::build_prefix(Prefix::EdgePlays)),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));

        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let player = ObjectType::new(edge.from().into_owned());
            let role = RoleType::new(edge.to().into_owned());
            let plays = Plays::new(player, role);
            let cache = PlaysCache { common_capability_cache: CommonCapabilityCache::create(snapshot, plays.clone()) };
            map.insert(plays.clone(), cache);
        }
        map
    }
}

impl RelatesCache {
    pub(super) fn create(snapshot: &impl ReadableSnapshot) -> HashMap<Relates<'static>, RelatesCache> {
        let mut map = HashMap::new();
        let mut it = snapshot.iterate_range(KeyRange::new_within(
            RangeStart::Inclusive(TypeEdge::build_prefix(Prefix::EdgeRelates)),
            TypeEdge::FIXED_WIDTH_ENCODING,
        ));

        while let Some((key, _)) = it.next().transpose().unwrap() {
            let edge = TypeEdge::new(Bytes::reference(key.bytes()));
            let relation = RelationType::new(edge.from().into_owned());
            let role = RoleType::new(edge.to().into_owned());
            let relates = Relates::new(relation, role);
            let is_specialising = TypeReader::is_relates_specialising(snapshot, relates.clone()).unwrap();
            let cache = RelatesCache {
                is_specialising,
                common_capability_cache: CommonCapabilityCache::create(snapshot, relates.clone()),
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
        let label = Arc::new(TypeReader::get_label(snapshot, type_.clone()).unwrap().unwrap());
        let annotations_declared = TypeReader::get_type_annotations_declared(snapshot, type_.clone()).unwrap();
        let constraints = TypeReader::get_type_constraints(snapshot, type_.clone()).unwrap();
        let supertype = TypeReader::get_supertype(snapshot, type_.clone()).unwrap();
        let supertypes_transitive = TypeReader::get_supertypes_transitive(snapshot, type_.clone()).unwrap();
        let subtypes = TypeReader::get_subtypes(snapshot, type_.clone()).unwrap();
        let subtypes_transitive = TypeReader::get_subtypes_transitive(snapshot, type_.clone()).unwrap();
        CommonTypeCache {
            type_,
            label,
            annotations_declared,
            constraints,
            supertype,
            supertypes_transitive,
            subtypes,
            subtypes_transitive,
        }
    }
}

impl<CAP: Capability<'static>> CommonCapabilityCache<CAP> {
    fn create<Snapshot>(snapshot: &Snapshot, capability: CAP) -> CommonCapabilityCache<CAP>
    where
        Snapshot: ReadableSnapshot,
    {
        let annotations_declared =
            TypeReader::get_capability_annotations_declared(snapshot, capability.clone()).unwrap();
        let constraints = TypeReader::get_capability_constraints(snapshot, capability.clone()).unwrap();
        CommonCapabilityCache { capability, annotations_declared, constraints }
    }
}

impl ObjectCache {
    fn create<Snapshot, T>(snapshot: &Snapshot, type_: T) -> ObjectCache
    where
        Snapshot: ReadableSnapshot,
        T: KindAPI<'static> + ObjectTypeAPI<'static> + PlayerAPI<'static>,
    {
        let object_type = type_.into_owned_object_type();
        let owns_declared =
            TypeReader::get_capabilities_declared::<Owns<'static>>(snapshot, object_type.clone()).unwrap();
        let owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, object_type.clone(), false).unwrap();
        let owns_with_specialised =
            TypeReader::get_capabilities::<Owns<'static>>(snapshot, object_type.clone(), true).unwrap();
        let owned_attribute_type_constraints =
            TypeReader::get_type_capabilities_constraints::<Owns<'static>>(snapshot, object_type.clone()).unwrap();
        let plays_declared =
            TypeReader::get_capabilities_declared::<Plays<'static>>(snapshot, object_type.clone()).unwrap();
        let plays = TypeReader::get_capabilities::<Plays<'static>>(snapshot, object_type.clone(), false).unwrap();
        let plays_with_specialised =
            TypeReader::get_capabilities::<Plays<'static>>(snapshot, object_type.clone(), true).unwrap();
        let played_role_type_constraints =
            TypeReader::get_type_capabilities_constraints::<Plays<'static>>(snapshot, object_type.clone()).unwrap();

        ObjectCache {
            owns_declared,
            owns,
            owns_with_specialised,
            owned_attribute_type_constraints,
            plays_declared,
            plays,
            plays_with_specialised,
            played_role_type_constraints,
        }
    }
}
