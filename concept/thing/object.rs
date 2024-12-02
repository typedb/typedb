/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

use bytes::Bytes;
use encoding::{
    graph::thing::{
        edge::{ThingEdgeHas, ThingEdgeHasReverse},
        vertex_object::ObjectVertex,
    },
    layout::prefix::Prefix,
    value::{decode_value_u64, value::Value},
    Prefixed,
};
use lending_iterator::{higher_order::Hkt, LendingIterator};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::Attribute,
        entity::Entity,
        has::Has,
        relation::{Relation, RelationRoleIterator},
        thing_manager::{validation::operation_time_validation::OperationTimeValidation, ThingManager},
        HKInstance, ThingAPI,
    },
    type_::{
        attribute_type::AttributeType, object_type::ObjectType, role_type::RoleType, ObjectTypeAPI, Ordering, OwnerAPI,
    },
    ConceptStatus,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Object {
    Entity(Entity),
    Relation(Relation),
}

impl Object {
    pub fn unwrap_entity(self) -> Entity {
        match self {
            Self::Entity(entity) => entity,
            Self::Relation(relation) => panic!("called `Object::unwrap_entity()` on a `Relation` value: {relation:?}"),
        }
    }

    pub fn unwrap_relation(self) -> Relation {
        match self {
            Self::Relation(relation) => relation,
            Self::Entity(entity) => panic!("called `Object::unwrap_relation()` on an `Entity` value: {entity:?}"),
        }
    }

    pub fn type_(&self) -> ObjectType {
        match self {
            Object::Entity(entity) => ObjectType::Entity(entity.type_()),
            Object::Relation(relation) => ObjectType::Relation(relation.type_()),
        }
    }
}

impl ThingAPI for Object {
    type TypeAPI = ObjectType;
    type Vertex = ObjectVertex;
    type Owned = Object;
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexEntity, Prefix::VertexRelation);

    fn new(object_vertex: Self::Vertex) -> Self {
        match object_vertex.prefix() {
            Prefix::VertexEntity => Object::Entity(Entity::new(object_vertex)),
            Prefix::VertexRelation => Object::Relation(Relation::new(object_vertex)),
            _ => unreachable!("Object creation requires either Entity or Relation vertex."),
        }
    }

    fn vertex(&self) -> Self::Vertex {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    fn into_owned(self) -> Self::Owned {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }

    fn iid(&self) -> Bytes<'_, BUFFER_KEY_INLINE> {
        match self {
            Object::Entity(entity) => entity.iid(),
            Object::Relation(relation) => relation.iid(),
        }
    }

    fn set_required(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptReadError>> {
        match self {
            Object::Entity(entity) => entity.set_required(snapshot, thing_manager),
            Object::Relation(relation) => relation.set_required(snapshot, thing_manager),
        }
    }

    fn get_status(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> ConceptStatus {
        match self {
            Object::Entity(entity) => entity.get_status(snapshot, thing_manager),
            Object::Relation(relation) => relation.get_status(snapshot, thing_manager),
        }
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        match self {
            Object::Entity(entity) => entity.delete(snapshot, thing_manager),
            Object::Relation(relation) => relation.delete(snapshot, thing_manager),
        }
    }

    fn prefix_for_type(type_: Self::TypeAPI) -> Prefix {
        match type_ {
            ObjectType::Entity(entity) => Entity::prefix_for_type(entity),
            ObjectType::Relation(relation) => Relation::prefix_for_type(relation),
        }
    }
}

pub trait ObjectAPI: ThingAPI<Vertex = ObjectVertex> + Copy + fmt::Debug {
    fn type_(&self) -> impl ObjectTypeAPI;

    fn into_owned_object(self) -> Object;

    fn has_attribute_with_value(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_attribute_with_value(snapshot, self, attribute_type, value)
    }

    fn has_attribute(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute: &Attribute,
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_attribute(snapshot, self, attribute)
    }

    fn get_has_unordered<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> HasAttributeIterator {
        thing_manager.get_has_from_thing_unordered(snapshot, self)
    }

    fn get_has_type_unordered<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_type: AttributeType,
    ) -> HasAttributeIterator {
        thing_manager.get_has_from_thing_to_type_unordered(snapshot, self, attribute_type)
    }

    fn get_has_type_ordered<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_type: AttributeType,
    ) -> Result<Vec<Attribute>, Box<ConceptReadError>> {
        thing_manager.get_has_from_thing_to_type_ordered(snapshot, self, attribute_type)
    }

    fn get_has_types_range_unordered<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_types_defining_range: impl Iterator<Item = AttributeType>,
    ) -> Result<HasIterator, Box<ConceptReadError>> {
        thing_manager.get_has_from_thing_to_type_range_unordered(snapshot, self, attribute_types_defining_range)
    }

    fn set_has_unordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute: &Attribute,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_owner_exists_to_set_has(snapshot, thing_manager, self)
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        OperationTimeValidation::validate_object_type_owns_attribute_type(
            snapshot,
            thing_manager,
            self.type_(),
            attribute.type_(),
        )
        .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        let owns = self.type_().try_get_owns_attribute(snapshot, thing_manager.type_manager(), attribute.type_())?;
        match owns.get_ordering(snapshot, thing_manager.type_manager())? {
            Ordering::Unordered => (),
            Ordering::Ordered => return Err(Box::new(ConceptWriteError::SetHasUnorderedOwnsOrdered {})),
        }

        OperationTimeValidation::validate_owns_is_not_abstract(snapshot, thing_manager, self, attribute.type_())
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        thing_manager.set_has_unordered(snapshot, self, attribute)
    }

    fn unset_has_unordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute: &Attribute,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_owner_exists_to_unset_has(snapshot, thing_manager, self)
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        OperationTimeValidation::validate_object_type_owns_attribute_type(
            snapshot,
            thing_manager,
            self.type_(),
            attribute.type_(),
        )
        .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        let owns = self.type_().try_get_owns_attribute(snapshot, thing_manager.type_manager(), attribute.type_())?;
        match owns.get_ordering(snapshot, thing_manager.type_manager())? {
            Ordering::Unordered => (),
            Ordering::Ordered => return Err(Box::new(ConceptWriteError::UnsetHasUnorderedOwnsOrdered {})),
        }

        thing_manager.unset_has(snapshot, self, attribute);
        Ok(())
    }

    fn set_has_ordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        new_attributes: Vec<Attribute>,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_owner_exists_to_set_has(snapshot, thing_manager, self)
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        OperationTimeValidation::validate_object_type_owns_attribute_type(
            snapshot,
            thing_manager,
            self.type_(),
            attribute_type,
        )
        .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        let owns = self.type_().try_get_owns_attribute(snapshot, thing_manager.type_manager(), attribute_type)?;
        match owns.get_ordering(snapshot, thing_manager.type_manager())? {
            Ordering::Unordered => return Err(Box::new(ConceptWriteError::SetHasOrderedOwnsUnordered {})),
            Ordering::Ordered => (),
        }

        OperationTimeValidation::validate_owns_is_not_abstract(snapshot, thing_manager, self, attribute_type)
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        let mut new_counts = BTreeMap::<_, u64>::new();
        for attr in &new_attributes {
            *new_counts.entry(attr).or_default() += 1;
        }

        OperationTimeValidation::validate_owns_distinct_constraint(
            snapshot,
            thing_manager,
            self,
            attribute_type,
            &new_counts,
        )
        .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        // 1. get owned list
        let old_attributes = thing_manager.get_has_from_thing_to_type_ordered(snapshot, self, attribute_type)?;

        let mut old_counts = BTreeMap::<_, u64>::new();
        for attr in &old_attributes {
            *old_counts.entry(attr).or_default() += 1;
        }

        // 2. Delete existing but no-longer necessary has, and add new ones, with the correct counts (!)
        for attr in old_counts.keys() {
            if !new_counts.contains_key(attr) {
                thing_manager.unset_has(snapshot, self, attr);
            }
        }
        for (attr, count) in new_counts {
            // Don't skip unchanged count to ensure that locks are placed correctly
            thing_manager.set_has_count(snapshot, self, attr, count)?;
        }

        // 3. Overwrite owned list
        thing_manager.set_has_ordered(snapshot, self, attribute_type, new_attributes)
    }

    fn unset_has_ordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_owner_exists_to_unset_has(snapshot, thing_manager, self)
            .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        OperationTimeValidation::validate_object_type_owns_attribute_type(
            snapshot,
            thing_manager,
            self.type_(),
            attribute_type,
        )
        .map_err(|error| ConceptWriteError::DataValidation { typedb_source: error })?;

        let owns = self.type_().try_get_owns_attribute(snapshot, thing_manager.type_manager(), attribute_type)?;
        let ordering = owns.get_ordering(snapshot, thing_manager.type_manager())?;
        match ordering {
            Ordering::Unordered => Err(Box::new(ConceptWriteError::UnsetHasOrderedOwnsUnordered {})),
            Ordering::Ordered => {
                for attribute in self.get_has_type_ordered(snapshot, thing_manager, attribute_type)? {
                    thing_manager.unset_has(snapshot, self, &attribute);
                }
                thing_manager.unset_has_ordered(snapshot, self, attribute_type);
                Ok(())
            }
        }
    }

    fn get_relations<'m>(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> impl Iterator<Item = Result<Relation, Box<ConceptReadError>>> {
        thing_manager.get_relations_player(snapshot, self)
    }

    fn get_relations_by_role<'m>(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        role_type: RoleType,
    ) -> impl Iterator<Item = Result<(Relation, u64), Box<ConceptReadError>>> {
        thing_manager.get_relations_player_role(snapshot, self, role_type)
    }

    fn get_relations_roles<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> RelationRoleIterator {
        thing_manager.get_relations_roles(snapshot, self)
    }

    fn get_has_counts(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<HashMap<AttributeType, u64>, Box<ConceptReadError>> {
        let mut counts = HashMap::new();
        let mut has_iter = self.get_has_unordered(snapshot, thing_manager);
        while let Some((attribute, count)) = has_iter.next().transpose()? {
            let value = counts.entry(attribute.type_()).or_insert(0);
            *value += count;
        }
        Ok(counts)
    }

    fn get_played_roles_counts(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<HashMap<RoleType, u64>, Box<ConceptReadError>> {
        let mut counts = HashMap::new();
        let mut relation_role_iter = self.get_relations_roles(snapshot, thing_manager);
        while let Some((_, role_type, count)) = relation_role_iter.next().transpose()? {
            let value = counts.entry(role_type).or_insert(0);
            *value += count;
        }
        Ok(counts)
    }
}

impl ObjectAPI for Object {
    fn type_(&self) -> impl ObjectTypeAPI {
        self.type_()
    }

    fn into_owned_object(self) -> Object {
        self.into_owned()
    }
}

impl HKInstance for Object {}

impl Hkt for Object {
    type HktSelf<'a> = Object;
}

impl Ord for Object {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vertex().cmp(&other.vertex())
    }
}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Entity(entity) => fmt::Display::fmt(entity, f),
            Object::Relation(relation) => fmt::Display::fmt(relation, f),
        }
    }
}

fn storage_key_has_edge_to_has_attribute<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Attribute, u64) {
    let edge = ThingEdgeHas::new(storage_key.into_bytes());
    (Attribute::new(edge.to()), decode_value_u64(&value))
}

fn storage_key_has_edge_to_has<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Has, u64) {
    (Has::new_from_edge(ThingEdgeHas::new(storage_key.into_bytes())), decode_value_u64(&value))
}

fn storage_key_has_reverse_edge_to_has<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Has, u64) {
    (Has::new_from_edge_reverse(ThingEdgeHasReverse::new(storage_key.into_bytes())), decode_value_u64(&value))
}

edge_iterator!(
    HasAttributeIterator;
    (Attribute, u64);
    storage_key_has_edge_to_has_attribute
);

edge_iterator!(
    HasIterator;
    (Has, u64);
    storage_key_has_edge_to_has
);

edge_iterator!(
    HasReverseIterator;
    (Has, u64);
    storage_key_has_reverse_edge_to_has
);
