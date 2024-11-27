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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Object<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
}

impl<'a> Object<'a> {
    pub fn unwrap_entity(self) -> Entity<'a> {
        match self {
            Self::Entity(entity) => entity,
            Self::Relation(relation) => panic!("called `Object::unwrap_entity()` on a `Relation` value: {relation:?}"),
        }
    }

    pub fn unwrap_relation(self) -> Relation<'a> {
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

    pub fn as_reference(&self) -> Object<'_> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.as_reference()),
            Object::Relation(relation) => Object::Relation(relation.as_reference()),
        }
    }

    pub fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }
}

impl<'a> ThingAPI<'a> for Object<'a> {
    type TypeAPI<'b> = ObjectType;
    type Vertex<'b> = ObjectVertex<'b>;
    type Owned = Object<'static>;
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexEntity, Prefix::VertexRelation);

    fn new(object_vertex: Self::Vertex<'a>) -> Self {
        match object_vertex.prefix() {
            Prefix::VertexEntity => Object::Entity(Entity::new(object_vertex)),
            Prefix::VertexRelation => Object::Relation(Relation::new(object_vertex)),
            _ => unreachable!("Object creation requires either Entity or Relation vertex."),
        }
    }

    fn vertex(&self) -> Self::Vertex<'_> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    fn into_vertex(self) -> ObjectVertex<'a> {
        match self {
            Object::Entity(entity) => entity.into_vertex(),
            Object::Relation(relation) => relation.into_vertex(),
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

    fn prefix_for_type(type_: Self::TypeAPI<'_>) -> Prefix {
        match type_ {
            ObjectType::Entity(entity) => Entity::prefix_for_type(entity),
            ObjectType::Relation(relation) => Relation::prefix_for_type(relation),
        }
    }
}

pub trait ObjectAPI<'a>: for<'b> ThingAPI<'a, Vertex<'b> = ObjectVertex<'b>> + Clone + fmt::Debug {
    fn type_(&self) -> impl ObjectTypeAPI<'static>;

    fn into_owned_object(self) -> Object<'static>;

    fn has_attribute_with_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_attribute_with_value(snapshot, self, attribute_type, value)
    }

    fn has_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute: Attribute<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_attribute(snapshot, self, attribute)
    }

    fn get_has_unordered<'m>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> HasAttributeIterator {
        thing_manager.get_has_from_thing_unordered(snapshot, self)
    }

    fn get_has_type_unordered<'m>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_type: AttributeType,
    ) -> HasAttributeIterator {
        thing_manager.get_has_from_thing_to_type_unordered(snapshot, self, attribute_type)
    }

    fn get_has_type_ordered<'m>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_type: AttributeType,
    ) -> Result<Vec<Attribute<'static>>, Box<ConceptReadError>> {
        thing_manager.get_has_from_thing_to_type_ordered(snapshot, self, attribute_type)
    }

    fn get_has_types_range_unordered<'m>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        attribute_types_defining_range: impl Iterator<Item = AttributeType>,
    ) -> Result<HasIterator, Box<ConceptReadError>> {
        thing_manager.get_has_from_thing_to_type_range_unordered(snapshot, self, attribute_types_defining_range)
    }

    fn set_has_unordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute: Attribute<'_>,
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

        thing_manager.set_has_unordered(snapshot, self, attribute.as_reference())
    }

    fn unset_has_unordered(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute: Attribute<'_>,
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
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        new_attributes: Vec<Attribute<'_>>,
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
                thing_manager.unset_has(snapshot, self, attr.as_reference());
            }
        }
        for (attr, count) in new_counts {
            // Don't skip unchanged count to ensure that locks are placed correctly
            thing_manager.set_has_count(snapshot, self, attr.as_reference(), count)?;
        }

        // 3. Overwrite owned list
        thing_manager.set_has_ordered(snapshot, self, attribute_type, new_attributes)
    }

    fn unset_has_ordered(
        &self,
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
                    thing_manager.unset_has(snapshot, self, attribute);
                }
                thing_manager.unset_has_ordered(snapshot, self, attribute_type);
                Ok(())
            }
        }
    }

    fn get_relations<'m>(
        &'a self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> impl for<'x> LendingIterator<Item<'x> = Result<Relation<'x>, Box<ConceptReadError>>> {
        thing_manager.get_relations_player(snapshot, self)
    }

    fn get_relations_by_role<'m>(
        &'a self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        role_type: RoleType,
    ) -> impl for<'x> LendingIterator<Item<'x> = Result<(Relation<'x>, u64), Box<ConceptReadError>>> {
        thing_manager.get_relations_player_role(snapshot, self, role_type)
    }

    fn get_relations_roles<'m>(
        &self,
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

impl<'a> ObjectAPI<'a> for Object<'a> {
    fn type_(&self) -> impl ObjectTypeAPI<'static> {
        self.type_()
    }

    fn into_owned_object(self) -> Object<'static> {
        self.into_owned()
    }
}

impl HKInstance for Object<'static> {}

impl Hkt for Object<'static> {
    type HktSelf<'a> = Object<'a>;
}

impl<'a> Ord for Object<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vertex().cmp(&other.vertex())
    }
}

impl<'a> PartialOrd for Object<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> fmt::Display for Object<'a> {
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
) -> (Attribute<'a>, u64) {
    let edge = ThingEdgeHas::new(storage_key.into_bytes());
    (Attribute::new(edge.into_to()), decode_value_u64(&value))
}

fn storage_key_has_edge_to_has<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Has<'a>, u64) {
    (Has::new_from_edge(ThingEdgeHas::new(storage_key.into_bytes())), decode_value_u64(&value))
}

fn storage_key_has_reverse_edge_to_has<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Has<'a>, u64) {
    (Has::new_from_edge_reverse(ThingEdgeHasReverse::new(storage_key.into_bytes())), decode_value_u64(&value))
}

edge_iterator!(
    HasAttributeIterator;
    'a -> (Attribute<'a>, u64);
    storage_key_has_edge_to_has_attribute
);

edge_iterator!(
    HasIterator;
    'a -> (Has<'a>, u64);
    storage_key_has_edge_to_has
);

edge_iterator!(
    HasReverseIterator;
    'a -> (Has<'a>, u64);
    storage_key_has_reverse_edge_to_has
);
