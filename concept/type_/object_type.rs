/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::graph::type_::vertex::TypeVertex;
use encoding::layout::prefix::Prefix;
use encoding::Prefixed;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{ConceptAPI, error::ConceptReadError, type_::{
    attribute_type::AttributeType, entity_type::EntityType, OwnerAPI, owns::Owns, PlayerAPI,
    plays::Plays, relation_type::RelationType, role_type::RoleType, type_manager::TypeManager,
}};
use crate::type_::{Ordering, TypeAPI};
use crate::error::ConceptWriteError;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ObjectType<'a> {
    Entity(EntityType<'a>),
    Relation(RelationType<'a>),
}

impl<'a> ObjectType<'a> {
    pub(crate) fn new(vertex: TypeVertex<'a>) -> Self {
        match vertex.prefix() {
            Prefix::VertexEntityType => ObjectType::Entity(EntityType::new(vertex)),
            Prefix::VertexRelationType => ObjectType::Relation(RelationType::new(vertex)),
            _ => unreachable!("Object type creation requires either entity type or relation type vertex."),
        }
    }
}

impl<'a> OwnerAPI<'a> for ObjectType<'a> {
    fn set_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, ConceptWriteError> {
        // TODO: decide behaviour (ok or error) if already owning
        match self {
            ObjectType::Entity(entity) => entity.set_owns(snapshot, type_manager, attribute_type, ordering),
            ObjectType::Relation(relation) => relation.set_owns(snapshot, type_manager, attribute_type, ordering),
        }
    }

    fn delete_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>
    ) -> Result<(), ConceptWriteError> {
        match self {
            ObjectType::Entity(entity) => entity.delete_owns(snapshot, type_manager, attribute_type),
            ObjectType::Relation(relation) => relation.delete_owns(snapshot, type_manager, attribute_type),
        }
    }

    fn get_owns<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_owns(snapshot, type_manager),
            ObjectType::Relation(relation) => relation.get_owns(snapshot, type_manager),
        }
    }

    fn get_owns_attribute<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_owns_attribute(snapshot, type_manager, attribute_type),
            ObjectType::Relation(relation) => relation.get_owns_attribute(snapshot, type_manager, attribute_type),
        }
    }

    fn get_owns_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_owns_transitive(snapshot, type_manager),
            ObjectType::Relation(relation) => relation.get_owns_transitive(snapshot, type_manager),
        }
    }
}

impl<'a> ConceptAPI<'a> for ObjectType<'a> {}

impl<'a> TypeAPI<'a> for ObjectType<'a> {
    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        match self {
            ObjectType::Entity(entity) => entity.vertex(),
            ObjectType::Relation(relation) => relation.vertex(),
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        match self {
            ObjectType::Entity(entity) => entity.into_vertex(),
            ObjectType::Relation(relation) => relation.into_vertex(),
        }
    }

    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.is_abstract(snapshot, type_manager),
            ObjectType::Relation(relation) => relation.is_abstract(snapshot, type_manager)
        }
    }

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>
    ) -> Result<(), ConceptWriteError> {
        match self {
            ObjectType::Entity(entity) => entity.delete(snapshot, type_manager),
            ObjectType::Relation(relation) => relation.delete(snapshot, type_manager)
        }
    }
}

impl<'a> PlayerAPI<'a> for ObjectType<'a> {
    fn set_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>
    ) -> Result<Plays<'static>, ConceptWriteError> {
        match self {
            ObjectType::Entity(entity) => entity.set_plays(snapshot, type_manager, role_type),
            ObjectType::Relation(relation) => relation.set_plays(snapshot, type_manager, role_type),
        }
    }

    fn delete_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>
    ) -> Result<(), ConceptWriteError> {
        match self {
            ObjectType::Entity(entity) => entity.delete_plays(snapshot, type_manager, role_type),
            ObjectType::Relation(relation) => relation.delete_plays(snapshot, type_manager, role_type),
        }
    }

    fn get_plays<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_plays(snapshot, type_manager),
            ObjectType::Relation(relation) => relation.get_plays(snapshot, type_manager),
        }
    }

    fn get_plays_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_plays_role(snapshot, type_manager, role_type),
            ObjectType::Relation(relation) => relation.get_plays_role(snapshot, type_manager, role_type),
        }
    }
}
