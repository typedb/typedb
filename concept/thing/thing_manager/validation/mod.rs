/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use encoding::value::{label::Label, value::Value, value_type::ValueType};

use crate::{
    error::ConceptReadError,
    thing::{attribute::Attribute, object::Object, relation::Relation},
    type_::{
        attribute_type::AttributeType, constraint::ConstraintError, entity_type::EntityType, object_type::ObjectType,
        owns::Owns, plays::Plays, relates::Relates, relation_type::RelationType, role_type::RoleType,
    },
};

pub(crate) mod commit_time_validation;
pub(crate) mod operation_time_validation;
pub(crate) mod validation;

#[derive(Debug, Clone)]
pub enum DataValidationError {
    ConceptRead(ConceptReadError),
    CannotAddOwnerInstanceForNotOwnedAttributeType(Label<'static>, Label<'static>),
    CannotAddPlayerInstanceForNotPlayedRoleType(Label<'static>, Label<'static>),
    CannotAddPlayerInstanceForNotRelatedRoleType(Label<'static>, Label<'static>),
    EntityTypeConstraintViolated {
        entity_type: EntityType<'static>,
        constraint_source: EntityType<'static>,
        error_source: ConstraintError,
    },
    RelationTypeConstraintViolated {
        relation_type: RelationType<'static>,
        constraint_source: RelationType<'static>,
        error_source: ConstraintError,
    },
    AttributeTypeConstraintViolated {
        attribute_type: AttributeType<'static>,
        constraint_source: AttributeType<'static>,
        error_source: ConstraintError,
    },
    KeyConstraintViolated {
        owner: Object<'static>,
        attribute_type: AttributeType<'static>,
        attribute: Option<Attribute<'static>>,
        constraint_source: Owns<'static>,
        error_source: ConstraintError,
    },
    OwnsConstraintViolated {
        owner: Object<'static>,
        attribute_type: AttributeType<'static>,
        attribute: Option<Attribute<'static>>,
        constraint_source: Owns<'static>,
        error_source: ConstraintError,
    },
    RelatesConstraintViolated {
        relation: Relation<'static>,
        role_type: RoleType<'static>,
        player: Option<Object<'static>>,
        constraint_source: Relates<'static>,
        error_source: ConstraintError,
    },
    PlaysConstraintViolated {
        player: Object<'static>,
        role_type: RoleType<'static>,
        relation: Option<Relation<'static>>,
        constraint_source: Plays<'static>,
        error_source: ConstraintError,
    },
    UniqueValueTaken {
        owner_type: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
        taken_owner_type: ObjectType<'static>,
        taken_attribute_type: AttributeType<'static>,
        value: Value<'static>,
    },
    ValueTypeMismatchWithAttributeType {
        attribute_type: AttributeType<'static>,
        expected: Option<ValueType>,
        provided: ValueType,
    },
    SetHasOnDeletedOwner {
        owner: Object<'static>,
    },
    UnsetHasOnDeletedOwner {
        owner: Object<'static>,
    },
    AddPlayerOnDeletedRelation {
        relation: Relation<'static>,
    },
    RemovePlayerOnDeletedRelation {
        relation: Relation<'static>,
    },
}

impl fmt::Display for DataValidationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DataValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead(source) => Some(source),
            Self::CannotAddOwnerInstanceForNotOwnedAttributeType(_, _) => None,
            Self::CannotAddPlayerInstanceForNotPlayedRoleType(_, _) => None,
            Self::CannotAddPlayerInstanceForNotRelatedRoleType(_, _) => None,
            Self::EntityTypeConstraintViolated { error_source, .. } => Some(error_source),
            Self::RelationTypeConstraintViolated { error_source, .. } => Some(error_source),
            Self::AttributeTypeConstraintViolated { error_source, .. } => Some(error_source),
            Self::KeyConstraintViolated { error_source, .. } => Some(error_source),
            Self::OwnsConstraintViolated { error_source, .. } => Some(error_source),
            Self::RelatesConstraintViolated { error_source, .. } => Some(error_source),
            Self::PlaysConstraintViolated { error_source, .. } => Some(error_source),
            Self::UniqueValueTaken { .. } => None,
            Self::ValueTypeMismatchWithAttributeType { .. } => None,
            Self::SetHasOnDeletedOwner { .. } => None,
            Self::UnsetHasOnDeletedOwner { .. } => None,
            Self::AddPlayerOnDeletedRelation { .. } => None,
            Self::RemovePlayerOnDeletedRelation { .. } => None,
        }
    }
}
