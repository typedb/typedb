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
        annotation::{AnnotationCardinality, AnnotationRange, AnnotationRegex, AnnotationValues},
        attribute_type::AttributeType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
        Ordering,
    },
};

pub(crate) mod commit_time_validation;
pub(crate) mod operation_time_validation;
pub(crate) mod validation;

#[derive(Debug, Clone)]
pub enum DataValidationError {
    ConceptRead(ConceptReadError),
    CannotCreateInstanceOfAbstractType(Label<'static>),
    CannotAddOwnerInstanceForNotOwnedAttributeType(Label<'static>, Label<'static>),
    CannotAddPlayerInstanceForNotPlayedRoleType(Label<'static>, Label<'static>),
    CannotAddPlayerInstanceForNotRelatedRoleType(Label<'static>, Label<'static>),
    PlayerViolatesDistinctRelatesConstraint {
        role_type: RoleType<'static>,
        player: Object<'static>,
        count: u64,
    },
    AttributeViolatesDistinctOwnsConstraint {
        owns: Owns<'static>,
        attribute: Attribute<'static>,
        count: u64,
    },
    AttributeViolatesRegexConstraint {
        attribute_type: AttributeType<'static>,
        value: Value<'static>,
        regex: AnnotationRegex,
    },
    AttributeViolatesRangeConstraint {
        attribute_type: AttributeType<'static>,
        value: Value<'static>,
        range: AnnotationRange,
    },
    AttributeViolatesValuesConstraint {
        attribute_type: AttributeType<'static>,
        value: Value<'static>,
        values: AnnotationValues,
    },
    HasViolatesRegexConstraint {
        owns: Owns<'static>,
        value: Value<'static>,
        regex: AnnotationRegex,
    },
    HasViolatesRangeConstraint {
        owns: Owns<'static>,
        value: Value<'static>,
        range: AnnotationRange,
    },
    HasViolatesValuesConstraint {
        owns: Owns<'static>,
        value: Value<'static>,
        values: AnnotationValues,
    },
    KeyValueTaken {
        owner_type: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
        taken_owner_type: ObjectType<'static>,
        taken_attribute_type: AttributeType<'static>,
        value: Value<'static>,
    },
    UniqueValueTaken {
        owner_type: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
        taken_owner_type: ObjectType<'static>,
        taken_attribute_type: AttributeType<'static>,
        value: Value<'static>,
    },
    KeyCardinalityViolated {
        owner: Object<'static>,
        owns: Owns<'static>,
        count: u64,
    },
    OwnsCardinalityViolated {
        owner: Object<'static>,
        owns: Owns<'static>,
        count: u64,
        cardinality: AnnotationCardinality,
    },
    RelatesCardinalityViolated {
        relation: Relation<'static>,
        relates: Relates<'static>,
        count: u64,
        cardinality: AnnotationCardinality,
    },
    PlaysCardinalityViolated {
        player: Object<'static>,
        plays: Plays<'static>,
        count: u64,
        cardinality: AnnotationCardinality,
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
            Self::CannotCreateInstanceOfAbstractType(_) => None,
            Self::CannotAddOwnerInstanceForNotOwnedAttributeType(_, _) => None,
            Self::CannotAddPlayerInstanceForNotPlayedRoleType(_, _) => None,
            Self::CannotAddPlayerInstanceForNotRelatedRoleType(_, _) => None,
            Self::PlayerViolatesDistinctRelatesConstraint { .. } => None,
            Self::AttributeViolatesDistinctOwnsConstraint { .. } => None,
            Self::AttributeViolatesRegexConstraint { .. } => None,
            Self::AttributeViolatesRangeConstraint { .. } => None,
            Self::AttributeViolatesValuesConstraint { .. } => None,
            Self::HasViolatesRegexConstraint { .. } => None,
            Self::HasViolatesRangeConstraint { .. } => None,
            Self::HasViolatesValuesConstraint { .. } => None,
            Self::KeyValueTaken { .. } => None,
            Self::UniqueValueTaken { .. } => None,
            Self::KeyCardinalityViolated { .. } => None,
            Self::OwnsCardinalityViolated { .. } => None,
            Self::RelatesCardinalityViolated { .. } => None,
            Self::PlaysCardinalityViolated { .. } => None,
            Self::ValueTypeMismatchWithAttributeType { .. } => None,
            Self::SetHasOnDeletedOwner { .. } => None,
            Self::UnsetHasOnDeletedOwner { .. } => None,
            Self::AddPlayerOnDeletedRelation { .. } => None,
            Self::RemovePlayerOnDeletedRelation { .. } => None,
        }
    }
}
