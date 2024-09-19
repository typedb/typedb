/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::{label::Label, value::Value};
use storage::snapshot::ReadableSnapshot;

use crate::{
    thing::{
        attribute::Attribute,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::validation::DataValidationError,
    },
    type_::{
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint, ConstraintError, TypeConstraint},
        entity_type::EntityType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, TypeAPI,
    },
};

pub(crate) fn get_label_or_data_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, DataValidationError> {
    type_
        .get_label(snapshot, type_manager)
        .map(|label| label.clone().into_owned())
        .map_err(DataValidationError::ConceptRead)
}

macro_rules! create_data_validation_type_abstractness_error_methods {
    ($(
        fn $method_name:ident($type_decl:ident) -> $error:ident = $type_:ident;
    )*) => {
        $(
            pub(crate) fn $method_name<'a>(
                constraint: &TypeConstraint<$type_decl<'static>>,
            ) -> DataValidationError {
                debug_assert!(constraint.description().unwrap_abstract().is_ok());
                let constraint_source = constraint.source();
                let error_source = ConstraintError::ViolatedAbstract;
                DataValidationError::$error { $type_: constraint_source.clone(), constraint_source, error_source }
            }
        )*
    }
}

macro_rules! create_data_validation_capability_abstractness_error_methods {
    ($(
        fn $method_name:ident($capability_type:ident, $object_decl:ident) -> $error:ident = $object:ident + $interface_type:ident + $interface:ident;
    )*) => {
        $(
            pub(crate) fn $method_name<'a>(
                constraint: &CapabilityConstraint<$capability_type<'static>>,
                $object: $object_decl<'a>,
            ) -> DataValidationError {
                debug_assert!(constraint.description().unwrap_abstract().is_ok());
                let constraint_source = constraint.source();
                let error_source = ConstraintError::ViolatedAbstract;
                let $interface_type = constraint_source.interface();
                DataValidationError::$error { $object: $object.into_owned(), $interface_type, $interface: None, constraint_source, error_source }
            }
        )*
    }
}

pub(crate) struct DataValidation {}

impl DataValidation {
    pub(crate) fn validate_owns_instances_cardinality_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute_type: AttributeType<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        constraint.validate_cardinality(count).map_err(|error_source| {
            let constraint_source = constraint.source();
            let is_key = match constraint_source.is_key(snapshot, type_manager) {
                Ok(is_key) => is_key,
                Err(err) => return DataValidationError::ConceptRead(err),
            };
            if is_key {
                DataValidationError::KeyConstraintViolated {
                    owner: owner.into_owned(),
                    attribute_type,
                    attribute: None,
                    constraint_source,
                    error_source,
                }
            } else {
                DataValidationError::OwnsConstraintViolated {
                    owner: owner.into_owned(),
                    attribute_type,
                    attribute: None,
                    constraint_source,
                    error_source,
                }
            }
        })
    }

    pub(crate) fn validate_plays_instances_cardinality_constraint(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Plays<'static>>,
        player: Object<'_>,
        role_type: RoleType<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        constraint.validate_cardinality(count).map_err(|error_source| {
            let constraint_source = constraint.source();
            let player = player.clone().into_owned();
            DataValidationError::PlaysConstraintViolated {
                player: player.into_owned(),
                role_type,
                relation: None,
                constraint_source,
                error_source,
            }
        })
    }

    pub(crate) fn validate_relates_instances_cardinality_constraint<'a>(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Relates<'static>>,
        relation: Relation<'a>,
        role_type: RoleType<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        constraint.validate_cardinality(count).map_err(|error_source| {
            let constraint_source = constraint.source();
            let relation = relation.clone().into_owned();
            DataValidationError::RelatesConstraintViolated {
                relation: relation.into_owned(),
                role_type,
                player: None,
                constraint_source,
                error_source,
            }
        })
    }

    pub(crate) fn validate_attribute_regex_constraint(
        constraint: &TypeConstraint<AttributeType<'static>>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_regex(value).map_err(|error_source| DataValidationError::AttributeTypeConstraintViolated {
            attribute_type: attribute_type.clone(),
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_attribute_range_constraint(
        constraint: &TypeConstraint<AttributeType<'static>>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_range(value).map_err(|error_source| DataValidationError::AttributeTypeConstraintViolated {
            attribute_type: attribute_type.clone(),
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_attribute_values_constraint(
        constraint: &TypeConstraint<AttributeType<'static>>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_values(value).map_err(|error_source| DataValidationError::AttributeTypeConstraintViolated {
            attribute_type: attribute_type.clone(),
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_owns_regex_constraint(
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_regex(value).map_err(|error_source| DataValidationError::OwnsConstraintViolated {
            owner: owner.into_owned(),
            attribute_type,
            attribute: None,
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_owns_range_constraint(
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_range(value).map_err(|error_source| DataValidationError::OwnsConstraintViolated {
            owner: owner.into_owned(),
            attribute_type,
            attribute: None,
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_owns_values_constraint(
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        constraint.validate_values(value).map_err(|error_source| DataValidationError::OwnsConstraintViolated {
            owner: owner.into_owned(),
            attribute_type,
            attribute: None,
            constraint_source: constraint.source(),
            error_source,
        })
    }

    pub(crate) fn validate_owns_distinct_constraint(
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute: Attribute<'_>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        debug_assert!(constraint.description().unwrap_distinct().is_ok());
        CapabilityConstraint::<Owns<'static>>::validate_distinct(count).map_err(|error_source| {
            DataValidationError::OwnsConstraintViolated {
                owner: owner.into_owned(),
                attribute_type: attribute.type_(),
                attribute: Some(attribute.into_owned()),
                constraint_source: constraint.source(),
                error_source,
            }
        })
    }

    pub(crate) fn validate_relates_distinct_constraint(
        constraint: &CapabilityConstraint<Relates<'static>>,
        relation: Relation<'_>,
        role_type: RoleType<'static>,
        player: Object<'_>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        debug_assert!(constraint.description().unwrap_distinct().is_ok());
        CapabilityConstraint::<Relates<'static>>::validate_distinct(count).map_err(|error_source| {
            DataValidationError::RelatesConstraintViolated {
                relation: relation.into_owned(),
                role_type,
                player: Some(player.into_owned()),
                constraint_source: constraint.source(),
                error_source,
            }
        })
    }

    create_data_validation_type_abstractness_error_methods! {
        fn create_data_validation_entity_type_abstractness_error(EntityType) -> EntityTypeConstraintViolated = entity_type;
        fn create_data_validation_relation_type_abstractness_error(RelationType) -> RelationTypeConstraintViolated = relation_type;
        fn create_data_validation_attribute_type_abstractness_error(AttributeType) -> AttributeTypeConstraintViolated = attribute_type;
    }

    create_data_validation_capability_abstractness_error_methods! {
        fn create_data_validation_owns_abstractness_error(Owns, Object) -> OwnsConstraintViolated = owner + attribute_type + attribute;
        fn create_data_validation_plays_abstractness_error(Plays, Object) -> PlaysConstraintViolated = player + role_type + relation;
        fn create_data_validation_relates_abstractness_error(Relates, Relation) -> RelatesConstraintViolated = relation + role_type + player;
    }

    pub(crate) fn create_data_validation_uniqueness_error(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns<'static>>,
        owner: Object<'_>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> DataValidationError {
        debug_assert!(constraint.description().unwrap_unique().is_ok());
        let constraint_source = constraint.source();
        let error_source = ConstraintError::ViolatedUnique { value: value.into_owned() };
        let is_key = match constraint_source.is_key(snapshot, type_manager) {
            Ok(is_key) => is_key,
            Err(err) => return DataValidationError::ConceptRead(err),
        };
        if is_key {
            DataValidationError::KeyConstraintViolated {
                owner: owner.into_owned(),
                attribute_type,
                attribute: None,
                constraint_source,
                error_source,
            }
        } else {
            DataValidationError::OwnsConstraintViolated {
                owner: owner.into_owned(),
                attribute_type,
                attribute: None,
                constraint_source,
                error_source,
            }
        }
    }
}
