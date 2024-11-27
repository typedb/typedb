/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::util::HexBytesFormatter;
use encoding::value::{label::Label, value::Value};
use storage::snapshot::ReadableSnapshot;

use crate::{
    thing::{
        attribute::Attribute, object::Object, relation::Relation, thing_manager::validation::DataValidationError,
        ThingAPI,
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
) -> Result<Label<'static>, Box<DataValidationError>> {
    type_
        .get_label(snapshot, type_manager)
        .map(|label| label.clone().into_owned())
        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))
}

macro_rules! create_data_validation_type_abstractness_error_methods {
    ($(
        fn $method_name:ident($type_decl:ident) -> $error:ident = $type_:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                constraint: &TypeConstraint<$type_decl>,
                snapshot: &impl ReadableSnapshot,
                type_manager: &TypeManager,
            ) -> Box<DataValidationError> {
                debug_assert!(constraint.description().unwrap_abstract().is_ok());
                let constraint_source = constraint.source();
                let typedb_source = Box::new(ConstraintError::ViolatedAbstract {});
                Box::new(DataValidationError::$error {
                    $type_: constraint_source.get_label(snapshot, type_manager).unwrap().to_owned(),
                    typedb_source
                })
            }
        )*
    }
}

macro_rules! create_data_validation_capability_abstractness_error_methods {
    ($(
        fn $method_name:ident($capability_type:ident, $object_decl:ident) -> $error:ident = $object:ident + $object_type:ident + $interface_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                constraint: &CapabilityConstraint<$capability_type>,
                $object: $object_decl<'_>,
                snapshot: &impl ReadableSnapshot,
                type_manager: &TypeManager,
            ) -> Box<DataValidationError> {
                debug_assert!(constraint.description().unwrap_abstract().is_ok());
                let constraint_source = constraint.source();
                let typedb_source = Box::new(ConstraintError::ViolatedAbstract {});
                let $interface_type = constraint_source.interface();
                Box::new(DataValidationError::$error {
                    $object: HexBytesFormatter::owned(Vec::from($object.iid())),
                    $object_type: $object.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                    $interface_type: $interface_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                    // constraint_source,
                    typedb_source
                })
            }
        )*
    }
}

pub(crate) struct DataValidation {}

impl DataValidation {
    pub(crate) fn validate_owns_instances_cardinality_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute_type: AttributeType,
        count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_cardinality(count).map_err(|typedb_source| {
            let constraint_source = constraint.source();
            let is_key = match constraint_source.is_key(snapshot, type_manager) {
                Ok(is_key) => is_key,
                Err(err) => return Box::new(DataValidationError::ConceptRead { source: err }),
            };
            if is_key {
                Box::new(DataValidationError::KeyConstraintViolatedCard {
                    owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                    owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                    attribute_type: attribute_type
                        .get_label(snapshot, type_manager)
                        .unwrap()
                        .as_reference()
                        .into_owned(),
                    attribute_count: count,
                    constraint_source,
                    typedb_source,
                })
            } else {
                Box::new(DataValidationError::OwnsConstraintViolated {
                    owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                    owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                    attribute_type: attribute_type
                        .get_label(snapshot, type_manager)
                        .unwrap()
                        .as_reference()
                        .into_owned(),
                    // constraint_source,
                    typedb_source,
                })
            }
        })
    }

    pub(crate) fn validate_plays_instances_cardinality_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Plays>,
        player: Object<'_>,
        role_type: RoleType,
        count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_cardinality(count).map_err(|typedb_source| {
            let player = player.clone().into_owned();
            Box::new(DataValidationError::PlaysConstraintViolated {
                player_iid: HexBytesFormatter::owned(Vec::from(player.iid())),
                player_type: player.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                role_type: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_relates_instances_cardinality_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Relates>,
        relation: Relation<'_>,
        role_type: RoleType,
        count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_cardinality(count).map_err(|typedb_source| {
            let relation = relation.clone().into_owned();
            Box::new(DataValidationError::RelatesConstraintViolated {
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
                relation_type: relation.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                role_type: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_attribute_regex_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &TypeConstraint<AttributeType>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_regex(value).map_err(|typedb_source| {
            Box::new(DataValidationError::AttributeTypeConstraintViolated {
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source().get_label(snapshot, type_manager).unwrap().to_owned(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_attribute_range_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &TypeConstraint<AttributeType>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_range(value).map_err(|typedb_source| {
            Box::new(DataValidationError::AttributeTypeConstraintViolated {
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source().get_label(snapshot, type_manager).unwrap().to_owned(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_attribute_values_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &TypeConstraint<AttributeType>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_values(value).map_err(|typedb_source| {
            Box::new(DataValidationError::AttributeTypeConstraintViolated {
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source().get_label(snapshot, type_manager).unwrap().to_owned(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_owns_regex_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_regex(value).map_err(|typedb_source| {
            Box::new(DataValidationError::OwnsConstraintViolated {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_owns_range_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_range(value).map_err(|typedb_source| {
            Box::new(DataValidationError::OwnsConstraintViolated {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_owns_values_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        constraint.validate_values(value).map_err(|typedb_source| {
            Box::new(DataValidationError::OwnsConstraintViolated {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_owns_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute: Attribute<'_>,
        count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        debug_assert!(constraint.description().unwrap_distinct().is_ok());
        CapabilityConstraint::<Owns>::validate_distinct(count).map_err(|typedb_source| {
            Box::new(DataValidationError::OwnsConstraintViolated {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute
                    .type_()
                    .get_label(snapshot, type_manager)
                    .unwrap()
                    .as_reference()
                    .into_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    pub(crate) fn validate_relates_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Relates>,
        relation: Relation<'_>,
        role_type: RoleType,
        _player: Object<'_>,
        count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        debug_assert!(constraint.description().unwrap_distinct().is_ok());
        CapabilityConstraint::<Relates>::validate_distinct(count).map_err(|typedb_source| {
            Box::new(DataValidationError::RelatesConstraintViolated {
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
                relation_type: relation.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                role_type: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        })
    }

    create_data_validation_type_abstractness_error_methods! {
        fn create_data_validation_entity_type_abstractness_error(EntityType) -> EntityTypeConstraintViolated = entity_type;
        fn create_data_validation_relation_type_abstractness_error(RelationType) -> RelationTypeConstraintViolated = relation_type;
        fn create_data_validation_attribute_type_abstractness_error(AttributeType) -> AttributeTypeConstraintViolated = attribute_type;
    }

    create_data_validation_capability_abstractness_error_methods! {
        fn create_data_validation_owns_abstractness_error(Owns, Object) -> OwnsConstraintViolated = owner_iid + owner_type + attribute_type;
        fn create_data_validation_plays_abstractness_error(Plays, Object) -> PlaysConstraintViolated = player_iid + player_type + role_type;
        fn create_data_validation_relates_abstractness_error(Relates, Relation) -> RelatesConstraintViolated = relation_iid + relation_type + role_type;
    }

    pub(crate) fn create_data_validation_uniqueness_error(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<Owns>,
        owner: Object<'_>,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Box<DataValidationError> {
        debug_assert!(constraint.description().unwrap_unique().is_ok());
        let constraint_source = constraint.source();
        let typedb_source = Box::new(ConstraintError::ViolatedUnique { value: value.clone().into_owned() });
        let is_key = match constraint_source.is_key(snapshot, type_manager) {
            Ok(is_key) => is_key,
            Err(err) => return Box::new(DataValidationError::ConceptRead { source: err }),
        };
        if is_key {
            Box::new(DataValidationError::KeyConstraintViolatedUniqueness {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                value: value.into_owned(),
                constraint_source: constraint.source(),
                typedb_source,
            })
        } else {
            Box::new(DataValidationError::OwnsConstraintViolated {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                owner_type: owner.type_().get_label(snapshot, type_manager).unwrap().to_owned(),
                attribute_type: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
                // constraint_source: constraint.source(),
                typedb_source,
            })
        }
    }
}
