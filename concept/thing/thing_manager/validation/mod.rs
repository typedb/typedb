/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use bytes::util::HexBytesFormatter;
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use error::typedb_error;

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

typedb_error!(
    pub DataValidationError(component = "Data validation", prefix = "DVL") {
        ConceptRead(1, "Data validation failed due to concept read error.", ( source: ConceptReadError)),
        CannotAddOwnerInstanceForNotOwnedAttributeType(
            2,
            "Type '{owner}' cannot own attribute type '{attribute}'.",
            owner: Label<'static>,
            attribute: Label<'static>
        ),
        CannotAddPlayerInstanceForNotPlayedRoleType(
            3,
            "Type '{player}' cannot play role '{role}'.",
            player: Label<'static>,
            role: Label<'static>
        ),
        CannotAddPlayerInstanceForNotRelatedRoleType(
            4,
            "Relation type '{relation}' cannot relate '{role}'.",
            relation: Label<'static>,
            role: Label<'static>
        ),
        EntityTypeConstraintViolated(
            5,
            "Constraint on entity type '{entity_type}' was violated.",
            entity_type: Label<'static>,
            (source: ConstraintError)
        ),
        RelationTypeConstraintViolated(
            6,
            "Constraint on relation type '{relation_type}' was violated.",
            relation_type: Label<'static>,
            (source: ConstraintError)
        ),
        AttributeTypeConstraintViolated(
            7,
            "Constraint on attribute type '{attribute_type}' was violated.",
            attribute_type: Label<'static>,
            (source: ConstraintError)
        ),
        KeyConstraintViolatedCard(
            8,
            "Instance {owner_iid} of type '{owner_type}' has a key constraint violation for attribute ownership of '{attribute_type}', since it owns {attribute_count} instead of exactly 1.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label<'static>,
            attribute_type: Label<'static>,
            attribute_count: u64,
            constraint_source: Owns<'static>,
            (source: ConstraintError)
        ),
        KeyConstraintViolatedUniqueness(
            9,
            "Instance {owner_iid} of type '{owner_type}' has a key constraint violation for attribute ownership of '{attribute_type}', since it is not the unique owner of attribute '{value}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label<'static>,
            attribute_type: Label<'static>,
            value: Value<'static>,
            constraint_source: Owns<'static>,
            (source: ConstraintError)
        ),
        OwnsConstraintViolated(
            10,
            "Instance {owner_iid} of type '{owner_type}' has an attribute ownership constraint violation for attribute ownership of type '{attribute_type}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label<'static>,
            attribute_type: Label<'static>,
            // constraint_source: Owns<'static>,
            (source: ConstraintError)
        ),
        RelatesConstraintViolated(
            11,
            "Instance {relation_iid} of relation type '{relation_type}' has an relates constraint violation for role type '{role_type}'.",
            relation_iid: HexBytesFormatter<'static>,
            relation_type: Label<'static>,
            role_type: Label<'static>,
            // player_iid: HexBytesFormatter<'static>,
            // constraint_source: Relates<'static>,
            (source: ConstraintError)
        ),
        PlaysConstraintViolated(
            12,
            "Instance {player_iid} of type '{player_type}' violated constraint for playing role type '{role_type}'.",
            player_iid: HexBytesFormatter<'static>,
            player_type: Label<'static>,
            role_type: Label<'static>,
            // relation: Option<Relation<'static>>,
            // constraint_source: Plays<'static>,
            (source: ConstraintError)
        ),
        UniqueValueTaken(
            13,
            "Instance {owner_iid} of type '{owner_type}' violated unique-ownership constraint for attribute '{value}' with type '{attribute_type}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label<'static>,
            attribute_type: Label<'static>,
            // taken_owner_type: Label<'static>,
            // taken_attribute_type: Label<'static>,
            value: Value<'static>
        ),
        AttributeTypeHasNoValueType(
            14,
            "Attribute type '{attribute_type}' has no defined value type, but received value '{provided_value}' of type '{provided_value_type}'.",
            attribute_type: Label<'static>,
            provided_value: Value<'static>,
            provided_value_type: ValueType
        ),
        ValueTypeMismatchWithAttributeType(
            15,
            "Attribute type '{attribute_type}' expects values of type '{expected_value_type}' but received value '{provided_value}' of type '{provided_value_type}'.",
            attribute_type: Label<'static>,
            expected_value_type: ValueType,
            provided_value: Value<'static>,
            provided_value_type: ValueType
        ),
        SetHasOnDeletedOwner(
            16,
            "Cannot set attribute ownership on an owner {owner_iid} that has already been deleted.",
            owner_iid: HexBytesFormatter<'static>
        ),
        UnsetHasOnDeletedOwner(
            17,
            "Cannot unset attribute ownership on an owner {owner_iid} that has already been deleted.",
            owner_iid: HexBytesFormatter<'static>
        ),
        AddPlayerOnDeletedRelation(
            18,
            "Cannot add role player on to a relation {relation_iid} that has already been deleted.",
            relation_iid: HexBytesFormatter<'static>
        ),
        RemovePlayerOnDeletedRelation(
            19,
            "Cannot remove role player from a relation {relation_iid} that has already been deleted.",
            relation_iid: HexBytesFormatter<'static>
        ),
    }
);
