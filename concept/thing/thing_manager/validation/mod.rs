/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::util::HexBytesFormatter;
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use error::typedb_error;

use crate::{
    error::ConceptReadError,
    type_::{constraint::ConstraintError, owns::Owns},
};

pub(crate) mod commit_time_validation;
pub(crate) mod operation_time_validation;
pub(crate) mod validation;

typedb_error!(
    pub DataValidationError(component = "Data validation", prefix = "DVL") {
        ConceptRead(1, "Data validation failed due to concept read error.", typedb_source: Box<ConceptReadError>),
        CannotHaveOwnerInstanceForNotOwnedAttributeType(
            2,
            "Type '{owner}' cannot own attribute type '{attribute}'.",
            owner: Label,
            attribute: Label
        ),
        CannotHavePlayerInstanceForNotPlayedRoleType(
            3,
            "Type '{player}' cannot play role '{role}'.",
            player: Label,
            role: Label
        ),
        CannotHavePlayerInstanceForNotRelatedRoleType(
            4,
            "Relation type '{relation}' cannot relate '{role}'.",
            relation: Label,
            role: Label
        ),
        EntityTypeConstraintViolated(
            5,
            "Constraint on entity type '{entity_type}' was violated.",
            entity_type: Label,
            typedb_source: Box<ConstraintError>
        ),
        RelationTypeConstraintViolated(
            6,
            "Constraint on relation type '{relation_type}' was violated.",
            relation_type: Label,
            typedb_source: Box<ConstraintError>
        ),
        AttributeTypeConstraintViolated(
            7,
            "Constraint on attribute type '{attribute_type}' was violated.",
            attribute_type: Label,
            typedb_source: Box<ConstraintError>
        ),
        KeyConstraintViolatedCard(
            8,
            "Instance {owner_iid} of type '{owner_type}' has a key constraint violation for attribute ownership of '{attribute_type}', since it owns {attribute_count} instead of exactly 1.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label,
            attribute_type: Label,
            attribute_count: u64,
            constraint_source: Owns,
            typedb_source: Box<ConstraintError>
        ),
        KeyConstraintViolatedUniqueness(
            9,
            "Instance {owner_iid} of type '{owner_type}' has a key constraint violation for attribute ownership of '{attribute_type}', since it is not the unique owner of attribute '{value}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label,
            attribute_type: Label,
            value: Value<'static>,
            constraint_source: Owns,
            typedb_source: Box<ConstraintError>
        ),
        OwnsConstraintViolated(
            10,
            "Instance {owner_iid} of type '{owner_type}' has an attribute ownership constraint violation for attribute ownership of type '{attribute_type}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label,
            attribute_type: Label,
            // constraint_source: Owns
            typedb_source: Box<ConstraintError>
        ),
        RelatesConstraintViolated(
            11,
            "Instance {relation_iid} of relation type '{relation_type}' has a relates constraint violation for role type '{role_type}'.",
            relation_iid: HexBytesFormatter<'static>,
            relation_type: Label,
            role_type: Label,
            // player_iid: HexBytesFormatter<'static>,
            // constraint_source: Relates,
            typedb_source: Box<ConstraintError>
        ),
        PlaysConstraintViolated(
            12,
            "Instance {player_iid} of type '{player_type}' violated constraint for playing role type '{role_type}'.",
            player_iid: HexBytesFormatter<'static>,
            player_type: Label,
            role_type: Label,
            // relation: Option<Relation<'static>>,
            // constraint_source: Plays,
            typedb_source: Box<ConstraintError>
        ),
        UniqueValueTaken(
            13,
            "Instance {owner_iid} of type '{owner_type}' violated unique-ownership constraint for attribute '{value}' with type '{attribute_type}'.",
            owner_iid: HexBytesFormatter<'static>,
            owner_type: Label,
            attribute_type: Label,
            // taken_owner_type: Label,
            // taken_attribute_type: Label,
            value: Value<'static>
        ),
        AttributeTypeHasNoValueType(
            14,
            "Attribute type '{attribute_type}' has no defined value type, but received value '{provided_value}' of type '{provided_value_type}'.",
            attribute_type: Label,
            provided_value: Value<'static>,
            provided_value_type: ValueType
        ),
        ValueTypeMismatchWithAttributeType(
            15,
            "Attribute type '{attribute_type}' expects values of type '{expected_value_type}' but received value '{provided_value}' of type '{provided_value_type}'.",
            attribute_type: Label,
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
        SetHasDeletedAttribute(
            20,
            "Cannot set attribute ownership of a deleted attribute of type {attribute_type} on an owner {owner_iid}.",
            owner_iid: HexBytesFormatter<'static>,
            attribute_type: Label,
        ),
        AddDeletedPlayer(
            21,
            "Cannot add a deleted role player {player_iid} to a relation {relation_iid}.",
            player_iid: HexBytesFormatter<'static>,
            relation_iid: HexBytesFormatter<'static>
        ),
        RemoveDeletedPlayers(
            22,
            "Cannot delete {decrement_count} role players {player_iid} of type {role} of a relation {relation_iid}: only {current_count} role players exists.",
            player_iid: HexBytesFormatter<'static>,
            relation_iid: HexBytesFormatter<'static>,
            role: Label,
            decrement_count: u64,
            current_count: u64,
        ),
    }
);
