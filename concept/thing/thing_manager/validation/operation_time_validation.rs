/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, HashSet};

use encoding::value::{value::Value, value_type::ValueType, ValueEncodable};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::Attribute,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{
            validation::{
                validation::{get_label_or_data_err, DataValidation},
                DataValidationError,
            },
            ThingManager,
        },
    },
    type_::{
        attribute_type::AttributeType, constraint::Constraint, entity_type::EntityType, object_type::ObjectType,
        owns::Owns, plays::Plays, relates::Relates, relation_type::RelationType, role_type::RoleType, Capability,
        ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_entity_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = entity_type
            .get_constraint_abstract(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_entity_type_abstractness_error(&abstract_constraint))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relation_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = relation_type
            .get_constraint_abstract(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_relation_type_abstractness_error(&abstract_constraint))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_attribute_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = attribute_type
            .get_constraint_abstract(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_attribute_type_abstractness_error(&abstract_constraint))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_owns_is_not_abstract<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = owner
            .type_()
            .get_owned_attribute_type_constraint_abstract(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_owns_abstractness_error(
                &abstract_constraint,
                owner.clone().into_owned_object(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_plays_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: &Object<'_>,
        role_type: RoleType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = player
            .type_()
            .get_played_role_type_constraint_abstract(snapshot, thing_manager.type_manager(), role_type)
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_plays_abstractness_error(
                &abstract_constraint,
                player.as_reference(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'_>,
        role_type: RoleType<'static>,
    ) -> Result<(), DataValidationError> {
        if let Some(abstract_constraint) = relation
            .type_()
            .get_related_role_type_constraint_abstract(snapshot, thing_manager.type_manager(), role_type)
            .map_err(DataValidationError::ConceptRead)?
        {
            Err(DataValidation::create_data_validation_relates_abstractness_error(
                &abstract_constraint,
                relation.as_reference(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_object_type_plays_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), DataValidationError> {
        let has_plays = object_type
            .get_plays_role(snapshot, &thing_manager.type_manager, role_type.clone())
            .map_err(DataValidationError::ConceptRead)?
            .is_some();
        if has_plays {
            Ok(())
        } else {
            Err(DataValidationError::CannotAddPlayerInstanceForNotPlayedRoleType(
                get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
            ))
        }
    }

    pub(crate) fn validate_relation_type_relates_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), DataValidationError> {
        let has_relates = relation_type
            .get_relates_role(snapshot, &thing_manager.type_manager, role_type.clone())
            .map_err(DataValidationError::ConceptRead)?
            .is_some();
        if has_relates {
            Ok(())
        } else {
            Err(DataValidationError::CannotAddPlayerInstanceForNotRelatedRoleType(
                get_label_or_data_err(snapshot, &thing_manager.type_manager, relation_type)?,
                get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
            ))
        }
    }

    pub(crate) fn validate_object_type_owns_attribute_type<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: impl ObjectTypeAPI<'a>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), DataValidationError> {
        let has_owns = object_type
            .get_owns_attribute(snapshot, &thing_manager.type_manager, attribute_type.clone())
            .map_err(DataValidationError::ConceptRead)?
            .is_some();
        if has_owns {
            Ok(())
        } else {
            Err(DataValidationError::CannotAddOwnerInstanceForNotOwnedAttributeType(
                get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                get_label_or_data_err(snapshot, &thing_manager.type_manager, attribute_type)?,
            ))
        }
    }

    pub(crate) fn validate_relates_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation<'_>,
        role_type: RoleType<'static>,
        players_counts: &HashMap<&Object<'_>, u64>,
    ) -> Result<(), DataValidationError> {
        let distinct = relation
            .type_()
            .get_related_role_type_constraints_distinct(snapshot, thing_manager.type_manager(), role_type.clone())
            .map_err(DataValidationError::ConceptRead)?;
        if let Some(distinct_constraint) = distinct.into_iter().next() {
            for (player, count) in players_counts {
                DataValidation::validate_relates_distinct_constraint(
                    &distinct_constraint,
                    relation.as_reference(),
                    role_type.clone(),
                    player.as_reference(),
                    *count,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_owns_distinct_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        attributes_counts: &BTreeMap<&Attribute<'_>, u64>,
    ) -> Result<(), DataValidationError> {
        let distinct = owner
            .type_()
            .get_owned_attribute_type_constraints_distinct(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(DataValidationError::ConceptRead)?;
        if let Some(distinct_constraint) = distinct.into_iter().next() {
            for (attribute, count) in attributes_counts {
                DataValidation::validate_owns_distinct_constraint(
                    &distinct_constraint,
                    owner.clone().into_owned_object(),
                    attribute.as_reference(),
                    *count,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_regex_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in attribute_type
            .get_constraints_regex(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_attribute_regex_constraint(
                &constraint,
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_range_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in attribute_type
            .get_constraints_range(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_attribute_range_constraint(
                &constraint,
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_values_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in attribute_type
            .get_constraints_values(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_attribute_values_constraint(
                &constraint,
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_regex_constraints<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_regex(snapshot, thing_manager.type_manager(), attribute_type.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_owns_regex_constraint(
                &constraint,
                owner.clone().into_owned_object(),
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_range_constraints<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_range(snapshot, thing_manager.type_manager(), attribute_type.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_owns_range_constraint(
                &constraint,
                owner.clone().into_owned_object(),
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_values_constraints<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_values(snapshot, thing_manager.type_manager(), attribute_type.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            DataValidation::validate_owns_values_constraint(
                &constraint,
                owner.clone().into_owned_object(),
                attribute_type.clone(),
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_unique_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        if let Some(constraint) = owner
            .type_()
            .get_owned_attribute_type_constraint_unique(snapshot, thing_manager.type_manager(), attribute_type.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            if count > 1 {
                return Err(DataValidation::create_data_validation_uniqueness_error(
                    snapshot,
                    thing_manager.type_manager(),
                    &constraint,
                    owner.clone().into_owned_object(),
                    attribute_type,
                    value,
                ));
            }

            let root_owner_type = constraint.source().owner();
            let root_owner_subtypes = root_owner_type
                .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            let owner_and_subtypes: HashSet<ObjectType<'static>> =
                TypeAPI::chain_types(root_owner_type.clone(), root_owner_subtypes.into_iter().cloned()).collect();

            let root_attribute_type = constraint.source().attribute();
            let root_attribute_subtypes = root_attribute_type
                .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            let attribute_and_subtypes: HashSet<AttributeType<'static>> =
                TypeAPI::chain_types(root_attribute_type.clone(), root_attribute_subtypes.into_iter().cloned())
                    .collect();

            for checked_owner_type in owner_and_subtypes {
                let mut objects = thing_manager.get_objects_in(snapshot, checked_owner_type.clone());
                while let Some(object) = objects.next().transpose().map_err(DataValidationError::ConceptRead)? {
                    for checked_attribute_type in &attribute_and_subtypes {
                        if object
                            .has_attribute_with_value(
                                snapshot,
                                thing_manager,
                                checked_attribute_type.clone(),
                                value.clone(),
                            )
                            .map_err(DataValidationError::ConceptRead)?
                        {
                            return Err(DataValidation::create_data_validation_uniqueness_error(
                                snapshot,
                                thing_manager.type_manager(),
                                &constraint,
                                owner.clone().into_owned_object(),
                                attribute_type,
                                value,
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_value_type_matches_attribute_type_for_write(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        let type_value_type = attribute_type.get_value_type_without_source(snapshot, thing_manager.type_manager())?;
        if Some(value_type.clone()) == type_value_type {
            Ok(())
        } else {
            Err(ConceptWriteError::DataValidation {
                source: DataValidationError::ValueTypeMismatchWithAttributeType {
                    attribute_type,
                    expected: type_value_type,
                    provided: value_type,
                },
            })
        }
    }

    pub(crate) fn validate_value_type_matches_attribute_type_for_read(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptReadError> {
        let type_value_type = attribute_type.get_value_type_without_source(snapshot, thing_manager.type_manager())?;
        if Some(value_type.clone()) == type_value_type {
            Ok(())
        } else {
            Err(ConceptReadError::ValueTypeMismatchWithAttributeType {
                attribute_type,
                expected: type_value_type,
                provided: value_type,
            })
        }
    }

    pub(crate) fn validate_owner_exists_to_set_has<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
    ) -> Result<(), DataValidationError> {
        if thing_manager.object_exists(snapshot, owner).map_err(DataValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(DataValidationError::SetHasOnDeletedOwner { owner: owner.clone().into_owned_object() })
        }
    }

    pub(crate) fn validate_relation_exists_to_add_player(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'_>,
    ) -> Result<(), DataValidationError> {
        if thing_manager.object_exists(snapshot, relation).map_err(DataValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(DataValidationError::AddPlayerOnDeletedRelation { relation: relation.clone().into_owned() })
        }
    }

    pub(crate) fn validate_owner_exists_to_unset_has<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &impl ObjectAPI<'a>,
    ) -> Result<(), DataValidationError> {
        if thing_manager.object_exists(snapshot, owner).map_err(DataValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(DataValidationError::UnsetHasOnDeletedOwner { owner: owner.clone().into_owned_object() })
        }
    }

    pub(crate) fn validate_relation_exists_to_remove_player(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'_>,
    ) -> Result<(), DataValidationError> {
        if thing_manager.object_exists(snapshot, relation).map_err(DataValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(DataValidationError::RemovePlayerOnDeletedRelation { relation: relation.clone().into_owned() })
        }
    }
}
