/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, Bound, HashMap, HashSet};

use bytes::util::HexBytesFormatter;
use encoding::value::{value::Value, value_type::ValueType};
use iterator::minmax_or;
use resource::profile::StorageCounters;
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
        ThingAPI,
    },
    type_::{
        attribute_type::AttributeType, constraint::Constraint, entity_type::EntityType, object_type::ObjectType,
        relation_type::RelationType, role_type::RoleType, ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_entity_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) =
            entity_type
                .get_constraint_abstract(snapshot, thing_manager.type_manager())
                .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_entity_type_abstractness_error(
                &abstract_constraint,
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relation_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) = relation_type
            .get_constraint_abstract(snapshot, thing_manager.type_manager())
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_relation_type_abstractness_error(
                &abstract_constraint,
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_attribute_type_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) = attribute_type
            .get_constraint_abstract(snapshot, thing_manager.type_manager())
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_attribute_type_abstractness_error(
                &abstract_constraint,
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_owns_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) = owner
            .type_()
            .get_owned_attribute_type_constraint_abstract(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_owns_abstractness_error(
                &abstract_constraint,
                owner.into_object(),
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_plays_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: Object,
        role_type: RoleType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) = player
            .type_()
            .get_played_role_type_constraint_abstract(snapshot, thing_manager.type_manager(), role_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_plays_abstractness_error(
                &abstract_constraint,
                player,
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        role_type: RoleType,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(abstract_constraint) = relation
            .type_()
            .get_related_role_type_constraint_abstract(snapshot, thing_manager.type_manager(), role_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            Err(DataValidation::create_data_validation_relates_abstractness_error(
                &abstract_constraint,
                relation,
                snapshot,
                thing_manager.type_manager(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_object_type_plays_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: ObjectType,
        role_type: RoleType,
    ) -> Result<(), Box<DataValidationError>> {
        let has_plays = object_type
            .get_plays_role(snapshot, &thing_manager.type_manager, role_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
            .is_some();
        if has_plays {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::CannotHavePlayerInstanceForNotPlayedRoleType {
                player: get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                role: get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
            }))
        }
    }

    pub(crate) fn validate_relation_type_relates_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
        role_type: RoleType,
    ) -> Result<(), Box<DataValidationError>> {
        let has_relates = relation_type
            .get_relates_role(snapshot, &thing_manager.type_manager, role_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
            .is_some();
        if has_relates {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::CannotHavePlayerInstanceForNotRelatedRoleType {
                relation: get_label_or_data_err(snapshot, &thing_manager.type_manager, relation_type)?,
                role: get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
            }))
        }
    }

    pub(crate) fn validate_object_type_owns_attribute_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: impl ObjectTypeAPI,
        attribute_type: AttributeType,
    ) -> Result<(), Box<DataValidationError>> {
        let has_owns = object_type
            .get_owns_attribute(snapshot, &thing_manager.type_manager, attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
            .is_some();
        if has_owns {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::CannotHaveOwnerInstanceForNotOwnedAttributeType {
                owner: get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                attribute: get_label_or_data_err(snapshot, &thing_manager.type_manager, attribute_type)?,
            }))
        }
    }

    pub(crate) fn validate_relates_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        role_type: RoleType,
        players_counts: &HashMap<Object, u64>,
    ) -> Result<(), Box<DataValidationError>> {
        let distinct = relation
            .type_()
            .get_related_role_type_constraints_distinct(snapshot, thing_manager.type_manager(), role_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
        if let Some(distinct_constraint) = distinct.into_iter().next() {
            for (&player, &count) in players_counts {
                DataValidation::validate_relates_distinct_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    &distinct_constraint,
                    relation,
                    role_type,
                    player,
                    count,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_owns_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        attributes_counts: &BTreeMap<&Attribute, u64>,
    ) -> Result<(), Box<DataValidationError>> {
        let distinct = owner
            .type_()
            .get_owned_attribute_type_constraints_distinct(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
        if let Some(distinct_constraint) = distinct.into_iter().next() {
            for (&attribute, &count) in attributes_counts {
                DataValidation::validate_owns_distinct_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    &distinct_constraint,
                    owner.into_object(),
                    attribute,
                    count,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_regex_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in attribute_type
            .get_constraints_regex(snapshot, thing_manager.type_manager())
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_attribute_regex_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_range_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in attribute_type
            .get_constraints_range(snapshot, thing_manager.type_manager())
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_attribute_range_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_values_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in attribute_type
            .get_constraints_values(snapshot, thing_manager.type_manager())
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_attribute_values_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_regex_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_regex(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_owns_regex_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                owner.into_object(),
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_range_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_range(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_owns_range_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                owner.into_object(),
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_values_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
    ) -> Result<(), Box<DataValidationError>> {
        for constraint in owner
            .type_()
            .get_owned_attribute_type_constraints_values(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            DataValidation::validate_owns_values_constraint(
                snapshot,
                thing_manager.type_manager(),
                &constraint,
                owner.into_object(),
                attribute_type,
                value.as_reference(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_has_unique_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute_type: AttributeType,
        value: Value<'_>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if let Some(constraint) = owner
            .type_()
            .get_owned_attribute_type_constraint_unique(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
        {
            let owner = owner.into_object();
            let root_owner_type = constraint.source().owner();
            let root_owner_subtypes =
                root_owner_type
                    .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
            let owner_and_subtypes: HashSet<ObjectType> =
                TypeAPI::chain_types(root_owner_type, root_owner_subtypes.into_iter().cloned()).collect();
            let (owner_type_min, owner_type_max) = minmax_or!(
                TypeAPI::chain_types(root_owner_type, root_owner_subtypes.into_iter().cloned()),
                unreachable!("Expected at least one object type")
            );
            let owner_type_range = (Bound::Included(owner_type_min), Bound::Included(owner_type_max));

            let root_attribute_type = constraint.source().attribute();
            let root_attribute_subtypes = root_attribute_type
                .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
            let attribute_and_subtypes =
                TypeAPI::chain_types(root_attribute_type, root_attribute_subtypes.into_iter().cloned());

            for attribute_type in attribute_and_subtypes {
                if let Some(attribute) = thing_manager
                    .get_attribute_with_value(snapshot, attribute_type, value.clone(), storage_counters.clone())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                {
                    let mut has_iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                        snapshot,
                        &attribute,
                        &owner_type_range,
                        storage_counters.clone(),
                    );

                    while let Some((has, _)) = has_iterator
                        .next()
                        .transpose()
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                    {
                        // Iterator can return types outside the list based on the storage specifics
                        if has.owner() != owner && owner_and_subtypes.contains(&has.owner().type_()) {
                            return Err(DataValidation::create_data_validation_uniqueness_error(
                                snapshot,
                                thing_manager.type_manager(),
                                &constraint,
                                owner.into_object(),
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
        attribute_type: AttributeType,
        value_type: ValueType,
        value: Value<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let type_value_type = attribute_type.get_value_type_without_source(snapshot, thing_manager.type_manager())?;
        match type_value_type {
            Some(type_value_type) if value_type.is_trivially_castable_to(type_value_type.category()) => Ok(()),
            Some(type_value_type) => Err(Box::new(ConceptWriteError::DataValidation {
                typedb_source: Box::new(DataValidationError::ValueTypeMismatchWithAttributeType {
                    attribute_type: attribute_type.get_label(snapshot, thing_manager.type_manager())?.clone(),
                    expected_value_type: type_value_type,
                    provided_value_type: value_type,
                    provided_value: value.into_owned(),
                }),
            })),
            None => Err(Box::new(ConceptWriteError::DataValidation {
                typedb_source: Box::new(DataValidationError::AttributeTypeHasNoValueType {
                    attribute_type: attribute_type.get_label(snapshot, thing_manager.type_manager())?.clone(),
                    provided_value_type: value_type,
                    provided_value: value.into_owned(),
                }),
            })),
        }
    }

    pub(crate) fn validate_value_type_matches_attribute_type_for_read(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value_type: ValueType,
    ) -> Result<(), Box<ConceptReadError>> {
        let type_value_type = attribute_type.get_value_type_without_source(snapshot, thing_manager.type_manager())?;
        match type_value_type {
            Some(type_value_type) if value_type.is_trivially_castable_to(type_value_type.category()) => Ok(()),
            _ => Err(Box::new(ConceptReadError::ValueTypeMismatchWithAttributeType {
                attribute_type,
                expected: type_value_type,
                provided: value_type,
            })),
        }
    }

    pub(crate) fn validate_owner_exists_to_set_has(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, &owner, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::SetHasOnDeletedOwner {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
            }))
        }
    }

    pub(crate) fn validate_attribute_exists_to_set_has(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        attribute: &Attribute,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, attribute, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::SetHasDeletedAttribute {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
                attribute_type: attribute
                    .type_()
                    .get_label(snapshot, thing_manager.type_manager())
                    .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
                    .clone(),
            }))
        }
    }

    pub(crate) fn validate_relation_exists_to_add_player(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, &relation, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::AddPlayerOnDeletedRelation {
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
            }))
        }
    }

    pub(crate) fn validate_role_player_exists_to_add_player(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        player: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, &player, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::AddDeletedPlayer {
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
                player_iid: HexBytesFormatter::owned(Vec::from(player.iid())),
            }))
        }
    }

    pub(crate) fn validate_owner_exists_to_unset_has(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: impl ObjectAPI,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, &owner, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::UnsetHasOnDeletedOwner {
                owner_iid: HexBytesFormatter::owned(Vec::from(owner.iid())),
            }))
        }
    }

    pub(crate) fn validate_relation_exists_to_remove_player(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        if thing_manager
            .instance_exists(snapshot, &relation, storage_counters)
            .map_err(|typedb_source| Box::new(DataValidationError::ConceptRead { typedb_source }))?
        {
            Ok(())
        } else {
            Err(Box::new(DataValidationError::RemovePlayerOnDeletedRelation {
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
            }))
        }
    }

    pub(crate) fn validate_links_count_to_remove_players(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        player: impl ObjectAPI,
        role_type: RoleType,
        current_count: Option<u64>,
        decrement_count: u64,
    ) -> Result<(), Box<DataValidationError>> {
        let current_count = current_count.unwrap_or(0);
        if current_count < decrement_count {
            Err(Box::new(DataValidationError::RemoveDeletedPlayers {
                player_iid: HexBytesFormatter::owned(Vec::from(player.iid())),
                relation_iid: HexBytesFormatter::owned(Vec::from(relation.iid())),
                role: get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
                decrement_count,
                current_count,
            }))
        } else {
            Ok(())
        }
    }
}
