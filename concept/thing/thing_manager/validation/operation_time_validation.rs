/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, VecDeque};

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
                validation::{get_label_or_data_err},
                DataValidationError,
            },
            ThingManager,
        },
    },
    type_::{
        annotation::Annotation, attribute_type::AttributeType, object_type::ObjectType, owns::Owns,
        relation_type::RelationType, role_type::RoleType, Capability, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI,
        TypeAPI,
    },
};

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_type_instance_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), DataValidationError> {
        if type_.is_abstract(snapshot, &thing_manager.type_manager).map_err(DataValidationError::ConceptRead)? {
            Err(DataValidationError::CannotCreateInstanceOfAbstractType(get_label_or_data_err(
                snapshot,
                &thing_manager.type_manager,
                type_,
            )?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_object_type_plays_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: ObjectType<'_>,
        role_type: RoleType<'_>,
    ) -> Result<(), DataValidationError> {
        let has_plays = object_type
            .get_plays(snapshot, &thing_manager.type_manager)
            .map_err(DataValidationError::ConceptRead)?
            .into_iter()
            .any(|plays| &plays.role() == &role_type.clone());
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
        relation_type: RelationType<'_>,
        role_type: RoleType<'_>,
    ) -> Result<(), DataValidationError> {
        let has_relates = relation_type
            .get_relates(snapshot, &thing_manager.type_manager)
            .map_err(DataValidationError::ConceptRead)?
            .into_iter()
            .any(|relates| &relates.role() == &role_type.clone());
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
        attribute_type: AttributeType<'_>,
    ) -> Result<(), DataValidationError> {
        let has_owns = object_type
            .get_owns(snapshot, &thing_manager.type_manager)
            .map_err(DataValidationError::ConceptRead)?
            .into_iter()
            .any(|owns| owns.attribute() == attribute_type.clone());
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
        role_type: RoleType<'static>,
        players_counts: &HashMap<&Object<'_>, u64>,
    ) -> Result<(), DataValidationError> {
        let relates =
            role_type.get_relates(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;
        let distinct =
            relates.is_distinct(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;

        match distinct {
            true => {
                let duplicated = players_counts.iter().find(|(_, count)| **count > 1);
                match duplicated {
                    Some((player, count)) => Err(DataValidationError::PlayerViolatesDistinctRelatesConstraint {
                        role_type,
                        player: player.clone().clone().into_owned(),
                        count: count.clone(),
                    }),
                    None => Ok(()),
                }
            }
            false => Ok(()),
        }
    }

    pub(crate) fn validate_owns_distinct_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        attributes_counts: &BTreeMap<&Attribute<'_>, u64>,
    ) -> Result<(), DataValidationError> {
        let distinct =
            owns.is_distinct(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;

        match distinct {
            true => {
                let duplicated = attributes_counts.iter().find(|(_, count)| **count > 1);
                match duplicated {
                    Some((attribute, count)) => Err(DataValidationError::AttributeViolatesDistinctOwnsConstraint {
                        owns,
                        attribute: attribute.clone().clone().into_owned(),
                        count: count.clone(),
                    }),
                    None => Ok(()),
                }
            }
            false => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_regex_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let regex = attribute_type
            .get_constraint_regex(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match regex {
            Some(regex) => match &value {
                Value::String(string_value) => {
                    if !regex.value_valid(&string_value) {
                        Err(DataValidationError::AttributeViolatesRegexConstraint {
                            attribute_type,
                            value: value.into_owned(),
                            regex,
                        })
                    } else {
                        Ok(())
                    }
                }
                _ => Err(DataValidationError::ConceptRead(
                    ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                        get_label_or_data_err(snapshot, thing_manager.type_manager(), attribute_type)?,
                        value.value_type(),
                        Annotation::Regex(regex.clone()),
                    ),
                )),
            },
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_range_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let range = attribute_type
            .get_constraint_range(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match range {
            Some(range) if !range.value_valid(value.clone()) => {
                Err(DataValidationError::AttributeViolatesRangeConstraint {
                    attribute_type,
                    value: value.into_owned(),
                    range,
                })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_values_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let values = attribute_type
            .get_constraint_values(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match values {
            Some(values) if !values.value_valid(value.clone()) => {
                Err(DataValidationError::AttributeViolatesValuesConstraint {
                    attribute_type,
                    value: value.into_owned(),
                    values,
                })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_regex_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let regex = owns
            .get_constraint_regex(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match regex {
            Some(regex) => match &value {
                Value::String(string_value) => {
                    if !regex.value_valid(string_value) {
                        Err(DataValidationError::HasViolatesRegexConstraint { owns, value: value.into_owned(), regex })
                    } else {
                        Ok(())
                    }
                }
                _ => Err(DataValidationError::ConceptRead(
                    ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                        get_label_or_data_err(snapshot, thing_manager.type_manager(), owns.attribute())?,
                        value.value_type(),
                        Annotation::Regex(regex.clone()),
                    ),
                )),
            },
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_range_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let range = owns
            .get_constraint_range(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match range {
            Some(range) if !range.value_valid(value.clone()) => {
                Err(DataValidationError::HasViolatesRangeConstraint { owns, value: value.into_owned(), range })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_values_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let values = owns
            .get_constraint_values(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match values {
            Some(values) if !values.value_valid(value.clone()) => {
                Err(DataValidationError::HasViolatesValuesConstraint { owns, value: value.into_owned(), values })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_unique_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'_>,
    ) -> Result<(), DataValidationError> {
        let uniqueness_source =
            owns.get_uniqueness_source(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;
        if let Some(unique_root) = uniqueness_source {
            let mut queue = VecDeque::from([(unique_root.owner(), unique_root.clone())]);

            while let Some((current_owner_type, current_owns)) = queue.pop_back() {
                let mut objects = thing_manager.get_objects_in(snapshot, current_owner_type.clone());
                while let Some(object) = objects.next().transpose().map_err(DataValidationError::ConceptRead)? {
                    if object
                        .has_attribute_with_value(snapshot, thing_manager, current_owns.attribute(), value.clone())
                        .map_err(DataValidationError::ConceptRead)?
                    {
                        return if owns
                            .is_key(snapshot, thing_manager.type_manager())
                            .map_err(DataValidationError::ConceptRead)?
                        {
                            Err(DataValidationError::KeyValueTaken {
                                owner_type: owns.owner(),
                                attribute_type: owns.attribute(),
                                taken_owner_type: current_owner_type,
                                taken_attribute_type: current_owns.attribute(),
                                value: value.into_owned(),
                            })
                        } else {
                            Err(DataValidationError::UniqueValueTaken {
                                owner_type: owns.owner(),
                                attribute_type: owns.attribute(),
                                taken_owner_type: current_owner_type,
                                taken_attribute_type: current_owns.attribute(),
                                value: value.into_owned(),
                            })
                        };
                    }
                }

                current_owner_type
                    .get_subtypes(snapshot, thing_manager.type_manager())
                    .map_err(DataValidationError::ConceptRead)?
                    .into_iter()
                    .try_for_each(|subtype| {
                        let overrides = subtype
                            .get_owns_overrides(snapshot, thing_manager.type_manager())
                            .map_err(DataValidationError::ConceptRead)?;
                        let mut overridings = overrides.iter().filter_map(|(overriding, overridden)| {
                            if &current_owns == overridden {
                                Some(overriding.clone())
                            } else {
                                None
                            }
                        });

                        if overridings.clone().peekable().peek().is_some() {
                            while let Some(overriding) = overridings.next() {
                                queue.push_front((subtype.clone(), overriding));
                            }
                        } else {
                            queue.push_front((subtype.clone(), current_owns.clone()));
                        }

                        Ok(())
                    })?;
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
        let type_value_type = attribute_type.get_value_type(snapshot, thing_manager.type_manager())?;
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
        let type_value_type = attribute_type.get_value_type(snapshot, thing_manager.type_manager())?;
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
