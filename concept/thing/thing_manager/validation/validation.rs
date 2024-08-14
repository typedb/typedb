/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::Deref,
};

use encoding::value::{label::Label, value::Value, value_type::ValueType, ValueEncodable};
use lending_iterator::LendingIterator;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::Attribute,
        has::Has,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{validation::DataValidationError, ThingManager},
    },
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationKey, AnnotationRegex, AnnotationUnique,
        },
        attribute_type::AttributeType,
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
        type_manager::{validation::validation::get_label_or_concept_read_err, TypeManager},
        Capability, ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub struct Validation {}

impl Validation {
    pub(crate) fn get_label_or_concept_read_err<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl TypeAPI<'a>,
    ) -> Result<Label<'static>, ConceptReadError> {
        type_.get_label(snapshot, type_manager).map(|label| label.clone())
    }

    pub(crate) fn get_label_or_data_err<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl TypeAPI<'a>,
    ) -> Result<Label<'static>, DataValidationError> {
        Self::get_label_or_concept_read_err(snapshot, type_manager, type_).map_err(DataValidationError::ConceptRead)
    }

    pub(crate) fn validate_type_instance_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), DataValidationError> {
        if type_.is_abstract(snapshot, &thing_manager.type_manager).map_err(DataValidationError::ConceptRead)? {
            Err(DataValidationError::CannotCreateInstanceOfAbstractType(Self::get_label_or_data_err(
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
                Self::get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                Self::get_label_or_data_err(snapshot, &thing_manager.type_manager, role_type)?,
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
                Self::get_label_or_data_err(snapshot, &thing_manager.type_manager, object_type)?,
                Self::get_label_or_data_err(snapshot, &thing_manager.type_manager, attribute_type)?,
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
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let regex = attribute_type
            .get_regex_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match regex {
            Some(regex) => match &value {
                Value::String(string_value) => {
                    if !regex.value_valid(&string_value) {
                        Err(DataValidationError::AttributeViolatesRegexConstraint { attribute_type, value, regex })
                    } else {
                        Ok(())
                    }
                }
                _ => Err(DataValidationError::ConceptRead(
                    ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                        Self::get_label_or_data_err(snapshot, thing_manager.type_manager(), attribute_type)?,
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
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let range = attribute_type
            .get_range_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match range {
            Some(range) if !range.value_valid(value.clone()) => {
                Err(DataValidationError::AttributeViolatesRangeConstraint { attribute_type, value, range })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_values_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let values = attribute_type
            .get_values_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match values {
            Some(values) if !values.value_valid(value.clone()) => {
                Err(DataValidationError::AttributeViolatesValuesConstraint { attribute_type, value, values })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_regex_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let regex = owns
            .get_regex_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match regex {
            Some(regex) => match &value {
                Value::String(string_value) => {
                    if !regex.value_valid(string_value) {
                        Err(DataValidationError::HasViolatesRegexConstraint { owns, value, regex })
                    } else {
                        Ok(())
                    }
                }
                _ => Err(DataValidationError::ConceptRead(
                    ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                        Self::get_label_or_data_err(snapshot, thing_manager.type_manager(), owns.attribute())?,
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
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let range = owns
            .get_range_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match range {
            Some(range) if !range.value_valid(value.clone()) => {
                Err(DataValidationError::HasViolatesRangeConstraint { owns, value, range })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_values_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let values = owns
            .get_values_constraint(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;

        match values {
            Some(values) if !values.value_valid(value.clone()) => {
                Err(DataValidationError::HasViolatesValuesConstraint { owns, value, values })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_has_unique_constraint(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        value: Value<'static>,
    ) -> Result<(), DataValidationError> {
        let uniqueness_source = Self::get_uniqueness_source(snapshot, thing_manager, owns.clone())
            .map_err(DataValidationError::ConceptRead)?;
        if let Some(unique_root) = uniqueness_source {
            let mut queue = VecDeque::from([(unique_root.owner(), unique_root.clone())]);

            while let Some((current_owner_type, current_owns)) = queue.pop_back() {
                let mut objects = thing_manager.get_objects_in(snapshot, current_owner_type.clone());
                while let Some(object) = objects.next() {
                    let object = object.map_err(DataValidationError::ConceptRead)?;
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
                                value,
                            })
                        } else {
                            Err(DataValidationError::UniqueValueTaken {
                                owner_type: owns.owner(),
                                attribute_type: owns.attribute(),
                                taken_owner_type: current_owner_type,
                                taken_attribute_type: current_owns.attribute(),
                                value,
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

    fn get_uniqueness_source(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        debug_assert!(
            !AnnotationCategory::Unique.declarable_below(AnnotationCategory::Key)
                && AnnotationCategory::Key.declarable_below(AnnotationCategory::Unique),
            "This function uses the fact that @key is always below @unique. Revalidate the logic!"
        );

        if owns.is_unique(snapshot, thing_manager.type_manager())? {
            let unique_source = thing_manager.type_manager().get_capability_annotation_source(
                snapshot,
                owns.clone(),
                OwnsAnnotation::Unique(AnnotationUnique),
            )?;
            Ok(match unique_source {
                Some(_) => unique_source,
                None => {
                    let key_source = thing_manager.type_manager().get_capability_annotation_source(
                        snapshot,
                        owns.clone(),
                        OwnsAnnotation::Key(AnnotationKey),
                    )?;
                    match key_source {
                        Some(_) => key_source,
                        None => panic!("AnnotationUnique or AnnotationKey should exist if owns is unique!"),
                    }
                }
            })
        } else {
            Ok(None)
        }
    }

    pub(crate) fn validate_owns_cardinality_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &Object<'a>,
        owns: Owns<'static>,
        counts: &HashMap<AttributeType<'static>, u64>,
    ) -> Result<(), DataValidationError> {
        let count = counts.get(&owns.attribute()).unwrap_or(&0).clone();
        Self::check_owns_cardinality(snapshot, thing_manager, owner, owns.clone(), count)?;

        let mut next_owns = owns;
        while let Some(checked_owns) = &*next_owns
            .get_override(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            let overriding = checked_owns
                .get_overriding_transitive(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            let count = overriding.iter().filter_map(|overriding| counts.get(&overriding.attribute())).sum();
            Self::check_owns_cardinality(snapshot, thing_manager, owner, checked_owns.clone(), count)?;

            next_owns = checked_owns.clone();
        }

        Ok(())
    }

    fn check_owns_cardinality<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &Object<'a>,
        owns: Owns<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        let cardinality =
            owns.get_cardinality(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;
        let is_key: bool =
            owns.is_key(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;
        if !cardinality.value_valid(count) {
            let owner = owner.clone().into_owned();
            if is_key {
                Err(DataValidationError::KeyCardinalityViolated { owner, owns, count })
            } else {
                Err(DataValidationError::OwnsCardinalityViolated { owner, owns, count, cardinality })
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_plays_cardinality_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: &Object<'a>,
        plays: Plays<'static>,
        counts: &HashMap<RoleType<'static>, u64>,
    ) -> Result<(), DataValidationError> {
        let count = counts.get(&plays.role()).unwrap_or(&0).clone();
        Self::check_plays_cardinality(snapshot, thing_manager, player, plays.clone(), count)?;

        let mut next_plays = plays;
        while let Some(checked_plays) = &*next_plays
            .get_override(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?
        {
            let overriding = checked_plays
                .get_overriding_transitive(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            let count = overriding.iter().filter_map(|overriding| counts.get(&overriding.role())).sum();
            Self::check_plays_cardinality(snapshot, thing_manager, player, checked_plays.clone(), count)?;

            next_plays = checked_plays.clone();
        }

        Ok(())
    }

    fn check_plays_cardinality<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: &Object<'a>,
        plays: Plays<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        let cardinality =
            plays.get_cardinality(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?;
        if !cardinality.value_valid(count) {
            let player = player.clone().into_owned();
            Err(DataValidationError::PlaysCardinalityViolated { player, plays, count, cardinality })
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_cardinality_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'a>,
        relates: Relates<'static>,
        counts: &HashMap<RoleType<'static>, u64>,
    ) -> Result<(), DataValidationError> {
        let count = counts.get(&relates.role()).unwrap_or(&0).clone();
        Self::check_relates_cardinality(snapshot, thing_manager, relation, relates.clone(), count)?;

        let mut next_role = relates.role();
        while let Some(checked_role) =
            next_role.get_supertype(snapshot, thing_manager.type_manager()).map_err(DataValidationError::ConceptRead)?
        {
            let subtypes = checked_role
                .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            let count = subtypes.iter().filter_map(|overriding| counts.get(overriding)).sum();
            let checked_relates = checked_role
                .get_relates(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?;
            Self::check_relates_cardinality(snapshot, thing_manager, relation, checked_relates.clone(), count)?;

            next_role = checked_role;
        }

        Ok(())
    }

    fn check_relates_cardinality<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'a>,
        relates: Relates<'static>,
        count: u64,
    ) -> Result<(), DataValidationError> {
        let cardinality = relates
            .get_cardinality(snapshot, thing_manager.type_manager())
            .map_err(DataValidationError::ConceptRead)?;
        if !cardinality.value_valid(count) {
            let relation = relation.clone().into_owned();
            Err(DataValidationError::RelatesCardinalityViolated { relation, relates, count, cardinality })
        } else {
            Ok(())
        }
    }
}
