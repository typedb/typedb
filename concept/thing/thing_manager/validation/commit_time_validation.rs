/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use itertools::Itertools;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    thing::{
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{validation::DataValidationError, ThingManager},
    },
    type_::{
        annotation::AnnotationCardinality, attribute_type::AttributeType, owns::Owns, plays::Plays, relates::Relates,
        role_type::RoleType, Capability, OwnerAPI, PlayerAPI,
    },
};

macro_rules! collect_errors {
    ($vec:ident, $expr:expr, $wrap:expr) => {
        if let Err(e) = $expr {
            $vec.push($wrap(e));
        }
    };

    ($vec:ident, $expr:expr) => {
        if let Err(e) = $expr {
            $vec.push(e);
        }
    };
}
pub(crate) use collect_errors;

macro_rules! validate_capability_cardinality_constraint {
    ($func_name:ident, $capability_type:ident, $interface_type:ident, $object_instance:ident, $check_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            owner: &$object_instance<'_>,
            capability: $capability_type<'static>,
            counts: &HashMap<$interface_type<'static>, u64>,
        ) -> Result<(), DataValidationError> {
            if !CommitTimeValidation::needs_cardinality_validation(snapshot, thing_manager, capability.clone())
                .map_err(DataValidationError::ConceptRead)?
            {
                return Ok(());
            }

            let count = counts.get(&capability.interface()).unwrap_or(&0).clone();
            $check_func(snapshot, thing_manager, owner, capability.clone(), count)?;

            let mut next_capability = capability;
            while let Some(checked_capability) = &*next_capability
                .get_override(snapshot, thing_manager.type_manager())
                .map_err(DataValidationError::ConceptRead)?
            {
                let overriding = checked_capability
                    .get_overriding_transitive(snapshot, thing_manager.type_manager())
                    .map_err(DataValidationError::ConceptRead)?;
                let count = overriding
                    .iter()
                    .map(|overriding| overriding.interface())
                    .unique()
                    .filter_map(|interface_type| counts.get(&interface_type))
                    .sum();
                $check_func(snapshot, thing_manager, owner, checked_capability.clone(), count)?;

                next_capability = checked_capability.clone();
            }

            Ok(())
        }
    };
}

pub struct CommitTimeValidation {}

impl CommitTimeValidation {
    pub(crate) fn validate_object_has(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: &Object<'_>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = object.type_();
        let object_owns = type_.get_owns(snapshot, thing_manager.type_manager())?;
        let has_counts = object.get_has_counts(snapshot, thing_manager)?;

        for owns in object_owns.iter() {
            let cardinality_check = CommitTimeValidation::validate_owns_cardinality_constraint(
                snapshot,
                thing_manager,
                object,
                owns.clone().into_owned(),
                &has_counts,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    pub(crate) fn validate_object_links(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: &Object<'_>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = object.type_();
        let object_plays = type_.get_plays(snapshot, thing_manager.type_manager())?;
        let played_roles_counts = object.get_played_roles_counts(snapshot, thing_manager)?;

        for plays in object_plays.iter() {
            let cardinality_check = Self::validate_plays_cardinality_constraint(
                snapshot,
                thing_manager,
                object,
                plays.clone().into_owned(),
                &played_roles_counts,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    pub(crate) fn validate_relation_links(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'_>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = relation.type_();
        let relation_relates = type_.get_relates(snapshot, thing_manager.type_manager())?;
        let role_player_count = relation.get_player_counts(snapshot, thing_manager)?;

        for relates in relation_relates.iter() {
            let cardinality_check = Self::validate_relates_cardinality_constraint(
                snapshot,
                thing_manager,
                relation,
                relates.clone().into_owned(),
                &role_player_count,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    fn check_owns_cardinality(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &Object<'_>,
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

    fn check_plays_cardinality(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: &Object<'_>,
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

    fn check_relates_cardinality(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'_>,
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

    fn needs_cardinality_validation<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        capability: CAP,
    ) -> Result<bool, ConceptReadError> {
        let cardinality = capability.get_cardinality(snapshot, thing_manager.type_manager())?;
        Ok(cardinality != AnnotationCardinality::unchecked())
    }

    validate_capability_cardinality_constraint!(
        validate_owns_cardinality_constraint,
        Owns,
        AttributeType,
        Object,
        Self::check_owns_cardinality
    );
    validate_capability_cardinality_constraint!(
        validate_plays_cardinality_constraint,
        Plays,
        RoleType,
        Object,
        Self::check_plays_cardinality
    );
    validate_capability_cardinality_constraint!(
        validate_relates_cardinality_constraint,
        Relates,
        RoleType,
        Relation,
        Self::check_relates_cardinality
    );
}
