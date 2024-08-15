/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

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
        role_type::RoleType, Capability, OwnerAPI, PlayerAPI, TypeAPI,
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

pub struct CommitTimeValidation {}

impl CommitTimeValidation {
    pub(crate) fn validate_object_has<'a>(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: &Object<'a>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = object.type_();
        let object_owns = type_.get_owns(snapshot, thing_manager.type_manager())?;
        let has_counts = object.get_has_counts(snapshot, thing_manager)?;

        for owns in object_owns.iter() {
            let cardinality_check = CommitTimeValidation::validate_owns_cardinality_constraint(
                snapshot,
                thing_manager,
                &object,
                owns.clone().into_owned(),
                &has_counts,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    pub(crate) fn validate_object_links<'a>(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: &Object<'a>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = object.type_();
        let object_plays = type_.get_plays(snapshot, thing_manager.type_manager())?;
        let played_roles_counts = object.get_played_roles_counts(snapshot, thing_manager)?;

        for plays in object_plays.iter() {
            let cardinality_check = Self::validate_plays_cardinality_constraint(
                snapshot,
                thing_manager,
                &object,
                plays.clone().into_owned(),
                &played_roles_counts,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    pub(crate) fn validate_relation_links<'a>(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation: &Relation<'a>,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = relation.type_();
        let relation_relates = type_.get_relates(snapshot, thing_manager.type_manager())?;
        let role_player_count = relation.get_player_counts(snapshot, thing_manager)?;

        for relates in relation_relates.iter() {
            let cardinality_check = Self::validate_relates_cardinality_constraint(
                snapshot,
                thing_manager,
                &relation,
                relates.clone().into_owned(),
                &role_player_count,
            );
            collect_errors!(out_errors, cardinality_check);
        }
        Ok(())
    }

    pub(crate) fn validate_owns_cardinality_constraint<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: &Object<'a>,
        owns: Owns<'static>,
        counts: &HashMap<AttributeType<'static>, u64>,
    ) -> Result<(), DataValidationError> {
        if !Self::needs_cardinality_validation(snapshot, thing_manager, owns.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            return Ok(());
        }

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
        if !Self::needs_cardinality_validation(snapshot, thing_manager, plays.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            return Ok(());
        }

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
        if !Self::needs_cardinality_validation(snapshot, thing_manager, relates.clone())
            .map_err(DataValidationError::ConceptRead)?
        {
            return Ok(());
        }

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

    fn needs_cardinality_validation<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        capability: CAP,
    ) -> Result<bool, ConceptReadError> {
        let cardinality = capability.get_cardinality(snapshot, thing_manager.type_manager())?;
        Ok(cardinality != AnnotationCardinality::unchecked())
    }
}
