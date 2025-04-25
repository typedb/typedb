/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    thing::{
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{
            validation::{validation::DataValidation, DataValidationError},
            ThingManager,
        },
    },
    type_::{
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint},
        owns::Owns,
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
        Capability, OwnerAPI, PlayerAPI, TypeAPI,
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
    ($func_name:ident, $capability_type:ident, $object_instance:ident, $get_cardinality_constraints_func:ident, $get_interface_counts_func:ident, $check_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            interface_types_to_check: HashSet<<$capability_type as Capability>::InterfaceType>,
            storage_counters: StorageCounters,
        ) -> Result<(), Box<DataValidationError>> {
            let mut cardinality_constraints: HashSet<CapabilityConstraint<$capability_type>> = HashSet::new();
            let counts = object
                .$get_interface_counts_func(snapshot, thing_manager, storage_counters)
                .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;

            for interface_type in interface_types_to_check {
                for constraint in object
                    .type_()
                    .$get_cardinality_constraints_func(snapshot, thing_manager.type_manager(), interface_type.clone())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                    .into_iter()
                {
                    cardinality_constraints.insert(constraint);
                }
            }

            for constraint in cardinality_constraints {
                if !constraint
                    .description()
                    .unwrap_cardinality()
                    .map_err(|source| Box::new(ConceptReadError::Constraint { typedb_source: source }))
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                    .requires_validation()
                {
                    continue;
                }

                let source_interface_type = constraint.source().interface();
                let sub_interface_types = source_interface_type
                    .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
                let count =
                    TypeAPI::chain_types(source_interface_type.clone(), sub_interface_types.into_iter().cloned())
                        .filter_map(|interface_type| counts.get(&interface_type))
                        .sum();
                $check_func(snapshot, thing_manager.type_manager(), &constraint, object, source_interface_type, count)?;
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
        object: Object,
        modified_attribute_types: HashSet<AttributeType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = CommitTimeValidation::validate_owns_cardinality_constraint(
            snapshot,
            thing_manager,
            object,
            modified_attribute_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    pub(crate) fn validate_object_links(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        modified_role_types: HashSet<RoleType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = Self::validate_plays_cardinality_constraint(
            snapshot,
            thing_manager,
            object,
            modified_role_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    pub(crate) fn validate_relation_links(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        modified_role_types: HashSet<RoleType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = Self::validate_relates_cardinality_constraint(
            snapshot,
            thing_manager,
            relation,
            modified_role_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    validate_capability_cardinality_constraint!(
        validate_owns_cardinality_constraint,
        Owns,
        Object,
        get_owned_attribute_type_constraints_cardinality,
        get_has_counts,
        DataValidation::validate_owns_instances_cardinality_constraint
    );
    validate_capability_cardinality_constraint!(
        validate_plays_cardinality_constraint,
        Plays,
        Object,
        get_played_role_type_constraints_cardinality,
        get_played_roles_counts,
        DataValidation::validate_plays_instances_cardinality_constraint
    );
    validate_capability_cardinality_constraint!(
        validate_relates_cardinality_constraint,
        Relates,
        Relation,
        get_related_role_type_constraints_cardinality,
        get_player_counts,
        DataValidation::validate_relates_instances_cardinality_constraint
    );
}
