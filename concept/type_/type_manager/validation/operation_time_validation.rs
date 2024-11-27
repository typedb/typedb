/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet, VecDeque};

use encoding::{
    graph::{
        definition::definition_key::DefinitionKey,
        type_::{CapabilityKind, Kind},
    },
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::{
        object::ObjectAPI,
        thing_manager::{
            validation::{validation::DataValidation, DataValidationError},
            ThingManager,
        },
    },
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationDistinct, AnnotationKey, AnnotationRange,
            AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::{
            filter_by_constraint_category, filter_by_scope, filter_out_unchecked_constraints, get_abstract_constraints,
            get_checked_constraints, get_distinct_constraints, get_range_constraints, get_regex_constraints,
            get_values_constraints, type_get_constraints_closest_source, CapabilityConstraint, Constraint,
            ConstraintDescription, ConstraintScope, TypeConstraint,
        },
        entity_type::EntityType,
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::Plays,
        relates::{Relates, RelatesAnnotation},
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                validation::{
                    get_label_or_schema_err, get_opt_label_or_schema_err, validate_role_name_uniqueness_non_transitive,
                    validate_role_type_supertype_ordering_match, validate_sibling_owns_ordering_match_for_type,
                    validate_type_declared_constraints_narrowing_of_supertype_constraints,
                    validate_type_supertype_abstractness,
                },
                SchemaValidationError,
            },
            TypeManager,
        },
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

macro_rules! for_type_and_subtypes_transitive {
    ($snapshot:ident, $type_manager:ident, $type_:ident, $closure:expr) => {
        let subtypes = $type_
            .get_subtypes_transitive($snapshot, $type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        TypeAPI::chain_types($type_.clone(), subtypes.into_iter().cloned())
            .try_for_each(|subtype| $closure(subtype))?;
    };
}

macro_rules! type_or_subtype_without_declared_capability_instances_existence_validation {
    (
        $func_name:ident,
        $capability_type:ident,
        $object_type:ident,
        $interface_type:ident,
        $single_type_validation_func:path
    ) => {
        fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type,
            interface_type: $interface_type,
        ) -> Result<Option<$object_type>, Box<ConceptReadError>> {
            let mut type_that_has_instances = None;

            let mut object_types = VecDeque::new();
            object_types.push_front(object_type);

            while let Some(current_object_type) = object_types.pop_back() {
                if $single_type_validation_func(
                    snapshot,
                    thing_manager,
                    current_object_type.clone(),
                    interface_type.clone(),
                )? {
                    type_that_has_instances = Some(current_object_type.clone());
                    break; // We may want to return multiple (HashSet instead of Option)
                }

                let subtypes = current_object_type.get_subtypes(snapshot, type_manager)?;
                for subtype in subtypes.into_iter() {
                    let subtype_capabilities = TypeReader::get_capabilities_declared::<$capability_type>(
                        snapshot,
                        subtype.clone().into_owned(),
                    )?;
                    if !subtype_capabilities.iter().map(|capability| capability.interface()).contains(&interface_type) {
                        object_types.push_front(subtype.clone());
                    }
                }
            }

            Ok(type_that_has_instances)
        }
    };
}

macro_rules! cannot_unset_capability_with_existing_instances_validation {
    (
        $func_name:ident,
        $capability_kind:path,
        $capability_type:ident,
        $object_type:ident,
        $interface_type:ident,
        $existing_instances_validation_func:path
    ) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type,
            interface_type: $interface_type,
        ) -> Result<(), Box<SchemaValidationError>> {
            if let Some(supertype) = object_type
                .get_supertype(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            {
                let supertype_capabilities =
                    TypeReader::get_capabilities::<$capability_type>(snapshot, supertype, false)
                        .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                if supertype_capabilities.iter().any(|capability| &capability.interface() == &interface_type) {
                    return Ok(());
                }
            }

            let type_having_instances = $existing_instances_validation_func(
                snapshot,
                type_manager,
                thing_manager,
                object_type.clone(),
                interface_type.clone(),
            )
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            if type_having_instances.is_some() {
                Err(Box::new(SchemaValidationError::CannotUnsetCapabilityWithExistingInstances {
                    cap: $capability_kind,
                    label: get_label_or_schema_err(snapshot, type_manager, object_type)?,
                    interface: get_label_or_schema_err(snapshot, type_manager, interface_type)?,
                }))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation {
    (
        $func_name:ident,
        $capability_kind:path,
        $object_type:ident,
        $get_lost_capabilities_func:path,
        $existing_instances_validation_func:path
    ) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            subtype: $object_type,
            supertype: Option<$object_type>,
        ) -> Result<(), Box<SchemaValidationError>> {
            let lost_capabilities = $get_lost_capabilities_func(snapshot, subtype.clone(), supertype.clone())?;

            for capability in lost_capabilities {
                let interface_type = capability.interface();
                let type_having_instances = $existing_instances_validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    subtype.clone(),
                    interface_type.clone(),
                )
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

                if type_having_instances.is_some() {
                    return Err(Box::new(
                        SchemaValidationError::CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances {
                            cap: $capability_kind,
                            label: get_label_or_schema_err(snapshot, type_manager, subtype)?,
                            new_super_label: get_opt_label_or_schema_err(snapshot, type_manager, supertype)?,
                            interface: get_label_or_schema_err(snapshot, type_manager, interface_type)?,
                        },
                    ));
                }
            }

            Ok(())
        }
    };
}

macro_rules! new_acquired_capability_instances_validation {
    ($func_name:ident, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            capability: $capability_type,
            default_constraints: HashSet<CapabilityConstraint<$capability_type>>,
        ) -> Result<(), Box<SchemaValidationError>> {
            let affected_object_types = TypeAPI::chain_types(
                capability.object(),
                capability
                    .object()
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();
            let affected_interface_types = TypeAPI::chain_types(
                capability.interface(),
                capability
                    .interface()
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();

            $validation_func(
                snapshot,
                type_manager,
                thing_manager,
                &affected_object_types,
                &affected_interface_types,
                &HashMap::new(), // all interface supertypes are read from storage
                &get_checked_constraints(default_constraints.into_iter()),
            )
            .map_err(|typedb_source| {
                Box::new(SchemaValidationError::CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint {
                    typedb_source,
                })
            })
        }
    };
}

macro_rules! new_annotation_constraints_compatible_with_capability_instances_validation {
    ($func_name:ident, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            capability: $capability_type,
            annotation: Annotation,
        ) -> Result<(), Box<SchemaValidationError>> {
            let affected_object_types = TypeAPI::chain_types(
                capability.object(),
                capability
                    .object()
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();
            let affected_interface_types = TypeAPI::chain_types(
                capability.interface(),
                capability
                    .interface()
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();
            let constraints =
                get_checked_constraints(annotation.to_capability_constraints(capability.clone()).into_iter());

            $validation_func(
                snapshot,
                type_manager,
                thing_manager,
                &affected_object_types,
                &affected_interface_types,
                &HashMap::new(), // all interface supertypes are read from storage
                &constraints,
            )
            .map_err(|typedb_source| {
                Box::new(
                    SchemaValidationError::CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint {
                        typedb_source,
                    },
                )
            })
        }
    };
}

macro_rules! updated_constraints_compatible_with_capability_instances_on_object_supertype_change_validation {
    ($func_name:ident, $capability_type:ident, $object_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $object_type,
            new_supertype: $object_type,
        ) -> Result<(), Box<SchemaValidationError>> {
            let affected_object_types = TypeAPI::chain_types(
                type_.clone(),
                type_
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();
            let type_capabilities_declared =
                TypeReader::get_capabilities_declared::<$capability_type>(snapshot, type_.clone())
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            let type_capabilities =
                TypeReader::get_capabilities::<$capability_type>(snapshot, type_.clone(), false)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            let new_capabilities =
                TypeReader::get_capabilities::<$capability_type>(snapshot, new_supertype.clone(), false)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            for new_capability in new_capabilities.into_iter() {
                let affected_interface_types = TypeAPI::chain_types(
                    new_capability.interface(),
                    new_capability
                        .interface()
                        .get_subtypes_transitive(snapshot, type_manager)
                        .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                        .into_iter()
                        .cloned(),
                )
                .collect();
                let mut constraints = get_checked_constraints(
                    new_capability
                        .get_constraints(snapshot, type_manager)
                        .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                        .into_iter()
                        .cloned(),
                );

                // If the new_capability is not specialised by a declared capability of type_,
                // ConstraintScope::SingleInstanceOfType constraints should affect type_'s capability
                // As the actual capabilities for this interface
                let single_instance_of_type_constraints: HashSet<CapabilityConstraint<$capability_type>> =
                    filter_by_scope!(constraints.iter().cloned(), ConstraintScope::SingleInstanceOfType).collect();
                for constraint in single_instance_of_type_constraints {
                    let affected_interface_type = constraint.source().interface();
                    if let Some(type_capability) =
                        type_capabilities.iter().find(|capability| &capability.interface() == &affected_interface_type)
                    {
                        if type_capabilities_declared
                            .iter()
                            .find(|capability_declared| &capability_declared.interface() == &affected_interface_type)
                            .is_none()
                        {
                            let new_constraint =
                                CapabilityConstraint::new(constraint.description(), type_capability.clone());
                            constraints.remove(&constraint);
                            constraints.insert(new_constraint);
                        }
                    }
                }

                $validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    &affected_object_types,
                    &affected_interface_types,
                    &HashMap::new(), // all interface supertypes are read from storage
                    &constraints,
                )
                .map_err(|typedb_source| {
                    SchemaValidationError::CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances {
                        typedb_source
                    }
                })?;
            }
            Ok(())
        }
    };
}

macro_rules! affected_constraints_compatible_with_capability_instances_on_interface_supertype_unset_validation {
    ($func_name:ident, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            interface_type: <$capability_type as Capability<'static>>::InterfaceType,
        ) -> Result<(), Box<SchemaValidationError>> {
            let unset_supertype = if let Some(type_) = interface_type
                .get_supertype(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            {
                type_
            } else {
                return Ok(());
            };

            let mut objects_with_interface_supertypes_and_constraints: HashMap<
                <$capability_type as Capability<'static>>::ObjectType,
                HashSet<CapabilityConstraint<$capability_type>>,
            > = HashMap::new();
            collect_object_types_with_interface_type_and_supertypes_constraints(
                snapshot,
                type_manager,
                unset_supertype.clone(),
                &mut objects_with_interface_supertypes_and_constraints,
            )
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            let mut objects_with_interface_subtypes_and_constraints: HashSet<
                <$capability_type as Capability<'static>>::ObjectType,
            > = HashSet::new();
            collect_object_types_with_interface_type_and_subtypes::<$capability_type>(
                snapshot,
                type_manager,
                interface_type.clone(),
                &mut objects_with_interface_subtypes_and_constraints,
            )
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            let unset_supertype_root = unset_supertype
                .get_supertype_root(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .unwrap_or(unset_supertype);
            let unset_supertype_root_subtypes = unset_supertype_root
                .get_subtypes_transitive(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            let affected_interface_types =
                TypeAPI::chain_types(unset_supertype_root, unset_supertype_root_subtypes.into_iter().cloned())
                    .collect();
            let updated_supertypes = HashMap::from([(interface_type, None)]);

            for (affected_object_type, constraints) in objects_with_interface_supertypes_and_constraints.into_iter() {
                if !objects_with_interface_subtypes_and_constraints.contains(&affected_object_type) {
                    continue;
                }

                let affected_constraints = filter_out_unchecked_constraints!(constraints.into_iter())
                    .filter(|constraint| match constraint.scope() {
                        ConstraintScope::SingleInstanceOfType | ConstraintScope::SingleInstanceOfTypeOrSubtype => false,
                        ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes
                        | ConstraintScope::AllInstancesOfTypeOrSubtypes => true,
                    })
                    .collect();

                $validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    &HashSet::from([affected_object_type]),
                    &affected_interface_types,
                    &updated_supertypes,
                    &affected_constraints,
                )
                .map_err(|typedb_source| {
                    SchemaValidationError::CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances {
                        typedb_source
                    }
                })?;
            }

            Ok(())
        }
    };
}

macro_rules! affected_constraints_compatible_with_capability_instances_on_interface_supertype_change_validation {
    ($func_name:ident, $capability_type:ident, $unset_validation_func:path, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            interface_type: <$capability_type as Capability<'static>>::InterfaceType,
            interface_supertype: <$capability_type as Capability<'static>>::InterfaceType,
        ) -> Result<(), Box<SchemaValidationError>> {
            if let Some(old_supertype) = interface_type
                .get_supertype(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            {
                if &old_supertype == &interface_supertype {
                    return Ok(());
                }
            }

            $unset_validation_func(snapshot, type_manager, thing_manager, interface_type.clone())?;

            let mut objects_with_interface_supertypes_and_constraints: HashMap<
                <$capability_type as Capability<'static>>::ObjectType,
                HashSet<CapabilityConstraint<$capability_type>>,
            > = HashMap::new();
            collect_object_types_with_interface_type_and_supertypes_constraints(
                snapshot,
                type_manager,
                interface_supertype.clone(),
                &mut objects_with_interface_supertypes_and_constraints,
            )
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            let _all_interface_supertype_subtypes = interface_supertype
                .get_subtypes_transitive(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            let mut objects_with_interface_subtypes_and_constraints: HashSet<
                <$capability_type as Capability<'static>>::ObjectType,
            > = HashSet::new();
            collect_object_types_with_interface_type_and_subtypes::<$capability_type>(
                snapshot,
                type_manager,
                interface_type.clone(),
                &mut objects_with_interface_subtypes_and_constraints,
            )
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            let supertype_root = interface_supertype
                .get_supertype_root(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .unwrap_or(interface_supertype.clone());
            let supertype_root_subtypes = supertype_root
                .get_subtypes_transitive(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            let all_supertype_root_subtypes =
                TypeAPI::chain_types(supertype_root, supertype_root_subtypes.into_iter().cloned());
            let interface_subtypes = interface_type
                .get_subtypes_transitive(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            let all_interface_subtypes =
                TypeAPI::chain_types(interface_type.clone(), interface_subtypes.into_iter().cloned());

            let affected_interface_types = all_supertype_root_subtypes.chain(all_interface_subtypes).collect();
            let updated_supertypes = HashMap::from([(interface_type, Some(interface_supertype))]);

            for (affected_object_type, constraints) in objects_with_interface_supertypes_and_constraints.into_iter() {
                if !objects_with_interface_subtypes_and_constraints.contains(&affected_object_type) {
                    continue;
                }

                $validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    &HashSet::from([affected_object_type]),
                    &affected_interface_types,
                    &updated_supertypes,
                    &get_checked_constraints(constraints.into_iter()),
                )
                .map_err(|typedb_source| {
                    SchemaValidationError::CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances {
                        typedb_source
                    }
                })?;
            }

            Ok(())
        }
    };
}

macro_rules! new_annotation_constraints_compatible_with_type_and_sub_instances_validation {
    ($func_name:ident, $type_:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $type_,
            annotation: Annotation,
        ) -> Result<(), Box<SchemaValidationError>> {
            let constraints = get_checked_constraints(annotation.to_type_constraints(type_.clone()).into_iter());
            let affected_types = TypeAPI::chain_types(
                type_.clone(),
                type_
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();

            $validation_func(snapshot, type_manager, thing_manager, &affected_types, &constraints).map_err(
                |typedb_source| {
                    Box::new(SchemaValidationError::CannotSetAnnotationAsExistingInstancesViolateItsConstraint {
                        typedb_source,
                    })
                },
            )
        }
    };
}

macro_rules! updated_constraints_compatible_with_type_and_sub_instances_on_supertype_change_validation {
    ($func_name:ident, $type_:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $type_,
            new_supertype: $type_,
        ) -> Result<(), Box<SchemaValidationError>> {
            let constraints = get_checked_constraints(
                new_supertype
                    .get_constraints(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            );
            debug_assert!(
                constraints.iter().all(|constraint| match constraint.scope() {
                    | ConstraintScope::SingleInstanceOfType | ConstraintScope::SingleInstanceOfTypeOrSubtype => true,
                    | ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes
                    | ConstraintScope::AllInstancesOfTypeOrSubtypes => false,
                }),
                concat!(
                    "It is expected that all type constraints are scoped to a single instance. ",
                    "If it changes, more validations could be required (e.g. on unset supertype)"
                )
            );

            let affected_types = TypeAPI::chain_types(
                type_.clone(),
                type_
                    .get_subtypes_transitive(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .into_iter()
                    .cloned(),
            )
            .collect();

            $validation_func(snapshot, type_manager, thing_manager, &affected_types, &constraints).map_err(
                |typedb_source| {
                    Box::new(
                        SchemaValidationError::CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances {
                            typedb_source,
                        },
                    )
                },
            )
        }
    };
}

fn collect_object_types_with_interface_type_and_subtypes<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    interface_type: CAP::InterfaceType,
    out_object_types: &mut HashSet<CAP::ObjectType>,
) -> Result<(), Box<ConceptReadError>> {
    let interface_subtypes = interface_type.get_subtypes_transitive(snapshot, type_manager)?;
    let all_interface_subtypes = TypeAPI::chain_types(interface_type, interface_subtypes.into_iter().cloned());

    for interface_subtype in all_interface_subtypes {
        out_object_types.extend(
            TypeReader::get_object_types_with_capabilities_for_interface::<CAP>(snapshot, interface_subtype.clone())?
                .into_keys(),
        );
    }

    Ok(())
}

fn collect_object_types_with_interface_type_and_supertypes_constraints<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    interface_type: CAP::InterfaceType,
    out_object_types: &mut HashMap<CAP::ObjectType, HashSet<CapabilityConstraint<CAP>>>,
) -> Result<(), Box<ConceptReadError>> {
    let interface_supertypes = interface_type.get_supertypes_transitive(snapshot, type_manager)?;
    let all_interface_supertypes = TypeAPI::chain_types(interface_type, interface_supertypes.into_iter().cloned());

    for interface_supertype in all_interface_supertypes {
        for object_type_with_interface_supertype in
            TypeReader::get_object_types_with_capabilities_for_interface::<CAP>(snapshot, interface_supertype.clone())?
                .into_keys()
        {
            #[allow(clippy::map_entry, reason = "false positive")]
            if !out_object_types.contains_key(&object_type_with_interface_supertype) {
                let constraints = TypeReader::get_type_capability_constraints::<CAP>(
                    snapshot,
                    object_type_with_interface_supertype.clone(),
                    interface_supertype.clone(),
                )?;
                out_object_types.insert(object_type_with_interface_supertype, constraints);
            }
        }
    }

    Ok(())
}

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_type_exists(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), Box<SchemaValidationError>> {
        TypeReader::get_label(snapshot, type_)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        Ok(())
    }

    pub(crate) fn validate_no_subtypes_for_type_deletion<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        let no_subtypes = type_
            .get_subtypes(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::CannotDeleteTypeWithExistingSubtypes {
                label: get_label_or_schema_err(snapshot, type_manager, type_)?,
            }))
        }
    }

    pub(crate) fn validate_no_attribute_subtypes_to_unset_abstractness(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let no_subtypes = attribute_type
            .get_subtypes(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes {
                attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
            }))
        }
    }

    pub(crate) fn validate_label_uniqueness(
        snapshot: &impl ReadableSnapshot,
        new_label: &Label<'static>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if TypeReader::get_labelled_type::<AttributeType>(snapshot, new_label)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_some()
        {
            Err(Box::new(SchemaValidationError::LabelShouldBeUnique {
                label: new_label.clone(),
                existing_kind: Kind::Attribute,
            }))
        } else if TypeReader::get_labelled_type::<RelationType>(snapshot, new_label)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_some()
        {
            Err(Box::new(SchemaValidationError::LabelShouldBeUnique {
                label: new_label.clone(),
                existing_kind: Kind::Relation,
            }))
        } else if TypeReader::get_labelled_type::<EntityType>(snapshot, new_label)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_some()
        {
            Err(Box::new(SchemaValidationError::LabelShouldBeUnique {
                label: new_label.clone(),
                existing_kind: Kind::Entity,
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_new_role_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        label: &Label<'static>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let existing_relation_supertypes = relation_type
            .get_supertypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let existing_relation_subtypes = relation_type
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        validate_role_name_uniqueness_non_transitive(snapshot, type_manager, relation_type, label)?;
        for relation_supertype in existing_relation_supertypes.into_iter() {
            validate_role_name_uniqueness_non_transitive(snapshot, type_manager, *relation_supertype, label)?;
        }
        for relation_subtype in existing_relation_subtypes.into_iter() {
            validate_role_name_uniqueness_non_transitive(snapshot, type_manager, *relation_subtype, label)?;
        }

        Ok(())
    }

    pub(crate) fn validate_role_names_compatible_with_new_relation_supertype_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_subtype: RelationType,
        relation_supertype: RelationType,
    ) -> Result<(), Box<SchemaValidationError>> {
        for_type_and_subtypes_transitive!(snapshot, type_manager, relation_subtype, |type_: RelationType| {
            Self::validate_role_names_compatible_with_new_relation_supertype(
                snapshot,
                type_manager,
                type_,
                relation_supertype,
            )
        });
        Ok(())
    }

    fn validate_role_names_compatible_with_new_relation_supertype(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_subtype: RelationType,
        relation_supertype: RelationType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let relation_supertype_supertypes = relation_supertype
            .get_supertypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let affected_supertypes =
            TypeAPI::chain_types(relation_supertype, relation_supertype_supertypes.into_iter().cloned()).collect_vec();
        let subtype_relates_root = relation_subtype
            .get_relates_root(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for subtype_relates in subtype_relates_root.into_iter() {
            let role = subtype_relates.role();
            let role_label = get_label_or_schema_err(snapshot, type_manager, role)?;

            affected_supertypes.iter().try_for_each(|supertype| {
                validate_role_name_uniqueness_non_transitive(snapshot, type_manager, *supertype, &role_label)
            })?;
        }

        Ok(())
    }

    pub(crate) fn validate_struct_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<(), Box<SchemaValidationError>> {
        let struct_clash = TypeReader::get_struct_definition_key(snapshot, name)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .is_some();
        // TODO: Check other types clash?

        if struct_clash {
            Err(Box::new(SchemaValidationError::StructNameShouldBeUnique { name: name.to_owned() }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_deleted_struct_is_not_used_in_schema(
        snapshot: &impl ReadableSnapshot,
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        let usages_in_attribute_types = TypeReader::get_struct_definition_usages_in_attribute_types(snapshot)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        if let Some(owners) = usages_in_attribute_types.get(definition_key) {
            return Err(Box::new(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes {
                name: struct_definition.name.to_owned(),
                usages: owners.len(),
            }));
        }

        let usages_in_struct_definition_fields =
            TypeReader::get_struct_definition_usages_in_struct_definitions(snapshot)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        if let Some(owners) = usages_in_struct_definition_fields.get(definition_key) {
            return Err(Box::new(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs {
                name: struct_definition.name.to_owned(),
                usages: owners.len(),
            }));
        }

        Ok(())
    }

    pub(crate) fn validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        subtype: AttributeType,
        supertype: Option<AttributeType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let subtype_declared_value_type = subtype
            .get_value_type_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let subtype_transitive_value_type = subtype
            .get_value_type_without_source(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let supertype_value_type = match &supertype {
            None => None,
            Some(supertype) => supertype
                .get_value_type_without_source(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        };

        match (&subtype_declared_value_type, &subtype_transitive_value_type, &supertype_value_type) {
            (None, None, None) => Ok(()),
            (None, None, Some(_)) => Ok(()),
            (None, Some(_), None) => Self::validate_when_attribute_type_loses_value_type_transitive(
                snapshot,
                type_manager,
                thing_manager,
                subtype,
                subtype_transitive_value_type,
            ),
            (Some(_), None, None) => Ok(()),
            (Some(_), Some(_), None) => Ok(()),
            | (None, Some(old_value_type), Some(new_value_type))
            | (Some(old_value_type), None, Some(new_value_type))
            | (Some(old_value_type), Some(_), Some(new_value_type)) => {
                if old_value_type == new_value_type {
                    Ok(())
                } else {
                    Err(Box::new(
                        SchemaValidationError::ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes {
                            attribute: get_label_or_schema_err(snapshot, type_manager, subtype)?,
                            value_type: subtype_declared_value_type,
                            new_super_value_type: new_value_type.clone(),
                        },
                    ))
                }
            }
        }
    }

    pub(crate) fn validate_value_type_can_be_unset(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let value_type_with_source = attribute_type
            .get_value_type(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        match value_type_with_source {
            Some((value_type, source)) => {
                if source != attribute_type {
                    return Err(Box::new(SchemaValidationError::CannotUnsetInheritedValueType {
                        value_type,
                        subtype: get_label_or_schema_err(snapshot, type_manager, source)?,
                    }));
                }

                let attribute_supertype = attribute_type
                    .get_supertype(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                match &attribute_supertype {
                    Some(supertype) => {
                        let supertype_value_type_with_source = supertype
                            .get_value_type(snapshot, type_manager)
                            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                        match supertype_value_type_with_source {
                            Some(_) => Ok(()),
                            None => Self::validate_when_attribute_type_loses_value_type_transitive(
                                snapshot,
                                type_manager,
                                thing_manager,
                                attribute_type,
                                Some(value_type),
                            ),
                        }
                    }
                    None => Self::validate_when_attribute_type_loses_value_type_transitive(
                        snapshot,
                        type_manager,
                        thing_manager,
                        attribute_type,
                        Some(value_type),
                    ),
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_value_type_compatible_with_abstractness(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
        abstract_set: Option<bool>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let is_abstract = abstract_set.unwrap_or(
            attribute_type
                .is_abstract(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        );

        match &value_type {
            Some(_) => Ok(()),
            None => {
                if is_abstract {
                    Ok(())
                } else {
                    Err(Box::new(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract {
                        attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                    }))
                }
            }
        }
    }

    pub(crate) fn validate_annotation_regex_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if AnnotationRegex::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::ValueTypeIsNotCompatibleWithRegexAnnotation {
                attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                value_type,
            }))
        }
    }

    pub(crate) fn validate_annotation_range_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if AnnotationRange::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::ValueTypeIsNotCompatibleWithRangeAnnotation {
                attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                value_type,
            }))
        }
    }

    pub(crate) fn validate_annotation_values_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if AnnotationValues::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::ValueTypeIsNotCompatibleWithValuesAnnotation {
                attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                value_type,
            }))
        }
    }

    pub(crate) fn validate_type_regex_narrows_supertype_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        regex: AnnotationRegex,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Regex(regex);
        if let Some(supertype) = attribute_type
            .get_supertype(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            let supertype_constraints = supertype
                .get_constraints_regex(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            for supertype_constraint in supertype_constraints.into_iter() {
                supertype_constraint.validate_narrowed_by_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let supertype_label = match get_label_or_schema_err(snapshot, type_manager, supertype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: attribute_type_label,
                            supertype: supertype_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_type_range_narrows_supertype_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        range: AnnotationRange,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Range(range);
        if let Some(supertype) = attribute_type
            .get_supertype(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            let supertype_constraints = supertype
                .get_constraints_range(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            for supertype_constraint in supertype_constraints.into_iter() {
                supertype_constraint.validate_narrowed_by_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let supertype_label = match get_label_or_schema_err(snapshot, type_manager, supertype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: attribute_type_label,
                            supertype: supertype_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_type_values_narrows_supertype_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        values: AnnotationValues,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Values(values);
        if let Some(supertype) = attribute_type
            .get_supertype(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            let supertype_constraints = supertype
                .get_constraints_values(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            for supertype_constraint in supertype_constraints.into_iter() {
                supertype_constraint.validate_narrowed_by_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let supertype_label = match get_label_or_schema_err(snapshot, type_manager, supertype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: attribute_type_label,
                            supertype: supertype_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_regex(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        regex: AnnotationRegex,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Regex(regex);
        let subtypes = attribute_type
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for subtype in subtypes.into_iter() {
            let regex_constraints = subtype
                .get_constraints_regex(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for regex_constraint in regex_constraints {
                regex_constraint.validate_narrows_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let subtype_label = match get_label_or_schema_err(snapshot, type_manager, *subtype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: subtype_label,
                            supertype: attribute_type_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_range(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        range: AnnotationRange,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Range(range);
        let subtypes = attribute_type
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for subtype in subtypes.into_iter() {
            let range_constraints = subtype
                .get_constraints_range(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for range_constraint in range_constraints {
                range_constraint.validate_narrows_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let subtype_label = match get_label_or_schema_err(snapshot, type_manager, *subtype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: subtype_label,
                            supertype: attribute_type_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_values(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        values: AnnotationValues,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Values(values);
        let subtypes = attribute_type
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for subtype in subtypes.into_iter() {
            let values_constraints = subtype
                .get_constraints_regex(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for values_constraint in values_constraints {
                values_constraint.validate_narrows_strictly_same_type(&constraint_description).map_err(
                    |typedb_source| {
                        let subtype_label = match get_label_or_schema_err(snapshot, type_manager, *subtype) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        let attribute_type_label = match get_label_or_schema_err(snapshot, type_manager, attribute_type)
                        {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::SubtypeConstraintDoesNotNarrowSupertypeConstraint {
                            subtype: subtype_label,
                            supertype: attribute_type_label,
                            typedb_source,
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_constraints_on_capabilities_narrow_regex_on_interface_type_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_regex: AnnotationRegex,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Regex(type_regex);
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            Self::validate_constraints_on_capabilities_narrow_regex_on_interface_type(
                snapshot,
                type_manager,
                type_,
                &constraint_description,
            )
        });
        Ok(())
    }

    fn validate_constraints_on_capabilities_narrow_regex_on_interface_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_constraint_description: &ConstraintDescription,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_owns = attribute_type
            .get_owns(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for owns in all_owns.into_iter() {
            let regex_constraints = owns
                .get_constraints_regex(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for regex_constraint in regex_constraints {
                regex_constraint.validate_narrows_strictly_same_type(type_constraint_description)
                    .map_err(|typedb_source| {
                        let label = match get_label_or_schema_err(snapshot, type_manager, attribute_type) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint {
                            interface: label,
                            typedb_source,
                        })
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_constraints_on_capabilities_narrow_range_on_interface_type_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_range: AnnotationRange,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Range(type_range);
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            Self::validate_constraints_on_capabilities_narrow_range_on_interface_type(
                snapshot,
                type_manager,
                type_,
                &constraint_description,
            )
        });
        Ok(())
    }

    fn validate_constraints_on_capabilities_narrow_range_on_interface_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_constraint_description: &ConstraintDescription,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_owns = attribute_type
            .get_owns(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for owns in all_owns.into_iter() {
            let range_constraints = owns
                .get_constraints_range(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for range_constraint in range_constraints {
                range_constraint.validate_narrows_strictly_same_type(type_constraint_description)
                    .map_err(|typedb_source| {
                        let label = match get_label_or_schema_err(snapshot, type_manager, attribute_type) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint {
                            interface: label,
                            typedb_source,
                        })
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_constraints_on_capabilities_narrow_values_on_interface_type_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_values: AnnotationValues,
    ) -> Result<(), Box<SchemaValidationError>> {
        let constraint_description = ConstraintDescription::Values(type_values);
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            Self::validate_constraints_on_capabilities_narrow_values_on_interface_type(
                snapshot,
                type_manager,
                type_,
                &constraint_description,
            )
        });
        Ok(())
    }

    fn validate_constraints_on_capabilities_narrow_values_on_interface_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        type_constraint_description: &ConstraintDescription,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_owns = attribute_type
            .get_owns(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for owns in all_owns.into_iter() {
            let values_constraints = owns
                .get_constraints_values(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            for values_constraint in values_constraints {
                values_constraint.validate_narrows_strictly_same_type(type_constraint_description)
                    .map_err(|typedb_source| {
                        let label = match get_label_or_schema_err(snapshot, type_manager, attribute_type) {
                            Ok(label) => label,
                            Err(err) => return err,
                        };
                        Box::new(SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint {
                            interface: label,
                            typedb_source,
                        })
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_capability_regex_constraint_narrows_interface_type_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        capability_regex: AnnotationRegex,
    ) -> Result<(), Box<SchemaValidationError>> {
        let capability_constraint_description = ConstraintDescription::Regex(capability_regex);
        let attribute_type_regex_constraints = owns
            .attribute()
            .get_constraints_regex(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for regex_constraint in attribute_type_regex_constraints {
            regex_constraint.validate_narrowed_by_strictly_same_type(&capability_constraint_description)
                .map_err(|typedb_source| {
                    let owner_label = match get_label_or_schema_err(snapshot, type_manager, owns.owner()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    let attribute_label = match get_label_or_schema_err(snapshot, type_manager, owns.attribute()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    Box::new(SchemaValidationError::CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint {
                        cap: CapabilityKind::Owns,
                        label: owner_label,
                        interface: attribute_label,
                        typedb_source,
                    })
                })?;
        }
        Ok(())
    }

    pub(crate) fn validate_capability_range_constraint_narrows_interface_type_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        capability_range: AnnotationRange,
    ) -> Result<(), Box<SchemaValidationError>> {
        let capability_constraint_description = ConstraintDescription::Range(capability_range);
        let attribute_type_range_constraints = owns
            .attribute()
            .get_constraints_range(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for range_constraint in attribute_type_range_constraints {
            range_constraint.validate_narrowed_by_strictly_same_type(&capability_constraint_description)
                .map_err(|typedb_source| {
                    let owner_label = match get_label_or_schema_err(snapshot, type_manager, owns.owner()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    let attribute_label = match get_label_or_schema_err(snapshot, type_manager, owns.attribute()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    Box::new( SchemaValidationError::CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint {
                        cap: CapabilityKind::Owns,
                        interface: attribute_label,
                        label: owner_label,
                        typedb_source,
                    })
                })?;
        }
        Ok(())
    }

    pub(crate) fn validate_capability_values_constraint_narrows_interface_type_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        capability_values: AnnotationValues,
    ) -> Result<(), Box<SchemaValidationError>> {
        let capability_constraint_description = ConstraintDescription::Values(capability_values);
        let attribute_type_values_constraints = owns
            .attribute()
            .get_constraints_values(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for values_constraint in attribute_type_values_constraints {
            values_constraint.validate_narrowed_by_strictly_same_type(&capability_constraint_description)
                .map_err(|typedb_source| {
                    let owner_label = match get_label_or_schema_err(snapshot, type_manager, owns.owner()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    let attribute_label = match get_label_or_schema_err(snapshot, type_manager, owns.attribute()) {
                        Ok(label) => label,
                        Err(err) => return err,
                    };
                    Box::new(SchemaValidationError::CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint {
                        cap: CapabilityKind::Owns,
                        label: owner_label,
                        interface: attribute_label,
                        typedb_source,
                    })
                })?;
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_type_supertype_is_abstract<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        if type_
            .is_abstract(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::AttributeTypeSupertypeIsNotAbstract {
                attribute: get_label_or_schema_err(snapshot, type_manager, type_)?,
            }))
        }
    }

    pub(crate) fn validate_cardinality_arguments(
        cardinality: AnnotationCardinality,
    ) -> Result<(), Box<SchemaValidationError>> {
        if cardinality.valid() {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidCardinalityArguments { card: cardinality }))
        }
    }

    pub(crate) fn validate_regex_arguments(regex: AnnotationRegex) -> Result<(), Box<SchemaValidationError>> {
        if regex.valid() {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidRegexArguments { regex }))
        }
    }

    pub(crate) fn validate_range_arguments(
        range: AnnotationRange,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if range.valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidRangeArgumentsForValueType { range, value_type }))
        }
    }

    pub(crate) fn validate_values_arguments(
        values: AnnotationValues,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if values.valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidValuesArgumentsForValueType { values, value_type }))
        }
    }

    // TODO: Capabilities constraints narrowing checks are currently disabled
    pub(crate) fn validate_cardinality_of_inheritance_line_with_updated_capabilities<CAP: Capability<'static>>(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        _capability: CAP,
        _is_set: bool,
    ) -> Result<(), Box<SchemaValidationError>> {
        // let mut validation_errors = vec![];
        // let updated_capabilities = HashMap::from([(capability.clone(), is_set)]);
        // let object_and_subtypes = once(capability.object()).chain(capability.object().get_subtypes_transitive(snapshot, type_manager)?.into_iter());
        //
        // for object_type in object_and_subtypes {
        //     validate_capabilities_cardinalities_narrowing::<CAP>(
        //         snapshot,
        //         type_manager,
        //         object_type,
        //         &updated_capabilities,
        //         &HashMap::new(), // read all cardinalities from storage as it's not changed
        //         &HashMap::new(), // read all hidden from storage as it's not changed
        //         &mut validation_errors,
        //     )
        //     .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source}) )?;
        //
        //     if let Some(error) = validation_errors.first() {
        //         return Err(error.clone());
        //     }
        // }

        Ok(())
    }

    // TODO: Capabilities constraints narrowing checks are currently disabled
    pub(crate) fn validate_cardinality_of_inheritance_line_with_updated_cardinality<CAP: Capability<'static>>(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        _capability: CAP,
        _cardinality: AnnotationCardinality,
    ) -> Result<(), Box<SchemaValidationError>> {
        // let mut validation_errors = vec![];
        // let updated_cardinalities = HashMap::from([(capability.clone(), cardinality)]);
        // let object_and_subtypes = once(capability.object())
        //     .chain(capability.object().get_subtypes_transitive(snapshot, type_manager)?.into_iter());
        //
        // for object_type in object_and_subtypes {
        //     validate_capabilities_cardinalities_narrowing::<CAP>(
        //         snapshot,
        //         type_manager,
        //         object_type,
        //         &HashMap::new(), // read all capabilities from storage as it's not changed
        //         &updated_cardinalities,
        //         &HashMap::new(), // read all hidden from storage as it's not changed
        //         &mut validation_errors,
        //     )
        //     .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source}) )?;
        //
        //     if let Some(error) = validation_errors.first() {
        //         return Err(error.clone());
        //     }
        // }

        Ok(())
    }

    // TODO: Capabilities constraints narrowing checks are currently disabled
    pub(crate) fn validate_cardinality_of_inheritance_line_with_updated_interface_type_supertype<
        CAP: Capability<'static>,
    >(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        _interface_type: CAP::InterfaceType,
        _new_supertype: Option<CAP::InterfaceType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        // TODO: Implement

        Ok(())
    }

    pub(crate) fn validate_type_supertype_abstractness_to_set_abstract_annotation<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        validate_type_supertype_abstractness(
            snapshot,
            type_manager,
            type_,
            None,       // supertype is read from storage
            Some(true), // set_subtype_abstract
            None,       // supertype is abstract is read from storage
        )
    }

    pub(crate) fn validate_no_abstract_subtypes_to_unset_abstract_annotation<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        type_
            .get_subtypes(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .into_iter()
            .try_for_each(|subtype| {
                validate_type_supertype_abstractness(
                    snapshot,
                    type_manager,
                    subtype.clone(),
                    Some(type_.clone()), // supertype
                    None,                // subtype is abstract is read from storage
                    Some(false),         // set_supertype_abstract
                )
            })?;
        Ok(())
    }

    pub(crate) fn validate_relates_is_not_specialising_to_unset_abstract_annotation(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relates: Relates,
    ) -> Result<(), Box<SchemaValidationError>> {
        if relates
            .is_specialising(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            Err(Box::new(SchemaValidationError::CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates {
                role: relates.role().get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_specialises_compatible_with_new_supertype_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_subtype: RelationType,
        relation_supertype: Option<RelationType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let supertype_relates = match &relation_supertype {
            None => MaybeOwns::Owned(HashSet::new()),
            Some(relation_supertype) => relation_supertype
                .get_relates(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        };

        let subtype_relates_declared = relation_subtype
            .get_relates_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let subtype_relates = relation_subtype
            .get_relates(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for subtype_capability in &subtype_relates_declared {
            if let Some(subtype_role_supertype) = subtype_capability
                .role()
                .get_supertype(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            {
                let subtype_role_supertype_relates = subtype_role_supertype
                    .get_relates_root(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                if !supertype_relates
                    .iter()
                    .any(|supertype_relates| supertype_relates == &subtype_role_supertype_relates)
                {
                    return Err(Box::new(
                        SchemaValidationError::CannotChangeRelationTypeSupertypeAsRelatesSpecialiseIsLost {
                            relation: relation_subtype
                                .get_label(snapshot, type_manager)
                                .unwrap()
                                .as_reference()
                                .into_owned(),
                            // relation_supertype,
                            role_1: subtype_capability
                                .role()
                                .get_label(snapshot, type_manager)
                                .unwrap()
                                .as_reference()
                                .into_owned(),
                            role_2: subtype_role_supertype
                                .get_label(snapshot, type_manager)
                                .unwrap()
                                .as_reference()
                                .into_owned(),
                        },
                    ));
                }
            }
        }

        let subtype_subtypes = relation_subtype
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for subsubtype in subtype_subtypes.into_iter() {
            let subsubtype_relates_declared = subsubtype
                .get_relates_declared(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            for subsubtype_relates in subsubtype_relates_declared.into_iter() {
                if let Some(subtype_role_supertype) = subsubtype_relates
                    .role()
                    .get_supertype(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                {
                    let subtype_role_supertype_relates = subtype_role_supertype
                        .get_relates_root(snapshot, type_manager)
                        .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                    let is_in_subtype = subtype_relates.contains(&subtype_role_supertype_relates);
                    let is_in_subtype_declared = subtype_relates_declared.contains(&subtype_role_supertype_relates);
                    let is_lost = is_in_subtype && !is_in_subtype_declared;
                    if is_lost
                        && !supertype_relates
                            .iter()
                            .any(|supertype_relates| supertype_relates == &subtype_role_supertype_relates)
                    {
                        return Err(Box::new(
                            SchemaValidationError::CannotChangeRelationTypeSupertypeAsRelatesSpecialiseIsLost {
                                relation: relation_subtype
                                    .get_label(snapshot, type_manager)
                                    .unwrap()
                                    .as_reference()
                                    .into_owned(),
                                // relation_supertype,
                                role_1: subsubtype_relates
                                    .role()
                                    .get_label(snapshot, type_manager)
                                    .unwrap()
                                    .as_reference()
                                    .into_owned(),
                                role_2: subtype_role_supertype
                                    .get_label(snapshot, type_manager)
                                    .unwrap()
                                    .as_reference()
                                    .into_owned(),
                            },
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_set_owns_does_not_conflict_with_same_existing_owns_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        existing_owns: Owns,
        new_owns_ordering: Ordering,
    ) -> Result<(), Box<SchemaValidationError>> {
        let existing_ordering = existing_owns
            .get_ordering(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        if existing_ordering == new_owns_ordering {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering {
                owner: get_label_or_schema_err(snapshot, type_manager, existing_owns.owner())?,
                conflicting_owner: get_label_or_schema_err(snapshot, type_manager, existing_owns.attribute())?,
                // or existing_ordering,
            }))
        }
    }

    pub(crate) fn validate_updated_owns_does_not_conflict_with_any_sibling_owns_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        owns_ordering: Ordering,
    ) -> Result<(), Box<SchemaValidationError>> {
        let updated_owns = HashMap::from([(owns, owns_ordering)]);
        let owner_subtypes = owns
            .owner()
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let affected_owner_types = TypeAPI::chain_types(owns.owner(), owner_subtypes.into_iter().cloned());

        for owner_type in affected_owner_types {
            validate_sibling_owns_ordering_match_for_type(snapshot, type_manager, owner_type, &updated_owns)?;
        }

        Ok(())
    }

    pub(crate) fn validate_new_attribute_type_supertype_owns_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        new_supertype: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let attribute_type_owners = attribute_type
            .get_owner_types(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let new_supertype_owners = new_supertype
            .get_owner_types(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for (attribute_type_owner, attribute_type_owns) in attribute_type_owners.into_iter() {
            if let Some(new_supertype_owns) = new_supertype_owners.get(attribute_type_owner) {
                let attribute_type_ordering = attribute_type_owns
                    .get_ordering(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                let new_supertype_ordering = new_supertype_owns
                    .get_ordering(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
                if attribute_type_ordering != new_supertype_ordering {
                    return Err(Box::new(
                        SchemaValidationError::OrderingDoesNotMatchWithCapabilityOfSupertypeInterface {
                            label: get_label_or_schema_err(snapshot, type_manager, *attribute_type_owner)?,
                            super_label: get_label_or_schema_err(snapshot, type_manager, new_supertype)?,
                            interface: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                            expected: new_supertype_ordering,
                            found: attribute_type_ordering,
                        },
                    ));
                }
            }
        }

        Ok(())
    }

    fn is_ordering_compatible_with_distinct_constraint(ordering: Ordering, distinct_set: bool) -> bool {
        if distinct_set {
            ordering == Ordering::Ordered
        } else {
            true
        }
    }

    pub(crate) fn validate_relates_distinct_constraint_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relates: Relates,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let role = relates.role();
        let ordering = ordering.unwrap_or(
            TypeReader::get_type_ordering(snapshot, role)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        );
        let distinct_set = distinct_set.unwrap_or(
            relates
                .get_annotations_declared(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .contains(&RelatesAnnotation::Distinct(AnnotationDistinct)),
        );

        if Self::is_ordering_compatible_with_distinct_constraint(ordering, distinct_set) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidOrderingForDistinctAnnotation {
                label: get_label_or_schema_err(snapshot, type_manager, role)?,
                ordering,
            }))
        }
    }

    pub(crate) fn validate_owns_distinct_constraint_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let attribute = owns.attribute();
        let ordering = ordering.unwrap_or(
            owns.get_ordering(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        );
        let distinct_set = distinct_set.unwrap_or(
            owns.get_annotations_declared(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .contains(&OwnsAnnotation::Distinct(AnnotationDistinct)),
        );

        if Self::is_ordering_compatible_with_distinct_constraint(ordering, distinct_set) {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::InvalidOrderingForDistinctAnnotation {
                label: get_label_or_schema_err(snapshot, type_manager, attribute)?,
                ordering,
            }))
        }
    }

    pub(crate) fn validate_role_supertype_ordering_match(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        subtype_role: RoleType,
        supertype_role: RoleType,
        set_subtype_role_ordering: Option<Ordering>,
        set_supertype_role_ordering: Option<Ordering>,
    ) -> Result<(), Box<SchemaValidationError>> {
        validate_role_type_supertype_ordering_match(
            snapshot,
            type_manager,
            subtype_role,
            supertype_role,
            set_subtype_role_ordering,
            set_supertype_role_ordering,
        )
    }

    pub(crate) fn validate_type_supertype_abstractness_to_change_supertype<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        subtype: T,
        supertype: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        validate_type_supertype_abstractness(
            snapshot,
            type_manager,
            subtype,
            Some(supertype),
            None, // subtype is abstract is read from storage
            None, // supertype is abstract is read from storage
        )
    }

    pub(crate) fn validate_type_declared_constraints_narrowing_of_supertype_constraints<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        subtype: T,
        supertype: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        validate_type_declared_constraints_narrowing_of_supertype_constraints(
            snapshot,
            type_manager,
            subtype.clone(),
            supertype.clone(),
        )
    }

    pub(crate) fn validate_sub_does_not_create_cycle<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
        supertype: T,
    ) -> Result<(), Box<SchemaValidationError>> {
        let existing_supertypes = supertype
            .get_supertypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        if supertype == type_ || existing_supertypes.contains(&type_) {
            Err(Box::new(SchemaValidationError::CycleFoundInTypeHierarchy {
                start: get_label_or_schema_err(snapshot, type_manager, type_)?,
                end: get_label_or_schema_err(snapshot, type_manager, supertype)?,
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_is_inherited(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        role_type: RoleType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let super_relation = relation_type
            .get_supertype(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        if let Some(super_relation) = super_relation {
            let is_inherited = super_relation
                .get_relates(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .iter()
                .any(|relates| relates.role() == role_type);
            if is_inherited {
                Ok(())
            } else {
                Err(Box::new(SchemaValidationError::RelatesNotInherited {
                    relation: relation_type.get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
                    role: role_type.get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
                }))
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_specialised_relates_is_not_specialising(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        specialised_relates: Relates,
    ) -> Result<(), Box<SchemaValidationError>> {
        if !specialised_relates
            .is_specialising(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::RelatesIsAlreadySpecialisedByASupertype {
                relation: relation_type.get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
                role: specialised_relates.role().get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
            }))
        }
    }

    pub(crate) fn validate_relates_is_not_specialising_to_manage_annotations(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relates: Relates,
    ) -> Result<(), Box<SchemaValidationError>> {
        if !relates
            .is_specialising(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
        {
            Ok(())
        } else {
            Err(Box::new(SchemaValidationError::CannotManageAnnotationsForSpecialisingRelates {
                role: relates.role().get_label(snapshot, type_manager).unwrap().as_reference().into_owned(),
            }))
        }
    }

    pub(crate) fn validate_value_type_compatible_with_inherited_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: ValueType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let inherited_value_type_with_source = attribute_type
            .get_value_type(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        match inherited_value_type_with_source {
            Some((inherited_value_type, inherited_value_type_source)) => {
                if inherited_value_type == value_type || inherited_value_type_source == attribute_type {
                    Ok(())
                } else {
                    Err(Box::new(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueType {
                        label: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                        super_label: get_label_or_schema_err(snapshot, type_manager, inherited_value_type_source)?,
                        value_type,
                        super_value_type: inherited_value_type,
                    }))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_subtypes_value_types_compatible_with_new_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: ValueType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let subtypes = attribute_type
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for subtype in subtypes.into_iter() {
            if let Some(subtype_value_type) = subtype
                .get_value_type_declared(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            {
                if subtype_value_type != value_type {
                    return Err(Box::new(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueType {
                        label: get_label_or_schema_err(snapshot, type_manager, *subtype)?,
                        super_label: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                        value_type: subtype_value_type,
                        super_value_type: value_type,
                    }));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_when_attribute_type_loses_value_type_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let mut affected_sources: HashSet<AttributeType> = attribute_type
            .get_supertypes_transitive(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
            .into_iter()
            .cloned()
            .collect();
        affected_sources.insert(attribute_type);
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            let type_value_type_with_source = type_
                .get_value_type(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
            if let Some((type_value_type, type_value_type_source)) = type_value_type_with_source {
                if affected_sources.contains(&type_value_type_source) {
                    debug_assert!(value_type.clone().unwrap_or(type_value_type.clone()) == type_value_type);
                    Self::validate_when_attribute_type_loses_value_type(
                        snapshot,
                        type_manager,
                        thing_manager,
                        type_,
                        value_type.clone(),
                    )?;
                }
            }
            Ok::<_, Box<_>>(())
        });
        Ok(())
    }

    fn validate_when_attribute_type_loses_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        match value_type {
            Some(_) => {
                Self::validate_value_type_compatible_with_abstractness(
                    snapshot,
                    type_manager,
                    attribute_type,
                    None,
                    None,
                )?;
                Self::validate_attribute_type_value_type_compatible_with_annotations(
                    snapshot,
                    type_manager,
                    attribute_type,
                    None,
                )?;
                Self::validate_value_type_compatible_with_all_owns_annotations(
                    snapshot,
                    type_manager,
                    attribute_type,
                    None,
                )?;
                Self::validate_no_instances_to_lose_value_type(snapshot, type_manager, thing_manager, attribute_type)
            }
            None => Ok(()),
        }
    }

    // TODO: Cascade constraint does not exist. Revisit it after cascade returns.
    // pub(crate) fn validate_relation_type_does_not_acquire_cascade_constraint_to_lose_instances_with_new_supertype(
    //     snapshot: &impl ReadableSnapshot,
    //     type_manager: &TypeManager,
    //     thing_manager: &ThingManager,
    //     relation_type: RelationType,
    //     new_supertype: RelationType,
    // ) -> Result<(), Box<SchemaValidationError>> {
    //     let old_annotation_with_source = type_get_annotation_with_source_by_category(
    //         snapshot,
    //         type_manager,
    //         relation_type.clone(),
    //         AnnotationCategory::Cascade,
    //     )
    //     .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
    //     match old_annotation_with_source {
    //         None => {
    //             let new_supertype_annotation = type_get_annotation_with_source_by_category(
    //                 snapshot,
    //                 type_manager,
    //                 new_supertype.clone(),
    //                 AnnotationCategory::Cascade,
    //             )
    //             .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
    //             match new_supertype_annotation {
    //                 None => Ok(()),
    //                 Some(_) => {
    //                     for_type_and_subtypes_transitive!(
    //                         snapshot,
    //                         type_manager,
    //                         relation_type,
    //                         |type_: RelationType| {
    //                             let type_annotation = type_get_annotation_with_source_by_category(
    //                                 snapshot,
    //                                 type_manager,
    //                                 type_.clone(),
    //                                 AnnotationCategory::Cascade,
    //                             )
    //                             .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
    //                             if type_annotation.is_none() {
    //                                 let type_has_instances =
    //                                     Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
    //                                         .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
    //                                 if type_has_instances {
    //                                     return Err(Box::new(SchemaValidationError::ChangingRelationSupertypeLeadsToImplicitCascadeAnnotationAcquisitionAndUnexpectedDataLoss(
    //                                     get_label_or_schema_err(snapshot, type_manager, relation_type.clone())?,
    //                                     get_label_or_schema_err(snapshot, type_manager, new_supertype.clone())?,
    //                                     get_label_or_schema_err(snapshot, type_manager, type_.clone())?,
    //                                 ));
    //                                 }
    //                             }
    //                             Ok(())
    //                         }
    //                     );
    //                     Ok(())
    //                 }
    //             }
    //         }
    //         Some(_) => Ok(()),
    //     }
    // }

    pub(crate) fn validate_attribute_type_does_not_lose_instances_with_independent_constraint_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        new_supertype: Option<AttributeType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let subtype_constraint_source = type_get_constraints_closest_source(
            snapshot,
            type_manager,
            attribute_type
                .get_constraints_independent(snapshot, type_manager)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                .iter(),
        );
        let supertype_constraint_source = match &new_supertype {
            None => None,
            Some(new_supertype) => type_get_constraints_closest_source(
                snapshot,
                type_manager,
                new_supertype
                    .get_constraints_independent(snapshot, type_manager)
                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                    .iter(),
            ),
        };

        match (subtype_constraint_source, supertype_constraint_source) {
            (Some(subtype_source), None) => {
                if subtype_source != attribute_type {
                    let lost_source = subtype_source;
                    for_type_and_subtypes_transitive!(
                        snapshot,
                        type_manager,
                        attribute_type,
                        |type_: AttributeType| {
                            let sub_subtype_constraint_source = type_get_constraints_closest_source(
                                snapshot,
                                type_manager,
                                type_
                                    .get_constraints_independent(snapshot, type_manager)
                                    .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?
                                    .iter(),
                            );
                            match sub_subtype_constraint_source {
                                None => {
                                    debug_assert!(
                                        false,
                                        "This annotation should be inherited by all the subtypes of the attribute type"
                                    );
                                    Ok(())
                                }
                                Some(sub_subtype_constraint_source) => {
                                    if lost_source == sub_subtype_constraint_source {
                                        let type_has_instances =
                                            Self::has_instances_of_type(snapshot, type_manager, thing_manager, type_)
                                                .map_err(|source| {
                                                Box::new(SchemaValidationError::ConceptRead { source })
                                            })?;
                                        if type_has_instances {
                                            return Err(Box::new(SchemaValidationError::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss{
                                                attribute: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                                                supertype: get_opt_label_or_schema_err(snapshot, type_manager, new_supertype)?,
                                            }));
                                        }
                                    }
                                    Ok(())
                                }
                            }
                        }
                    );
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_unset_owns_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owner: ObjectType,
        attribute_type: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_owns = owner
            .get_owns(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let found_owns = all_owns.iter().find(|owns| owns.attribute() == attribute_type);

        match found_owns {
            Some(owns) => {
                if owner == owns.owner() {
                    Ok(())
                } else {
                    Err(Box::new(SchemaValidationError::CannotUnsetInheritedOwns {
                        subtype: get_label_or_schema_err(snapshot, type_manager, attribute_type)?,
                        supertype: get_label_or_schema_err(snapshot, type_manager, owns.owner())?,
                    }))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_plays_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        player: ObjectType,
        role_type: RoleType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_plays = player
            .get_plays(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let found_plays = all_plays.iter().find(|plays| plays.role() == role_type);

        match found_plays {
            Some(plays) => {
                if player == plays.player() {
                    Ok(())
                } else {
                    Err(Box::new(SchemaValidationError::CannotUnsetInheritedPlays {
                        subtype: get_label_or_schema_err(snapshot, type_manager, role_type)?,
                        supertype: get_label_or_schema_err(snapshot, type_manager, plays.player())?,
                    }))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_declared_type_annotation_is_compatible_with_declared_annotations(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<SchemaValidationError>> {
        let existing_annotations = type_
            .get_annotations_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for existing_annotation in existing_annotations.into_iter() {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                return Err(Box::new(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation {
                    annotation: annotation_category,
                    declared_annotation: existing_annotation_category,
                    label: get_label_or_schema_err(snapshot, type_manager, type_)?,
                }));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_declared_capability_annotation_is_compatible_with_declared_annotations<
        CAP: Capability<'static>,
    >(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<SchemaValidationError>> {
        let existing_annotations = capability
            .get_annotations_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for existing_annotation in existing_annotations.into_iter() {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                let interface = capability.interface();
                return Err(Box::new(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation {
                    annotation: annotation_category,
                    declared_annotation: existing_annotation_category,
                    label: get_label_or_schema_err(snapshot, type_manager, interface)?,
                }));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_unique_annotation(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if AnnotationUnique::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(
                SchemaValidationError::ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns {
                    owner: get_label_or_schema_err(snapshot, type_manager, owns.owner())?,
                    attribute: get_label_or_schema_err(snapshot, type_manager, owns.attribute())?,
                    value_type,
                },
            ))
        }
    }

    pub(crate) fn validate_owns_value_type_compatible_with_key_annotation(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        if AnnotationKey::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(Box::new(
                SchemaValidationError::ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns {
                    owner: get_label_or_schema_err(snapshot, type_manager, owns.owner())?,
                    attribute: get_label_or_schema_err(snapshot, type_manager, owns.attribute())?,
                    value_type,
                },
            ))
        }
    }

    pub(crate) fn validate_attribute_type_value_type_compatible_with_annotations_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            Self::validate_attribute_type_value_type_compatible_with_annotations(
                snapshot,
                type_manager,
                type_,
                value_type.clone(),
            )
        });
        Ok(())
    }

    fn validate_attribute_type_value_type_compatible_with_annotations(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let annotations = attribute_type
            .get_annotations_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for annotation in annotations.into_iter() {
            match annotation {
                AttributeTypeAnnotation::Regex(regex) => {
                    Self::validate_annotation_regex_compatible_value_type(
                        snapshot,
                        type_manager,
                        attribute_type,
                        value_type.clone(),
                    )?;
                    Self::validate_regex_arguments(regex.clone())?
                }
                AttributeTypeAnnotation::Range(range) => {
                    Self::validate_annotation_range_compatible_value_type(
                        snapshot,
                        type_manager,
                        attribute_type,
                        value_type.clone(),
                    )?;
                    Self::validate_range_arguments(range.clone(), value_type.clone())?
                }
                AttributeTypeAnnotation::Values(values) => {
                    Self::validate_annotation_values_compatible_value_type(
                        snapshot,
                        type_manager,
                        attribute_type,
                        value_type.clone(),
                    )?;
                    Self::validate_values_arguments(values.clone(), value_type.clone())?
                }
                | AttributeTypeAnnotation::Abstract(_) | AttributeTypeAnnotation::Independent(_) => {}
            }
        }

        Ok(())
    }

    pub(crate) fn validate_value_type_compatible_with_all_owns_annotations_transitive(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            Self::validate_value_type_compatible_with_all_owns_annotations(
                snapshot,
                type_manager,
                type_,
                value_type.clone(),
            )
        });
        Ok(())
    }

    fn validate_value_type_compatible_with_all_owns_annotations(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let all_owns = attribute_type
            .get_owns(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        for owns in all_owns.into_iter() {
            Self::validate_owns_value_type_compatible_with_annotations(
                snapshot,
                type_manager,
                *owns,
                value_type.clone(),
            )?
        }
        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_annotations(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owns: Owns,
        value_type: Option<ValueType>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let annotations = owns
            .get_annotations_declared(snapshot, type_manager)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        for annotation in annotations.into_iter() {
            match annotation {
                OwnsAnnotation::Unique(_) => Self::validate_owns_value_type_compatible_with_unique_annotation(
                    snapshot,
                    type_manager,
                    owns,
                    value_type.clone(),
                )?,
                OwnsAnnotation::Key(_) => Self::validate_owns_value_type_compatible_with_key_annotation(
                    snapshot,
                    type_manager,
                    owns,
                    value_type.clone(),
                )?,
                OwnsAnnotation::Regex(regex) => {
                    Self::validate_annotation_regex_compatible_value_type(
                        snapshot,
                        type_manager,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_regex_arguments(regex.clone())?
                }
                OwnsAnnotation::Range(range) => {
                    Self::validate_annotation_range_compatible_value_type(
                        snapshot,
                        type_manager,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_range_arguments(range.clone(), value_type.clone())?
                }
                OwnsAnnotation::Values(values) => {
                    Self::validate_annotation_values_compatible_value_type(
                        snapshot,
                        type_manager,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_values_arguments(values.clone(), value_type.clone())?
                }
                | OwnsAnnotation::Distinct(_) | OwnsAnnotation::Cardinality(_) => {}
            }
        }
        Ok(())
    }

    pub(crate) fn validate_no_instances_to_delete<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'a>,
    ) -> Result<(), Box<SchemaValidationError>> {
        let has_instances = Self::has_instances_of_type(snapshot, type_manager, thing_manager, type_.clone())
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        if has_instances {
            Err(Box::new(SchemaValidationError::CannotDeleteTypeWithExistingInstances {
                label: get_label_or_schema_err(snapshot, type_manager, type_)?,
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_no_instances_to_change_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            let has_instances = Self::has_instances_of_type(snapshot, type_manager, thing_manager, type_)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            if has_instances {
                Err(Box::new(SchemaValidationError::CannotChangeValueTypeWithExistingInstances {
                    label: get_label_or_schema_err(snapshot, type_manager, type_)?,
                }))
            } else {
                Ok(())
            }
        });
        Ok(())
    }

    fn validate_no_instances_to_lose_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<SchemaValidationError>> {
        for_type_and_subtypes_transitive!(snapshot, type_manager, attribute_type, |type_: AttributeType| {
            let has_instances = Self::has_instances_of_type(snapshot, type_manager, thing_manager, type_)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

            if has_instances {
                Err(Box::new(SchemaValidationError::CannotUnsetValueTypeWithExistingInstances {
                    attribute: get_label_or_schema_err(snapshot, type_manager, type_)?,
                }))
            } else {
                Ok(())
            }
        });
        Ok(())
    }

    pub(crate) fn validate_no_role_instances_to_set_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<SchemaValidationError>> {
        let has_instances = Self::has_instances_of_type(snapshot, type_manager, thing_manager, role_type)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        if has_instances {
            Err(Box::new(SchemaValidationError::CannotSetRoleOrderingWithExistingInstances {
                label: get_label_or_schema_err(snapshot, type_manager, role_type)?,
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_no_owns_instances_to_set_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<SchemaValidationError>> {
        let has_instances = Self::has_instances_of_owns(snapshot, thing_manager, owns.owner(), owns.attribute())
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;

        if has_instances {
            Err(Box::new(SchemaValidationError::CannotSetOwnsOrderingWithExistingInstances {
                owner: get_label_or_schema_err(snapshot, type_manager, owns.owner())?,
                attribute: get_label_or_schema_err(snapshot, type_manager, owns.attribute())?,
            }))
        } else {
            Ok(())
        }
    }

    fn has_instances_of_type<'a, T: KindAPI<'a>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        type_: T,
    ) -> Result<bool, Box<ConceptReadError>> {
        match T::KIND {
            Kind::Entity => {
                let entity_type = EntityType::new(type_.vertex());
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Attribute => {
                let attribute_type = AttributeType::new(type_.vertex());
                let mut iterator = thing_manager.get_attributes_in(snapshot, attribute_type.into_owned())?;
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Relation => {
                let relation_type = RelationType::new(type_.vertex());
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Role => {
                let role_type = RoleType::new(type_.vertex());
                let relation_type = role_type.get_relates_root(snapshot, type_manager)?.relation();
                Self::has_instances_of_relates(snapshot, thing_manager, relation_type, role_type)
            }
        }
    }

    fn has_instances_of_owns(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner_type: ObjectType,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        let mut has_instances = false;

        let mut owner_iterator = thing_manager.get_objects_in(snapshot, owner_type.into_owned());
        while let Some(instance) = owner_iterator.next().transpose()? {
            let mut iterator = instance.get_has_type_unordered(snapshot, thing_manager, attribute_type.into_owned());

            if iterator.next().is_some() {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_plays(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player_type: ObjectType,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        let mut has_instances = false;

        let mut player_iterator = thing_manager.get_objects_in(snapshot, player_type.into_owned());
        while let Some(instance) = player_iterator.next().transpose()? {
            let mut iterator = instance.get_relations_by_role(snapshot, thing_manager, role_type.into_owned());

            if iterator.next().transpose()?.is_some() {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_relates(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        let mut has_instances = false;

        let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.into_owned());
        while let Some(instance) = relation_iterator.next().transpose()? {
            let mut iterator = instance.get_players_role_type(snapshot, thing_manager, role_type.into_owned());

            if iterator.next().transpose()?.is_some() {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn validate_entity_type_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        entity_types: &HashSet<EntityType>,
        constraints: &HashSet<TypeConstraint<EntityType>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let abstract_constraints = get_abstract_constraints(constraints.iter().cloned());

        debug_assert!(
            !abstract_constraints.is_empty(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        for entity_type in entity_types {
            let mut entity_iterator = thing_manager.get_entities_in(snapshot, *entity_type);
            if let Some(res) = entity_iterator.next() {
                if let Err(source) = res {
                    return Err(Box::new(DataValidationError::ConceptRead { source }));
                };

                for abstract_constraint in abstract_constraints.iter() {
                    debug_assert_eq!(
                        abstract_constraint.scope(),
                        ConstraintScope::SingleInstanceOfType,
                        "Reconsider the algorithm if constraint scope is changed!"
                    );
                    if &abstract_constraint.source() == entity_type {
                        return Err(DataValidation::create_data_validation_entity_type_abstractness_error(
                            abstract_constraint,
                            snapshot,
                            type_manager,
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_relation_type_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation_types: &HashSet<RelationType>,
        constraints: &HashSet<TypeConstraint<RelationType>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let abstract_constraints = get_abstract_constraints(constraints.iter().cloned());

        debug_assert!(
            !abstract_constraints.is_empty(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        for relation_type in relation_types {
            let mut relation_iterator = thing_manager.get_relations_in(snapshot, *relation_type);
            if let Some(res) = relation_iterator.next() {
                if let Err(source) = res {
                    return Err(Box::new(DataValidationError::ConceptRead { source }));
                };

                for abstract_constraint in abstract_constraints.iter() {
                    debug_assert_eq!(
                        abstract_constraint.scope(),
                        ConstraintScope::SingleInstanceOfType,
                        "Reconsider the algorithm if constraint scope is changed!"
                    );
                    if &abstract_constraint.source() == relation_type {
                        return Err(DataValidation::create_data_validation_relation_type_abstractness_error(
                            abstract_constraint,
                            snapshot,
                            type_manager,
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_attribute_type_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_types: &HashSet<AttributeType>,
        constraints: &HashSet<TypeConstraint<AttributeType>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let abstract_constraints = get_abstract_constraints(constraints.iter().cloned());
        let regex_constraints = get_regex_constraints(constraints.iter().cloned());
        let range_constraints = get_range_constraints(constraints.iter().cloned());
        let values_constraints = get_values_constraints(constraints.iter().cloned());

        debug_assert!(
            !abstract_constraints.is_empty()
                || !regex_constraints.is_empty()
                || !range_constraints.is_empty()
                || !values_constraints.is_empty(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        for attribute_type in attribute_types {
            let mut attribute_iterator = thing_manager
                .get_attributes_in(snapshot, *attribute_type)
                .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;
            while let Some(attribute) = attribute_iterator.next() {
                let attribute = attribute.map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                for abstract_constraint in abstract_constraints.iter() {
                    debug_assert_eq!(
                        abstract_constraint.scope(),
                        ConstraintScope::SingleInstanceOfType,
                        "Reconsider the algorithm if constraint scope is changed!"
                    );
                    if &abstract_constraint.source() == attribute_type {
                        return Err(DataValidation::create_data_validation_attribute_type_abstractness_error(
                            abstract_constraint,
                            snapshot,
                            type_manager,
                        ));
                    }
                }

                let value = attribute
                    .get_value(snapshot, thing_manager)
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                for regex_constraint in regex_constraints.iter() {
                    DataValidation::validate_attribute_regex_constraint(
                        snapshot,
                        type_manager,
                        regex_constraint,
                        *attribute_type,
                        value.as_reference(),
                    )?;
                }

                for range_constraint in range_constraints.iter() {
                    DataValidation::validate_attribute_range_constraint(
                        snapshot,
                        type_manager,
                        range_constraint,
                        *attribute_type,
                        value.as_reference(),
                    )?;
                }

                for values_constraint in values_constraints.iter() {
                    DataValidation::validate_attribute_values_constraint(
                        snapshot,
                        type_manager,
                        values_constraint,
                        *attribute_type,
                        value.as_reference(),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_role_type_instances_against_constraints(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        _thing_manager: &ThingManager,
        _role_types: &HashSet<RoleType>,
        constraints: &HashSet<TypeConstraint<RoleType>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        unreachable!(
            "Role Types do not have annotations and thus constraints! Revalidate the logic below when it changes."
        );

        // Filter your constraints here
        // debug_assert!(/*insert your constraints here*/, "At least one constraint should exist otherwise we don't need to iterate");
        //
        // for role_type in role_types {
        //     let all_relates = role_type.get_relation_types(snapshot, type_manager).map_err(
        //         |source| Box::new(DataValidationError::ConceptRead { source })
        //     )?;
        //     for relation_type in all_relates.into_keys() {
        //         let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type);
        //         while let Some(relation) = relation_iterator.next().transpose()? {
        //             let mut role_player_iterator =
        //                 thing_manager.get_role_players_role(snapshot, relation, role_type.clone());
        //             if let Some(_) = role_player_iterator.next().transpose()? {
        //                 // insert your constraints validations here
        //             }
        //         }
        //     }
        // }
        //
        // Ok(())
    }

    fn is_suitable_capability_constraint<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraint: &CapabilityConstraint<CAP>,
        capability: CAP,
        interface_type: CAP::InterfaceType,
        new_interface_supertypes: &HashMap<CAP::InterfaceType, Option<CAP::InterfaceType>>,
    ) -> Result<bool, Box<ConceptReadError>> {
        debug_assert!(capability.interface() == interface_type);
        match constraint.scope() {
            ConstraintScope::SingleInstanceOfType => {
                return Ok(constraint.source() == capability);
            }
            | ConstraintScope::SingleInstanceOfTypeOrSubtype
            | ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes
            | ConstraintScope::AllInstancesOfTypeOrSubtypes => (),
        }

        let source_interface_type = constraint.source().interface();

        if source_interface_type == interface_type {
            return Ok(true);
        }

        for (subtype, supertype) in new_interface_supertypes {
            if interface_type.is_subtype_transitive_of_or_same(snapshot, type_manager, subtype.clone())? {
                return Ok(match supertype {
                    None => false,
                    Some(supertype) => source_interface_type.is_subtype_transitive_of_or_same(
                        snapshot,
                        type_manager,
                        supertype.clone(),
                    )?,
                });
            }
        }

        // Not affected by new_interface_supertypes, can use storage
        interface_type.is_subtype_transitive_of_or_same(snapshot, type_manager, source_interface_type.clone())
    }

    fn validate_owns_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        object_types: &HashSet<ObjectType>,
        attribute_types: &HashSet<AttributeType>,
        new_attribute_supertypes: &HashMap<AttributeType, Option<AttributeType>>,
        constraints: &HashSet<CapabilityConstraint<Owns>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let cardinality_constraints_iter = filter_by_constraint_category!(constraints.iter(), Cardinality);
        let abstract_constraints: HashSet<CapabilityConstraint<Owns>> =
            get_abstract_constraints(constraints.iter().cloned());
        let distinct_constraints: HashSet<CapabilityConstraint<Owns>> =
            get_distinct_constraints(constraints.iter().cloned());
        let regex_constraints: HashSet<CapabilityConstraint<Owns>> = get_regex_constraints(constraints.iter().cloned());
        let range_constraints: HashSet<CapabilityConstraint<Owns>> = get_range_constraints(constraints.iter().cloned());
        let values_constraints: HashSet<CapabilityConstraint<Owns>> =
            get_values_constraints(constraints.iter().cloned());
        let unique_constraint = {
            let mut unique_constraint: Option<CapabilityConstraint<Owns>> = None;
            for constraint in filter_by_constraint_category!(constraints.iter(), Unique) {
                match &unique_constraint {
                    None => unique_constraint = Some(constraint.clone()),
                    Some(existing_unique_constraint) => {
                        if constraint
                            .source()
                            .attribute()
                            .is_supertype_transitive_of(
                                snapshot,
                                type_manager,
                                existing_unique_constraint.source().attribute(),
                            )
                            .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            unique_constraint = Some(constraint.clone());
                        }
                    }
                }
            }
            unique_constraint
        };

        debug_assert!(
            cardinality_constraints_iter.clone().count() > 0
                || !abstract_constraints.is_empty()
                || !distinct_constraints.is_empty()
                || !regex_constraints.is_empty()
                || !range_constraints.is_empty()
                || !values_constraints.is_empty()
                || unique_constraint.is_some(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        // TODO #7138: It is EXCEPTIONALLY memory-greedy and should be optimized before a non-alpha release!
        let mut unique_values = HashMap::new();

        for object_type in object_types {
            let mut object_iterator = thing_manager.get_objects_in(snapshot, (*object_type).into_owned());
            while let Some(object) = object_iterator
                .next()
                .transpose()
                .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
            {
                let mut cardinality_constraints_counts: HashMap<CapabilityConstraint<Owns>, u64> =
                    cardinality_constraints_iter.clone().map(|constraint| (constraint.clone(), 0)).collect();

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut has_attribute_iterator = object.get_has_unordered(snapshot, thing_manager);
                while let Some(attribute) = has_attribute_iterator.next() {
                    let (attribute, count) =
                        attribute.map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;
                    let attribute_type = attribute.type_();
                    if !attribute_types.contains(&attribute_type) {
                        continue;
                    }

                    let owns = object_type
                        .get_owns_attribute(snapshot, type_manager, attribute_type)
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        .ok_or_else(|| Box::new(ConceptReadError::CorruptFoundHasWithoutOwns))
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                    for abstract_constraint in abstract_constraints.iter() {
                        debug_assert_eq!(
                            abstract_constraint.scope(),
                            ConstraintScope::SingleInstanceOfType,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            abstract_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            return Err(DataValidation::create_data_validation_owns_abstractness_error(
                                abstract_constraint,
                                object.as_reference(),
                                snapshot,
                                type_manager,
                            ));
                        }
                    }

                    for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.iter_mut() {
                        debug_assert_eq!(
                            cardinality_constraint.scope(),
                            ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            cardinality_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            *constraint_counts += count;
                        }
                    }

                    for distinct_constraint in distinct_constraints.iter() {
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            distinct_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            DataValidation::validate_owns_distinct_constraint(
                                snapshot,
                                type_manager,
                                distinct_constraint,
                                object.as_reference(),
                                attribute.as_reference(),
                                count,
                            )?;
                            break;
                        }
                    }

                    let value = attribute
                        .get_value(snapshot, thing_manager)
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                    if let Some(unique_constraint) = &unique_constraint {
                        debug_assert_eq!(
                            unique_constraint.scope(),
                            ConstraintScope::AllInstancesOfTypeOrSubtypes,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            unique_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            let previous_owner =
                                unique_values.insert(value.clone().into_owned(), object.as_reference().into_owned());
                            if previous_owner.unwrap_or(object.as_reference()) != object {
                                return Err(DataValidation::create_data_validation_uniqueness_error(
                                    snapshot,
                                    type_manager,
                                    unique_constraint,
                                    object,
                                    attribute_type,
                                    value,
                                ));
                            }
                        }
                    }

                    for regex_constraint in regex_constraints.iter() {
                        debug_assert_eq!(
                            regex_constraint.scope(),
                            ConstraintScope::SingleInstanceOfTypeOrSubtype,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );

                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            regex_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            DataValidation::validate_owns_regex_constraint(
                                snapshot,
                                type_manager,
                                regex_constraint,
                                object.as_reference(),
                                attribute_type,
                                value.as_reference(),
                            )?;
                        }
                    }

                    for range_constraint in range_constraints.iter() {
                        debug_assert_eq!(
                            range_constraint.scope(),
                            ConstraintScope::SingleInstanceOfTypeOrSubtype,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            range_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            DataValidation::validate_owns_range_constraint(
                                snapshot,
                                type_manager,
                                range_constraint,
                                object.as_reference(),
                                attribute_type,
                                value.as_reference(),
                            )?;
                        }
                    }

                    for values_constraint in values_constraints.iter() {
                        debug_assert_eq!(
                            values_constraint.scope(),
                            ConstraintScope::SingleInstanceOfTypeOrSubtype,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            values_constraint,
                            owns,
                            attribute_type,
                            new_attribute_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            DataValidation::validate_owns_values_constraint(
                                snapshot,
                                type_manager,
                                values_constraint,
                                object.as_reference(),
                                attribute_type,
                                value.as_reference(),
                            )?;
                        }
                    }
                }

                for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.into_iter() {
                    DataValidation::validate_owns_instances_cardinality_constraint(
                        snapshot,
                        type_manager,
                        &cardinality_constraint,
                        object.as_reference(),
                        cardinality_constraint.source().attribute(),
                        constraint_counts,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_plays_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        object_types: &HashSet<ObjectType>,
        role_types: &HashSet<RoleType>,
        new_role_supertypes: &HashMap<RoleType, Option<RoleType>>,
        constraints: &HashSet<CapabilityConstraint<Plays>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let cardinality_constraints_iter = filter_by_constraint_category!(constraints.iter(), Cardinality);
        let abstract_constraints = get_abstract_constraints(constraints.iter().cloned());

        debug_assert!(
            cardinality_constraints_iter.clone().count() > 0 || !abstract_constraints.is_empty(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        for object_type in object_types {
            let mut object_iterator = thing_manager.get_objects_in(snapshot, (*object_type).into_owned());
            while let Some(object) = object_iterator
                .next()
                .transpose()
                .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
            {
                let mut cardinality_constraints_counts: HashMap<CapabilityConstraint<Plays>, u64> =
                    cardinality_constraints_iter.clone().map(|constraint| (constraint.clone(), 0)).collect();

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut relations_iterator = object.get_relations_roles(snapshot, thing_manager);
                while let Some(relation) = relations_iterator.next() {
                    let (_, role_type, count) =
                        relation.map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;
                    if !role_types.contains(&role_type) {
                        continue;
                    }

                    let plays = object_type
                        .get_plays_role(snapshot, type_manager, role_type)
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        .ok_or_else(|| Box::new(ConceptReadError::CorruptFoundLinksWithoutPlays))
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                    for abstract_constraint in abstract_constraints.iter() {
                        debug_assert_eq!(
                            abstract_constraint.scope(),
                            ConstraintScope::SingleInstanceOfType,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            abstract_constraint,
                            plays.clone(),
                            role_type,
                            new_role_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            return Err(DataValidation::create_data_validation_plays_abstractness_error(
                                abstract_constraint,
                                object.as_reference(),
                                snapshot,
                                type_manager,
                            ));
                        }
                    }

                    for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.iter_mut() {
                        debug_assert_eq!(
                            cardinality_constraint.scope(),
                            ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            cardinality_constraint,
                            plays.clone(),
                            role_type,
                            new_role_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            *constraint_counts += count;
                        }
                    }
                }

                for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.into_iter() {
                    DataValidation::validate_plays_instances_cardinality_constraint(
                        snapshot,
                        type_manager,
                        &cardinality_constraint,
                        object.as_reference(),
                        cardinality_constraint.source().role(),
                        constraint_counts,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_relates_instances_against_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation_types: &HashSet<RelationType>,
        role_types: &HashSet<RoleType>,
        new_role_supertypes: &HashMap<RoleType, Option<RoleType>>,
        constraints: &HashSet<CapabilityConstraint<Relates>>,
    ) -> Result<(), Box<DataValidationError>> {
        if constraints.is_empty() {
            return Ok(());
        }

        let cardinality_constraints_iter = filter_by_constraint_category!(constraints.iter(), Cardinality);
        let abstract_constraints = get_abstract_constraints(constraints.iter().cloned());
        let distinct_constraints = get_distinct_constraints(constraints.iter().cloned());

        debug_assert!(
            cardinality_constraints_iter.clone().count() > 0
                || !abstract_constraints.is_empty()
                || !distinct_constraints.is_empty(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        for relation_type in relation_types {
            let mut relation_iterator = thing_manager.get_relations_in(snapshot, (*relation_type).into_owned());
            while let Some(relation) = relation_iterator
                .next()
                .transpose()
                .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
            {
                let mut cardinality_constraints_counts: HashMap<CapabilityConstraint<Relates>, u64> =
                    cardinality_constraints_iter.clone().map(|constraint| (constraint.clone(), 0)).collect();

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut role_players_iterator = relation.get_players(snapshot, thing_manager);

                while let Some(role_players) = role_players_iterator.next() {
                    let (role_player, count) =
                        role_players.map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;
                    let role_type = role_player.role_type();
                    if !role_types.contains(&role_type) {
                        continue;
                    }

                    let relates = relation_type
                        .get_relates_role(snapshot, type_manager, role_type)
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        .ok_or_else(|| Box::new(ConceptReadError::CorruptFoundLinksWithoutRelates))
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?;

                    for abstract_constraint in abstract_constraints.iter() {
                        debug_assert_eq!(
                            abstract_constraint.scope(),
                            ConstraintScope::SingleInstanceOfType,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            abstract_constraint,
                            relates.clone(),
                            role_type,
                            new_role_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            return Err(DataValidation::create_data_validation_relates_abstractness_error(
                                abstract_constraint,
                                relation.as_reference(),
                                snapshot,
                                type_manager,
                            ));
                        }
                    }

                    for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.iter_mut() {
                        debug_assert_eq!(
                            cardinality_constraint.scope(),
                            ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            cardinality_constraint,
                            relates.clone(),
                            role_type,
                            new_role_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            *constraint_counts += count;
                        }
                    }

                    for distinct_constraint in distinct_constraints.iter() {
                        debug_assert_eq!(
                            distinct_constraint.scope(),
                            ConstraintScope::SingleInstanceOfTypeOrSubtype,
                            "Reconsider the algorithm if constraint scope is changed!"
                        );
                        if Self::is_suitable_capability_constraint(
                            snapshot,
                            type_manager,
                            distinct_constraint,
                            relates.clone(),
                            role_type,
                            new_role_supertypes,
                        )
                        .map_err(|source| Box::new(DataValidationError::ConceptRead { source }))?
                        {
                            DataValidation::validate_relates_distinct_constraint(
                                snapshot,
                                type_manager,
                                distinct_constraint,
                                relation.as_reference(),
                                role_type,
                                role_player.player(),
                                count,
                            )?;
                            break;
                        }
                    }
                }

                for (cardinality_constraint, constraint_counts) in cardinality_constraints_counts.into_iter() {
                    DataValidation::validate_relates_instances_cardinality_constraint(
                        snapshot,
                        type_manager,
                        &cardinality_constraint,
                        relation.as_reference(),
                        cardinality_constraint.source().role(),
                        constraint_counts,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn get_lost_capabilities_if_supertype_is_changed<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: CAP::ObjectType,
        new_supertype: Option<CAP::ObjectType>,
    ) -> Result<HashSet<CAP>, Box<SchemaValidationError>> {
        let new_inherited_capabilities = match new_supertype {
            None => HashSet::new(),
            Some(new_supertype) => TypeReader::get_capabilities::<CAP>(snapshot, new_supertype.clone(), false)
                .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?,
        };

        let current_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, type_.clone(), false)
            .map_err(|source| Box::new(SchemaValidationError::ConceptRead { source }))?;
        let current_inherited_capabilities =
            current_capabilities.iter().filter(|capability| capability.object() != type_);

        Ok(current_inherited_capabilities
            .filter(|capability| {
                !new_inherited_capabilities
                    .iter()
                    .any(|inherited_capability| inherited_capability.interface() == capability.interface())
            })
            .cloned()
            .collect())
    }

    type_or_subtype_without_declared_capability_instances_existence_validation!(
        type_or_subtype_without_declared_capability_that_has_instances_of_owns,
        Owns,
        ObjectType,
        AttributeType,
        Self::has_instances_of_owns
    );
    type_or_subtype_without_declared_capability_instances_existence_validation!(
        type_or_subtype_without_declared_capability_that_has_instances_of_plays,
        Plays,
        ObjectType,
        RoleType,
        Self::has_instances_of_plays
    );
    type_or_subtype_without_declared_capability_instances_existence_validation!(
        type_or_subtype_without_declared_capability_that_has_instances_of_relates,
        Relates,
        RelationType,
        RoleType,
        Self::has_instances_of_relates
    );

    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_corrupted_instances_to_unset_owns,
        CapabilityKind::Owns,
        Owns,
        ObjectType,
        AttributeType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_owns
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_corrupted_instances_to_unset_plays,
        CapabilityKind::Plays,
        Plays,
        ObjectType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_plays
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_corrupted_instances_to_unset_relates,
        CapabilityKind::Relates,
        Relates,
        RelationType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_relates
    );

    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Owns,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Owns>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_owns
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Plays,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Plays>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_plays
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Relates,
        RelationType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Relates>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_relates
    );

    new_acquired_capability_instances_validation!(
        validate_new_acquired_owns_compatible_with_instances,
        Owns,
        Self::validate_owns_instances_against_constraints
    );
    new_acquired_capability_instances_validation!(
        validate_new_acquired_plays_compatible_with_instances,
        Plays,
        Self::validate_plays_instances_against_constraints
    );
    new_acquired_capability_instances_validation!(
        validate_new_acquired_relates_compatible_with_instances,
        Relates,
        Self::validate_relates_instances_against_constraints
    );

    new_annotation_constraints_compatible_with_capability_instances_validation!(
        validate_new_annotation_constraints_compatible_with_owns_instances,
        Owns,
        Self::validate_owns_instances_against_constraints
    );
    new_annotation_constraints_compatible_with_capability_instances_validation!(
        validate_new_annotation_constraints_compatible_with_plays_instances,
        Plays,
        Self::validate_plays_instances_against_constraints
    );
    new_annotation_constraints_compatible_with_capability_instances_validation!(
        validate_new_annotation_constraints_compatible_with_relates_instances,
        Relates,
        Self::validate_relates_instances_against_constraints
    );

    updated_constraints_compatible_with_capability_instances_on_object_supertype_change_validation!(
        validate_updated_constraints_compatible_with_owns_instances_on_object_supertype_change,
        Owns,
        ObjectType,
        Self::validate_owns_instances_against_constraints
    );
    updated_constraints_compatible_with_capability_instances_on_object_supertype_change_validation!(
        validate_updated_constraints_compatible_with_plays_instances_on_object_supertype_change,
        Plays,
        ObjectType,
        Self::validate_plays_instances_against_constraints
    );
    updated_constraints_compatible_with_capability_instances_on_object_supertype_change_validation!(
        validate_updated_constraints_compatible_with_relates_instances_on_relation_supertype_change,
        Relates,
        RelationType,
        Self::validate_relates_instances_against_constraints
    );

    affected_constraints_compatible_with_capability_instances_on_interface_supertype_unset_validation!(
        validate_affected_constraints_compatible_with_owns_instances_on_attribute_supertype_unset,
        Owns,
        Self::validate_owns_instances_against_constraints
    );
    affected_constraints_compatible_with_capability_instances_on_interface_supertype_unset_validation!(
        validate_affected_constraints_compatible_with_plays_instances_on_role_supertype_unset,
        Plays,
        Self::validate_plays_instances_against_constraints
    );
    affected_constraints_compatible_with_capability_instances_on_interface_supertype_unset_validation!(
        validate_affected_constraints_compatible_with_relates_instances_on_role_supertype_unset,
        Relates,
        Self::validate_relates_instances_against_constraints
    );

    affected_constraints_compatible_with_capability_instances_on_interface_supertype_change_validation!(
        validate_affected_constraints_compatible_with_owns_instances_on_attribute_supertype_change,
        Owns,
        Self::validate_affected_constraints_compatible_with_owns_instances_on_attribute_supertype_unset,
        Self::validate_owns_instances_against_constraints
    );
    affected_constraints_compatible_with_capability_instances_on_interface_supertype_change_validation!(
        validate_affected_constraints_compatible_with_plays_instances_on_role_supertype_change,
        Plays,
        Self::validate_affected_constraints_compatible_with_plays_instances_on_role_supertype_unset,
        Self::validate_plays_instances_against_constraints
    );
    affected_constraints_compatible_with_capability_instances_on_interface_supertype_change_validation!(
        validate_affected_constraints_compatible_with_relates_instances_on_role_supertype_change,
        Relates,
        Self::validate_affected_constraints_compatible_with_relates_instances_on_role_supertype_unset,
        Self::validate_relates_instances_against_constraints
    );

    new_annotation_constraints_compatible_with_type_and_sub_instances_validation!(
        validate_new_annotation_constraints_compatible_with_entity_type_and_sub_instances,
        EntityType,
        Self::validate_entity_type_instances_against_constraints
    );
    new_annotation_constraints_compatible_with_type_and_sub_instances_validation!(
        validate_new_annotation_constraints_compatible_with_relation_type_and_sub_instances,
        RelationType,
        Self::validate_relation_type_instances_against_constraints
    );
    new_annotation_constraints_compatible_with_type_and_sub_instances_validation!(
        validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances,
        AttributeType,
        Self::validate_attribute_type_instances_against_constraints
    );
    new_annotation_constraints_compatible_with_type_and_sub_instances_validation!(
        validate_new_annotation_constraints_compatible_with_role_type_and_sub_instances,
        RoleType,
        Self::validate_role_type_instances_against_constraints
    );

    updated_constraints_compatible_with_type_and_sub_instances_on_supertype_change_validation!(
        validate_updated_constraints_compatible_with_entity_type_and_sub_instances_on_supertype_change,
        EntityType,
        Self::validate_entity_type_instances_against_constraints
    );
    updated_constraints_compatible_with_type_and_sub_instances_on_supertype_change_validation!(
        validate_updated_constraints_compatible_with_relation_type_and_sub_instances_on_supertype_change,
        RelationType,
        Self::validate_relation_type_instances_against_constraints
    );
    updated_constraints_compatible_with_type_and_sub_instances_on_supertype_change_validation!(
        validate_updated_constraints_compatible_with_attribute_type_and_sub_instances_on_supertype_change,
        AttributeType,
        Self::validate_attribute_type_instances_against_constraints
    );
    updated_constraints_compatible_with_type_and_sub_instances_on_supertype_change_validation!(
        validate_updated_constraints_compatible_with_role_type_and_sub_instances_on_supertype_change,
        RoleType,
        Self::validate_role_type_instances_against_constraints
    );
}
