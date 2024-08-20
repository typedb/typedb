/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    iter::once,
};

use encoding::{
    graph::{
        definition::definition_key::DefinitionKey,
        thing::ThingVertex,
        type_::{CapabilityKind, Kind},
    },
    value::{label::Label, value::Value, value_type::ValueType, ValueEncodable},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use paste::paste;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationKey, AnnotationRange,
            AnnotationRegex, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::{Constraint, ConstraintValidationMode},
        entity_type::EntityType,
        object_type::{with_object_type, ObjectType},
        owns::{Owns, OwnsAnnotation},
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                validation::{
                    capability_get_annotation_by_category, capability_get_declared_annotation_by_category,
                    capability_get_owner_of_annotation_category, get_label_or_concept_read_err,
                    get_label_or_schema_err, get_opt_label_or_schema_err, is_interface_hidden_by_overrides,
                    is_ordering_compatible_with_distinct_annotation, is_type_transitive_supertype_or_same,
                    type_get_annotation_by_category, type_get_declared_annotation_by_category,
                    type_get_source_of_annotation_category, validate_capabilities_cardinality,
                    validate_cardinality_narrows_inherited_cardinality,
                    validate_declared_annotation_is_compatible_with_inherited_annotations,
                    validate_declared_capability_annotation_is_compatible_with_inherited_annotations,
                    validate_edge_annotations_narrowing_of_inherited_annotations,
                    validate_edge_range_narrows_inherited_range, validate_edge_regex_narrows_inherited_regex,
                    validate_edge_values_narrows_inherited_values, validate_owns_override_ordering_match,
                    validate_role_name_uniqueness_non_transitive, validate_role_type_supertype_ordering_match,
                    validate_type_annotations_narrowing_of_inherited_annotations,
                    validate_type_range_narrows_inherited_range, validate_type_regex_narrows_inherited_regex,
                    validate_type_supertype_abstractness, validate_type_values_narrows_inherited_values,
                },
                SchemaValidationError,
            },
            TypeManager,
        },
        Capability, KindAPI, ObjectTypeAPI, Ordering, TypeAPI,
    },
};

macro_rules! object_type_match {
    ($obj_var:ident, $block:block) => {
        match &$obj_var {
            ObjectType::Entity($obj_var) => $block
            ObjectType::Relation($obj_var) => $block
        }
    };
}

macro_rules! for_type_and_subtypes_transitive {
    ($snapshot:ident, $type_:ident, $closure:expr) => {
        $closure($type_.clone())?;
        TypeReader::get_subtypes_transitive($snapshot, $type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| $closure(subtype))?;
    };
}

macro_rules! for_type_and_supertypes_transitive {
    ($snapshot:ident, $type_:ident, $closure:expr) => {
        $closure($type_.clone())?;
        TypeReader::get_supertypes_transitive($snapshot, $type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|supertype| $closure(supertype))?;
    };
}

macro_rules! for_capability_and_overriding_capabilities_transitive {
    ($snapshot:ident, $capability:ident, $closure:expr) => {
        $closure($capability.clone())?;
        TypeReader::get_overriding_capabilities_transitive($snapshot, $capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|current_capability| $closure(current_capability))?;
    };
}

macro_rules! with_annotation_constraint_if_exists {
    ($snapshot:ident, $type_:ident, $constraint_name:ident, $get_func:path, |$constraint:ident| $expr:expr) => {
        if let Some(annotation) = $get_func($snapshot, $type_.clone(), AnnotationCategory::$constraint_name)
            .map_err(SchemaValidationError::ConceptRead)?
        {
            match &annotation {
                Annotation::$constraint_name($constraint) => $expr,
                _ => unreachable!("Should not reach it for {}-related function", stringify!($constraint_name)),
            }
        }
    };
}

macro_rules! type_or_subtype_without_declared_capability_instances_existence_validation {
    ($func_name:ident, $capability_type:ident, $object_type:ident, $interface_type:ident, $single_type_validation_func:path) => {
        fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &'a TypeManager,
            thing_manager: &'a ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<Option<$object_type<'a>>, ConceptReadError> {
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
                    break; // TODO: Maybe we want to return all the corrupted owns here, just moving forward for now
                }

                let subtypes = current_object_type.get_subtypes(snapshot, type_manager)?;
                for subtype in subtypes.into_iter() {
                    let subtype_capabilities = TypeReader::get_capabilities_declared::<$capability_type<'static>>(
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
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $object_type:ident, $interface_type:ident, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<(), SchemaValidationError> {
            if let Some(supertype) = TypeReader::get_supertype(snapshot, object_type.clone().into_owned())
                .map_err(SchemaValidationError::ConceptRead)?
            {
                let supertype_capabilities =
                    TypeReader::get_capabilities::<$capability_type<'static>>(snapshot, supertype)
                        .map_err(SchemaValidationError::ConceptRead)?;
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
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_instances) = type_having_instances {
                Err(SchemaValidationError::CannotUnsetCapabilityWithExistingInstances(
                    $capability_kind,
                    get_label_or_schema_err(snapshot, object_type)?,
                    get_label_or_schema_err(snapshot, type_having_instances)?,
                    get_label_or_schema_err(snapshot, interface_type)?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! cannot_override_capability_with_existing_instances_validation {
    ($func_name:ident, $capability_kind:path, $object_type:ident, $interface_type:ident, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<(), SchemaValidationError> {
            let type_having_instances = $existing_instances_validation_func(
                snapshot,
                type_manager,
                thing_manager,
                object_type.clone(),
                interface_type.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_instances) = type_having_instances {
                Err(SchemaValidationError::CannotOverrideCapabilityWithExistingInstances(
                    $capability_kind,
                    get_label_or_schema_err(snapshot, object_type)?,
                    get_label_or_schema_err(snapshot, type_having_instances)?,
                    get_label_or_schema_err(snapshot, interface_type)?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation {
    ($func_name:ident, $capability_kind:path, $object_type:ident, $get_lost_capabilities_func:path, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            subtype: $object_type<'static>,
            supertype: Option<$object_type<'static>>,
        ) -> Result<(), SchemaValidationError> {
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
                .map_err(SchemaValidationError::ConceptRead)?;

                if let Some(type_having_instances) = type_having_instances {
                    return Err(SchemaValidationError::CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(
                        $capability_kind,
                        get_label_or_schema_err(snapshot, subtype)?,
                        get_opt_label_or_schema_err(snapshot, supertype)?,
                        get_label_or_schema_err(snapshot, type_having_instances)?,
                        get_label_or_schema_err(snapshot, interface_type)?,
                    ));
                }
            }

            Ok(())
        }
    };
}

macro_rules! capability_or_its_overriding_capability_with_violated_new_annotation_constraints {
    ($func_name:ident, $capability_type:ident, $object_type:ident, $interface_type:ident, $existing_instances_validation_func:path) => {
        paste! {
            fn $func_name<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                object_type: $object_type<'a>,
                capability: $capability_type<'static>,
                sorted_annotations: HashMap<&ConstraintValidationMode, HashSet<Annotation>>,
            ) -> Result<Option<(Vec<($object_type<'a>, $interface_type<'a>)>, AnnotationCategory)>, ConceptReadError> {
                for (validation_mode, annotations) in sorted_annotations {
                    match validation_mode {
                        &ConstraintValidationMode::Type => {
                            let result = Self::[< $func_name _type >](
                                snapshot,
                                type_manager,
                                thing_manager,
                                object_type.clone(),
                                capability.clone(),
                                annotations
                            )?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        &ConstraintValidationMode::TypeAndSiblings => {
                            let result = Self::[< $func_name _type_and_siblings >](
                                snapshot,
                                type_manager,
                                thing_manager,
                                object_type.clone(),
                                capability.clone(),
                                annotations
                            )?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        &ConstraintValidationMode::TypeAndSiblingsAndSubtypes => {
                            let result = Self::[< $func_name _type_and_siblings_and_subtypes >](
                                snapshot,
                                type_manager,
                                thing_manager,
                                object_type.clone(),
                                capability.clone(),
                                annotations
                            )?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                    }
                }
                Ok(None)
            }

            fn [< $func_name _type >]<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                object_type: $object_type<'a>,
                capability: $capability_type<'static>,
                annotations: HashSet<Annotation>,
            ) -> Result<Option<(Vec<($object_type<'a>, $interface_type<'a>)>, AnnotationCategory)>, ConceptReadError> {
                if annotations.is_empty() {
                    return Ok(None);
                }

                let mut type_and_interface_with_violations = None;
                let mut capabilities_and_annotations_to_check = VecDeque::new();
                capabilities_and_annotations_to_check.push_front((
                    object_type,
                    capability,
                    annotations,
                ));

                while let Some((current_object_type, current_capability, annotations_to_revalidate)) =
                    capabilities_and_annotations_to_check.pop_back()
                {
                    if let Some(violated_constraint) = $existing_instances_validation_func(
                        snapshot,
                        type_manager,
                        thing_manager,
                        &HashSet::from([current_object_type.clone()]),
                        &HashSet::from([current_capability.interface()]),
                        &annotations_to_revalidate,
                    )? {
                        type_and_interface_with_violations =
                            Some((vec![(current_object_type, current_capability.interface())], violated_constraint));
                        break;
                    }

                    let mut inherited_annotations = annotations_to_revalidate.clone();
                    inherited_annotations.retain(|annotation| annotation.category().inheritable());

                    let subtypes = current_object_type.get_subtypes(snapshot, type_manager)?;
                    for subtype in subtypes.into_iter() {
                        // If subtype has another capability of the same interface (subtype -> interface), but it doesn't override (type -> interface), we ignore its existence
                        // and still validate the capability against these constraints
                        let overrides = TypeReader::get_object_capabilities_overrides::<$capability_type<'static>>(
                            snapshot,
                            subtype.clone().into_owned(),
                        )?;
                        let mut overridings = overrides
                            .iter()
                            .filter_map(|(overriding, overridden)| {
                                if &current_capability == overridden {
                                    Some(overriding.clone())
                                } else {
                                    None
                                }
                            });

                        if overridings.clone().peekable().peek().is_some() {
                            while let Some(overriding) = overridings.next()
                            {
                                let mut overriding_annotations_to_revalidate = inherited_annotations.clone();
                                let declared_annotations =
                                    TypeReader::get_type_edge_annotations_declared(snapshot, overriding.clone())?;
                                overriding_annotations_to_revalidate
                                    .retain(|annotation| !declared_annotations.contains(&annotation.clone().try_into().unwrap()));

                                capabilities_and_annotations_to_check.push_front((
                                    subtype.clone(),
                                    overriding,
                                    overriding_annotations_to_revalidate,
                                ));
                            }
                        } else {
                            capabilities_and_annotations_to_check.push_front((
                                subtype.clone(),
                                current_capability.clone(),
                                inherited_annotations.clone(),
                            ));
                        }
                    }
                }

                Ok(type_and_interface_with_violations)
            }

            fn [< $func_name _type_and_siblings >]<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                object_type: $object_type<'a>,
                capability: $capability_type<'static>,
                annotations: HashSet<Annotation>,
            ) -> Result<Option<(Vec<($object_type<'a>, $interface_type<'a>)>, AnnotationCategory)>, ConceptReadError> {
                if annotations.is_empty() {
                    return Ok(None);
                }

                let mut inherited_annotations = annotations.clone();
                inherited_annotations.retain(|annotation| annotation.category().inheritable());

                let mut type_and_interface_with_violations = None;

                let mut capabilities_and_annotations_to_check = VecDeque::new();
                capabilities_and_annotations_to_check.push_front((
                    object_type,
                    HashSet::from([capability]),
                    annotations,
                ));

                while let Some((current_object_type, current_capabilities, annotations_to_check)) =
                    capabilities_and_annotations_to_check.pop_back()
                {
                    let current_interface_types = current_capabilities.iter().map(|capability| capability.interface()).collect();
                    if let Some(violated_constraint) = $existing_instances_validation_func(
                        snapshot,
                        type_manager,
                        thing_manager,
                        &HashSet::from([current_object_type.clone()]),
                        &current_interface_types,
                        &annotations_to_check,
                    )? {
                        type_and_interface_with_violations = Some((
                            current_interface_types
                                .iter()
                                .map(|interface_type| (current_object_type.clone(), interface_type.clone()))
                                .collect_vec(),
                            violated_constraint
                        ));
                        break;
                    }

                    let subtypes = current_object_type.get_subtypes(snapshot, type_manager)?;
                    for subtype in subtypes.into_iter() {
                    let mut subtype_capabilities = HashSet::with_capacity(current_capabilities.len());
                        for current_capability in &current_capabilities {
                            let overrides = TypeReader::get_object_capabilities_overrides::<$capability_type<'static>>(
                                snapshot,
                                subtype.clone().into_owned(),
                            )?;
                            let mut overridings = overrides
                                .iter()
                                .filter(|(_, overridden)| &current_capability == overridden)
                                .map(|(overriding, _)| overriding.clone());

                            if overridings.clone().peekable().peek().is_some() {
                                while let Some(overriding) = overridings.next() {
                                    subtype_capabilities.insert(overriding);
                                }
                            } else {
                                subtype_capabilities.insert(current_capability.clone());
                            }
                        }

                        capabilities_and_annotations_to_check.push_front((
                            subtype.clone(),
                            subtype_capabilities,
                            inherited_annotations.clone(),
                        ));
                    }
                }

                Ok(type_and_interface_with_violations)
            }

            fn [< $func_name _type_and_siblings_and_subtypes >]<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                object_type: $object_type<'a>,
                capability: $capability_type<'static>,
                annotations: HashSet<Annotation>,
            ) -> Result<Option<(Vec<($object_type<'a>, $interface_type<'a>)>, AnnotationCategory)>, ConceptReadError> {
                if annotations.is_empty() {
                    return Ok(None);
                }

                let mut type_and_interface_with_violations = None;

                let mut object_types = HashSet::from([object_type.clone()]);
                let mut interface_types = HashSet::from([capability.interface()]);

                let mut stack = Vec::from([(object_type.clone(), HashSet::from([capability.clone()]))]);
                while let Some((current_object_type, current_capabilities)) = stack.pop() {
                    let subtypes = current_object_type.get_subtypes(snapshot, type_manager)?;
                    for subtype in subtypes.into_iter() {
                        let mut subtype_capabilities = HashSet::with_capacity(current_capabilities.len());

                        for current_capability in &current_capabilities {
                            let overrides = TypeReader::get_object_capabilities_overrides::<$capability_type<'static>>(
                                snapshot,
                                subtype.clone().into_owned(),
                            )?;
                            let mut overridings = overrides
                                .iter()
                                .filter(|(_, overridden)| &current_capability == overridden)
                                .map(|(overriding, _)| overriding.clone());

                            if overridings.clone().peekable().peek().is_some() {
                                while let Some(overriding) = overridings.next() {
                                    interface_types.insert(overriding.interface());
                                    subtype_capabilities.insert(overriding.clone());
                                }
                            } else {
                                subtype_capabilities.insert(current_capability.clone());
                            }
                        }

                        object_types.insert(subtype.clone());
                        stack.push((subtype.clone(), subtype_capabilities));
                    }
                }

                if let Some(violated_constraint) = $existing_instances_validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    &object_types,
                    &interface_types,
                    &annotations,
                )? {
                    type_and_interface_with_violations = Some((
                        interface_types
                            .iter()
                            .map(|interface_type| (object_type.clone(), interface_type.clone()))
                            .collect_vec(),
                        violated_constraint
                    ));
                }

                Ok(type_and_interface_with_violations)
            }
        }
    };
}

macro_rules! new_acquired_capability_instances_validation {
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            capability: $capability_type<'static>,
            annotations: HashSet<Annotation>,
        ) -> Result<(), SchemaValidationError> {
            let sorted_annotations = Constraint::annotations_by_validation_modes(annotations)
                .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

            let violation = $validation_func(
                snapshot,
                type_manager,
                thing_manager,
                capability.object(),
                capability.clone(),
                sorted_annotations,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some((violating_objects_with_interfaces, violated_constraint)) = violation {
                Err(SchemaValidationError::CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(
                    $capability_kind,
                    violated_constraint,
                    get_label_or_schema_err(snapshot, capability.object())?,
                    get_label_or_schema_err(snapshot, capability.interface())?,
                    violating_objects_with_interfaces
                        .iter()
                        .map(|(violating_object, violating_interface)| {
                            match (
                                get_label_or_schema_err(snapshot, violating_object.clone()),
                                get_label_or_schema_err(snapshot, violating_interface.clone()),
                            ) {
                                (Ok(object_label), Ok(interface_label)) => Ok((object_label, interface_label)),
                                (Err(err), _) | (_, Err(err)) => Err(err),
                            }
                        })
                        .collect::<Result<Vec<(Label<'static>, Label<'static>)>, SchemaValidationError>>()?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! new_annotation_compatible_with_capability_and_overriding_capabilities_instances_validation {
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            capability: $capability_type<'static>,
            annotation: Annotation,
        ) -> Result<(), SchemaValidationError> {
            let mut annotations = HashSet::from([annotation]);
            OperationTimeValidation::filter_capability_annotation_constraints_by_not_declared(
                snapshot,
                capability.clone(),
                &mut annotations,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            let sorted_annotations = Constraint::annotations_by_validation_modes(annotations)
                .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

            let violation = $validation_func(
                snapshot,
                type_manager,
                thing_manager,
                capability.object(),
                capability.clone(),
                sorted_annotations,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some((violating_objects_with_interfaces, violated_constraint)) = violation {
                Err(SchemaValidationError::CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(
                    $capability_kind,
                    violated_constraint,
                    get_label_or_schema_err(snapshot, capability.object())?,
                    get_label_or_schema_err(snapshot, capability.interface())?,
                    violating_objects_with_interfaces
                        .iter()
                        .map(|(violating_object, violating_interface)| {
                            match (
                                get_label_or_schema_err(snapshot, violating_object.clone()),
                                get_label_or_schema_err(snapshot, violating_interface.clone()),
                            ) {
                                (Ok(object_label), Ok(interface_label)) => Ok((object_label, interface_label)),
                                (Err(err), _) | (_, Err(err)) => Err(err),
                            }
                        })
                        .collect::<Result<Vec<(Label<'static>, Label<'static>)>, SchemaValidationError>>()?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_override_validation {
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            capability: $capability_type<'static>,
            capability_override: Option<$capability_type<'static>>,
        ) -> Result<(), SchemaValidationError> {
            let annotations_to_revalidate = OperationTimeValidation::get_updated_annotations_if_capability_changes_override::<$capability_type<'static>>(
                snapshot,
                type_manager,
                capability.clone(),
                capability_override.clone()
            )?;

            let sorted_annotations =
                Constraint::annotations_by_validation_modes(annotations_to_revalidate)
                    .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

            let violation =
                $validation_func(snapshot, type_manager, thing_manager, capability.object(), capability.clone(), sorted_annotations)
                    .map_err(SchemaValidationError::ConceptRead)?;

            if let Some((violating_objects_with_interfaces, violated_constraint)) = violation {
                let (override_object, override_interface) = if let Some(capability_override) = capability_override {
                    (Some(capability_override.object()), Some(capability_override.interface()))
                } else {
                    (None, None)
                };
                Err(SchemaValidationError::CannotChangeCapabilityOverrideAsUpdatedAnnotationsConstraintIsViolatedByExistingInstances(
                    $capability_kind,
                    violated_constraint,
                    get_label_or_schema_err(snapshot, capability.object())?,
                    get_label_or_schema_err(snapshot, capability.interface())?,
                    get_opt_label_or_schema_err(snapshot, override_object)?,
                    get_opt_label_or_schema_err(snapshot, override_interface)?,
                    violating_objects_with_interfaces
                        .iter()
                        .map(|(violating_object, violating_interface)| {
                            match (
                                get_label_or_schema_err(snapshot, violating_object.clone()),
                                get_label_or_schema_err(snapshot, violating_interface.clone()),
                            ) {
                                (Ok(object_label), Ok(interface_label)) => Ok((object_label, interface_label)),
                                (Err(err), _) | (_, Err(err)) => Err(err),
                            }
                        })
                        .collect::<Result<Vec<(Label<'static>, Label<'static>)>, SchemaValidationError>>()?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_supertype_change_validation {
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $object_type:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $object_type<'static>,
            new_supertype: $object_type<'static>,
        ) -> Result<(), SchemaValidationError> {
            let updated_capabilities_annotations = OperationTimeValidation::get_updated_capabilities_with_annotations_if_supertype_is_changed::<$capability_type<'static>>(
                snapshot,
                type_.clone(),
                new_supertype.clone()
            )?;

            for (capability, annotations_to_revalidate) in updated_capabilities_annotations {
                let sorted_annotations =
                    Constraint::annotations_by_validation_modes(annotations_to_revalidate)
                        .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

                let violation =
                    $validation_func(snapshot, type_manager, thing_manager, type_.clone(), capability.clone(), sorted_annotations)
                        .map_err(SchemaValidationError::ConceptRead)?;

                if let Some((violating_objects_with_interfaces, violated_constraint)) = violation {
                    return Err(SchemaValidationError::CannotChangeSupertypeAsUpdatedAnnotationsConstraintOnCapabilityOrNewAcquiredCapabilityIsViolatedByExistingInstances(
                        $capability_kind,
                        violated_constraint,
                        get_label_or_schema_err(snapshot, type_.clone())?,
                        get_label_or_schema_err(snapshot, new_supertype)?,
                        get_label_or_schema_err(snapshot, type_)?,
                        get_label_or_schema_err(snapshot, capability.interface())?,
                        violating_objects_with_interfaces
                            .iter()
                            .map(|(violating_object, violating_interface)| {
                                match (
                                    get_label_or_schema_err(snapshot, violating_object.clone()),
                                    get_label_or_schema_err(snapshot, violating_interface.clone()),
                                ) {
                                    (Ok(object_label), Ok(interface_label)) => Ok((object_label, interface_label)),
                                    (Err(err), _) | (_, Err(err)) => Err(err),
                                }
                            })
                            .collect::<Result<Vec<(Label<'static>, Label<'static>)>, SchemaValidationError>>()?,
                    ));
                }
            }

            Ok(())
        }
    };
}

macro_rules! type_or_its_subtype_with_violated_new_annotation_constraints {
    ($func_name:ident, $type_:ident, $existing_instances_validation_func:path) => {
        paste! {
            pub(crate) fn $func_name<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                type_: $type_<'static>,
                sorted_annotations: HashMap<&ConstraintValidationMode, HashSet<Annotation>>,
            ) -> Result<Option<(Vec<$type_<'a>>, AnnotationCategory)>, ConceptReadError> {
                for (validation_mode, annotations) in sorted_annotations {
                    match validation_mode {
                        &ConstraintValidationMode::Type => {
                            let result = Self::[< $func_name _type >](
                                snapshot,
                                type_manager,
                                thing_manager,
                                type_.clone(),
                                annotations
                            )?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        &ConstraintValidationMode::TypeAndSiblings =>
                            unreachable!("Types do not have TypeAndSiblings constraints"),
                        &ConstraintValidationMode::TypeAndSiblingsAndSubtypes =>
                            unreachable!("Types do not have TypeAndSiblingsAndSubtypes constraints"),
                    }
                }
                Ok(None)
            }

            fn [< $func_name _type >]<'a>(
                snapshot: &impl ReadableSnapshot,
                type_manager: &'a TypeManager,
                thing_manager: &ThingManager,
                type_: $type_<'static>,
                annotations: HashSet<Annotation>,
            ) -> Result<Option<(Vec<$type_<'a>>, AnnotationCategory)>, ConceptReadError> {
                if annotations.is_empty() {
                    return Ok(None);
                }

                let mut type_with_violations = None;
                let mut types_and_annotations_to_check = VecDeque::new();
                types_and_annotations_to_check.push_front((type_, annotations));

                while let Some((current_type, annotations_to_revalidate)) = types_and_annotations_to_check.pop_back() {
                    if let Some(violated_constraint) = $existing_instances_validation_func(
                        snapshot,
                        thing_manager,
                        current_type.clone(),
                        &annotations_to_revalidate,
                    )? {
                        type_with_violations = Some((vec![current_type], violated_constraint));
                        break;
                    }

                    let subtypes = current_type.get_subtypes(snapshot, type_manager)?;
                    for subtype in subtypes.into_iter() {
                        let mut subtype_annotations_to_revalidate = annotations_to_revalidate.clone();
                        let declared_annotations =
                            TypeReader::get_type_annotations_declared(snapshot, subtype.clone())?;
                        subtype_annotations_to_revalidate
                            .retain(|annotation| annotation.category().inheritable()
                                && !declared_annotations.contains(&annotation.clone().try_into().unwrap())
                            );

                        types_and_annotations_to_check.push_front((
                            subtype.clone(),
                            subtype_annotations_to_revalidate,
                        ));
                    }
                }

                Ok(type_with_violations)
            }
        }
    };
}

macro_rules! new_annotation_compatible_with_type_and_subtypes_instances_validation {
    ($func_name:ident, $type_:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $type_<'static>,
            annotation: Annotation,
        ) -> Result<(), SchemaValidationError> {
            let mut annotations = HashSet::from([annotation]);
            OperationTimeValidation::filter_type_annotation_constraints_by_not_declared(
                snapshot,
                type_.clone(),
                &mut annotations,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            let sorted_annotations = Constraint::annotations_by_validation_modes(annotations)
                .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

            let violation = $validation_func(snapshot, type_manager, thing_manager, type_.clone(), sorted_annotations)
                .map_err(SchemaValidationError::ConceptRead)?;

            if let Some((violating_types, violated_constraint)) = violation {
                Err(SchemaValidationError::CannotSetAnnotationAsExistingInstancesViolateItsConstraint(
                    violated_constraint,
                    get_label_or_schema_err(snapshot, type_)?,
                    violating_types
                        .iter()
                        .map(|violating_type| get_label_or_schema_err(snapshot, violating_type.clone()))
                        .collect::<Result<Vec<Label<'static>>, SchemaValidationError>>()?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! updated_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change_validation {
    ($func_name:ident, $type_:ident, $validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            type_: $type_<'static>,
            new_supertype: $type_<'static>,
        ) -> Result<(), SchemaValidationError> {
            let updated_annotations = OperationTimeValidation::get_updated_annotations_if_type_changes_supertype(
                snapshot,
                type_.clone(),
                new_supertype.clone()
            )?;

            let sorted_annotations =
                Constraint::annotations_by_validation_modes(updated_annotations)
                    .map_err(|source| SchemaValidationError::ConceptRead(ConceptReadError::Annotation { source }))?;

            let violation =
                $validation_func(snapshot, type_manager, thing_manager, type_.clone(), sorted_annotations)
                    .map_err(SchemaValidationError::ConceptRead)?;

            if let Some((violating_types, violated_constraint)) = violation {
                return Err(SchemaValidationError::CannotChangeSupertypeAsUpdatedAnnotationsConstraintIsViolatedByExistingInstances(
                    violated_constraint,
                    get_label_or_schema_err(snapshot, type_.clone())?,
                    get_label_or_schema_err(snapshot, new_supertype)?,
                    violating_types
                        .iter()
                        .map(|violating_type| get_label_or_schema_err(snapshot, violating_type.clone()))
                        .collect::<Result<Vec<Label<'static>>, SchemaValidationError>>()?,
                ));
            }

            Ok(())
        }
    };
}

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_type_exists(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_label(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?;
        Ok(())
    }

    pub(crate) fn validate_no_subtypes_for_type_deletion<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        let no_subtypes =
            TypeReader::get_subtypes(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?.is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(SchemaValidationError::CannotDeleteTypeWithExistingSubtypes(get_label_or_schema_err(snapshot, type_)?))
        }
    }

    pub(crate) fn validate_no_subtypes_for_type_abstractness_unset(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let no_subtypes =
            TypeReader::get_subtypes(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?.is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(SchemaValidationError::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(get_label_or_schema_err(
                snapshot, type_,
            )?))
        }
    }

    pub(crate) fn validate_no_overriding_capabilities_for_capability_unset<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        let overriding_capabilities = TypeReader::get_overriding_capabilities(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        match overriding_capabilities.iter().next() {
            Some(overriding_capability) => {
                Err(SchemaValidationError::CannotUnsetCapabilityWithExistingOverridingCapabilities(
                    CAP::KIND,
                    get_label_or_schema_err(snapshot, capability.object())?,
                    get_label_or_schema_err(snapshot, capability.interface())?,
                    get_label_or_schema_err(snapshot, overriding_capability.object())?,
                    get_label_or_schema_err(snapshot, overriding_capability.interface())?,
                ))
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_no_capabilities_with_abstract_interfaces_to_unset_abstractness<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_capabilities_declared::<CAP>(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .iter()
            .map(CAP::interface)
            .try_for_each(|interface_type| {
                if interface_type.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)? {
                    Err(SchemaValidationError::CannotUnsetAbstractnessAsItHasDeclaredCapabilityOfAbstractInterface(
                        CAP::KIND,
                        get_label_or_schema_err(snapshot, type_.clone())?,
                        get_label_or_schema_err(snapshot, interface_type)?,
                    ))
                } else {
                    Ok(())
                }
            })?;

        Ok(())
    }

    pub(crate) fn validate_label_uniqueness(
        snapshot: &impl ReadableSnapshot,
        new_label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        if TypeReader::get_labelled_type::<AttributeType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Attribute })
        } else if TypeReader::get_labelled_type::<RelationType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Relation })
        } else if TypeReader::get_labelled_type::<EntityType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Entity })
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_new_role_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        let existing_relation_supertypes =
            TypeReader::get_supertypes_transitive(snapshot, relation_type.clone().into_owned())
                .map_err(SchemaValidationError::ConceptRead)?;

        let existing_relation_subtypes =
            TypeReader::get_subtypes_transitive(snapshot, relation_type.clone().into_owned())
                .map_err(SchemaValidationError::ConceptRead)?;

        validate_role_name_uniqueness_non_transitive(snapshot, relation_type, label)?;
        for relation_supertype in existing_relation_supertypes {
            validate_role_name_uniqueness_non_transitive(snapshot, relation_supertype, label)?;
        }
        for relation_subtype in existing_relation_subtypes {
            validate_role_name_uniqueness_non_transitive(snapshot, relation_subtype, label)?;
        }

        Ok(())
    }

    pub(crate) fn validate_role_names_compatible_with_new_relation_supertype_transitive(
        snapshot: &impl ReadableSnapshot,
        relation_subtype: RelationType<'static>,
        relation_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, relation_subtype, |type_: RelationType<'static>| {
            Self::validate_role_names_compatible_with_new_relation_supertype(
                snapshot,
                type_,
                relation_supertype.clone(),
            )
        });
        Ok(())
    }

    fn validate_role_names_compatible_with_new_relation_supertype(
        snapshot: &impl ReadableSnapshot,
        relation_subtype: RelationType<'static>,
        relation_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_relates_declared =
            TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_subtype)
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_relates in subtype_relates_declared {
            let role = subtype_relates.role();
            let role_label = get_label_or_schema_err(snapshot, role)?;
            for_type_and_supertypes_transitive!(snapshot, relation_supertype, |type_: RelationType<'static>| {
                validate_role_name_uniqueness_non_transitive(snapshot, type_.clone(), &role_label)
            });
        }

        Ok(())
    }

    pub(crate) fn validate_capabilities_are_not_overridden_in_the_new_supertype_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_subtype: CAP::ObjectType,
        object_supertype: CAP::ObjectType,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, object_subtype, |type_: CAP::ObjectType| {
            Self::validate_owns_are_not_overridden_in_the_new_supertype::<CAP>(
                snapshot,
                type_,
                object_supertype.clone(),
            )
        });
        Ok(())
    }

    pub(crate) fn validate_owns_are_not_overridden_in_the_new_supertype<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_subtype: CAP::ObjectType,
        object_supertype: CAP::ObjectType,
    ) -> Result<(), SchemaValidationError> {
        let subtype_capabilities_declared: HashSet<CAP> =
            TypeReader::get_capabilities_declared(snapshot, object_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_capability in subtype_capabilities_declared {
            let interface_type = subtype_capability.interface();
            if is_interface_hidden_by_overrides::<CAP>(snapshot, object_supertype.clone(), interface_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?
            {
                return Err(SchemaValidationError::CannotChangeSupertypeAsCapabilityIsOverriddenInTheNewSupertype(
                    CAP::KIND,
                    get_label_or_schema_err(snapshot, object_subtype)?,
                    get_label_or_schema_err(snapshot, object_supertype)?,
                    get_label_or_schema_err(snapshot, interface_type)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_capability_overrides_compatible_with_new_supertype_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_subtype: CAP::ObjectType,
        object_supertype: Option<CAP::ObjectType>,
    ) -> Result<(), SchemaValidationError> {
        let supertype_capabilities = match &object_supertype {
            None => HashSet::new(),
            Some(object_supertype) => TypeReader::get_capabilities::<CAP>(snapshot, object_supertype.clone())
                .map_err(SchemaValidationError::ConceptRead)?,
        };

        let subtype_capabilities_declared =
            TypeReader::get_capabilities_declared::<CAP>(snapshot, object_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
        let subtype_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, object_subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_capability in &subtype_capabilities_declared {
            if let Some(old_capability_override) =
                TypeReader::get_capability_override(snapshot, subtype_capability.clone())
                    .map_err(SchemaValidationError::ConceptRead)?
            {
                if !supertype_capabilities
                    .iter()
                    .any(|supertype_capability| supertype_capability == &old_capability_override)
                {
                    // Ordering and annotations don't need to be rechecked as it's the same edge
                    return Err(SchemaValidationError::CannotChangeSupertypeAsCapabilityOverrideIsImplicitlyLost(
                        CAP::KIND,
                        get_label_or_schema_err(snapshot, object_subtype)?,
                        get_opt_label_or_schema_err(snapshot, object_supertype)?,
                        get_label_or_schema_err(snapshot, subtype_capability.object())?,
                        get_label_or_schema_err(snapshot, subtype_capability.interface())?,
                        get_label_or_schema_err(snapshot, old_capability_override.object())?,
                        get_label_or_schema_err(snapshot, old_capability_override.interface())?,
                    ));
                }
            }
        }

        let subtype_subtypes = TypeReader::get_subtypes_transitive(snapshot, object_subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subsubtype in subtype_subtypes {
            let subsubtype_capabilities_declared: HashSet<CAP> =
                TypeReader::get_capabilities_declared(snapshot, subsubtype.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            for subsubtype_capability in subsubtype_capabilities_declared {
                if let Some(old_capability_override) =
                    TypeReader::get_capability_override(snapshot, subsubtype_capability.clone())
                        .map_err(SchemaValidationError::ConceptRead)?
                {
                    let is_in_subtype = subtype_capabilities.contains(&old_capability_override);
                    let is_in_subtype_declared = subtype_capabilities_declared.contains(&old_capability_override);
                    let is_lost = is_in_subtype && !is_in_subtype_declared;
                    if is_lost {
                        if !supertype_capabilities
                            .iter()
                            .any(|supertype_capability| supertype_capability == &old_capability_override)
                        {
                            // Ordering and annotations don't need to be rechecked as it's the same edge
                            return Err(
                                SchemaValidationError::CannotChangeSupertypeAsCapabilityOverrideIsImplicitlyLost(
                                    CAP::KIND,
                                    get_label_or_schema_err(snapshot, object_subtype)?,
                                    get_opt_label_or_schema_err(snapshot, object_supertype)?,
                                    get_label_or_schema_err(snapshot, subsubtype_capability.object())?,
                                    get_label_or_schema_err(snapshot, subsubtype_capability.interface())?,
                                    get_label_or_schema_err(snapshot, old_capability_override.object())?,
                                    get_label_or_schema_err(snapshot, old_capability_override.interface())?,
                                ),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_struct_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<(), SchemaValidationError> {
        let struct_clash = TypeReader::get_struct_definition_key(snapshot, name)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        // TODO: Check other types clash?

        if struct_clash {
            Err(SchemaValidationError::StructNameShouldBeUnique(name.to_owned()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        subtype: AttributeType<'static>,
        supertype: Option<AttributeType<'static>>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_value_type = TypeReader::get_value_type_declared(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let subtype_transitive_value_type = TypeReader::get_value_type_without_source(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let supertype_value_type = match &supertype {
            None => None,
            Some(supertype) => TypeReader::get_value_type_without_source(snapshot, supertype.clone())
                .map_err(SchemaValidationError::ConceptRead)?,
        };

        match (&subtype_declared_value_type, &subtype_transitive_value_type, &supertype_value_type) {
            (None, None, None) => Ok(()),
            (None, None, Some(_)) => Ok(()),
            (None, Some(_), None) => Self::validate_when_attribute_type_loses_value_type_transitive(
                snapshot,
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
                    Err(SchemaValidationError::ChangingAttributeTypeSupertypeWillImplicitlyChangeItsValueType(
                        get_label_or_schema_err(snapshot, subtype)?,
                        subtype_declared_value_type,
                    ))
                }
            }
        }
    }

    pub(crate) fn validate_value_type_can_be_unset(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let value_type_with_source =
            TypeReader::get_value_type(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        match value_type_with_source {
            Some((value_type, source)) => {
                if source != attribute_type {
                    return Err(SchemaValidationError::CannotUnsetInheritedValueType(
                        value_type,
                        get_label_or_schema_err(snapshot, source)?,
                    ));
                }

                let attribute_supertype = TypeReader::get_supertype(snapshot, attribute_type.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;
                match &attribute_supertype {
                    Some(supertype) => {
                        let supertype_value_type =
                            TypeReader::get_value_type_without_source(snapshot, supertype.clone())
                                .map_err(SchemaValidationError::ConceptRead)?;
                        match supertype_value_type {
                            Some(_) => Ok(()),
                            None => Self::validate_when_attribute_type_loses_value_type_transitive(
                                snapshot,
                                thing_manager,
                                attribute_type,
                                Some(value_type),
                            ),
                        }
                    }
                    None => Self::validate_when_attribute_type_loses_value_type_transitive(
                        snapshot,
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
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
        abstract_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let is_abstract = abstract_set.unwrap_or(
            type_get_annotation_by_category(snapshot, attribute_type.clone(), AnnotationCategory::Abstract)
                .map_err(SchemaValidationError::ConceptRead)?
                .is_some(),
        );

        match &value_type {
            Some(_) => Ok(()),
            None => {
                if is_abstract {
                    Ok(())
                } else {
                    Err(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract(get_label_or_schema_err(
                        snapshot,
                        attribute_type,
                    )?))
                }
            }
        }
    }

    pub(crate) fn validate_annotation_regex_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationRegex::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRegexAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_range_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationRange::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRangeAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_values_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationValues::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithValuesAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_set_only_for_interface_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, interface_type, |type_: CAP::InterfaceType| {
            Self::validate_annotation_set_only_for_interface::<CAP>(
                snapshot,
                type_.clone(),
                annotation_category.clone(),
            )
        });
        Ok(())
    }

    fn validate_annotation_set_only_for_interface<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let capabilities = TypeReader::get_capabilities_for_interface::<CAP>(snapshot, interface_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, capability) in capabilities {
            let capability_annotations = TypeReader::get_type_edge_annotations(snapshot, capability)
                .map_err(SchemaValidationError::ConceptRead)?;
            if capability_annotations
                .keys()
                .map(|annotation| annotation.clone().into().category())
                .contains(&annotation_category)
            {
                return Err(
                    SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsCapability(
                        get_label_or_schema_err(snapshot, interface_type)?,
                        annotation_category,
                    ),
                );
            }
        }

        Ok(())
    }

    pub(crate) fn validate_annotation_set_only_for_capability_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        for_capability_and_overriding_capabilities_transitive!(snapshot, capability, |cap: CAP| {
            Self::validate_annotation_set_only_for_capability::<CAP>(snapshot, cap.clone(), annotation_category.clone())
        });
        Ok(())
    }

    fn validate_annotation_set_only_for_capability<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let interface_annotations = TypeReader::get_type_annotations(snapshot, capability.interface())
            .map_err(SchemaValidationError::ConceptRead)?;
        if interface_annotations
            .keys()
            .map(|annotation| annotation.clone().into().category())
            .contains(&annotation_category)
        {
            return Err(SchemaValidationError::CannotSetAnnotationToCapabilityBecauseItAlreadyExistsForItsInterface(
                get_label_or_schema_err(snapshot, capability.object())?,
                get_label_or_schema_err(snapshot, capability.interface())?,
                annotation_category,
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_attribute_type_supertype_is_abstract<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        if type_.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(SchemaValidationError::AttributeTypeSupertypeIsNotAbstract(get_label_or_schema_err(snapshot, type_)?))
        }
    }

    pub(crate) fn validate_capability_abstractness<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        object: impl TypeAPI<'static>,
        interface_type: CAP::InterfaceType,
        set_abstract: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let is_object_abstract =
            object.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?;
        let is_interface_abstract = set_abstract
            .unwrap_or(interface_type.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?);

        match (&is_object_abstract, &is_interface_abstract) {
            (true, true) | (false, false) | (true, false) => Ok(()),
            (false, true) => Err(SchemaValidationError::NonAbstractTypeCannotHaveAbstractInterfaceCapability(
                CAP::KIND,
                get_label_or_schema_err(snapshot, object)?,
                get_label_or_schema_err(snapshot, interface_type)?,
            )),
        }
    }

    pub(crate) fn validate_cardinality_arguments(
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError> {
        if cardinality.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidCardinalityArguments(cardinality))
        }
    }

    pub(crate) fn validate_regex_arguments(regex: AnnotationRegex) -> Result<(), SchemaValidationError> {
        if regex.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidRegexArguments(regex))
        }
    }

    pub(crate) fn validate_range_arguments(
        range: AnnotationRange,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if range.valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidRangeArgumentsForValueType(range, value_type))
        }
    }

    pub(crate) fn validate_values_arguments(
        values: AnnotationValues,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if values.valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidValuesArgumentsForValueType(values, value_type))
        }
    }

    pub(crate) fn validate_overriding_capabilities_narrow_cardinality<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_overriding_capabilities_transitive(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_capability| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    overriding_capability,
                    Cardinality,
                    capability_get_declared_annotation_by_category,
                    |overriding_cardinality| {
                        return if cardinality.narrowed_correctly_by(overriding_cardinality) {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
                                CAP::KIND,
                                get_label_or_schema_err(snapshot, overriding_capability.object())?,
                                get_label_or_schema_err(snapshot, overriding_capability.interface())?,
                                get_label_or_schema_err(snapshot, capability.object())?,
                                get_label_or_schema_err(snapshot, capability.interface())?,
                                overriding_cardinality.clone(),
                                cardinality,
                            ))
                        };
                    }
                );
                Ok(())
            })?;

        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_regex(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    subtype,
                    Regex,
                    type_get_declared_annotation_by_category,
                    |subtype_regex| {
                        return if subtype_regex.regex() == regex.regex() {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::OnlyOneRegexCanBeSetForTypeHierarchy(
                                get_label_or_schema_err(snapshot, subtype)?,
                                get_label_or_schema_err(snapshot, attribute_type.clone())?,
                                subtype_regex.clone(),
                                regex.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_range(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    subtype,
                    Range,
                    type_get_declared_annotation_by_category,
                    |subtype_range| {
                        return if range.narrowed_correctly_by(subtype_range) {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::RangeShouldNarrowInheritedRange(
                                get_label_or_schema_err(snapshot, subtype)?,
                                get_label_or_schema_err(snapshot, attribute_type.clone())?,
                                subtype_range.clone(),
                                range.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_subtypes_narrow_values(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    subtype,
                    Values,
                    type_get_declared_annotation_by_category,
                    |subtype_values| {
                        return if values.narrowed_correctly_by(subtype_values) {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::ValuesShouldNarrowInheritedValues(
                                get_label_or_schema_err(snapshot, subtype)?,
                                get_label_or_schema_err(snapshot, attribute_type.clone())?,
                                subtype_values.clone(),
                                values.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_overriding_capabilities_narrow_regex(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_overriding_capabilities_transitive(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_owns| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    overriding_owns,
                    Regex,
                    capability_get_declared_annotation_by_category,
                    |overriding_regex| {
                        return if overriding_regex.regex() == regex.regex() {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::OnlyOneRegexCanBeSetForCapabilitiesHierarchy(
                                get_label_or_schema_err(snapshot, overriding_owns.object())?,
                                get_label_or_schema_err(snapshot, overriding_owns.interface())?,
                                get_label_or_schema_err(snapshot, owns.object())?,
                                get_label_or_schema_err(snapshot, owns.interface())?,
                                overriding_regex.clone(),
                                regex.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_overriding_capabilities_narrow_range(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_overriding_capabilities_transitive(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_owns| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    overriding_owns,
                    Range,
                    capability_get_declared_annotation_by_category,
                    |overriding_range| {
                        return if range.narrowed_correctly_by(overriding_range) {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::RangeShouldNarrowInheritedCapabilityRange(
                                get_label_or_schema_err(snapshot, overriding_owns.object())?,
                                get_label_or_schema_err(snapshot, overriding_owns.interface())?,
                                get_label_or_schema_err(snapshot, owns.object())?,
                                get_label_or_schema_err(snapshot, owns.interface())?,
                                overriding_range.clone(),
                                range.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_overriding_capabilities_narrow_values(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_overriding_capabilities_transitive(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_owns| {
                with_annotation_constraint_if_exists!(
                    snapshot,
                    overriding_owns,
                    Values,
                    capability_get_declared_annotation_by_category,
                    |overriding_values| {
                        return if values.narrowed_correctly_by(overriding_values) {
                            Ok(())
                        } else {
                            Err(SchemaValidationError::ValuesShouldNarrowInheritedCapabilityValues(
                                get_label_or_schema_err(snapshot, overriding_owns.object())?,
                                get_label_or_schema_err(snapshot, overriding_owns.interface())?,
                                get_label_or_schema_err(snapshot, owns.object())?,
                                get_label_or_schema_err(snapshot, owns.interface())?,
                                overriding_values.clone(),
                                values.clone(),
                            ))
                        };
                    }
                );
                Ok(())
            })?;
        Ok(())
    }

    pub(crate) fn validate_cardinality_narrows_inherited_cardinality<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        overridden_capability: CAP,
        cardinality: AnnotationCardinality,
        is_key: bool,
    ) -> Result<(), SchemaValidationError> {
        validate_cardinality_narrows_inherited_cardinality(
            snapshot,
            type_manager,
            capability,
            overridden_capability,
            cardinality,
            is_key,
        )
    }

    pub(crate) fn validate_updated_cardinality_against_inheritance_line<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError> {
        let mut validation_errors = vec![];
        let updated_cardinalities =
            Self::get_transitively_updated_cardinalities(snapshot, type_manager, capability.clone(), cardinality)
                .map_err(SchemaValidationError::ConceptRead)?;
        if let Some(_) = TypeReader::get_capability_override(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?
        {
            validate_capabilities_cardinality::<CAP>(
                type_manager,
                snapshot,
                capability.object(),
                &updated_cardinalities,
                &HashMap::new(), // read all overrides from storage as it's not changed
                &mut validation_errors,
            )
            .map_err(SchemaValidationError::ConceptRead)?;
        }

        if let Some(error) = validation_errors.first() {
            return Err(error.clone());
        }

        // Preserve inheritance ordering in validations
        // (expect errors from inheritance levels closer to the changed capability)
        let mut checked_object_types: HashSet<CAP::ObjectType> = HashSet::new();
        let overriding_capabilities = TypeReader::get_overriding_capabilities_transitive(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for overriding_capability in overriding_capabilities {
            let already_checked = checked_object_types.insert(overriding_capability.object());
            if already_checked {
                continue;
            }

            validate_capabilities_cardinality::<CAP>(
                type_manager,
                snapshot,
                overriding_capability.object(),
                &updated_cardinalities,
                &HashMap::new(), // read all overrides from storage as it's not changed
                &mut validation_errors,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(error) = validation_errors.first() {
                return Err(error.clone());
            }
        }

        Ok(())
    }

    pub(crate) fn validate_cardinality_of_inheritance_line_with_updated_override<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        overridden_capability: Option<CAP>,
    ) -> Result<(), SchemaValidationError> {
        let mut validation_errors = vec![];

        let cardinality_declared =
            capability.get_cardinality_declared(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?;
        let cardinality = if let Some(cardinality_declared) = cardinality_declared {
            // Does not check that declared cardinality narrows updated inherited one! Should be checked in a separate validation
            cardinality_declared
        } else if let Some(overridden_capability) = &overridden_capability {
            overridden_capability.get_cardinality(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?
        } else {
            capability.get_default_cardinality(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?
        };

        let updated_cardinalities =
            Self::get_transitively_updated_cardinalities(snapshot, type_manager, capability.clone(), cardinality)
                .map_err(SchemaValidationError::ConceptRead)?;
        let updated_overrides = HashMap::from([(capability.clone(), overridden_capability.clone())]);
        validate_capabilities_cardinality::<CAP>(
            type_manager,
            snapshot,
            capability.object(),
            &updated_cardinalities,
            &updated_overrides,
            &mut validation_errors,
        )
        .map_err(SchemaValidationError::ConceptRead)?;

        if let Some(error) = validation_errors.first() {
            return Err(error.clone());
        }

        // Preserve inheritance ordering in validations
        // (expect errors from inheritance levels closer to the changed capability)
        let mut checked_object_types: HashSet<CAP::ObjectType> = HashSet::new();
        let overriding_capabilities = TypeReader::get_overriding_capabilities_transitive(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for overriding_capability in overriding_capabilities {
            let not_checked = checked_object_types.insert(overriding_capability.object());
            if !not_checked {
                continue;
            }

            validate_capabilities_cardinality::<CAP>(
                type_manager,
                snapshot,
                overriding_capability.object(),
                &updated_cardinalities,
                &updated_overrides,
                &mut validation_errors,
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(error) = validation_errors.first() {
                return Err(error.clone());
            }
        }

        Ok(())
    }

    fn get_transitively_updated_cardinalities<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        capability_cardinality: AnnotationCardinality,
    ) -> Result<HashMap<CAP, AnnotationCardinality>, ConceptReadError> {
        let mut updated_cardinalities = HashMap::from([(capability.clone(), capability_cardinality.clone())]);

        // Do not care about the capability's override
        let mut capabilities_and_overrides = VecDeque::from([(capability.clone(), capability.clone())]);
        while let Some((current_capability, overridden)) = capabilities_and_overrides.pop_back() {
            if !updated_cardinalities.contains_key(&current_capability) {
                let cardinality_declared = current_capability.get_cardinality_declared(snapshot, type_manager)?;
                let updated_cardinality = if let Some(cardinality_declared) = cardinality_declared {
                    // Does not check that declared cardinality narrows updated inherited one! Should be checked in a separate validation
                    cardinality_declared
                } else {
                    match updated_cardinalities.get(&overridden) {
                        Some(inherited_cardinality) => inherited_cardinality.clone(),
                        None => {
                            debug_assert!(
                                false,
                                "Should not visit this capability until the overridden is recalculated"
                            );
                            overridden.get_cardinality(snapshot, type_manager)?
                        }
                    }
                };

                updated_cardinalities.insert(current_capability.clone(), updated_cardinality);
            }

            TypeReader::get_overriding_capabilities(snapshot, current_capability.clone())?.into_iter().for_each(
                |overriding_capability| {
                    capabilities_and_overrides.push_front((overriding_capability, current_capability.clone()))
                },
            );
        }

        Ok(updated_cardinalities)
    }

    pub(crate) fn validate_type_supertype_abstractness_to_set_abstract_annotation<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: T,
    ) -> Result<(), SchemaValidationError> {
        // Supertype is read from the storage, set_subtype_abstract is true
        validate_type_supertype_abstractness(
            snapshot,
            type_manager,
            attribute_type,
            None,       // supertype is read from storage
            Some(true), // set_subtype_abstract
            None,       // supertype is abstract is read from storage
        )
    }

    pub(crate) fn validate_no_abstract_subtypes_to_unset_abstract_annotation<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: T,
    ) -> Result<(), SchemaValidationError> {
        attribute_type
            .get_subtypes(snapshot, type_manager)
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| {
                validate_type_supertype_abstractness(
                    snapshot,
                    type_manager,
                    subtype.clone(),
                    Some(attribute_type.clone()), // supertype is read from storage
                    None,                         // subtype is abstract is read from storage
                    Some(false),                  // set_supertype_abstract
                )
            })?;
        Ok(())
    }

    pub(crate) fn validate_type_regex_narrows_inherited_regex<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        validate_type_regex_narrows_inherited_regex(snapshot, attribute_type, supertype, regex)
    }

    pub(crate) fn validate_capability_regex_narrows_inherited_regex<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_regex_narrows_inherited_regex(snapshot, owns, overridden_owns, regex)
    }

    pub(crate) fn validate_type_range_narrows_inherited_range<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        validate_type_range_narrows_inherited_range(snapshot, attribute_type, supertype, range)
    }

    // TODO: Wrap into a macro to call the same function from validation.rs?
    pub(crate) fn validate_capabilities_range_narrows_inherited_range<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_range_narrows_inherited_range(snapshot, owns, overridden_owns, range)
    }

    pub(crate) fn validate_type_values_narrows_inherited_values<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        validate_type_values_narrows_inherited_values(snapshot, attribute_type, supertype, values)
    }

    pub(crate) fn validate_capabilities_values_narrows_inherited_values<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_values_narrows_inherited_values(snapshot, owns, overridden_owns, values)
    }

    pub(crate) fn validate_relates_distinct_annotation_ordering(
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let role = relates.role();
        let ordering = ordering.unwrap_or(
            TypeReader::get_type_ordering(snapshot, role.clone()).map_err(SchemaValidationError::ConceptRead)?,
        );
        let distinct_set = distinct_set.unwrap_or(
            capability_get_annotation_by_category(snapshot, relates, AnnotationCategory::Distinct)
                .map_err(SchemaValidationError::ConceptRead)?
                .is_some(),
        );

        if is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label_or_schema_err(snapshot, role)?))
        }
    }

    pub(crate) fn validate_owns_distinct_annotation_ordering(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let attribute = owns.attribute();
        let ordering = ordering.unwrap_or(
            TypeReader::get_type_edge_ordering(snapshot, owns.clone()).map_err(SchemaValidationError::ConceptRead)?,
        );
        let distinct_set = distinct_set.unwrap_or(
            capability_get_annotation_by_category(snapshot, owns, AnnotationCategory::Distinct)
                .map_err(SchemaValidationError::ConceptRead)?
                .is_some(),
        );

        if is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label_or_schema_err(
                snapshot, attribute,
            )?))
        }
    }

    pub(crate) fn validate_role_supertype_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_role: RoleType<'static>,
        supertype_role: RoleType<'static>,
        set_subtype_role_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        validate_role_type_supertype_ordering_match(snapshot, subtype_role, supertype_role, set_subtype_role_ordering)
    }

    pub(crate) fn validate_overriding_owns_ordering_match(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        set_subtype_owns_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_overriding_capabilities_transitive(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_owns| {
                validate_owns_override_ordering_match(
                    snapshot,
                    overriding_owns,
                    owns.clone(),
                    set_subtype_owns_ordering,
                )
            })?;
        Ok(())
    }

    pub(crate) fn validate_owns_override_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_owns: Owns<'static>,
        supertype_owns: Owns<'static>,
        set_subtype_owns_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        validate_owns_override_ordering_match(snapshot, subtype_owns, supertype_owns, set_subtype_owns_ordering)
    }

    pub(crate) fn validate_supertype_annotations_compatibility<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        subtype: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_annotations = TypeReader::get_type_annotations_declared(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype_annotation in subtype_declared_annotations {
            let category = subtype_annotation.clone().into().category();

            Self::validate_declared_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                supertype.clone(),
                category,
            )?;

            validate_type_annotations_narrowing_of_inherited_annotations(
                snapshot,
                type_manager,
                subtype.clone(),
                supertype.clone(),
                subtype_annotation,
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_capability_override_annotations_compatibility_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        overridden_capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        for_capability_and_overriding_capabilities_transitive!(snapshot, capability, |cap: CAP| {
            Self::validate_capability_override_annotations_compatibility::<CAP>(
                snapshot,
                type_manager,
                cap.clone(),
                overridden_capability.clone(),
            )
        });
        Ok(())
    }

    fn validate_capability_override_annotations_compatibility<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        overridden_capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        let capability_declared_annotations =
            TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for capability_annotation in capability_declared_annotations {
            Self::validate_declared_capability_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                overridden_capability.clone(),
                capability_annotation.clone().into().category(),
            )?;

            validate_edge_annotations_narrowing_of_inherited_annotations(
                snapshot,
                type_manager,
                capability.clone(),
                overridden_capability.clone(),
                capability_annotation.clone().into(),
            )?;
        }

        Ok(())
    }

    pub(crate) fn validate_sub_does_not_create_cycle<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let existing_supertypes = TypeReader::get_supertypes_transitive(snapshot, supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        if supertype == type_ || existing_supertypes.contains(&type_) {
            Err(SchemaValidationError::CycleFoundInTypeHierarchy(
                get_label_or_schema_err(snapshot, type_)?,
                get_label_or_schema_err(snapshot, supertype)?,
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_is_inherited(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let super_relation =
            TypeReader::get_supertype(snapshot, relation_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        if let Some(super_relation) = super_relation {
            let is_inherited = TypeReader::get_capabilities::<Relates<'_>>(snapshot, super_relation)
                .map_err(SchemaValidationError::ConceptRead)?
                .iter()
                .any(|relates| &relates.role() == &role_type);
            if is_inherited {
                Ok(())
            } else {
                Err(SchemaValidationError::RelatesNotInherited(relation_type, role_type))
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_owns_is_inherited(
        snapshot: &impl ReadableSnapshot,
        owner: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let is_inherited = with_object_type!(owner, |owner| {
            let super_owner =
                TypeReader::get_supertype(snapshot, owner.clone()).map_err(SchemaValidationError::ConceptRead)?;
            if super_owner.is_none() {
                return Ok(());
            }
            let owns_transitive = TypeReader::get_capabilities::<Owns<'static>>(
                snapshot,
                super_owner.unwrap().clone().into_owned_object_type(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;
            owns_transitive.iter().any(|owns| &owns.attribute() == &attribute_type)
        });
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::OwnsNotInherited(owner, attribute_type))
        }
    }

    pub(crate) fn validate_overridden_interface_type_is_supertype_or_self<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        interface_type_overridden: CAP::InterfaceType,
    ) -> Result<(), SchemaValidationError> {
        if is_type_transitive_supertype_or_same(snapshot, capability.interface(), interface_type_overridden.clone())
            .map_err(SchemaValidationError::ConceptRead)?
        {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                CAP::KIND,
                get_label_or_schema_err(snapshot, capability.object())?,
                get_label_or_schema_err(snapshot, capability.interface())?,
                get_label_or_schema_err(snapshot, interface_type_overridden)?,
            ))
        }
    }

    pub(crate) fn validate_interface_not_overridden<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
        interface_type: CAP::InterfaceType,
    ) -> Result<(), SchemaValidationError> {
        if !is_interface_hidden_by_overrides::<CAP>(snapshot, object_type.clone(), interface_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?
        {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenCapabilityCannotBeRedeclared(
                CapabilityKind::Owns,
                get_label_or_schema_err(snapshot, object_type)?,
                get_label_or_schema_err(snapshot, interface_type)?,
            ))
        }
    }

    pub(crate) fn validate_plays_is_inherited(
        snapshot: &impl ReadableSnapshot,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let is_inherited = with_object_type!(player, |player| {
            let super_player =
                TypeReader::get_supertype(snapshot, player.clone()).map_err(SchemaValidationError::ConceptRead)?;
            if super_player.is_none() {
                return Ok(());
            }
            let plays_transitive = TypeReader::get_capabilities::<Plays<'static>>(
                snapshot,
                super_player.unwrap().clone().into_owned_object_type(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;
            plays_transitive.iter().any(|plays| &plays.role() == &role_type)
        });
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::PlaysNotInherited(player, role_type))
        }
    }

    pub(crate) fn validate_value_type_compatible_with_inherited_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), SchemaValidationError> {
        let inherited_value_type_with_source =
            TypeReader::get_value_type(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        match inherited_value_type_with_source {
            Some((inherited_value_type, inherited_value_type_source)) => {
                if inherited_value_type == value_type || inherited_value_type_source == attribute_type {
                    Ok(())
                } else {
                    Err(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueType(
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        get_label_or_schema_err(snapshot, inherited_value_type_source)?,
                        value_type,
                        inherited_value_type,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_subtypes_value_types_compatible_with_new_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), SchemaValidationError> {
        let subtypes = TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype in subtypes.into_iter() {
            if let Some(subtype_value_type) = TypeReader::get_value_type_declared(snapshot, subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?
            {
                if subtype_value_type != value_type {
                    return Err(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueType(
                        get_label_or_schema_err(snapshot, subtype)?,
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        subtype_value_type,
                        value_type,
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_when_attribute_type_loses_value_type_transitive(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let mut affected_sources: HashSet<AttributeType<'static>> =
            TypeReader::get_supertypes_transitive(snapshot, attribute_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?
                .into_iter()
                .collect();
        affected_sources.insert(attribute_type.clone());
        for_type_and_subtypes_transitive!(snapshot, attribute_type, |type_: AttributeType<'static>| {
            let type_value_type =
                TypeReader::get_value_type(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
            if let Some((type_value_type, type_value_type_source)) = type_value_type {
                if affected_sources.contains(&type_value_type_source) {
                    debug_assert!(value_type.clone().unwrap_or(type_value_type.clone()) == type_value_type);
                    Self::validate_when_attribute_type_loses_value_type(
                        snapshot,
                        thing_manager,
                        type_.clone(),
                        value_type.clone(),
                    )?;
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn validate_when_attribute_type_loses_value_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        match value_type {
            Some(_) => {
                Self::validate_value_type_compatible_with_abstractness(snapshot, attribute_type.clone(), None, None)?;
                // Check only declared as inherited ones should be checked on the supertype if it also loses its value type
                let annotations = TypeReader::get_type_annotations_declared(snapshot, attribute_type.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;
                Self::validate_attribute_type_value_type_compatible_with_annotations_and_arguments(
                    snapshot,
                    attribute_type.clone(),
                    None,
                    annotations,
                )?;
                Self::validate_value_type_compatible_with_all_owns_annotations(snapshot, attribute_type.clone(), None)?;
                Self::validate_no_instances_to_unset_value_type(snapshot, thing_manager, attribute_type)
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_interface_change_supertype_does_not_corrupt_capabilities_overrides_transitive<
        CAP: Capability<'static>,
    >(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
        new_interface_supertype: Option<CAP::InterfaceType>,
    ) -> Result<(), SchemaValidationError> {
        let old_interface_supertype =
            TypeReader::get_supertype(snapshot, interface_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        if let Some(old_interface_supertype) = old_interface_supertype {
            for_type_and_subtypes_transitive!(snapshot, interface_type, |type_: CAP::InterfaceType| {
                Self::validate_interface_change_supertype_does_not_corrupt_capabilities_overrides::<CAP>(
                    snapshot,
                    type_,
                    old_interface_supertype.clone(),
                    new_interface_supertype.clone(),
                )
            });
        }

        Ok(())
    }

    fn validate_interface_change_supertype_does_not_corrupt_capabilities_overrides<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
        old_interface_supertype: CAP::InterfaceType,
        new_interface_supertype: Option<CAP::InterfaceType>,
    ) -> Result<(), SchemaValidationError> {
        let capabilities: HashSet<CAP> =
            TypeReader::get_capabilities_for_interface_declared(snapshot, interface_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
        for capability in capabilities {
            let capability_override = TypeReader::get_capability_override(snapshot, capability.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            match capability_override {
                Some(capability_override) if capability_override.interface() != interface_type => {
                    if is_type_transitive_supertype_or_same(
                        snapshot,
                        old_interface_supertype.clone(),
                        capability_override.interface(),
                    )
                    .map_err(SchemaValidationError::ConceptRead)?
                    {
                        if let Some(new_interface_supertype) = &new_interface_supertype {
                            if is_type_transitive_supertype_or_same(
                                snapshot,
                                new_interface_supertype.clone(),
                                capability_override.interface(),
                            )
                            .map_err(SchemaValidationError::ConceptRead)?
                            {
                                continue;
                            }
                        }
                        return Err(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                            CAP::KIND,
                            get_label_or_schema_err(snapshot, capability.object())?,
                            get_label_or_schema_err(snapshot, interface_type)?,
                            get_label_or_schema_err(snapshot, capability_override.interface())?,
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn validate_relation_type_does_not_acquire_cascade_annotation_to_lose_instances_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'static>,
        new_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let old_annotation =
            type_get_annotation_by_category(snapshot, relation_type.clone(), AnnotationCategory::Cascade)
                .map_err(SchemaValidationError::ConceptRead)?;
        match old_annotation {
            None => {
                let new_supertype_annotation =
                    type_get_annotation_by_category(snapshot, new_supertype.clone(), AnnotationCategory::Cascade)
                        .map_err(SchemaValidationError::ConceptRead)?;
                match new_supertype_annotation {
                    None => Ok(()),
                    Some(_) => {
                        for_type_and_subtypes_transitive!(snapshot, relation_type, |type_: RelationType<'static>| {
                            let type_annotation =
                                type_get_annotation_by_category(snapshot, type_.clone(), AnnotationCategory::Cascade)
                                    .map_err(SchemaValidationError::ConceptRead)?;
                            if type_annotation.is_none() {
                                let type_has_instances =
                                    Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
                                        .map_err(SchemaValidationError::ConceptRead)?;
                                if type_has_instances {
                                    return Err(SchemaValidationError::ChangingRelationSupertypeLeadsToImplicitCascadeAnnotationAcquisitionAndUnexpectedDataLoss(
                                        get_label_or_schema_err(snapshot, relation_type.clone())?,
                                        get_label_or_schema_err(snapshot, new_supertype.clone())?,
                                        get_label_or_schema_err(snapshot, type_.clone())?,
                                    ));
                                }
                            }
                            Ok(())
                        });
                        Ok(())
                    }
                }
            }
            Some(_) => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_type_does_not_lose_instances_with_independent_annotation_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        new_supertype: Option<AttributeType<'static>>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_annotations_source =
            type_get_source_of_annotation_category(snapshot, attribute_type.clone(), AnnotationCategory::Independent)
                .map_err(SchemaValidationError::ConceptRead)?;
        let supertype_annotation = match &new_supertype {
            None => None,
            Some(new_supertype) => {
                type_get_annotation_by_category(snapshot, new_supertype.clone(), AnnotationCategory::Independent)
                    .map_err(SchemaValidationError::ConceptRead)?
            }
        };

        match (subtype_annotations_source, supertype_annotation) {
            (Some(annotation_source), None) => {
                if annotation_source != attribute_type {
                    let lost_source = annotation_source;
                    for_type_and_subtypes_transitive!(snapshot, attribute_type, |type_: AttributeType<'static>| {
                        let annotation_source = type_get_source_of_annotation_category(
                            snapshot,
                            type_.clone(),
                            AnnotationCategory::Independent,
                        )
                        .map_err(SchemaValidationError::ConceptRead)?;
                        match annotation_source {
                            None => {
                                debug_assert!(
                                    false,
                                    "This annotation should be inherited by all the subtypes of the attribute type"
                                );
                                Ok(())
                            }
                            Some(annotation_source) => {
                                if lost_source == annotation_source {
                                    let type_has_instances =
                                        Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
                                            .map_err(SchemaValidationError::ConceptRead)?;
                                    if type_has_instances {
                                        return Err(SchemaValidationError::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(
                                            get_label_or_schema_err(snapshot, attribute_type.clone())?,
                                            get_opt_label_or_schema_err(snapshot, new_supertype.clone())?,
                                            get_label_or_schema_err(snapshot, type_.clone())?,
                                        ));
                                    }
                                }
                                Ok(())
                            }
                        }
                    });
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_unset_owns_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        owner: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, owner.clone().into_owned_object_type())
            .map_err(SchemaValidationError::ConceptRead)?;
        let found_owns = all_owns.iter().find(|owns| owns.attribute() == attribute_type);

        match found_owns {
            Some(owns) => {
                if owner == owns.owner() {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedOwns(
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        get_label_or_schema_err(snapshot, owns.owner())?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_plays_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let all_plays =
            TypeReader::get_capabilities::<Plays<'static>>(snapshot, player.clone().into_owned_object_type())
                .map_err(SchemaValidationError::ConceptRead)?;
        let found_plays = all_plays.iter().find(|plays| plays.role() == role_type);

        match found_plays {
            Some(plays) => {
                if player == plays.player() {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedPlays(
                        get_label_or_schema_err(snapshot, role_type)?,
                        get_label_or_schema_err(snapshot, plays.player())?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_annotation_is_not_inherited<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let annotation_source = type_get_source_of_annotation_category(snapshot, type_.clone(), annotation_category)
            .map_err(SchemaValidationError::ConceptRead)?;
        match annotation_source {
            Some(source) => {
                if type_ == source {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedAnnotation(
                        annotation_category,
                        get_label_or_schema_err(snapshot, source)?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_edge_annotation_is_not_inherited<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let annotation_owner = capability_get_owner_of_annotation_category(snapshot, edge.clone(), annotation_category)
            .map_err(SchemaValidationError::ConceptRead)?;
        match annotation_owner {
            Some(owner) => {
                if edge == owner {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedEdgeAnnotation(
                        annotation_category,
                        get_label_or_schema_err(snapshot, edge.object())?,
                        get_label_or_schema_err(snapshot, edge.interface())?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_declared_annotation_is_compatible_with_inherited_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        validate_declared_annotation_is_compatible_with_inherited_annotations(snapshot, type_, annotation_category)
    }

    pub(crate) fn validate_declared_capability_annotation_is_compatible_with_inherited_annotations<CAP>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static>,
    {
        validate_declared_capability_annotation_is_compatible_with_inherited_annotations(
            snapshot,
            edge,
            annotation_category,
        )
    }

    pub(crate) fn validate_inherited_annotation_is_compatible_with_declared_annotations_of_subtypes(
        snapshot: &impl ReadableSnapshot,
        annotation_category: AnnotationCategory,
        type_: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        if !annotation_category.inheritable() {
            return Ok(());
        }

        TypeReader::get_subtypes_transitive(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|subtype| {
                let subtype_annotations = TypeReader::get_type_annotations_declared(snapshot, subtype.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;
                for existing_annotation in subtype_annotations {
                    let existing_annotation_category = existing_annotation.clone().into().category();
                    if !existing_annotation_category.declarable_below(annotation_category) {
                        return Err(SchemaValidationError::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(
                            annotation_category,
                            existing_annotation_category,
                            get_label_or_schema_err(snapshot, type_.clone())?,
                        ));
                    }
                }
                Ok(())
            })?;

        Ok(())
    }

    pub(crate) fn validate_inherited_annotation_is_compatible_with_declared_annotations_of_overriding_capabilities<
        CAP: Capability<'static>,
    >(
        snapshot: &impl ReadableSnapshot,
        annotation_category: AnnotationCategory,
        capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        if !annotation_category.inheritable() {
            return Ok(());
        }

        TypeReader::get_overriding_capabilities_transitive(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .try_for_each(|overriding_capability| {
                let overriding_annotations =
                    TypeReader::get_type_edge_annotations_declared(snapshot, overriding_capability.clone())
                        .map_err(SchemaValidationError::ConceptRead)?;
                for existing_annotation in overriding_annotations {
                    let existing_annotation_category = existing_annotation.clone().into().category();
                    if !existing_annotation_category.declarable_below(annotation_category) {
                        return Err(
                            SchemaValidationError::DeclaredCapabilityAnnotationIsNotCompatibleWithInheritedAnnotation(
                                annotation_category,
                                existing_annotation_category,
                                get_label_or_schema_err(snapshot, capability.object())?,
                                get_label_or_schema_err(snapshot, capability.interface())?,
                            ),
                        );
                    }
                }
                Ok(())
            })?;

        Ok(())
    }

    pub(crate) fn validate_declared_annotation_is_compatible_with_declared_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let existing_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for existing_annotation in existing_annotations {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label_or_schema_err(snapshot, type_)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn filter_type_annotation_constraints_by_not_declared<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotations: &mut HashSet<Annotation>,
    ) -> Result<(), ConceptReadError> {
        let declared_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())?;
        annotations.retain(|annotation| !declared_annotations.contains(&annotation.clone().try_into().unwrap()));
        Ok(())
    }

    pub(crate) fn filter_capability_annotation_constraints_by_not_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotations: &mut HashSet<Annotation>,
    ) -> Result<(), ConceptReadError> {
        let declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())?;
        annotations.retain(|annotation| !declared_annotations.contains(&annotation.clone().try_into().unwrap()));
        Ok(())
    }

    fn get_annotation_constraint_violated_by_entity_instances<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType<'a>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let is_abstract = Constraint::compute_abstract(annotations);
        debug_assert!(is_abstract.is_some(), "At least one constraint should exist otherwise we don't need to iterate");

        let mut entity_iterator = thing_manager.get_entities_in(snapshot, entity_type.clone().into_owned());
        if let Some(_) = entity_iterator.next().transpose()? {
            if is_abstract.is_some() {
                return Ok(Some(AnnotationCategory::Abstract));
            }
        }

        Ok(None)
    }

    fn get_annotation_constraint_violated_by_relation_instances<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'a>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let is_abstract = Constraint::compute_abstract(annotations);
        debug_assert!(is_abstract.is_some(), "At least one constraint should exist otherwise we don't need to iterate");

        let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
        if let Some(_) = relation_iterator.next().transpose()? {
            if is_abstract.is_some() {
                return Ok(Some(AnnotationCategory::Abstract));
            }
        }

        Ok(None)
    }

    fn get_annotation_constraint_violated_by_attribute_instances<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'a>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let is_abstract = Constraint::compute_abstract(annotations);
        let regex = Constraint::compute_regex(annotations);
        let range = Constraint::compute_range(annotations);
        let values = Constraint::compute_values(annotations);
        debug_assert!(
            is_abstract.is_some() || regex.is_some() || range.is_some() || values.is_some(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        let mut attribute_iterator = thing_manager.get_attributes_in(snapshot, attribute_type.clone().into_owned())?;
        while let Some(attribute) = attribute_iterator.next() {
            let mut attribute = attribute?;

            if is_abstract.is_some() {
                return Ok(Some(AnnotationCategory::Abstract));
            }

            let value = attribute.get_value(snapshot, thing_manager)?;

            if let Some(regex) = &regex {
                match &value {
                    Value::String(string_value) => {
                        if !regex.value_valid(&string_value) {
                            return Ok(Some(AnnotationCategory::Regex));
                        }
                    }
                    _ => {
                        return Err(ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                            get_label_or_concept_read_err(snapshot, attribute_type)?,
                            value.value_type(),
                            Annotation::Regex(regex.clone()),
                        ))
                    }
                }
            }

            if let Some(range) = &range {
                if !range.value_valid(value.clone()) {
                    return Ok(Some(AnnotationCategory::Range));
                }
            }

            if let Some(values) = &values {
                if !values.value_valid(value) {
                    return Ok(Some(AnnotationCategory::Values));
                }
            }
        }

        Ok(None)
    }

    fn get_annotation_constraint_violated_by_role_instances<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType<'a>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let is_abstract = Constraint::compute_abstract(annotations);
        debug_assert!(is_abstract.is_some(), "At least one constraint should exist otherwise we don't need to iterate");

        let role_type = role_type.clone().into_owned();
        let all_relates = TypeReader::get_role_type_relates(snapshot, role_type.clone())?;
        for relates in all_relates {
            let relation_type = relates.relation();
            let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.into_owned());
            while let Some(relation) = relation_iterator.next().transpose()? {
                let mut role_player_iterator =
                    thing_manager.get_role_players_role(snapshot, relation, role_type.clone());
                if let Some(_) = role_player_iterator.next().transpose()? {
                    if is_abstract.is_some() {
                        return Ok(Some(AnnotationCategory::Abstract));
                    }
                }
            }
        }

        Ok(None)
    }

    fn check_operation_time_cardinality_constraint(
        cardinality_constraint: Option<AnnotationCardinality>,
        real_cardinality: u64,
        only_abstract_types: bool,
    ) -> bool {
        match cardinality_constraint {
            None => true,
            Some(cardinality) => match cardinality.value_valid(real_cardinality) {
                true => true,
                // We check that all capabilities of abstract types are overridden only on commit time,
                // so card of only abstract interface types can be skipped here.
                // If this abstract type (where the cardinality comes from) is not overridden,
                // it will be a commit time violation.
                // If this abstract type is overridden, the concrete types will inherit the updated
                // cardinality and revalidate it against themselves in this function if needed.
                false => real_cardinality == 0 && only_abstract_types,
            },
        }
    }

    fn are_all_abstract<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        types: impl Iterator<Item = &'a impl TypeAPI<'a>>,
    ) -> Result<bool, ConceptReadError> {
        let is_abstracts = types.map(|type_| type_.is_abstract(snapshot, type_manager));
        for is_abstract in is_abstracts {
            match is_abstract {
                Err(e) => return Err(e),
                Ok(false) => return Ok(false),
                Ok(true) => continue,
            }
        }
        Ok(true)
    }

    fn get_annotation_constraint_violated_by_instances_of_owns<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        object_types: &HashSet<ObjectType<'a>>,
        attribute_types: &HashSet<AttributeType<'a>>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let distinct = Constraint::compute_distinct(annotations, None);
        let is_key = Constraint::compute_key(annotations).is_some();
        let unique = Constraint::compute_unique(annotations);
        let cardinality = Constraint::compute_cardinality(annotations, None);
        let regex = Constraint::compute_regex(annotations);
        let range = Constraint::compute_range(annotations);
        let values = Constraint::compute_values(annotations);
        debug_assert!(
            cardinality.is_some()
                || distinct.is_some()
                || unique.is_some()
                || regex.is_some()
                || range.is_some()
                || values.is_some(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        let only_abstract_types = Self::are_all_abstract(snapshot, type_manager, attribute_types.iter())?;

        // TODO #7138: It is EXCEPTIONALLY memory-greedy and should be optimized before a non-alpha release!
        let mut unique_values = HashSet::new();

        for object_type in object_types {
            let mut object_iterator = thing_manager.get_objects_in(snapshot, object_type.clone().into_owned());
            while let Some(object) = object_iterator.next().transpose()? {
                let mut real_cardinality = 0;

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut has_attribute_iterator = object.get_has_unordered(snapshot, thing_manager);
                while let Some(attribute) = has_attribute_iterator.next() {
                    let (mut attribute, count) = attribute?;
                    let attribute_type = attribute.type_();
                    if !attribute_types.contains(&attribute_type) {
                        continue;
                    }

                    real_cardinality += count;

                    if distinct.is_some() {
                        if count > 1 {
                            return Ok(Some(AnnotationCategory::Distinct));
                        }
                    }

                    let value = attribute.get_value(snapshot, thing_manager)?;

                    if unique.is_some() {
                        let new = unique_values.insert(value.clone().into_owned());
                        if !new {
                            return Ok(Some(if is_key { AnnotationCategory::Key } else { AnnotationCategory::Unique }));
                        }
                    }

                    if let Some(regex) = &regex {
                        match &value {
                            Value::String(string_value) => {
                                if !regex.value_valid(&string_value) {
                                    return Ok(Some(AnnotationCategory::Regex));
                                }
                            }
                            _ => {
                                return Err(
                                    ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(
                                        get_label_or_concept_read_err(snapshot, attribute_type)?,
                                        value.value_type(),
                                        Annotation::Regex(regex.clone()),
                                    ),
                                );
                            }
                        }
                    }

                    if let Some(range) = &range {
                        if !range.value_valid(value.clone()) {
                            return Ok(Some(AnnotationCategory::Range));
                        }
                    }

                    if let Some(values) = &values {
                        if !values.value_valid(value) {
                            return Ok(Some(AnnotationCategory::Values));
                        }
                    }
                }

                if !Self::check_operation_time_cardinality_constraint(
                    cardinality.clone(),
                    real_cardinality,
                    only_abstract_types,
                ) {
                    return Ok(Some(if is_key { AnnotationCategory::Key } else { AnnotationCategory::Cardinality }));
                }
            }
        }

        Ok(None)
    }

    fn get_annotation_constraint_violated_by_instances_of_plays<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        object_types: &HashSet<ObjectType<'a>>,
        role_types: &HashSet<RoleType<'a>>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let cardinality = Constraint::compute_cardinality(annotations, None);
        debug_assert!(cardinality.is_some(), "At least one constraint should exist otherwise we don't need to iterate");

        let only_abstract_types = Self::are_all_abstract(snapshot, type_manager, role_types.iter())?;

        for object_type in object_types {
            let mut object_iterator = thing_manager.get_objects_in(snapshot, object_type.clone().into_owned());
            while let Some(object) = object_iterator.next().transpose()? {
                let mut real_cardinality = 0;

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut relations_iterator = object.get_relations_roles(snapshot, thing_manager);
                while let Some(relation) = relations_iterator.next() {
                    let (_, role_type, count) = relation?;
                    if !role_types.contains(&role_type) {
                        continue;
                    }

                    real_cardinality += count;
                }

                if !Self::check_operation_time_cardinality_constraint(
                    cardinality.clone(),
                    real_cardinality,
                    only_abstract_types,
                ) {
                    return Ok(Some(AnnotationCategory::Cardinality));
                }
            }
        }

        Ok(None)
    }

    fn get_annotation_constraint_violated_by_instances_of_relates<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        relation_types: &HashSet<RelationType<'a>>,
        role_types: &HashSet<RoleType<'a>>,
        annotations: &HashSet<Annotation>,
    ) -> Result<Option<AnnotationCategory>, ConceptReadError> {
        if annotations.is_empty() {
            return Ok(None);
        }

        let distinct = Constraint::compute_distinct(annotations, None);
        let cardinality = Constraint::compute_cardinality(annotations, None);
        debug_assert!(
            cardinality.is_some() || distinct.is_some(),
            "At least one constraint should exist otherwise we don't need to iterate"
        );

        let only_abstract_types = Self::are_all_abstract(snapshot, type_manager, role_types.iter())?;

        for relation_type in relation_types {
            let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
            while let Some(relation) = relation_iterator.next().transpose()? {
                let mut real_cardinality = 0;

                // We assume that it's cheaper to open an iterator once and skip all the
                // non-interesting interfaces rather creating multiple iterators
                let mut role_players_iterator = relation.get_players(snapshot, thing_manager);

                while let Some(role_players) = role_players_iterator.next() {
                    let (role_player, count) = role_players?;
                    let role_type = role_player.role_type();
                    if !role_types.contains(&role_type) {
                        continue;
                    }

                    real_cardinality += count;

                    if distinct.is_some() {
                        if count > 1 {
                            return Ok(Some(AnnotationCategory::Distinct));
                        }
                    }
                }

                if !Self::check_operation_time_cardinality_constraint(
                    cardinality.clone(),
                    real_cardinality,
                    only_abstract_types,
                ) {
                    return Ok(Some(AnnotationCategory::Cardinality));
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn validate_declared_capability_annotation_is_compatible_with_declared_annotations<CAP>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static>,
    {
        let existing_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for existing_annotation in existing_annotations {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                let interface = edge.interface();
                return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label_or_schema_err(snapshot, interface)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_unique_annotation_transitive(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), SchemaValidationError> {
        for_capability_and_overriding_capabilities_transitive!(snapshot, owns, |current_owns: Owns<'static>| {
            let value_type = TypeReader::get_value_type_without_source(snapshot, current_owns.attribute())
                .map_err(SchemaValidationError::ConceptRead)?;
            Self::validate_owns_value_type_compatible_with_unique_annotation(snapshot, current_owns, value_type)
        });
        Ok(())
    }

    fn validate_owns_value_type_compatible_with_unique_annotation(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if Self::is_owns_value_type_keyable(value_type.clone()) {
            Ok(())
        } else {
            let owner = owns.owner();
            let attribute_type = owns.attribute();
            Err(SchemaValidationError::ValueTypeIsNotKeyableForUniqueAnnotation(
                get_label_or_schema_err(snapshot, owner)?,
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_owns_value_type_compatible_with_key_annotation_transitive(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), SchemaValidationError> {
        for_capability_and_overriding_capabilities_transitive!(snapshot, owns, |current_owns: Owns<'static>| {
            let value_type = TypeReader::get_value_type_without_source(snapshot, current_owns.attribute())
                .map_err(SchemaValidationError::ConceptRead)?;
            Self::validate_owns_value_type_compatible_with_key_annotation(snapshot, current_owns, value_type)
        });
        Ok(())
    }

    fn validate_owns_value_type_compatible_with_key_annotation(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if Self::is_owns_value_type_keyable(value_type.clone()) {
            Ok(())
        } else {
            let owner = owns.owner();
            let attribute_type = owns.attribute();
            Err(SchemaValidationError::ValueTypeIsNotKeyableForKeyAnnotation(
                get_label_or_schema_err(snapshot, owner)?,
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_attribute_type_value_type_compatible_with_annotations_transitive(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, attribute_type, |type_: AttributeType<'static>| {
            let annotations = TypeReader::get_type_annotations(snapshot, type_.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            Self::validate_attribute_type_value_type_compatible_with_annotations_and_arguments(
                snapshot,
                type_,
                value_type.clone(),
                annotations.into_keys().collect(),
            )
        });
        Ok(())
    }

    fn validate_attribute_type_value_type_compatible_with_annotations_and_arguments(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
        annotations: HashSet<AttributeTypeAnnotation>,
    ) -> Result<(), SchemaValidationError> {
        for annotation in annotations {
            match annotation {
                AttributeTypeAnnotation::Regex(regex) => {
                    Self::validate_annotation_regex_compatible_value_type(
                        snapshot,
                        attribute_type.clone(),
                        value_type.clone(),
                    )?;
                    Self::validate_regex_arguments(regex)?
                }
                AttributeTypeAnnotation::Range(range) => {
                    Self::validate_annotation_range_compatible_value_type(
                        snapshot,
                        attribute_type.clone(),
                        value_type.clone(),
                    )?;
                    Self::validate_range_arguments(range, value_type.clone())?
                }
                AttributeTypeAnnotation::Values(values) => {
                    Self::validate_annotation_values_compatible_value_type(
                        snapshot,
                        attribute_type.clone(),
                        value_type.clone(),
                    )?;
                    Self::validate_values_arguments(values, value_type.clone())?
                }
                | AttributeTypeAnnotation::Abstract(_) | AttributeTypeAnnotation::Independent(_) => {}
            }
        }

        Ok(())
    }

    pub(crate) fn validate_value_type_compatible_with_all_owns_annotations_transitive(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, attribute_type, |type_: AttributeType<'static>| {
            Self::validate_value_type_compatible_with_all_owns_annotations(snapshot, type_, value_type.clone())
        });
        Ok(())
    }

    fn validate_value_type_compatible_with_all_owns_annotations(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns = TypeReader::get_capabilities_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, owns) in all_owns {
            Self::validate_owns_value_type_compatible_with_annotations(snapshot, owns, value_type.clone())?
        }
        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_overridden_owns_annotations_transitive(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        overridden_owns: Owns<'static>,
    ) -> Result<(), SchemaValidationError> {
        for_capability_and_overriding_capabilities_transitive!(snapshot, owns, |current_owns: Owns<'static>| {
            let value_type = TypeReader::get_value_type_without_source(snapshot, current_owns.attribute())
                .map_err(SchemaValidationError::ConceptRead)?;
            Self::validate_owns_value_type_compatible_with_annotations(snapshot, overridden_owns.clone(), value_type)
        });
        Ok(())
    }

    pub(crate) fn validate_object_type_subtypes_do_not_override_new_overridden_capability<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        overridden_capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        let subtypes = TypeReader::get_subtypes_transitive(snapshot, capability.object())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype in subtypes.into_iter() {
            let subtype_overrides = TypeReader::get_object_capabilities_overrides_declared::<CAP>(snapshot, subtype)
                .map_err(SchemaValidationError::ConceptRead)?;
            let overriding = subtype_overrides
                .into_iter()
                .find(|(_, overridden)| overridden == &overridden_capability)
                .map(|(overriding, _)| overriding.clone());
            if let Some(overriding) = overriding {
                return Err(SchemaValidationError::CannotOverrideCapabilityAsItIsOverriddenForSubtype(
                    CAP::KIND,
                    get_label_or_schema_err(snapshot, capability.object())?,
                    get_label_or_schema_err(snapshot, capability.interface())?,
                    get_label_or_schema_err(snapshot, overridden_capability.object())?,
                    get_label_or_schema_err(snapshot, overridden_capability.interface())?,
                    get_label_or_schema_err(snapshot, overriding.object())?,
                    get_label_or_schema_err(snapshot, overriding.interface())?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_annotations(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let annotations = TypeReader::get_type_edge_annotations(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for (annotation, _) in annotations {
            match annotation {
                OwnsAnnotation::Unique(_) => Self::validate_owns_value_type_compatible_with_unique_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                OwnsAnnotation::Key(_) => Self::validate_owns_value_type_compatible_with_key_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                OwnsAnnotation::Regex(regex) => {
                    Self::validate_annotation_regex_compatible_value_type(
                        snapshot,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_regex_arguments(regex)?
                }
                OwnsAnnotation::Range(range) => {
                    Self::validate_annotation_range_compatible_value_type(
                        snapshot,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_range_arguments(range, value_type.clone())?
                }
                OwnsAnnotation::Values(values) => {
                    Self::validate_annotation_values_compatible_value_type(
                        snapshot,
                        owns.attribute(),
                        value_type.clone(),
                    )?;
                    Self::validate_values_arguments(values, value_type.clone())?
                }
                | OwnsAnnotation::Distinct(_) | OwnsAnnotation::Cardinality(_) => {}
            }
        }
        Ok(())
    }

    pub(crate) fn is_owns_value_type_keyable(value_type_opt: Option<ValueType>) -> bool {
        match value_type_opt {
            Some(value_type) => value_type.keyable(),
            None => true,
        }
    }

    pub(crate) fn validate_no_instances_to_delete<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'a>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(snapshot, type_)?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_no_instances_to_change_value_type<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        for_type_and_subtypes_transitive!(snapshot, attribute_type, |type_: AttributeType<'static>| {
            let has_instances = Self::has_instances_of_type(snapshot, thing_manager, attribute_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

            if has_instances {
                Err(SchemaValidationError::CannotChangeValueTypeWithExistingInstances(get_label_or_schema_err(
                    snapshot, type_,
                )?))
            } else {
                Ok(())
            }
        });
        Ok(())
    }

    fn validate_no_instances_to_unset_value_type<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'a>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_type(snapshot, thing_manager, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotUnsetValueTypeWithExistingInstances(get_label_or_schema_err(
                snapshot,
                attribute_type,
            )?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_new_abstract_annotation_compatible_with_type_and_subtypes_instances<'a, T: KindAPI<'a>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        let annotation = Annotation::Abstract(AnnotationAbstract);
        match T::ROOT_KIND {
            Kind::Entity => {
                let entity_type = EntityType::new(type_.vertex().into_owned());
                Self::validate_new_annotation_compatible_with_entity_type_and_subtypes_instances(
                    snapshot,
                    type_manager,
                    thing_manager,
                    entity_type,
                    annotation,
                )
            }
            Kind::Attribute => {
                let attribute_type = AttributeType::new(type_.vertex().into_owned());
                Self::validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances(
                    snapshot,
                    type_manager,
                    thing_manager,
                    attribute_type,
                    annotation,
                )
            }
            Kind::Relation => {
                let relation_type = RelationType::new(type_.vertex().into_owned());
                Self::validate_new_annotation_compatible_with_relation_type_and_subtypes_instances(
                    snapshot,
                    type_manager,
                    thing_manager,
                    relation_type,
                    annotation,
                )
            }
            Kind::Role => {
                let role_type = RoleType::new(type_.vertex().into_owned());
                Self::validate_new_annotation_compatible_with_role_type_and_subtypes_instances(
                    snapshot,
                    type_manager,
                    thing_manager,
                    role_type,
                    annotation,
                )
            }
        }
    }

    pub(crate) fn validate_changed_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change<
        'a,
        T: KindAPI<'a>,
    >(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        type_: T,
        new_supertype: T,
    ) -> Result<(), SchemaValidationError> {
        match T::ROOT_KIND {
            Kind::Entity => {
                let entity_type = EntityType::new(type_.vertex().into_owned());
                let entity_supertype = EntityType::new(new_supertype.vertex().into_owned());
                Self::validate_updated_annotations_compatible_with_entity_type_and_subtypes_instances_on_supertype_change(snapshot, type_manager, thing_manager, entity_type, entity_supertype)
            }
            Kind::Attribute => {
                let attribute_type = AttributeType::new(type_.vertex().into_owned());
                let attribute_supertype = AttributeType::new(new_supertype.vertex().into_owned());
                Self::validate_updated_annotations_compatible_with_attribute_type_and_subtypes_instances_on_supertype_change(snapshot, type_manager, thing_manager, attribute_type, attribute_supertype)
            }
            Kind::Relation => {
                let relation_type = RelationType::new(type_.vertex().into_owned());
                let relation_supertype = RelationType::new(new_supertype.vertex().into_owned());
                Self::validate_updated_annotations_compatible_with_relation_type_and_subtypes_instances_on_supertype_change(snapshot, type_manager, thing_manager, relation_type, relation_supertype)
            }
            Kind::Role => {
                let role_type = RoleType::new(type_.vertex().into_owned());
                let role_supertype = RoleType::new(new_supertype.vertex().into_owned());
                Self::validate_updated_annotations_compatible_with_role_type_and_subtypes_instances_on_supertype_change(
                    snapshot,
                    type_manager,
                    thing_manager,
                    role_type,
                    role_supertype,
                )
            }
        }
    }

    pub(crate) fn validate_no_role_instances_to_set_ordering(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_type(snapshot, thing_manager, role_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotSetRoleOrderingWithExistingInstances(get_label_or_schema_err(
                snapshot, role_type,
            )?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_no_owns_instances_to_set_ordering(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_owns(snapshot, thing_manager, owns.owner(), owns.attribute())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotSetOwnsOrderingWithExistingInstances(
                get_label_or_schema_err(snapshot, owns.owner())?,
                get_label_or_schema_err(snapshot, owns.attribute())?,
            ))
        } else {
            Ok(())
        }
    }

    fn has_instances_of_type<'a, T: KindAPI<'a>>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: T,
    ) -> Result<bool, ConceptReadError> {
        match T::ROOT_KIND {
            Kind::Entity => {
                let entity_type = EntityType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone().into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Attribute => {
                let attribute_type = AttributeType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_attributes_in(snapshot, attribute_type.clone().into_owned())?;
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Relation => {
                let relation_type = RelationType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Role => {
                let role_type = RoleType::new(type_.vertex().into_owned());
                let relation_type =
                    TypeReader::get_role_type_relates_declared(snapshot, role_type.clone().into_owned())?.relation();
                Self::has_instances_of_relates(snapshot, thing_manager, relation_type, role_type)
            }
        }
    }

    fn has_instances_of_owns<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner_type: ObjectType<'a>,
        attribute_type: AttributeType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;

        let mut owner_iterator = thing_manager.get_objects_in(snapshot, owner_type.clone().into_owned());
        while let Some(instance) = owner_iterator.next().transpose()? {
            let mut iterator =
                instance.get_has_type_unordered(snapshot, thing_manager, attribute_type.clone().into_owned())?;

            if iterator.next().is_some() {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_plays<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player_type: ObjectType<'a>,
        role_type: RoleType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;

        let mut player_iterator = thing_manager.get_objects_in(snapshot, player_type.clone().into_owned());
        while let Some(instance) = player_iterator.next().transpose()? {
            let mut iterator = instance.get_relations_by_role(snapshot, thing_manager, role_type.clone().into_owned());

            if let Some(_) = iterator.next().transpose()? {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_relates<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'a>,
        role_type: RoleType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;

        let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
        while let Some(instance) = relation_iterator.next().transpose()? {
            let mut iterator = instance.get_players_role_type(snapshot, thing_manager, role_type.clone().into_owned());

            if let Some(_) = iterator.next().transpose()? {
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn get_lost_capabilities_if_supertype_is_changed<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: CAP::ObjectType,
        new_supertype: Option<CAP::ObjectType>,
    ) -> Result<HashSet<CAP>, SchemaValidationError> {
        let new_inherited_capabilities = match new_supertype {
            None => HashSet::new(),
            Some(new_supertype) => TypeReader::get_capabilities::<CAP>(snapshot, new_supertype.clone())
                .map_err(SchemaValidationError::ConceptRead)?,
        };

        let current_capabilities =
            TypeReader::get_capabilities::<CAP>(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let current_inherited_capabilities =
            current_capabilities.iter().filter(|capability| capability.object() != type_);

        Ok(current_inherited_capabilities
            .filter(|capability| {
                !new_inherited_capabilities
                    .iter()
                    .any(|inherited_capability| inherited_capability.interface() == capability.interface())
            })
            .map(|capability| capability.clone())
            .collect())
    }

    fn get_updated_capabilities_with_annotations_if_supertype_is_changed<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: CAP::ObjectType,
        new_supertype: CAP::ObjectType,
    ) -> Result<HashMap<CAP, HashSet<Annotation>>, SchemaValidationError> {
        // It is expected that preserved capabilities have the same overrides and, thus, the same annotations,
        // so we check only the newly inherited capabilities.

        let new_inherited_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, new_supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        let old_capabilities =
            TypeReader::get_capabilities::<CAP>(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let old_inherited_capabilities = old_capabilities.iter().filter(|capability| capability.object() != type_);

        let mut updated_annotations_from_inheritance = HashMap::new();

        for old_capability in old_inherited_capabilities {
            if let Some(new_capability) =
                new_inherited_capabilities.iter().find(|cap| cap.interface() == old_capability.interface())
            {
                let old_annotations = TypeReader::get_type_edge_annotations(snapshot, old_capability.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;
                let new_annotations = TypeReader::get_type_edge_annotations(snapshot, new_capability.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

                let updated_annotations = new_annotations
                    .keys()
                    .filter(|new_annotation| !old_annotations.contains_key(*new_annotation))
                    .map(|new_annotation| new_annotation.clone().into())
                    .collect::<HashSet<Annotation>>();

                updated_annotations_from_inheritance.insert(old_capability.clone(), updated_annotations);
            }
        }

        for new_capability in new_inherited_capabilities.into_iter() {
            if !old_capabilities.iter().any(|cap| cap.interface() == new_capability.interface()) {
                let annotations = TypeReader::get_type_edge_annotations(snapshot, new_capability.clone())
                    .map_err(SchemaValidationError::ConceptRead)?
                    .keys()
                    .map(|annotation| annotation.clone().try_into().unwrap())
                    .collect();
                updated_annotations_from_inheritance.insert(new_capability, annotations);
            }
        }

        Ok(updated_annotations_from_inheritance)
    }

    fn get_updated_annotations_if_capability_changes_override<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        capability_override: Option<CAP>,
    ) -> Result<HashSet<Annotation>, SchemaValidationError> {
        let current_override = TypeReader::get_capability_override(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        if current_override == capability_override {
            return Ok(HashSet::new());
        }

        let declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let old_annotations = TypeReader::get_type_edge_annotations(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let old_inherited_annotations = old_annotations
            .into_iter()
            .filter(|(_, source)| &capability != source)
            .map(|(annotation, _)| annotation.clone().into())
            .collect::<HashSet<Annotation>>();
        let new_annotations = if let Some(capability_override) = &capability_override {
            TypeReader::get_type_edge_annotations(snapshot, capability_override.clone())
                .map_err(SchemaValidationError::ConceptRead)?
        } else {
            HashMap::new()
        };

        // Cardinality can be updated even if annotation does not exist
        let new_cardinality = capability
            .get_cardinality_declared(snapshot, type_manager)
            .map_err(SchemaValidationError::ConceptRead)?
            .unwrap_or(
                type_manager
                    .get_cardinality_inherited_or_default(snapshot, capability.clone(), Some(capability_override))
                    .map_err(SchemaValidationError::ConceptRead)?,
            );

        // We expect that our declared annotations correctly narrow new inherited annotations
        // (should be checked on the schema level), so we don't consider them updated
        Ok(new_annotations
            .keys()
            .chain(once(&Annotation::Cardinality(new_cardinality).try_into().unwrap()))
            .map(|new_annotation| new_annotation.clone().into())
            .filter(|new_annotation| {
                let new_annotation_category = new_annotation.category();
                !declared_annotations
                    .iter()
                    .map(|annotation| annotation.clone().into().category())
                    .contains(&new_annotation_category)
                    && !old_inherited_annotations.contains(new_annotation)
                    && new_annotation_category.inheritable()
            })
            .map(|new_annotation| new_annotation.clone())
            .collect::<HashSet<Annotation>>())
    }

    fn get_updated_annotations_if_type_changes_supertype<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        new_supertype: T,
    ) -> Result<HashSet<Annotation>, SchemaValidationError> {
        let current_supertype =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        if let Some(current_supertype) = current_supertype {
            if current_supertype == new_supertype {
                return Ok(HashSet::new());
            }
        }

        let declared_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let old_annotations =
            TypeReader::get_type_annotations(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let old_inherited_annotations = old_annotations
            .into_iter()
            .filter(|(_, source)| &type_ != source)
            .map(|(annotation, _)| annotation.clone().into())
            .collect::<HashSet<Annotation>>();
        let new_annotations = TypeReader::get_type_annotations(snapshot, new_supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        // We expect that our declared annotations correctly narrow new inherited annotations
        // (should be checked on the schema level), so we don't consider them updated
        Ok(new_annotations
            .keys()
            .map(|new_annotation| new_annotation.clone().into())
            .filter(|new_annotation| {
                !declared_annotations
                    .iter()
                    .map(|annotation| annotation.clone().into().category())
                    .contains(&new_annotation.category())
                    && !old_inherited_annotations.contains(&new_annotation)
                    && new_annotation.category().inheritable()
            })
            .collect::<HashSet<Annotation>>())
    }

    pub(crate) fn validate_deleted_struct_is_not_used_in_schema(
        snapshot: &impl ReadableSnapshot,
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), SchemaValidationError> {
        let struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        let usages_in_attribute_types = TypeReader::get_struct_definition_usages_in_attribute_types(snapshot)
            .map_err(SchemaValidationError::ConceptRead)?;
        if let Some(owners) = usages_in_attribute_types.get(definition_key) {
            return Err(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(
                struct_definition.name,
                owners.len(),
            ));
        }

        let usages_in_struct_definition_fields =
            TypeReader::get_struct_definition_usages_in_struct_definitions(snapshot)
                .map_err(SchemaValidationError::ConceptRead)?;
        if let Some(owners) = usages_in_struct_definition_fields.get(definition_key) {
            return Err(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(
                struct_definition.name,
                owners.len(),
            ));
        }

        Ok(())
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
        validate_no_instances_to_unset_owns,
        CapabilityKind::Owns,
        Owns,
        ObjectType,
        AttributeType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_owns
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_instances_to_unset_plays,
        CapabilityKind::Plays,
        Plays,
        ObjectType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_plays
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_instances_to_unset_relates,
        CapabilityKind::Relates,
        Relates,
        RelationType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_relates
    );

    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_owns,
        CapabilityKind::Owns,
        ObjectType,
        AttributeType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_owns
    );
    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_plays,
        CapabilityKind::Plays,
        ObjectType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_plays
    );
    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_relates,
        CapabilityKind::Relates,
        RelationType,
        RoleType,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_relates
    );

    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Owns,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Owns<'static>>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_owns
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Plays,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Plays<'static>>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_plays
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Relates,
        RelationType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Relates<'static>>,
        Self::type_or_subtype_without_declared_capability_that_has_instances_of_relates
    );

    capability_or_its_overriding_capability_with_violated_new_annotation_constraints!(
        get_owns_or_its_overriding_owns_with_violated_new_annotation_constraints,
        Owns,
        ObjectType,
        AttributeType,
        Self::get_annotation_constraint_violated_by_instances_of_owns
    );
    capability_or_its_overriding_capability_with_violated_new_annotation_constraints!(
        get_plays_or_its_overriding_plays_with_violated_new_annotation_constraints,
        Plays,
        ObjectType,
        RoleType,
        Self::get_annotation_constraint_violated_by_instances_of_plays
    );
    capability_or_its_overriding_capability_with_violated_new_annotation_constraints!(
        get_relates_or_its_overriding_relates_with_violated_new_annotation_constraints,
        Relates,
        RelationType,
        RoleType,
        Self::get_annotation_constraint_violated_by_instances_of_relates
    );

    new_acquired_capability_instances_validation!(
        validate_new_acquired_owns_compatible_with_instances,
        CapabilityKind::Owns,
        Owns,
        Self::get_owns_or_its_overriding_owns_with_violated_new_annotation_constraints
    );
    new_acquired_capability_instances_validation!(
        validate_new_acquired_plays_compatible_with_instances,
        CapabilityKind::Plays,
        Plays,
        Self::get_plays_or_its_overriding_plays_with_violated_new_annotation_constraints
    );
    new_acquired_capability_instances_validation!(
        validate_new_acquired_relates_compatible_with_instances,
        CapabilityKind::Relates,
        Relates,
        Self::get_relates_or_its_overriding_relates_with_violated_new_annotation_constraints
    );

    new_annotation_compatible_with_capability_and_overriding_capabilities_instances_validation!(
        validate_new_annotation_compatible_with_owns_and_overriding_owns_instances,
        CapabilityKind::Owns,
        Owns,
        Self::get_owns_or_its_overriding_owns_with_violated_new_annotation_constraints
    );
    new_annotation_compatible_with_capability_and_overriding_capabilities_instances_validation!(
        validate_new_annotation_compatible_with_plays_and_overriding_plays_instances,
        CapabilityKind::Plays,
        Plays,
        Self::get_plays_or_its_overriding_plays_with_violated_new_annotation_constraints
    );
    new_annotation_compatible_with_capability_and_overriding_capabilities_instances_validation!(
        validate_new_annotation_compatible_with_relates_and_overriding_relates_instances,
        CapabilityKind::Relates,
        Relates,
        Self::get_relates_or_its_overriding_relates_with_violated_new_annotation_constraints
    );

    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_override_validation!(
        validate_updated_annotations_compatible_with_owns_and_overriding_owns_instances_on_override,
        CapabilityKind::Owns,
        Owns,
        Self::get_owns_or_its_overriding_owns_with_violated_new_annotation_constraints
    );
    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_override_validation!(
        validate_updated_annotations_compatible_with_plays_and_overriding_plays_instances_on_override,
        CapabilityKind::Plays,
        Plays,
        Self::get_plays_or_its_overriding_plays_with_violated_new_annotation_constraints
    );
    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_override_validation!(
        validate_updated_annotations_compatible_with_relates_and_overriding_relates_instances_on_override,
        CapabilityKind::Relates,
        Relates,
        Self::get_relates_or_its_overriding_relates_with_violated_new_annotation_constraints
    );

    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_supertype_change_validation!(
        validate_changed_annotations_compatible_with_owns_and_overriding_owns_instances_on_supertype_change,
        CapabilityKind::Owns,
        Owns,
        ObjectType,
        Self::get_owns_or_its_overriding_owns_with_violated_new_annotation_constraints
    );
    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_supertype_change_validation!(
        validate_changed_annotations_compatible_with_plays_and_overriding_plays_instances_on_supertype_change,
        CapabilityKind::Plays,
        Plays,
        ObjectType,
        Self::get_plays_or_its_overriding_plays_with_violated_new_annotation_constraints
    );
    changed_annotations_compatible_with_capability_and_overriding_capabilities_instances_on_supertype_change_validation!(
        validate_changed_annotations_compatible_with_relates_and_overriding_relates_instances_on_supertype_change,
        CapabilityKind::Relates,
        Relates,
        RelationType,
        Self::get_relates_or_its_overriding_relates_with_violated_new_annotation_constraints
    );

    type_or_its_subtype_with_violated_new_annotation_constraints!(
        get_entity_type_or_its_subtype_with_violated_new_annotation_constraints,
        EntityType,
        Self::get_annotation_constraint_violated_by_entity_instances
    );
    type_or_its_subtype_with_violated_new_annotation_constraints!(
        get_relation_type_or_its_subtype_with_violated_new_annotation_constraints,
        RelationType,
        Self::get_annotation_constraint_violated_by_relation_instances
    );
    type_or_its_subtype_with_violated_new_annotation_constraints!(
        get_attribute_type_or_its_subtype_with_violated_new_annotation_constraints,
        AttributeType,
        Self::get_annotation_constraint_violated_by_attribute_instances
    );
    type_or_its_subtype_with_violated_new_annotation_constraints!(
        get_role_type_or_its_subtype_with_violated_new_annotation_constraints,
        RoleType,
        Self::get_annotation_constraint_violated_by_role_instances
    );

    new_annotation_compatible_with_type_and_subtypes_instances_validation!(
        validate_new_annotation_compatible_with_entity_type_and_subtypes_instances,
        EntityType,
        Self::get_entity_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    new_annotation_compatible_with_type_and_subtypes_instances_validation!(
        validate_new_annotation_compatible_with_relation_type_and_subtypes_instances,
        RelationType,
        Self::get_relation_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    new_annotation_compatible_with_type_and_subtypes_instances_validation!(
        validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances,
        AttributeType,
        Self::get_attribute_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    new_annotation_compatible_with_type_and_subtypes_instances_validation!(
        validate_new_annotation_compatible_with_role_type_and_subtypes_instances,
        RoleType,
        Self::get_role_type_or_its_subtype_with_violated_new_annotation_constraints
    );

    updated_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change_validation!(
        validate_updated_annotations_compatible_with_entity_type_and_subtypes_instances_on_supertype_change,
        EntityType,
        Self::get_entity_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    updated_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change_validation!(
        validate_updated_annotations_compatible_with_relation_type_and_subtypes_instances_on_supertype_change,
        RelationType,
        Self::get_relation_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    updated_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change_validation!(
        validate_updated_annotations_compatible_with_attribute_type_and_subtypes_instances_on_supertype_change,
        AttributeType,
        Self::get_attribute_type_or_its_subtype_with_violated_new_annotation_constraints
    );
    updated_annotations_compatible_with_type_and_subtypes_instances_on_supertype_change_validation!(
        validate_updated_annotations_compatible_with_role_type_and_subtypes_instances_on_supertype_change,
        RoleType,
        Self::get_role_type_or_its_subtype_with_violated_new_annotation_constraints
    );
}
