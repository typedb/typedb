/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::graph::definition::r#struct::StructDefinition;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType,
        constraint::{filter_by_source, Constraint, ConstraintDescription},
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                validation::{
                    get_label_or_concept_read_err, validate_role_name_uniqueness_non_transitive,
                    validate_role_type_supertype_ordering_match, validate_sibling_owns_ordering_match_for_type,
                    validate_type_declared_constraints_narrowing_of_supertype_constraints,
                    validate_type_supertype_abstractness,
                },
                SchemaValidationError,
            },
            TypeManager,
        },
        Capability, KindAPI, ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub struct CommitTimeValidation {}

macro_rules! validate_types {
    ($func_name:ident, $get_all_of_kind:ident, $type_:ident, $func:path) => {
        fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            validation_errors: &mut Vec<Box<SchemaValidationError>>,
        ) -> Result<(), Box<ConceptReadError>> {
            let roots = TypeReader::$get_all_of_kind(snapshot)?.into_iter().filter_map(|type_| {
                match type_.get_supertype(snapshot, type_manager) {
                    Ok(Some(_)) => None,
                    Ok(None) => Some(Ok(type_)),
                    Err(err) => Some(Err(err)),
                }
            });
            for root in roots {
                let root = root?;
                $func(snapshot, type_manager, root.clone(), validation_errors)?;
                for subtype in root.get_subtypes_transitive(snapshot, type_manager)?.into_iter() {
                    $func(snapshot, type_manager, subtype.clone(), validation_errors)?;
                }
            }
            Ok(())
        }
    };
}

macro_rules! produced_errors {
    ($errors:ident, $expr:expr) => {{
        let len_before = $errors.len();
        $expr;
        $errors.len() > len_before
    }};
}

// Some of the checks from this file can duplicate already existing operation time validations
// and never fire up, but they are left here for better safety as the algorithms to check
// the updated schema with the finalised snapshot is much-much-much simpler and more robust
// than the operation time ones.
// It is still a goal to try call as much as possible validations on operation time.
impl CommitTimeValidation {
    pub(crate) fn validate(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Vec<Box<SchemaValidationError>>, Box<ConceptReadError>> {
        let mut errors = Vec::new();
        Self::validate_entity_types(snapshot, type_manager, &mut errors)?;
        Self::validate_relation_types(snapshot, type_manager, &mut errors)?;
        Self::validate_attribute_types(snapshot, type_manager, &mut errors)?;
        Self::validate_struct_definitions(snapshot, type_manager, &mut errors)?;
        Ok(errors)
    }

    validate_types!(validate_entity_types, get_entity_types, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, get_relation_types, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, get_attribute_types, AttributeType, Self::validate_attribute_type);

    fn validate_entity_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: EntityType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_type_constraints(snapshot, type_manager, type_, validation_errors)?;
        Self::validate_object_type(snapshot, type_manager, type_.into_owned_object_type(), validation_errors)?;

        Ok(())
    }

    fn validate_relation_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: RelationType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_type_constraints(snapshot, type_manager, type_, validation_errors)?;
        Self::validate_object_type(snapshot, type_manager, type_.into_owned_object_type(), validation_errors)?;

        Self::validate_relation_type_has_relates(snapshot, type_manager, type_, validation_errors)?;
        Self::validate_relation_type_role_types(snapshot, type_manager, type_, validation_errors)?;

        Self::validate_relates(snapshot, type_manager, type_, validation_errors)?;
        // TODO: Capabilities constraints narrowing checks are currently disabled
        // validate_capabilities_cardinalities_narrowing::<Relates>(
        //     snapshot,
        //     type_manager,
        //     type_.clone(),
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     validation_errors,
        // )?;

        Ok(())
    }

    fn validate_attribute_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: AttributeType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let invalid_value_type = produced_errors!(
            validation_errors,
            Self::validate_attribute_type_value_type(snapshot, type_manager, type_, validation_errors)?
        );

        if !invalid_value_type {
            Self::validate_type_constraints(snapshot, type_manager, type_, validation_errors)?;
        }

        Ok(())
    }

    fn validate_struct_definitions(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let definitions = TypeReader::get_struct_definitions_all(snapshot)?;

        for (_key, struct_definition) in definitions {
            Self::validate_struct_definition_fields(snapshot, type_manager, struct_definition, validation_errors)?;
        }

        Ok(())
    }

    fn validate_relation_type_role_types(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let relates_declared = relation_type.get_relates_root(snapshot, type_manager)?;

        for relates in relates_declared.into_iter() {
            let role = relates.role();

            if !relates.is_specialising(snapshot, type_manager)? {
                Self::validate_role_is_unique_for_relation_type_hierarchy(
                    snapshot,
                    type_manager,
                    relation_type,
                    role,
                    validation_errors,
                )?;
            } else {
                debug_assert!(relates.relation() != relation_type);
            }

            Self::validate_type_ordering(snapshot, type_manager, role, validation_errors)?;
            Self::validate_type_constraints(snapshot, type_manager, role, validation_errors)?;
        }

        Ok(())
    }

    fn validate_object_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: ObjectType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_owns(snapshot, type_manager, type_, validation_errors)?;
        // TODO: Capabilities constraints narrowing checks are currently disabled
        // validate_capabilities_cardinalities_narrowing::<Owns>(
        //     snapshot,
        //     type_manager,
        //     type_.clone().into_owned_object_type(),
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     validation_errors,
        // )?;

        Self::validate_plays(snapshot, type_manager, type_, validation_errors)?;
        // TODO: Capabilities constraints narrowing checks are currently disabled
        // validate_capabilities_cardinalities_narrowing::<Plays>(
        //     snapshot,
        //     type_manager,
        //     type_.clone().into_owned_object_type(),
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     &HashMap::new(), // read everything from storage
        //     validation_errors,
        // )?;

        Ok(())
    }

    fn validate_owns(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: ObjectType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let owns_declared = type_.get_owns_declared(snapshot, type_manager)?;

        for owns in owns_declared.into_iter() {
            Self::validate_redundant_capabilities::<Owns>(snapshot, type_manager, *owns, validation_errors)?;

            Self::validate_capabilities_constraints::<Owns>(snapshot, type_manager, *owns, validation_errors)?;
        }

        Self::validate_capabilities_ordering(snapshot, type_manager, type_, validation_errors)?;

        Ok(())
    }

    fn validate_plays(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: ObjectType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let plays_declared = type_.get_plays_declared(snapshot, type_manager)?;

        for plays in plays_declared.into_iter() {
            Self::validate_redundant_capabilities::<Plays>(snapshot, type_manager, plays.clone(), validation_errors)?;

            Self::validate_capabilities_constraints::<Plays>(snapshot, type_manager, plays.clone(), validation_errors)?;
        }

        Ok(())
    }

    fn validate_relates(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let relates_declared = relation_type.get_relates_declared(snapshot, type_manager)?;
        debug_assert!({
            let relates_root = relation_type.get_relates_root(snapshot, type_manager)?;
            relates_root.into_iter().all(|relates| {
                relates_declared.contains(relates)
                    && !relates.is_specialising(snapshot, type_manager).unwrap()
                    && relates.role().get_label(snapshot, type_manager).unwrap().scope().unwrap()
                        == relates.relation().get_label(snapshot, type_manager).unwrap().name()
            })
        });

        for relates in relates_declared.into_iter() {
            Self::validate_specialised_relates(snapshot, type_manager, relates.clone(), validation_errors)?;

            Self::validate_capabilities_constraints::<Relates>(
                snapshot,
                type_manager,
                relates.clone(),
                validation_errors,
            )?;
        }

        Ok(())
    }

    fn validate_specialised_relates(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relates: Relates,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if relates.is_specialising(snapshot, type_manager)? && !relates.is_abstract(snapshot, type_manager)? {
            validation_errors.push(Box::new(SchemaValidationError::SpecialisingRelatesIsNotAbstract {
                relation: get_label_or_concept_read_err(snapshot, type_manager, relates.relation())?,
                role: get_label_or_concept_read_err(snapshot, type_manager, relates.role())?,
            }))
        }

        Ok(())
    }

    fn validate_redundant_capabilities<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if let Some(supertype) = capability.object().get_supertype(snapshot, type_manager)? {
            let supertype_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, supertype.clone(), false)?;

            let interface_type = capability.interface();
            if let Some(supertype_capability) =
                supertype_capabilities.iter().find(|cap| cap.interface() == interface_type)
            {
                let capability_annotations_declared = capability.get_annotations_declared(snapshot, type_manager)?;
                let supertype_capability_annotations_declared =
                    supertype_capability.get_annotations_declared(snapshot, type_manager)?;
                if capability_annotations_declared.is_empty()
                    || capability_annotations_declared == supertype_capability_annotations_declared
                {
                    validation_errors.push(Box::new(
                        SchemaValidationError::CannotRedeclareInheritedCapabilityWithoutSpecialisation {
                            cap: CAP::KIND,
                            // interface: get_label_or_concept_read_err(snapshot, type_manager, interface_type.clone())?,
                            subtype: get_label_or_concept_read_err(snapshot, type_manager, capability.object())?,
                            supertype: get_label_or_concept_read_err(
                                snapshot,
                                type_manager,
                                supertype_capability.object(),
                            )?,
                        },
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_type_constraints(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl KindAPI<'static>,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if let Some(supertype) = type_.get_supertype(snapshot, type_manager)? {
            if let Err(err) = validate_type_supertype_abstractness(
                snapshot,
                type_manager,
                type_.clone(),
                Some(supertype.clone()), // already found the supertype
                None,                    // read abstractness from storage
                None,                    // read abstractness from storage
            ) {
                validation_errors.push(err);
            }

            if let Err(err) = validate_type_declared_constraints_narrowing_of_supertype_constraints(
                snapshot,
                type_manager,
                type_.clone(),
                supertype,
            ) {
                validation_errors.push(err);
            }
        }

        Self::validate_redundant_type_constraints(snapshot, type_manager, type_.clone(), validation_errors)?;

        Ok(())
    }

    fn validate_capabilities_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_redundant_capability_constraints(snapshot, type_manager, capability, validation_errors)?;

        Ok(())
    }

    fn validate_redundant_type_constraints<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let constraints = type_.get_constraints(snapshot, type_manager)?;
        let declared_constraint_descriptions = filter_by_source!(constraints.iter().cloned(), type_.clone())
            .map(|constraint| constraint.description().clone());

        for constraint in constraints.into_iter() {
            if constraint.source() == type_ {
                continue;
            }
            if declared_constraint_descriptions.clone().contains(&constraint.description()) {
                validation_errors.push(Box::new(
                    SchemaValidationError::CannotRedeclareConstraintOnSubtypeWithoutSpecialisation {
                        // constraint: T::KIND,
                        subtype: get_label_or_concept_read_err(snapshot, type_manager, type_.clone())?,
                        // get_label_or_concept_read_err(snapshot, type_manager, constraint.source())?,
                        constraint: constraint.description(),
                    },
                ));
            }
        }

        Ok(())
    }

    fn validate_redundant_capability_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        capability: CAP,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let interface_type = capability.interface();
        let interface_type_constraint_descriptions: HashSet<ConstraintDescription> = interface_type
            .get_constraints(snapshot, type_manager)?
            .into_iter()
            .map(|constraint| constraint.description().clone())
            .collect();
        for constraint in capability.get_constraints(snapshot, type_manager)?.into_iter() {
            let description = constraint.description();
            if interface_type_constraint_descriptions.contains(&description) {
                validation_errors.push(Box::new(
                    SchemaValidationError::CapabilityConstraintAlreadyExistsForTheWholeInterfaceType {
                        cap: CAP::KIND,
                        label: get_label_or_concept_read_err(snapshot, type_manager, capability.object())?,
                        // get_label_or_concept_read_err(snapshot, type_manager, capability.interface())?,
                        constraint: description,
                    },
                ));
            }
        }

        Ok(())
    }

    fn validate_relation_type_has_relates(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let relates = relation_type.get_relates(snapshot, type_manager)?;

        if relates.is_empty() {
            validation_errors.push(Box::new(SchemaValidationError::RelationTypeMustRelateAtLeastOneRole {
                relation: get_label_or_concept_read_err(snapshot, type_manager, relation_type)?,
            }));
        }

        Ok(())
    }

    fn validate_role_is_unique_for_relation_type_hierarchy(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation_type: RelationType,
        role_type: RoleType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let role_label = role_type.get_label(snapshot, type_manager)?;
        let relation_supertypes = relation_type.get_supertypes_transitive(snapshot, type_manager)?;
        let relation_subtypes = relation_type.get_subtypes_transitive(snapshot, type_manager)?;

        for supertype in relation_supertypes.into_iter() {
            if let Err(err) =
                validate_role_name_uniqueness_non_transitive(snapshot, type_manager, *supertype, &role_label)
            {
                validation_errors.push(err);
            }
        }
        for subtype in relation_subtypes.into_iter() {
            if let Err(err) =
                validate_role_name_uniqueness_non_transitive(snapshot, type_manager, *subtype, &role_label)
            {
                validation_errors.push(err);
            }
        }

        Ok(())
    }

    fn validate_type_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: RoleType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if let Some(supertype) = type_.get_supertype(snapshot, type_manager)? {
            if let Err(err) =
                validate_role_type_supertype_ordering_match(snapshot, type_manager, type_, supertype, None, None)
            {
                validation_errors.push(err);
            }
        }
        Ok(())
    }

    fn validate_capabilities_ordering(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        object_type: ObjectType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if let Err(err) =
            validate_sibling_owns_ordering_match_for_type(snapshot, type_manager, object_type, &HashMap::new())
        {
            validation_errors.push(err);
        }
        Ok(())
    }

    fn validate_attribute_type_value_type(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        if let Some(supertype) = attribute_type.get_supertype(snapshot, type_manager)? {
            if let Some((_supertype_value_type, _)) = supertype.get_value_type(snapshot, type_manager)? {
                if let Some(declared_value_type) = attribute_type.get_value_type_declared(snapshot, type_manager)? {
                    let declared_value_type_annotations =
                        attribute_type.get_value_type_annotations_declared(snapshot, type_manager)?;
                    if declared_value_type_annotations.is_empty() {
                        validation_errors.push(Box::new(
                            SchemaValidationError::CannotRedeclareInheritedValueTypeWithoutSpecialisation {
                                label: get_label_or_concept_read_err(snapshot, type_manager, attribute_type)?,
                                super_label: get_label_or_concept_read_err(snapshot, type_manager, supertype)?,
                                value_type: declared_value_type,
                                // supertype_value_type,
                            },
                        ));
                    }
                }
            }
        }

        let value_type = attribute_type.get_value_type(snapshot, type_manager)?;
        if value_type.is_none() && !attribute_type.is_abstract(snapshot, type_manager)? {
            validation_errors.push(Box::new(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract {
                attribute: get_label_or_concept_read_err(snapshot, type_manager, attribute_type)?,
            }));
        }

        Ok(())
    }

    fn validate_struct_definition_fields(
        _snapshot: &impl ReadableSnapshot,
        _type_manager: &TypeManager,
        struct_definition: StructDefinition,
        validation_errors: &mut Vec<Box<SchemaValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        debug_assert_eq!(struct_definition.fields.len(), struct_definition.field_names.len());

        if struct_definition.fields.is_empty() {
            validation_errors.push(Box::new(SchemaValidationError::StructShouldHaveAtLeastOneField {
                name: struct_definition.name.to_owned(),
            }));
        }

        Ok(())
    }
}
