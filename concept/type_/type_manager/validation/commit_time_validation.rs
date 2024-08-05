/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::graph::{definition::r#struct::StructDefinition, type_::CapabilityKind};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationCardinality},
        attribute_type::AttributeType,
        entity_type::EntityType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                validation::{
                    get_label_or_concept_read_err, is_interface_hidden_by_overrides,
                    is_overridden_interface_object_declared_supertype_or_self,
                    is_overridden_interface_object_one_of_supertypes_or_self,
                    validate_declared_annotation_is_compatible_with_inherited_annotations,
                    validate_declared_capability_annotation_is_compatible_with_inherited_annotations,
                    validate_edge_annotations_narrowing_of_inherited_annotations,
                    validate_owns_override_ordering_match, validate_role_name_uniqueness_non_transitive,
                    validate_role_type_supertype_ordering_match,
                    validate_type_annotations_narrowing_of_inherited_annotations,
                },
                SchemaValidationError,
            },
            TypeManager,
        },
        Capability, KindAPI, ObjectTypeAPI, TypeAPI,
    },
};

pub struct CommitTimeValidation {}

macro_rules! validate_types {
    ($func_name:ident, $get_all_of_kind:ident, $type_:ident, $func:path) => {
        fn $func_name(
            type_manager: &TypeManager,
            snapshot: &impl ReadableSnapshot,
            validation_errors: &mut Vec<SchemaValidationError>,
        ) -> Result<(), ConceptReadError> {
            let roots = TypeReader::$get_all_of_kind(snapshot)?.into_iter().filter_map(|type_| {
                match type_.get_supertype(snapshot, type_manager) {
                    Ok(Some(_)) => None,
                    Ok(None) => Some(Ok(type_)),
                    Err(err) => Some(Err(err)),
                }
            });
            for root in roots {
                let root = root?;
                $func(type_manager, snapshot, root.clone(), validation_errors)?;
                for subtype in TypeReader::get_subtypes_transitive(snapshot, root)? {
                    $func(type_manager, snapshot, subtype, validation_errors)?;
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

impl CommitTimeValidation {
    pub(crate) fn validate(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();
        Self::validate_entity_types(type_manager, snapshot, &mut errors)?;
        Self::validate_relation_types(type_manager, snapshot, &mut errors)?;
        Self::validate_attribute_types(type_manager, snapshot, &mut errors)?;
        Self::validate_struct_definitions(type_manager, snapshot, &mut errors)?;
        Ok(errors)
    }

    validate_types!(validate_entity_types, get_entity_types, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, get_relation_types, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, get_attribute_types, AttributeType, Self::validate_attribute_type);

    fn validate_entity_type(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: EntityType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        Self::validate_type_annotations(type_manager, snapshot, type_.clone(), validation_errors)?;
        Self::validate_object_type(type_manager, snapshot, type_.clone(), validation_errors)?;

        Ok(())
    }

    fn validate_relation_type(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        Self::validate_type_annotations(type_manager, snapshot, type_.clone(), validation_errors)?;
        Self::validate_object_type(type_manager, snapshot, type_.clone(), validation_errors)?;

        Self::validate_relation_type_has_relates(type_manager, snapshot, type_.clone(), validation_errors)?;
        Self::validate_relation_type_role_types(type_manager, snapshot, type_.clone(), validation_errors)?;

        let invalid_relates = produced_errors!(
            validation_errors,
            Self::validate_declared_relates(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !invalid_relates {
            Self::validate_abstractness_matches_with_capability::<Relates<'static>>(
                type_manager,
                snapshot,
                type_.clone(),
                validation_errors,
            )?;
            Self::validate_capability_cardinality::<Relates<'static>>(
                type_manager,
                snapshot,
                type_.clone(),
                validation_errors,
            )?;
        }

        Ok(())
    }

    fn validate_attribute_type(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let invalid_value_type = produced_errors!(
            validation_errors,
            Self::validate_attribute_type_value_type(type_manager, snapshot, type_.clone(), validation_errors)?
        );

        if !invalid_value_type {
            Self::validate_type_annotations(type_manager, snapshot, type_.clone(), validation_errors)?;
        }

        Ok(())
    }

    fn validate_struct_definitions(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let definitions = TypeReader::get_struct_definitions_all(snapshot)?;

        for (_key, struct_definition) in definitions {
            Self::validate_struct_definition_fields(type_manager, snapshot, struct_definition, validation_errors)?;
        }

        Ok(())
    }

    fn validate_relation_type_role_types(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates_declared =
            TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            let role = relates.role();

            Self::validate_role_is_unique_for_relation_type_hierarchy(
                type_manager,
                snapshot,
                relation_type.clone(),
                role.clone(),
                validation_errors,
            )?;
            Self::validate_type_ordering(type_manager, snapshot, role.clone(), validation_errors)?;
            Self::validate_type_annotations(type_manager, snapshot, role.clone(), validation_errors)?;
        }

        Ok(())
    }

    fn validate_object_type<T: ObjectTypeAPI<'static> + KindAPI<'static>>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let invalid_owns = produced_errors!(
            validation_errors,
            Self::validate_declared_owns(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !invalid_owns {
            Self::validate_abstractness_matches_with_capability::<Owns<'static>>(
                type_manager,
                snapshot,
                type_.clone().into_owned_object_type(),
                validation_errors,
            )?;
            Self::validate_capability_cardinality::<Owns<'static>>(
                type_manager,
                snapshot,
                type_.clone(),
                validation_errors,
            )?;
        }

        let invalid_plays = produced_errors!(
            validation_errors,
            Self::validate_declared_plays(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !invalid_plays {
            Self::validate_abstractness_matches_with_capability::<Plays<'static>>(
                type_manager,
                snapshot,
                type_.clone().into_owned_object_type(),
                validation_errors,
            )?;
            Self::validate_capability_cardinality::<Plays<'static>>(
                type_manager,
                snapshot,
                type_.clone(),
                validation_errors,
            )?;
        }

        Ok(())
    }

    fn validate_declared_owns<T>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let owns_declared: HashSet<Owns<'static>> = TypeReader::get_capabilities_declared(snapshot, type_.clone())?;

        for owns in owns_declared {
            let invalid_overrides = produced_errors!(
                validation_errors,
                Self::validate_overridden_owns(type_manager, snapshot, owns.clone(), validation_errors)?
            );

            Self::validate_declared_capability_not_hidden_by_supertypes_overrides::<Owns<'static>>(
                type_manager,
                snapshot,
                owns.clone(),
                validation_errors,
            )?;

            if !invalid_overrides {
                Self::validate_redundant_capabilities::<Owns<'static>>(
                    type_manager,
                    snapshot,
                    owns.clone(),
                    validation_errors,
                )?;
                Self::validate_capabilities_annotations::<Owns<'static>>(
                    type_manager,
                    snapshot,
                    owns.clone(),
                    validation_errors,
                )?;
                Self::validate_capabilities_ordering(type_manager, snapshot, owns.clone(), validation_errors)?;
            }
        }

        Ok(())
    }

    fn validate_declared_plays<T>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let plays_declared: HashSet<Plays<'static>> = TypeReader::get_capabilities_declared(snapshot, type_.clone())?;

        for plays in plays_declared {
            let invalid_overrides = produced_errors!(
                validation_errors,
                Self::validate_overridden_plays(type_manager, snapshot, plays.clone(), validation_errors)?
            );

            Self::validate_declared_capability_not_hidden_by_supertypes_overrides::<Plays<'static>>(
                type_manager,
                snapshot,
                plays.clone(),
                validation_errors,
            )?;

            if !invalid_overrides {
                Self::validate_redundant_capabilities::<Plays<'static>>(
                    type_manager,
                    snapshot,
                    plays.clone(),
                    validation_errors,
                )?;
                Self::validate_capabilities_annotations::<Plays<'static>>(
                    type_manager,
                    snapshot,
                    plays.clone(),
                    validation_errors,
                )?;
            }
        }

        Ok(())
    }

    fn validate_declared_relates(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates_declared: HashSet<Relates<'static>> =
            TypeReader::get_capabilities_declared(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            let invalid_overrides = produced_errors!(
                validation_errors,
                Self::validate_overridden_relates(type_manager, snapshot, relates.clone(), validation_errors)?
            );

            if !invalid_overrides {
                Self::validate_capabilities_annotations::<Relates<'static>>(
                    type_manager,
                    snapshot,
                    relates,
                    validation_errors,
                )?;
            }
        }

        Ok(())
    }

    fn validate_abstractness_matches_with_capability<CAP: Capability<'static>>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: CAP::ObjectType,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if type_.is_abstract(snapshot, type_manager)? {
            return Ok(());
        }

        for interface_type in TypeReader::get_capabilities::<CAP>(snapshot, type_.clone())?.keys() {
            if interface_type.is_abstract(snapshot, type_manager)? {
                validation_errors.push(SchemaValidationError::NonAbstractTypeCannotHaveAbstractInterfaceCapability(
                    CAP::KIND,
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, interface_type.clone())?,
                ))
            }
        }

        Ok(())
    }

    fn validate_overridden_relates(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relation_type = relates.relation();
        let supertype = TypeReader::get_supertype(snapshot, relation_type.clone())?;

        if let Some(relates_override) = TypeReader::get_capability_override(snapshot, relates.clone())? {
            let role_type_overridden = relates_override.role();

            match &supertype {
                None => validation_errors.push(SchemaValidationError::RelatesOverrideIsNotInherited(
                    get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                )),
                Some(supertype) => {
                    let contains = TypeReader::get_capabilities::<Relates<'static>>(snapshot, supertype.clone())?
                        .keys()
                        .contains(&role_type_overridden);

                    if !contains {
                        validation_errors.push(SchemaValidationError::RelatesOverrideIsNotInherited(
                            get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                            get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                        ));
                    }
                }
            }

            let role_type = relates.role();
            // Only declared supertype (not transitive) fits as relates override == role subtype!
            // It is only a commit-time check as we verify that operation-time generation has been correct
            if !is_overridden_interface_object_declared_supertype_or_self(
                snapshot,
                role_type.clone(),
                role_type_overridden.clone(),
            )? {
                validation_errors.push(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                    CapabilityKind::Relates,
                    get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                ));
            }
        }

        Ok(())
    }

    fn validate_overridden_owns(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = owns.owner();
        let supertype = TypeReader::get_supertype(snapshot, type_.clone())?;

        if let Some(owns_override) = TypeReader::get_capability_override(snapshot, owns.clone())? {
            let attribute_type_overridden = owns_override.attribute();

            match &supertype {
                None => validation_errors.push(SchemaValidationError::OwnsOverrideIsNotInherited(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                )),
                Some(supertype) => {
                    let contains = TypeReader::get_capabilities::<Owns<'static>>(snapshot, supertype.clone())?
                        .keys()
                        .contains(&attribute_type_overridden);

                    if !contains {
                        validation_errors.push(SchemaValidationError::OwnsOverrideIsNotInherited(
                            get_label_or_concept_read_err(snapshot, type_.clone())?,
                            get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                        ));
                    }
                }
            }

            let attribute_type = owns.attribute();
            if !is_overridden_interface_object_one_of_supertypes_or_self(
                // Any supertype (even transitive) fits
                snapshot,
                attribute_type.clone(),
                attribute_type_overridden.clone(),
            )? {
                validation_errors.push(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                    CapabilityKind::Owns,
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                    get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                ));
            }
        }

        Ok(())
    }

    fn validate_overridden_plays(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let type_ = plays.player();

        if let Some(plays_override) = TypeReader::get_capability_override(snapshot, plays.clone())? {
            let role_type_overridden = plays_override.role();
            let supertype = TypeReader::get_supertype(snapshot, type_.clone())?;
            match &supertype {
                None => validation_errors.push(SchemaValidationError::PlaysOverrideIsNotInherited(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                )),
                Some(supertype) => {
                    let contains = TypeReader::get_capabilities::<Plays<'static>>(snapshot, supertype.clone())?
                        .keys()
                        .contains(&role_type_overridden);

                    if !contains {
                        validation_errors.push(SchemaValidationError::PlaysOverrideIsNotInherited(
                            get_label_or_concept_read_err(snapshot, type_.clone())?,
                            get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                        ));
                    }
                }
            }

            let role_type = plays.role();
            if !is_overridden_interface_object_one_of_supertypes_or_self(
                // Any supertype (even transitive) fits
                snapshot,
                role_type.clone(),
                role_type_overridden.clone(),
            )? {
                validation_errors.push(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                    CapabilityKind::Plays,
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                ));
            }
        }

        Ok(())
    }

    fn validate_declared_capability_not_hidden_by_supertypes_overrides<CAP: Capability<'static>>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, capability.object())? {
            let interface_type = capability.interface();
            if is_interface_hidden_by_overrides::<CAP>(snapshot, supertype.clone(), interface_type.clone())? {
                validation_errors.push(SchemaValidationError::OverriddenCapabilityCannotBeRedeclared(
                    CAP::KIND,
                    get_label_or_concept_read_err(snapshot, capability.object())?,
                    get_label_or_concept_read_err(snapshot, interface_type)?,
                ));
            }
        }
        Ok(())
    }

    fn validate_redundant_capabilities<CAP: Capability<'static>>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, capability.object())? {
            let supertype_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, supertype.clone())?;

            let interface_type = capability.interface();
            if let Some(supertype_capability) = supertype_capabilities.get(&interface_type) {
                let supertype_capability_object = supertype_capability.object();

                let capability_override = TypeReader::get_capability_override(snapshot, capability.clone())?;
                let correct_override = match capability_override {
                    None => false,
                    Some(capability_override) => &capability_override == supertype_capability,
                };
                if !correct_override {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedCapabilityWithoutSpecializationWithOverride(
                            CAP::KIND,
                            get_label_or_concept_read_err(snapshot, interface_type.clone())?,
                            get_label_or_concept_read_err(snapshot, capability.object())?,
                            get_label_or_concept_read_err(snapshot, supertype_capability_object.clone())?,
                        ),
                    );
                }

                let capability_annotations_declared =
                    TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())?;
                if capability_annotations_declared.is_empty() {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedCapabilityWithoutSpecializationWithOverride(
                            CAP::KIND,
                            get_label_or_concept_read_err(snapshot, interface_type.clone())?,
                            get_label_or_concept_read_err(snapshot, capability.object())?,
                            get_label_or_concept_read_err(snapshot, supertype_capability_object.clone())?,
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    fn validate_redundant_type_annotations<T: KindAPI<'static>>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation: &T::AnnotationType,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if !annotation.clone().into().category().inheritable() {
            return Ok(());
        }

        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            let supertype_annotations = TypeReader::get_type_annotations(snapshot, supertype.clone())?;

            if supertype_annotations.keys().contains(&annotation) {
                validation_errors.push(
                    SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForType(
                        T::ROOT_KIND,
                        get_label_or_concept_read_err(snapshot, type_.clone())?,
                        get_label_or_concept_read_err(snapshot, supertype.clone())?,
                        annotation.clone().into(),
                    ),
                );
            }
        }

        Ok(())
    }

    fn validate_redundant_edge_annotations<CAP: Capability<'static>>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation: &CAP::AnnotationType,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if !annotation.clone().into().category().inheritable() {
            return Ok(());
        }

        if let Some(overridden_edge) = TypeReader::get_capability_override(snapshot, edge.clone())? {
            let overridden_edge_annotations = TypeReader::get_type_edge_annotations(snapshot, overridden_edge.clone())?;

            if overridden_edge_annotations.keys().contains(&annotation) {
                validation_errors.push(
                    SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForCapability(
                        CAP::KIND,
                        get_label_or_concept_read_err(snapshot, edge.object())?,
                        get_label_or_concept_read_err(snapshot, overridden_edge.object())?,
                        get_label_or_concept_read_err(snapshot, edge.interface())?,
                        annotation.clone().into(),
                    ),
                );
            }
        }

        Ok(())
    }

    fn validate_capability_cardinality<CAP: Capability<'static>>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let mut cardinality_connections: HashMap<CAP, HashSet<CAP>> = HashMap::new();
        let mut cardinalities: HashMap<CAP, AnnotationCardinality> = HashMap::new();

        let capability_declared: HashSet<CAP> = TypeReader::get_capabilities_declared(snapshot, type_.clone())?;

        for capability in capability_declared {
            if !cardinalities.contains_key(&capability) {
                cardinalities.insert(capability.clone(), capability.get_cardinality(snapshot, type_manager)?);
            }

            let mut current_overridden_capability = TypeReader::get_capability_override(snapshot, capability.clone())?;
            while let Some(overridden_capability) = current_overridden_capability {
                if !cardinalities.contains_key(&overridden_capability) {
                    cardinalities.insert(
                        overridden_capability.clone(),
                        overridden_capability.get_cardinality(snapshot, type_manager)?,
                    );
                }

                if !cardinality_connections.contains_key(&overridden_capability) {
                    cardinality_connections.insert(overridden_capability.clone(), HashSet::new());
                }
                cardinality_connections.get_mut(&overridden_capability).unwrap().insert(capability.clone());

                let overridden_card = cardinalities.get(&overridden_capability).unwrap();
                let capability_card = cardinalities.get(&overridden_capability).unwrap();

                if !overridden_card.narrowed_correctly_by(capability_card) {
                    validation_errors.push(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
                        CAP::KIND,
                        get_label_or_concept_read_err(snapshot, capability.object())?,
                        get_label_or_concept_read_err(snapshot, capability.interface())?,
                        get_label_or_concept_read_err(snapshot, overridden_capability.object())?,
                        get_label_or_concept_read_err(snapshot, overridden_capability.interface())?,
                        *capability_card,
                        *overridden_card,
                    ));
                }

                current_overridden_capability =
                    TypeReader::get_capability_override(snapshot, overridden_capability.clone())?;
            }
        }

        for (root_capability, inheriting_capabilities) in cardinality_connections {
            let root_cardinality = cardinalities.get(&root_capability).unwrap();
            let inheriting_cardinality =
                inheriting_capabilities.iter().filter_map(|capability| cardinalities.get(capability).copied()).sum();

            if !root_cardinality.narrowed_correctly_by(&inheriting_cardinality) {
                validation_errors.push(SchemaValidationError::SummarizedCardinalityOfEdgesOverridingSingleEdgeOverflowsOverriddenCardinality(
                    CAP::KIND,
                    get_label_or_concept_read_err(snapshot, root_capability.object())?,
                    get_label_or_concept_read_err(snapshot, root_capability.interface())?,
                    *root_cardinality,
                    inheriting_cardinality,
                ));
            }
        }

        Ok(())
    }

    fn validate_relation_type_has_relates(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates = TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone())?;

        if relates.is_empty() {
            validation_errors.push(SchemaValidationError::RelationTypeMustRelateAtLeastOneRole(
                get_label_or_concept_read_err(snapshot, relation_type)?,
            ));
        }

        Ok(())
    }

    fn validate_role_is_unique_for_relation_type_hierarchy(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let role_label =
            TypeReader::get_label(snapshot, role_type)?.ok_or(ConceptReadError::CorruptMissingLabelOfType)?;
        let relation_supertypes = TypeReader::get_supertypes_transitive(snapshot, relation_type.clone())?;
        let relation_subtypes = TypeReader::get_subtypes_transitive(snapshot, relation_type.clone())?;

        for supertype in relation_supertypes {
            if let Err(err) = validate_role_name_uniqueness_non_transitive(snapshot, supertype, &role_label) {
                validation_errors.push(err);
            }
        }
        for subtype in relation_subtypes {
            if let Err(err) = validate_role_name_uniqueness_non_transitive(snapshot, subtype, &role_label) {
                validation_errors.push(err);
            }
        }

        Ok(())
    }

    fn validate_type_annotations(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let declared_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())?;

        for annotation in declared_annotations {
            let annotation_category = annotation.clone().into().category();

            if let Err(err) = validate_declared_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                type_.clone(),
                annotation_category,
            ) {
                validation_errors.push(err);
            }

            if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
                if let Err(err) = validate_type_annotations_narrowing_of_inherited_annotations(
                    snapshot,
                    type_.clone(),
                    supertype.clone(),
                    annotation.clone(),
                ) {
                    validation_errors.push(err);
                }
            }

            Self::validate_redundant_type_annotations(
                type_manager,
                snapshot,
                type_.clone(),
                &annotation,
                validation_errors,
            )?;
        }

        Ok(())
    }

    fn validate_capabilities_annotations<CAP: Capability<'static>>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())?;

        for annotation in declared_annotations {
            let annotation_category = annotation.clone().into().category();

            if let Err(err) = validate_declared_capability_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                edge.clone(),
                annotation_category,
            ) {
                validation_errors.push(err);
            }

            if let Some(overridden_edge) = TypeReader::get_capability_override(snapshot, edge.clone())? {
                if let Err(err) = validate_edge_annotations_narrowing_of_inherited_annotations(
                    snapshot,
                    type_manager,
                    edge.clone(),
                    overridden_edge.clone(),
                    annotation.clone().into(),
                ) {
                    validation_errors.push(err);
                }
            }

            Self::validate_redundant_edge_annotations(
                type_manager,
                snapshot,
                edge.clone(),
                &annotation,
                validation_errors,
            )?;
        }

        Ok(())
    }

    fn validate_type_ordering(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: RoleType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            if let Err(err) = validate_role_type_supertype_ordering_match(snapshot, type_, supertype, None) {
                validation_errors.push(err);
            }
        }
        Ok(())
    }

    fn validate_capabilities_ordering(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: Owns<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(overridden_edge) = TypeReader::get_capability_override(snapshot, edge.clone())? {
            if let Err(err) = validate_owns_override_ordering_match(snapshot, edge, overridden_edge, None) {
                validation_errors.push(err);
            }
        }
        Ok(())
    }

    fn validate_attribute_type_value_type(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, attribute_type.clone())? {
            if let Some(supertype_value_type) = TypeReader::get_value_type_without_source(snapshot, supertype.clone())?
            {
                if let Some(declared_value_type) =
                    TypeReader::get_value_type_declared(snapshot, attribute_type.clone())?
                {
                    validation_errors.push(SchemaValidationError::RedundantValueTypeDeclarationAsItsAlreadyInherited(
                        get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                        get_label_or_concept_read_err(snapshot, supertype)?,
                        declared_value_type,
                        supertype_value_type,
                    ));
                }
            }
        }

        let value_type = TypeReader::get_value_type_without_source(snapshot, attribute_type.clone())?;
        if value_type.is_none() && !attribute_type.is_abstract(snapshot, type_manager)? {
            validation_errors.push(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract(
                get_label_or_concept_read_err(snapshot, attribute_type)?,
            ));
        }

        Ok(())
    }

    fn validate_struct_definition_fields(
        _type_manager: &TypeManager,
        _snapshot: &impl ReadableSnapshot,
        struct_definition: StructDefinition,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        debug_assert_eq!(struct_definition.fields.len(), struct_definition.field_names.len());

        if struct_definition.fields.is_empty() {
            validation_errors.push(SchemaValidationError::StructShouldHaveAtLeastOneField(struct_definition.name));
        }

        Ok(())
    }
}
