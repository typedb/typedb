/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, hash::Hash};

use encoding::{
    graph::type_::{CapabilityKind, Kind},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::Annotation,
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
                    get_label_or_concept_read_err, is_attribute_type_owns_overridden,
                    is_overridden_interface_object_declared_supertype_or_self,
                    is_overridden_interface_object_one_of_supertypes_or_self, is_role_type_plays_overridden,
                    type_is_abstract, validate_declared_annotation_is_compatible_with_other_inherited_annotations,
                    validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations,
                    validate_edge_annotations_narrowing_of_inherited_annotations,
                    validate_edge_override_ordering_match, validate_role_name_uniqueness_non_transitive,
                    validate_type_annotations_narrowing_of_inherited_annotations,
                    validate_type_supertype_ordering_match,
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
    ($func_name:ident, $kind:expr, $type_:ident, $func:path) => {
        fn $func_name(
            type_manager: &TypeManager,
            snapshot: &impl ReadableSnapshot,
            validation_errors: &mut Vec<SchemaValidationError>,
        ) -> Result<(), ConceptReadError> {
            let root_label = $kind.root_label();
            let root = TypeReader::get_labelled_type::<$type_<'static>>(snapshot, &root_label)?;

            match root {
                Some(root) => {
                    $func(type_manager, snapshot, root.clone(), validation_errors)?;

                    for subtype in TypeReader::get_subtypes_transitive(snapshot, root)? {
                        $func(type_manager, snapshot, subtype, validation_errors)?;
                    }
                }
                None => validation_errors.push(SchemaValidationError::RootHasBeenCorrupted(root_label)),
            };

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

macro_rules! ff {
    ($func_name:ident, $kind:expr, $type_:ident, $func:path) => {
        fn $func_name(
            type_manager: &TypeManager,
            snapshot: &impl ReadableSnapshot,
            validation_errors: &mut Vec<SchemaValidationError>,
        ) -> Result<(), ConceptReadError> {
            let root_label = $kind.root_label();
            let root = TypeReader::get_labelled_type::<$type_<'static>>(snapshot, &root_label)?;

            match root {
                Some(root) => {
                    $func(type_manager, snapshot, root.clone(), validation_errors)?;

                    for subtype in TypeReader::get_subtypes_transitive(snapshot, root)? {
                        $func(type_manager, snapshot, subtype, validation_errors)?;
                    }
                }
                None => validation_errors.push(SchemaValidationError::RootHasBeenCorrupted(root_label)),
            };

            Ok(())
        }
    };
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
        Ok(errors)
    }

    validate_types!(validate_entity_types, Kind::Entity, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, Kind::Relation, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, Kind::Attribute, AttributeType, Self::validate_attribute_type);

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

        let ill_relates = produced_errors!(
            validation_errors,
            Self::validate_declared_relates(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !ill_relates {
            Self::validate_abstractness_matches_with_relates(type_manager, snapshot, type_.clone(), validation_errors)?;
            Self::validate_relates_cardinality(type_manager, snapshot, type_.clone(), validation_errors)?;
        }

        Ok(())
    }

    fn validate_attribute_type(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let ill_value_type = produced_errors!(
            validation_errors,
            Self::validate_attribute_type_value_type(type_manager, snapshot, type_.clone(), validation_errors)?
        );

        if !ill_value_type {
            Self::validate_type_annotations(type_manager, snapshot, type_.clone(), validation_errors)?;
        }

        // TODO: Validate value type against annotations? Validate value type set?

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
        let ill_owns = produced_errors!(
            validation_errors,
            Self::validate_declared_owns(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !ill_owns {
            Self::validate_abstractness_matches_with_owns(type_manager, snapshot, type_.clone(), validation_errors)?;
            Self::validate_owns_cardinality(type_manager, snapshot, type_.clone(), validation_errors)?;
        }

        let ill_plays = produced_errors!(
            validation_errors,
            Self::validate_declared_plays(type_manager, snapshot, type_.clone(), validation_errors)?
        );
        if !ill_plays {
            Self::validate_abstractness_matches_with_plays(type_manager, snapshot, type_.clone(), validation_errors)?;
            Self::validate_plays_cardinality(type_manager, snapshot, type_.clone(), validation_errors)?;
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
            Self::validate_overridden_owns(type_manager, snapshot, owns.clone(), validation_errors)?;
            Self::validate_declared_owns_not_overridden_by_supertypes(
                type_manager,
                snapshot,
                owns.clone(),
                validation_errors,
            )?;
            Self::validate_redundant_owns(type_manager, snapshot, owns.clone(), validation_errors)?;
            Self::validate_edge_annotations::<Owns<'static>>(type_manager, snapshot, owns.clone(), validation_errors)?;
            Self::validate_edge_ordering(type_manager, snapshot, owns.clone(), validation_errors)?;
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
            Self::validate_overridden_plays(type_manager, snapshot, plays.clone(), validation_errors)?;
            Self::validate_declared_plays_not_overridden_by_supertypes(
                type_manager,
                snapshot,
                plays.clone(),
                validation_errors,
            )?;
            Self::validate_redundant_plays(type_manager, snapshot, plays.clone(), validation_errors)?;
            Self::validate_edge_annotations::<Plays<'static>>(
                type_manager,
                snapshot,
                plays.clone(),
                validation_errors,
            )?;
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
            Self::validate_overridden_relates(type_manager, snapshot, relates.clone(), validation_errors)?;
            Self::validate_edge_annotations::<Relates<'static>>(type_manager, snapshot, relates, validation_errors)?;
        }

        Ok(())
    }

    fn validate_abstractness_matches_with_owns<T>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        for attribute_type in
            TypeReader::get_capabilities::<Owns<'static>>(snapshot, type_.clone().into_owned_object_type())?.keys()
        {
            if type_is_abstract(snapshot, attribute_type.clone())? {
                validation_errors.push(SchemaValidationError::NonAbstractCannotOwnAbstract(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                ))
            }
        }

        Ok(())
    }

    // TODO: Add BDD tests and operation time check as for owns
    fn validate_abstractness_matches_with_plays<T>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        for role_type in
            TypeReader::get_capabilities::<Plays<'static>>(snapshot, type_.clone().into_owned_object_type())?.keys()
        {
            if type_is_abstract(snapshot, role_type.clone())? {
                validation_errors.push(SchemaValidationError::NonAbstractCannotPlayAbstract(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                ))
            }
        }

        Ok(())
    }

    // TODO: Add BDD tests and operation time check as for owns
    fn validate_abstractness_matches_with_relates(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if type_is_abstract(snapshot, relation_type.clone())? {
            return Ok(());
        }

        for role_type in TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone())?.keys() {
            let role_type = role_type.clone();
            if type_is_abstract(snapshot, role_type.clone())? {
                validation_errors.push(SchemaValidationError::NonAbstractCannotRelateAbstract(
                    get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                ));
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

        if let Some(relates_override) = TypeReader::get_capabilities_override(snapshot, relates.clone())? {
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
                validation_errors.push(SchemaValidationError::OverriddenRelatesRoleTypeIsNotSupertype(
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

        if let Some(owns_override) = TypeReader::get_capabilities_override(snapshot, owns.clone())? {
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
                validation_errors.push(SchemaValidationError::OverriddenOwnsAttributeTypeIsNotSupertype(
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

        if let Some(plays_override) = TypeReader::get_capabilities_override(snapshot, plays.clone())? {
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
                validation_errors.push(SchemaValidationError::OverriddenPlaysRoleTypeIsNotSupertype(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                ));
            }
        }

        Ok(())
    }

    fn validate_declared_owns_not_overridden_by_supertypes(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, owns.owner())? {
            let attribute_type = owns.attribute();
            if is_attribute_type_owns_overridden(snapshot, supertype.clone(), attribute_type.clone())? {
                validation_errors.push(SchemaValidationError::OverriddenOwnsCannotBeRedeclared(
                    get_label_or_concept_read_err(snapshot, owns.owner())?,
                    attribute_type,
                ));
            }
        }

        Ok(())
    }

    fn validate_declared_plays_not_overridden_by_supertypes(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, plays.player().clone())? {
            let role_type = plays.role();
            if is_role_type_plays_overridden(snapshot, supertype.clone(), role_type.clone())? {
                validation_errors.push(SchemaValidationError::OverriddenPlaysCannotBeRedeclared(
                    get_label_or_concept_read_err(snapshot, plays.player().clone())?,
                    role_type,
                ));
            }
        }

        Ok(())
    }

    fn validate_redundant_owns(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, owns.owner())? {
            let supertype_owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, supertype.clone())?;

            let attribute_type = owns.attribute();
            if let Some(supertype_owns) = supertype_owns.get(&attribute_type) {
                let supertype_owns_owner = supertype_owns.owner();

                let owns_override = TypeReader::get_capabilities_override(snapshot, owns.clone())?;
                let correct_override = match owns_override {
                    None => false,
                    Some(owns_override) => &owns_override == supertype_owns,
                };
                if !correct_override {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                            get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                            get_label_or_concept_read_err(snapshot, owns.owner())?,
                            get_label_or_concept_read_err(snapshot, supertype_owns_owner.clone())?,
                        ),
                    );
                }

                let owns_annotations_declared = TypeReader::get_type_edge_annotations_declared(snapshot, owns.clone())?;
                if owns_annotations_declared.is_empty() {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                            get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                            get_label_or_concept_read_err(snapshot, owns.owner())?,
                            get_label_or_concept_read_err(snapshot, supertype_owns_owner.clone())?,
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    fn validate_redundant_plays(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, plays.player())? {
            let supertype_plays = TypeReader::get_capabilities::<Plays<'static>>(snapshot, supertype.clone())?;

            let role_type = plays.role();
            if let Some(supertype_plays) = supertype_plays.get(&role_type) {
                let supertype_plays_player = supertype_plays.player();

                let plays_override = TypeReader::get_capabilities_override(snapshot, plays.clone())?;
                let correct_override = match plays_override {
                    None => false,
                    Some(plays_override) => &plays_override == supertype_plays,
                };
                if !correct_override {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                            get_label_or_concept_read_err(snapshot, role_type.clone())?,
                            get_label_or_concept_read_err(snapshot, plays.player())?,
                            get_label_or_concept_read_err(snapshot, supertype_plays_player.clone())?,
                        ),
                    );
                }

                let plays_annotations_declared =
                    TypeReader::get_type_edge_annotations_declared(snapshot, plays.clone())?;
                if plays_annotations_declared.is_empty() {
                    validation_errors.push(
                        SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                            get_label_or_concept_read_err(snapshot, role_type.clone())?,
                            get_label_or_concept_read_err(snapshot, plays.player())?,
                            get_label_or_concept_read_err(snapshot, supertype_plays_player.clone())?,
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

    fn validate_redundant_edge_annotations<EDGE: Capability<'static>>(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        annotation: &Annotation,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if !annotation.category().inheritable() {
            return Ok(());
        }

        if let Some(overridden_edge) = TypeReader::get_capabilities_override(snapshot, edge.clone())? {
            let overridden_edge_annotations = TypeReader::get_type_edge_annotations(snapshot, overridden_edge.clone())?;

            if overridden_edge_annotations.keys().contains(&annotation) {
                validation_errors.push(
                    SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForCapability(
                        EDGE::KIND,
                        get_label_or_concept_read_err(snapshot, edge.object())?,
                        get_label_or_concept_read_err(snapshot, overridden_edge.object())?,
                        get_label_or_concept_read_err(snapshot, edge.interface())?,
                        annotation.clone(),
                    ),
                );
            }
        }

        Ok(())
    }

    fn validate_owns_cardinality<T>(
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
            if let Some(owns_override) = TypeReader::get_capabilities_override(snapshot, owns.clone())? {
                let owns_cardinality = owns.get_cardinality(snapshot, type_manager)?;
                let owns_override_cardinality = owns_override.get_cardinality(snapshot, type_manager)?;

                if !owns_override_cardinality.narrowed_correctly_by(&owns_cardinality) {
                    validation_errors.push(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
                        CapabilityKind::Owns,
                        get_label_or_concept_read_err(snapshot, owns.owner())?,
                        get_label_or_concept_read_err(snapshot, owns_override.owner())?,
                        get_label_or_concept_read_err(snapshot, owns.attribute())?,
                        owns_cardinality,
                        owns_override_cardinality,
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_plays_cardinality<T>(
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
            if let Some(plays_override) = TypeReader::get_capabilities_override(snapshot, plays.clone())? {
                let plays_cardinality = plays.get_cardinality(snapshot, type_manager)?;
                let plays_override_cardinality = plays_override.get_cardinality(snapshot, type_manager)?;

                if !plays_override_cardinality.narrowed_correctly_by(&plays_cardinality) {
                    validation_errors.push(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
                        CapabilityKind::Plays,
                        get_label_or_concept_read_err(snapshot, plays.player())?,
                        get_label_or_concept_read_err(snapshot, plays_override.player())?,
                        get_label_or_concept_read_err(snapshot, plays.role())?,
                        plays_cardinality,
                        plays_override_cardinality,
                    ));
                }
            }
        }

        Ok(())
    }

    /*
    TODO: Ideally, in situation when we have several edges overriding one edge with card(X..Y), we may want to check
       that for edge(card(x1..y1)), edge(card(x2..y2)), ..., edge(card(xN..yN)) and overridden_edge(card(X..Y)) ->
       x1 + x2 + ... + xN >= X (and xi can be < X!) and y1 + y2 + ... + yN <= Y
       At the same time, it makes the schema not flexible enough, and I as a user would like to have it as an option.
       So maybe separate checks that xi >= X and yi <= Y are enough for the schema, and the data
       layer is checked naturally because edge is also an overridden_edge, so both cardinality constraints are checked!
       HOWEVER! The current behavior allows the following case:
       define
       single-parentship sub relation, relates parent @card(1, 1);
       parentship sub single-parentship, relates father as parent, relates mother as parent;
       insert
       $p (father, mother) isa parentship
    */

    fn validate_relates_cardinality(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates_declared: HashSet<Relates<'static>> =
            TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            if let Some(relates_override) = TypeReader::get_capabilities_override(snapshot, relates.clone())? {
                let relates_cardinality = relates.get_cardinality(snapshot, type_manager)?;
                let relates_override_cardinality = relates_override.get_cardinality(snapshot, type_manager)?;

                if !relates_override_cardinality.narrowed_correctly_by(&relates_cardinality) {
                    validation_errors.push(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
                        CapabilityKind::Relates,
                        get_label_or_concept_read_err(snapshot, relates.relation())?,
                        get_label_or_concept_read_err(snapshot, relates_override.relation())?,
                        get_label_or_concept_read_err(snapshot, relates.role())?,
                        relates_cardinality,
                        relates_override_cardinality,
                    ));
                }
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
            TypeReader::get_label(snapshot, role_type)?.ok_or(ConceptReadError::CannotGetLabelForExistingType)?;
        let relation_supertypes = TypeReader::get_supertypes(snapshot, relation_type.clone())?;
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

            if let Err(err) = validate_declared_annotation_is_compatible_with_other_inherited_annotations(
                snapshot,
                type_.clone(),
                annotation_category.clone(),
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

    fn validate_edge_annotations<EDGE>(
        type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        EDGE: Capability<'static>,
    {
        let declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())?;

        for annotation in declared_annotations {
            let annotation_category = annotation.category();

            if let Err(err) = validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations(
                snapshot,
                edge.clone(),
                annotation_category.clone(),
            ) {
                validation_errors.push(err);
            }

            if let Some(overridden_edge) = TypeReader::get_capabilities_override(snapshot, edge.clone())? {
                if let Err(err) = validate_edge_annotations_narrowing_of_inherited_annotations(
                    snapshot,
                    type_manager,
                    edge.clone(),
                    overridden_edge.clone(),
                    annotation.clone(),
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
            if let Err(err) = validate_type_supertype_ordering_match(snapshot, type_, supertype, None) {
                validation_errors.push(err);
            }
        }
        Ok(())
    }

    fn validate_edge_ordering(
        _type_manager: &TypeManager,
        snapshot: &impl ReadableSnapshot,
        edge: Owns<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if let Some(overridden_edge) = TypeReader::get_capabilities_override(snapshot, edge.clone())? {
            if let Err(err) = validate_edge_override_ordering_match(snapshot, edge, overridden_edge, None) {
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
}
