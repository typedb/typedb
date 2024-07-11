/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, hash::Hash};

use encoding::graph::type_::{edge::TypeEdgeEncoding, Kind};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
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
                    is_overridden_interface_object_supertype_or_self, is_role_type_plays_overridden, type_is_abstract,
                    validate_declared_annotation_is_compatible_with_declared_annotations,
                    validate_declared_annotation_is_compatible_with_inherited_annotations,
                    validate_declared_edge_annotation_is_compatible_with_declared_annotations,
                    validate_declared_edge_annotation_is_compatible_with_inherited_annotations,
                    validate_role_name_uniqueness_non_transitive,
                },
                SchemaValidationError,
            },
        },
        InterfaceImplementation, KindAPI, ObjectTypeAPI, TypeAPI,
    },
};

pub struct CommitTimeValidation {}

macro_rules! validate_types {
    ($func_name:ident, $kind:expr, $type_:ident, $func:path) => {
        fn $func_name(
            snapshot: &impl ReadableSnapshot,
            validation_errors: &mut Vec<SchemaValidationError>,
        ) -> Result<(), ConceptReadError> {
            let root_label = $kind.root_label();
            let root = TypeReader::get_labelled_type::<$type_<'static>>(snapshot, &root_label)?;

            match root {
                Some(root) => {
                    $func(snapshot, root.clone(), validation_errors)?;

                    for subtype in TypeReader::get_subtypes_transitive(snapshot, root)? {
                        $func(snapshot, subtype, validation_errors)?;
                    }
                }
                None => validation_errors.push(SchemaValidationError::RootHasBeenCorrupted(root_label)),
            };

            Ok(())
        }
    };
}

impl CommitTimeValidation {
    pub(crate) fn validate(snapshot: &impl ReadableSnapshot) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();
        Self::validate_entity_types(snapshot, &mut errors)?;
        Self::validate_relation_types(snapshot, &mut errors)?;
        Self::validate_attribute_types(snapshot, &mut errors)?;
        Ok(errors)
    }

    validate_types!(validate_entity_types, Kind::Entity, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, Kind::Relation, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, Kind::Attribute, AttributeType, Self::validate_attribute_type);

    fn validate_entity_type(
        snapshot: &impl ReadableSnapshot,
        type_: EntityType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        Self::validate_annotations(snapshot, type_.clone(), validation_errors)?;

        Self::validate_object_type(snapshot, type_.clone(), validation_errors)?;

        Ok(())
    }

    fn validate_relation_type(
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        Self::validate_annotations(snapshot, type_.clone(), validation_errors)?;
        Self::validate_relation_type_has_relates(snapshot, type_.clone(), validation_errors)?;
        Self::validate_abstractness_matches_with_relates(snapshot, type_.clone(), validation_errors)?;
        Self::validate_overridden_relates(snapshot, type_.clone(), validation_errors)?;
        Self::validate_edge_annotations::<RelationType<'static>, Relates<'static>>(
            snapshot,
            type_.clone(),
            validation_errors,
        )?;

        Self::validate_object_type(snapshot, type_.clone(), validation_errors)?;

        Self::validate_relation_type_role_types(snapshot, type_.clone(), validation_errors)?;

        Ok(())
    }

    fn validate_relation_type_role_types(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates_declared = TypeReader::get_relates_declared(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            let role = relates.role();

            Self::validate_role_is_unique_for_relation_type_hierarchy(
                snapshot,
                relation_type.clone(),
                role.clone(),
                validation_errors,
            )?;
            Self::validate_annotations(snapshot, role.clone(), validation_errors)?;
        }

        Ok(())
    }

    fn validate_attribute_type(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        Self::validate_annotations(snapshot, type_.clone(), validation_errors)?;

        // TODO: Validate value type against annotations? Validate value type set?

        Ok(())
    }

    fn validate_object_type<T: ObjectTypeAPI<'static> + KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        // TODO: Refactor it to just iterate over owns and call multiple checks in place!
        Self::validate_abstractness_matches_with_owns(snapshot, type_.clone(), validation_errors)?;
        Self::validate_abstractness_matches_with_plays(snapshot, type_.clone(), validation_errors)?;

        Self::validate_overridden_owns(snapshot, type_.clone(), validation_errors)?;
        Self::validate_overridden_plays(snapshot, type_.clone(), validation_errors)?;

        Self::validate_declared_owns_not_overridden(snapshot, type_.clone(), validation_errors)?;
        Self::validate_declared_plays_not_overridden(snapshot, type_.clone(), validation_errors)?;

        Self::validate_redundant_owns(snapshot, type_.clone(), validation_errors)?;
        Self::validate_redundant_plays(snapshot, type_.clone(), validation_errors)?;

        Self::validate_edge_annotations::<T, Plays<'static>>(snapshot, type_.clone(), validation_errors)?;
        Self::validate_edge_annotations::<T, Owns<'static>>(snapshot, type_.clone(), validation_errors)?;

        Ok(())
    }

    fn validate_abstractness_matches_with_owns<T>(
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
            TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, type_.clone())?.keys()
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

        for role_type in TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, type_.clone())?.keys() {
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
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        for role_type in TypeReader::get_relates(snapshot, type_.clone())?.keys() {
            let role_type = role_type.clone();
            if type_is_abstract(snapshot, role_type.clone())? {
                validation_errors.push(SchemaValidationError::NonAbstractCannotRelateAbstract(
                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                ));
            }
        }

        Ok(())
    }

    fn validate_overridden_relates(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let supertype = TypeReader::get_supertype(snapshot, relation_type.clone())?;

        let relates_declared: HashSet<Relates<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            let relates_override_opt = TypeReader::get_implementation_override(snapshot, relates.clone())?;

            if let Some(relates_override) = relates_override_opt {
                let role_type_overridden = relates_override.role();

                let role = relates.role();
                let role_supertype = TypeReader::get_supertype(snapshot, role.clone())?;
                match role_supertype {
                    None => validation_errors.push(SchemaValidationError::RelatesOverrideDoesNotMatchWithRoleSubtype(
                        get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                        get_label_or_concept_read_err(snapshot, role)?,
                        get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                        None,
                    )),
                    Some(role_supertype) => {
                        if role_type_overridden != role_supertype {
                            validation_errors.push(SchemaValidationError::RelatesOverrideDoesNotMatchWithRoleSubtype(
                                get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                                get_label_or_concept_read_err(snapshot, role.clone())?,
                                get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                                Some(get_label_or_concept_read_err(snapshot, role_supertype.clone())?),
                            ));
                        }
                    }
                }

                match &supertype {
                    None => validation_errors.push(SchemaValidationError::RelatesOverrideIsNotInherited(
                        get_label_or_concept_read_err(snapshot, relation_type.clone())?,
                        get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                    )),
                    Some(supertype) => {
                        let contains = TypeReader::get_relates(snapshot, supertype.clone())?
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
            }
        }

        Ok(())
    }

    fn validate_overridden_owns<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let supertype = TypeReader::get_supertype(snapshot, type_.clone())?;

        let owns_declared: HashSet<Owns<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

        for owns in owns_declared {
            let owns_override_opt = TypeReader::get_implementation_override(snapshot, owns.clone())?;

            if let Some(owns_override) = owns_override_opt {
                let attribute_type_overridden = owns_override.attribute();
                match &supertype {
                    None => validation_errors.push(SchemaValidationError::OwnsOverrideIsNotInherited(
                        get_label_or_concept_read_err(snapshot, type_.clone())?,
                        get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                    )),
                    Some(supertype) => {
                        let contains =
                            TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, supertype.clone())?
                                .keys()
                                .contains(&attribute_type_overridden);

                        if !contains {
                            validation_errors.push(SchemaValidationError::OwnsOverrideIsNotInherited(
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                            ));
                        }

                        let attribute_type = owns.attribute();
                        if !is_overridden_interface_object_supertype_or_self(
                            snapshot,
                            attribute_type.clone(),
                            attribute_type_overridden.clone(),
                        )? {
                            validation_errors.push(SchemaValidationError::OverriddenOwnsAttributeTypeIsNotSupertype(
                                get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                                get_label_or_concept_read_err(snapshot, attribute_type_overridden.clone())?,
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_overridden_plays<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let supertype = TypeReader::get_supertype(snapshot, type_.clone())?;

        let plays_declared: HashSet<Plays<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

        for plays in plays_declared {
            let plays_override_opt = TypeReader::get_implementation_override(snapshot, plays.clone())?;

            if let Some(plays_override) = plays_override_opt {
                let role_type_overridden = plays_override.role();
                match &supertype {
                    None => validation_errors.push(SchemaValidationError::PlaysOverrideIsNotInherited(
                        get_label_or_concept_read_err(snapshot, type_.clone())?,
                        get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                    )),
                    Some(supertype) => {
                        let contains =
                            TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, supertype.clone())?
                                .keys()
                                .contains(&role_type_overridden);

                        if !contains {
                            validation_errors.push(SchemaValidationError::PlaysOverrideIsNotInherited(
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                            ));
                        }

                        let role_type = plays.role();
                        if !is_overridden_interface_object_supertype_or_self(
                            snapshot,
                            role_type.clone(),
                            role_type_overridden.clone(),
                        )? {
                            validation_errors.push(SchemaValidationError::OverriddenPlaysRoleTypeIsNotSupertype(
                                get_label_or_concept_read_err(snapshot, role_type.clone())?,
                                get_label_or_concept_read_err(snapshot, role_type_overridden.clone())?,
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_declared_owns_not_overridden<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            let owns_declared: HashSet<Owns<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

            for owns in owns_declared {
                let attribute_type = owns.attribute();
                if is_attribute_type_owns_overridden(snapshot, supertype.clone(), attribute_type.clone())? {
                    validation_errors.push(SchemaValidationError::OverriddenOwnsCannotBeRedeclared(
                        get_label_or_concept_read_err(snapshot, type_.clone())?,
                        attribute_type,
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_declared_plays_not_overridden<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            let plays_declared: HashSet<Plays<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

            for plays in plays_declared {
                let role_type = plays.role();
                if is_role_type_plays_overridden(snapshot, supertype.clone(), role_type.clone())? {
                    validation_errors.push(SchemaValidationError::OverriddenPlaysCannotBeRedeclared(
                        get_label_or_concept_read_err(snapshot, type_.clone())?,
                        role_type,
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_redundant_owns<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            let owns_declared: HashSet<Owns<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

            let supertype_owns =
                TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, supertype.clone())?;

            for owns in owns_declared {
                let attribute_type = owns.attribute();
                if let Some(supertype_owns) = supertype_owns.get(&attribute_type) {
                    let supertype_owns_owner = supertype_owns.owner();

                    let owns_override = TypeReader::get_implementation_override(snapshot, owns.clone())?;
                    let correct_override = match owns_override {
                        None => false,
                        Some(owns_override) => &owns_override == supertype_owns,
                    };
                    if !correct_override {
                        validation_errors.push(
                            SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                                get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, supertype_owns_owner.clone())?,
                            ),
                        );
                    }

                    let owns_annotations_declared = TypeReader::get_type_edge_annotations_declared(snapshot, owns)?;
                    if owns_annotations_declared.is_empty() {
                        validation_errors.push(
                            SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                                get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, supertype_owns_owner.clone())?,
                            ),
                        );
                    }

                    let supertype_owns_annotations =
                        TypeReader::get_type_edge_annotations(snapshot, supertype_owns.clone())?;

                    for owns_annotation in owns_annotations_declared {
                        if supertype_owns_annotations.keys().contains(&owns_annotation) {
                            validation_errors.push(
                                SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForOwns(
                                    get_label_or_concept_read_err(snapshot, attribute_type.clone())?,
                                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                                    get_label_or_concept_read_err(snapshot, supertype_owns_owner.clone())?,
                                    owns_annotation,
                                ),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_redundant_plays<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) = TypeReader::get_supertype(snapshot, type_.clone())? {
            let plays_declared: HashSet<Plays<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())?;

            let supertype_plays =
                TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, supertype.clone())?;

            for plays in plays_declared {
                let role_type = plays.role();
                if let Some(supertype_plays) = supertype_plays.get(&role_type) {
                    let supertype_plays_player = supertype_plays.player();

                    let plays_override = TypeReader::get_implementation_override(snapshot, plays.clone())?;
                    let correct_override = match plays_override {
                        None => false,
                        Some(plays_override) => &plays_override == supertype_plays,
                    };
                    if !correct_override {
                        validation_errors.push(
                            SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                                get_label_or_concept_read_err(snapshot, role_type.clone())?,
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, supertype_plays_player.clone())?,
                            ),
                        );
                    }

                    let plays_annotations_declared = TypeReader::get_type_edge_annotations_declared(snapshot, plays)?;
                    if plays_annotations_declared.is_empty() {
                        validation_errors.push(
                            SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                                get_label_or_concept_read_err(snapshot, role_type.clone())?,
                                get_label_or_concept_read_err(snapshot, type_.clone())?,
                                get_label_or_concept_read_err(snapshot, supertype_plays_player.clone())?,
                            ),
                        );
                    }

                    let supertype_plays_annotations =
                        TypeReader::get_type_edge_annotations(snapshot, supertype_plays.clone())?;

                    for plays_annotation in plays_annotations_declared {
                        if supertype_plays_annotations.keys().contains(&plays_annotation) {
                            validation_errors.push(
                                SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForPlays(
                                    get_label_or_concept_read_err(snapshot, role_type.clone())?,
                                    get_label_or_concept_read_err(snapshot, type_.clone())?,
                                    get_label_or_concept_read_err(snapshot, supertype_plays_player.clone())?,
                                    plays_annotation,
                                ),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_relation_type_has_relates(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let relates = TypeReader::get_relates(snapshot, relation_type.clone())?;

        if relates.is_empty() {
            validation_errors.push(SchemaValidationError::RelationTypeMustRelateAtLeastOneRole(
                get_label_or_concept_read_err(snapshot, relation_type)?,
            ));
        }

        Ok(())
    }

    fn validate_role_is_unique_for_relation_type_hierarchy(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let role_label =
            TypeReader::get_label(snapshot, role_type)?.ok_or(ConceptReadError::CannotGetLabelForExistingType)?;
        let relation_supertypes = TypeReader::get_supertypes(snapshot, relation_type.clone().into_owned())?;
        let relation_subtypes = TypeReader::get_subtypes_transitive(snapshot, relation_type.clone().into_owned())?;

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

    fn validate_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError> {
        let declared_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())?;

        for annotation in declared_annotations {
            let as_annotation = annotation.into();
            let annotation_category = as_annotation.category();

            if let Err(err) = validate_declared_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                type_.clone(),
                annotation_category.clone(),
            ) {
                validation_errors.push(err);
            }

            if let Err(err) = validate_declared_annotation_is_compatible_with_declared_annotations(
                snapshot,
                type_.clone(),
                annotation_category,
            ) {
                validation_errors.push(err);
            }

            // TODO: Validate specific annotation constraints that are called when we set annotations and check their parents... Somehow make it smart...
        }

        Ok(())
    }

    fn validate_edge_annotations<T, EDGE>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        validation_errors: &mut Vec<SchemaValidationError>,
    ) -> Result<(), ConceptReadError>
    where
        T: TypeAPI<'static>,
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Hash + Eq + Clone,
    {
        let interface_implementations = TypeReader::get_implemented_interfaces_declared::<EDGE>(snapshot, type_)?;

        for implementation in interface_implementations {
            let declared_annotations =
                TypeReader::get_type_edge_annotations_declared(snapshot, implementation.clone())?;

            for annotation in declared_annotations {
                let annotation_category = annotation.category();

                if let Err(err) = validate_declared_edge_annotation_is_compatible_with_inherited_annotations(
                    snapshot,
                    implementation.clone(),
                    annotation_category.clone(),
                ) {
                    validation_errors.push(err);
                }

                if let Err(err) = validate_declared_edge_annotation_is_compatible_with_declared_annotations(
                    snapshot,
                    implementation.clone(),
                    annotation_category,
                ) {
                    validation_errors.push(err);
                }

                // TODO: Validate specific annotation constraints that are called when we set annotations and check their parents... Somehow make it smart...
            }
        }

        Ok(())
    }
}
