/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

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
                get_label,
                validation::{
                    is_attribute_type_owns_overridden, is_overridden_interface_object_supertype_or_self,
                    is_role_type_plays_overridden, type_is_abstract,
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
        fn $func_name(snapshot: &impl ReadableSnapshot) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
            let root_label = $kind.root_label();
            let root = TypeReader::get_labelled_type::<$type_<'static>>(snapshot, &root_label)?
                .ok_or(SchemaValidationError::RootHasBeenCorrupted(root_label));

            match root {
                Ok(root) => {
                    let mut errors = Vec::new();
                    errors.append(&mut $func(snapshot, root.clone())?);

                    for subtype in TypeReader::get_subtypes_transitive(snapshot, root)? {
                        errors.append(&mut $func(snapshot, subtype)?);
                    }

                    Ok(errors)
                }
                Err(error) => Ok(vec![error]),
            }
        }
    };
}

impl CommitTimeValidation {
    pub(crate) fn validate(snapshot: &impl ReadableSnapshot) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();
        errors.append(&mut Self::validate_entity_types(snapshot)?);
        errors.append(&mut Self::validate_relation_types(snapshot)?);
        errors.append(&mut Self::validate_attribute_types(snapshot)?);
        Ok(errors)
    }

    validate_types!(validate_entity_types, Kind::Entity, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, Kind::Relation, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, Kind::Attribute, AttributeType, Self::validate_attribute_type);

    fn validate_entity_type(
        snapshot: &impl ReadableSnapshot,
        type_: EntityType<'static>,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        if let Err(error) = Self::validate_annotations(snapshot, type_.clone()) {
            errors.push(error)
        }

        errors.append(&mut Self::validate_object_type(snapshot, type_.clone())?);

        Ok(errors)
    }

    fn validate_relation_type(
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        if let Err(error) = Self::validate_annotations(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_relation_type_has_relates(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_abstractness_matches_with_relates(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_overridden_relates(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) =
            Self::validate_edge_annotations::<RelationType<'static>, Relates<'static>>(snapshot, type_.clone())
        {
            errors.push(error)
        }

        errors.append(&mut Self::validate_object_type(snapshot, type_.clone())?);

        errors.append(&mut Self::validate_relation_type_role_types(snapshot, type_.clone())?);

        Ok(errors)
    }

    fn validate_relation_type_role_types(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        let relates_declared = TypeReader::get_relates_declared(snapshot, relation_type.clone())?;

        for relates in relates_declared {
            let role = relates.role();
            if let Err(error) =
                Self::validate_role_is_unique_for_relation_type_hierarchy(snapshot, relation_type.clone(), role.clone())
            {
                errors.push(error)
            }

            if let Err(error) = Self::validate_annotations(snapshot, role.clone()) {
                errors.push(error)
            }
        }

        Ok(errors)
    }

    fn validate_attribute_type(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        if let Err(error) = Self::validate_annotations(snapshot, type_.clone()) {
            errors.push(error)
        }

        Ok(errors)
    }

    fn validate_object_type<T: ObjectTypeAPI<'static> + KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        if let Err(error) = Self::validate_abstractness_matches_with_owns(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_abstractness_matches_with_plays(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_overridden_owns(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_overridden_plays(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_declared_owns_not_overridden(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_declared_plays_not_overridden(snapshot, type_.clone()) {
            errors.push(error)
        }

        // TODO: Refactor it to just iterate over owns and call multiple checks in place!

        if let Err(error) = Self::validate_redundant_owns(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_redundant_plays(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_edge_annotations::<T, Plays<'static>>(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_edge_annotations::<T, Owns<'static>>(snapshot, type_.clone()) {
            errors.push(error)
        }

        Ok(errors)
    }

    // TODO: return Vec<Errors> everywhere here!

    fn validate_abstractness_matches_with_owns<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .keys()
            .try_for_each(|attribute_type: &AttributeType<'static>| {
                let attribute_type = attribute_type.clone();
                if type_is_abstract(snapshot, attribute_type.clone())? {
                    let owner = type_.clone();
                    Err(SchemaValidationError::NonAbstractCannotOwnAbstract(
                        get_label!(snapshot, owner),
                        get_label!(snapshot, attribute_type),
                    ))
                } else {
                    Ok(())
                }
            })?;

        Ok(())
    }

    // TODO: Add BDD tests and operation time check as for owns
    fn validate_abstractness_matches_with_plays<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .keys()
            .try_for_each(|role_type: &RoleType<'static>| {
                let role_type = role_type.clone();
                if type_is_abstract(snapshot, role_type.clone())? {
                    let owner = type_.clone();
                    Err(SchemaValidationError::NonAbstractCannotPlayAbstract(
                        get_label!(snapshot, owner),
                        get_label!(snapshot, role_type),
                    ))
                } else {
                    Ok(())
                }
            })?;

        Ok(())
    }

    // TODO: Add BDD tests and operation time check as for owns
    fn validate_abstractness_matches_with_relates(
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        TypeReader::get_relates(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .keys()
            .try_for_each(|role_type: &RoleType<'static>| {
                let role_type = role_type.clone();
                if type_is_abstract(snapshot, role_type.clone())? {
                    let owner = type_.clone();
                    Err(SchemaValidationError::NonAbstractCannotRelateAbstract(
                        get_label!(snapshot, owner),
                        get_label!(snapshot, role_type),
                    ))
                } else {
                    Ok(())
                }
            })?;

        Ok(())
    }

    fn validate_overridden_relates(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let supertype =
            TypeReader::get_supertype(snapshot, relation_type.clone()).map_err(SchemaValidationError::ConceptRead)?;

        let relates_declared: HashSet<Relates<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, relation_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for relates in relates_declared {
            let relates_override_opt = TypeReader::get_implementation_override(snapshot, relates.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(relates_override) = relates_override_opt {
                let role_type_overridden = relates_override.role();

                let role = relates.role();
                let role_supertype =
                    TypeReader::get_supertype(snapshot, role.clone()).map_err(SchemaValidationError::ConceptRead)?;
                match role_supertype {
                    None => {
                        return Err(SchemaValidationError::RelatesOverrideDoesNotMatchWithRoleSubtype(
                            get_label!(snapshot, relation_type),
                            get_label!(snapshot, role),
                            get_label!(snapshot, role_type_overridden),
                            None,
                        ))
                    }
                    Some(role_supertype) => {
                        if role_type_overridden != role_supertype {
                            return Err(SchemaValidationError::RelatesOverrideDoesNotMatchWithRoleSubtype(
                                get_label!(snapshot, relation_type),
                                get_label!(snapshot, role),
                                get_label!(snapshot, role_type_overridden),
                                Some(get_label!(snapshot, role_supertype)),
                            ));
                        }
                    }
                }

                match &supertype {
                    None => {
                        return Err(SchemaValidationError::RelatesOverrideIsNotInherited(
                            get_label!(snapshot, relation_type),
                            get_label!(snapshot, role_type_overridden),
                        ))
                    }
                    Some(supertype) => {
                        let contains = TypeReader::get_relates(snapshot, supertype.clone())
                            .map_err(SchemaValidationError::ConceptRead)?
                            .keys()
                            .contains(&role_type_overridden);

                        if !contains {
                            return Err(SchemaValidationError::RelatesOverrideIsNotInherited(
                                get_label!(snapshot, relation_type),
                                get_label!(snapshot, role_type_overridden),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_overridden_owns<T>(snapshot: &impl ReadableSnapshot, type_: T) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let supertype =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;

        let owns_declared: HashSet<Owns<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for owns in owns_declared {
            let owns_override_opt = TypeReader::get_implementation_override(snapshot, owns.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(owns_override) = owns_override_opt {
                let attribute_type_overridden = owns_override.attribute();
                match &supertype {
                    None => {
                        return Err(SchemaValidationError::OwnsOverrideIsNotInherited(
                            get_label!(snapshot, type_),
                            get_label!(snapshot, attribute_type_overridden),
                        ))
                    }
                    Some(supertype) => {
                        let contains =
                            TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, supertype.clone())
                                .map_err(SchemaValidationError::ConceptRead)?
                                .keys()
                                .contains(&attribute_type_overridden);

                        if !contains {
                            return Err(SchemaValidationError::OwnsOverrideIsNotInherited(
                                get_label!(snapshot, type_),
                                get_label!(snapshot, attribute_type_overridden),
                            ));
                        }

                        let attribute_type = owns.attribute();
                        if !is_overridden_interface_object_supertype_or_self(
                            snapshot,
                            attribute_type.clone(),
                            attribute_type_overridden.clone(),
                        )
                        .map_err(SchemaValidationError::ConceptRead)?
                        {
                            return Err(SchemaValidationError::OverriddenOwnsAttributeTypeIsNotSupertype(
                                get_label!(snapshot, attribute_type),
                                get_label!(snapshot, attribute_type_overridden),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_overridden_plays<T>(snapshot: &impl ReadableSnapshot, type_: T) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        let supertype =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;

        let plays_declared: HashSet<Plays<'static>> =
            TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for plays in plays_declared {
            let plays_override_opt = TypeReader::get_implementation_override(snapshot, plays.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(plays_override) = plays_override_opt {
                let role_type_overridden = plays_override.role();
                match &supertype {
                    None => {
                        return Err(SchemaValidationError::PlaysOverrideIsNotInherited(
                            get_label!(snapshot, type_),
                            get_label!(snapshot, role_type_overridden),
                        ))
                    }
                    Some(supertype) => {
                        let contains =
                            TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, supertype.clone())
                                .map_err(SchemaValidationError::ConceptRead)?
                                .keys()
                                .contains(&role_type_overridden);

                        if !contains {
                            return Err(SchemaValidationError::PlaysOverrideIsNotInherited(
                                get_label!(snapshot, type_),
                                get_label!(snapshot, role_type_overridden),
                            ));
                        }

                        let role_type = plays.role();
                        if !is_overridden_interface_object_supertype_or_self(
                            snapshot,
                            role_type.clone(),
                            role_type_overridden.clone(),
                        )
                        .map_err(SchemaValidationError::ConceptRead)?
                        {
                            return Err(SchemaValidationError::OverriddenPlaysRoleTypeIsNotSupertype(
                                get_label!(snapshot, role_type),
                                get_label!(snapshot, role_type_overridden),
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
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?
        {
            let owns_declared: HashSet<Owns<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            for owns in owns_declared {
                let attribute_type = owns.attribute();
                if is_attribute_type_owns_overridden(snapshot, supertype.clone(), attribute_type.clone())
                    .map_err(SchemaValidationError::ConceptRead)?
                {
                    return Err(SchemaValidationError::OverriddenOwnsCannotBeRedeclared(
                        get_label!(snapshot, type_),
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
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?
        {
            let plays_declared: HashSet<Plays<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            for plays in plays_declared {
                let role_type = plays.role();
                if is_role_type_plays_overridden(snapshot, supertype.clone(), role_type.clone())
                    .map_err(SchemaValidationError::ConceptRead)?
                {
                    return Err(SchemaValidationError::OverriddenPlaysCannotBeRedeclared(
                        get_label!(snapshot, type_),
                        role_type,
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_redundant_owns<T>(snapshot: &impl ReadableSnapshot, type_: T) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?
        {
            let owns_declared: HashSet<Owns<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            let supertype_owns =
                TypeReader::get_implemented_interfaces::<Owns<'static>, T>(snapshot, supertype.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            for owns in owns_declared {
                let attribute_type = owns.attribute();
                if let Some(supertype_owns) = supertype_owns.get(&attribute_type) {
                    let supertype_owns_owner = supertype_owns.owner();

                    let owns_override = TypeReader::get_implementation_override(snapshot, owns.clone())
                        .map_err(SchemaValidationError::ConceptRead)?;
                    let correct_override = match owns_override {
                        None => false,
                        Some(owns_override) => &owns_override == supertype_owns,
                    };
                    if !correct_override {
                        return Err(
                            SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                                get_label!(snapshot, attribute_type),
                                get_label!(snapshot, type_),
                                get_label!(snapshot, supertype_owns_owner),
                            ),
                        );
                    }

                    let owns_annotations_declared = TypeReader::get_type_edge_annotations_declared(snapshot, owns)
                        .map_err(SchemaValidationError::ConceptRead)?;
                    if owns_annotations_declared.is_empty() {
                        return Err(
                            SchemaValidationError::CannotRedeclareInheritedOwnsWithoutSpecializationWithOverride(
                                get_label!(snapshot, attribute_type),
                                get_label!(snapshot, type_),
                                get_label!(snapshot, supertype_owns_owner),
                            ),
                        );
                    }

                    let supertype_owns_annotations =
                        TypeReader::get_type_edge_annotations(snapshot, supertype_owns.clone())
                            .map_err(SchemaValidationError::ConceptRead)?;

                    for owns_annotation in owns_annotations_declared {
                        if supertype_owns_annotations.keys().contains(&owns_annotation) {
                            return Err(
                                SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForOwns(
                                    get_label!(snapshot, attribute_type),
                                    get_label!(snapshot, type_),
                                    get_label!(snapshot, supertype_owns_owner),
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

    fn validate_redundant_plays<T>(snapshot: &impl ReadableSnapshot, type_: T) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
        if let Some(supertype) =
            TypeReader::get_supertype(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?
        {
            let plays_declared: HashSet<Plays<'static>> =
                TypeReader::get_implemented_interfaces_declared(snapshot, type_.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            let supertype_plays =
                TypeReader::get_implemented_interfaces::<Plays<'static>, T>(snapshot, supertype.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;

            for plays in plays_declared {
                let role_type = plays.role();
                if let Some(supertype_plays) = supertype_plays.get(&role_type) {
                    let supertype_plays_player = supertype_plays.player();

                    let plays_override = TypeReader::get_implementation_override(snapshot, plays.clone())
                        .map_err(SchemaValidationError::ConceptRead)?;
                    let correct_override = match plays_override {
                        None => false,
                        Some(plays_override) => &plays_override == supertype_plays,
                    };
                    if !correct_override {
                        return Err(
                            SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                                get_label!(snapshot, role_type),
                                get_label!(snapshot, type_),
                                get_label!(snapshot, supertype_plays_player),
                            ),
                        );
                    }

                    let plays_annotations_declared = TypeReader::get_type_edge_annotations_declared(snapshot, plays)
                        .map_err(SchemaValidationError::ConceptRead)?;
                    if plays_annotations_declared.is_empty() {
                        return Err(
                            SchemaValidationError::CannotRedeclareInheritedPlaysWithoutSpecializationWithOverride(
                                get_label!(snapshot, role_type),
                                get_label!(snapshot, type_),
                                get_label!(snapshot, supertype_plays_player),
                            ),
                        );
                    }

                    let supertype_plays_annotations =
                        TypeReader::get_type_edge_annotations(snapshot, supertype_plays.clone())
                            .map_err(SchemaValidationError::ConceptRead)?;

                    for plays_annotation in plays_annotations_declared {
                        if supertype_plays_annotations.keys().contains(&plays_annotation) {
                            return Err(
                                SchemaValidationError::CannotRedeclareInheritedAnnotationWithoutSpecializationForPlays(
                                    get_label!(snapshot, role_type),
                                    get_label!(snapshot, type_),
                                    get_label!(snapshot, supertype_plays_player),
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
    ) -> Result<(), SchemaValidationError> {
        let relates =
            TypeReader::get_relates(snapshot, relation_type.clone()).map_err(SchemaValidationError::ConceptRead)?;

        if !relates.is_empty() {
            Ok(())
        } else {
            Err(SchemaValidationError::RelationTypeMustRelateAtLeastOneRole(get_label!(snapshot, relation_type)))
        }
    }

    fn validate_role_is_unique_for_relation_type_hierarchy(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let role_label = get_label!(snapshot, role_type);
        let relation_supertypes = TypeReader::get_supertypes(snapshot, relation_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?;
        let relation_subtypes = TypeReader::get_subtypes_transitive(snapshot, relation_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?;

        for supertype in relation_supertypes {
            validate_role_name_uniqueness_non_transitive(snapshot, supertype, &role_label)?;
        }
        for subtype in relation_subtypes {
            validate_role_name_uniqueness_non_transitive(snapshot, subtype, &role_label)?;
        }

        Ok(())
    }

    fn validate_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        let declared_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for annotation in declared_annotations {
            let as_annotation = annotation.into();
            let annotation_category = as_annotation.category();
            validate_declared_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                type_.clone(),
                annotation_category.clone(),
            )?;
            validate_declared_annotation_is_compatible_with_declared_annotations(
                snapshot,
                type_.clone(),
                annotation_category,
            )?;

            // TODO: Validate specific annotation constraints that are called when we set annotations and check their parents... Somehow make it smart...
        }

        Ok(())
    }

    fn validate_edge_annotations<T, EDGE>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: TypeAPI<'static>,
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Hash + Eq + Clone,
    {
        let interface_implementations = TypeReader::get_implemented_interfaces_declared::<EDGE>(snapshot, type_)
            .map_err(SchemaValidationError::ConceptRead)?;

        for implementation in interface_implementations {
            let declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, implementation.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

            for annotation in declared_annotations {
                let annotation_category = annotation.category();
                validate_declared_edge_annotation_is_compatible_with_inherited_annotations(
                    snapshot,
                    implementation.clone(),
                    annotation_category.clone(),
                )?;
                validate_declared_edge_annotation_is_compatible_with_declared_annotations(
                    snapshot,
                    implementation.clone(),
                    annotation_category,
                )?;

                // TODO: Validate specific annotation constraints that are called when we set annotations and check their parents... Somehow make it smart...
            }
        }

        Ok(())
    }
}
