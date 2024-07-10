/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::Kind;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType,
        entity_type::EntityType,
        owns::Owns,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                get_label,
                validation::{type_is_abstract, validate_role_name_uniqueness_non_transitive},
                SchemaValidationError,
            },
        },
        KindAPI, ObjectTypeAPI, TypeAPI,
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

        if let Err(error) = Self::validate_abstractness_matches_with_owns(snapshot, type_.clone()) {
            errors.push(error)
        }

        Ok(errors)
    }

    fn validate_relation_type(
        snapshot: &impl ReadableSnapshot,
        type_: RelationType<'static>,
    ) -> Result<Vec<SchemaValidationError>, ConceptReadError> {
        let mut errors = Vec::new();

        if let Err(error) = Self::validate_relation_type_has_relates(snapshot, type_.clone()) {
            errors.push(error)
        }

        if let Err(error) = Self::validate_abstractness_matches_with_owns(snapshot, type_.clone()) {
            errors.push(error)
        }

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
            if let Err(error) = Self::validate_role_is_unique_for_relation_type_hierarchy(
                snapshot,
                relation_type.clone(),
                relates.role(),
            ) {
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
        Ok(errors)
    }

    fn validate_abstractness_matches_with_owns<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static> + Clone,
    {
        if type_is_abstract(snapshot, type_.clone())? {
            return Ok(());
        }

        TypeReader::get_implemented_interfaces(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .values()
            .map(Owns::attribute)
            .try_for_each(|attribute_type: AttributeType<'static>| {
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
}
