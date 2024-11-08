/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use answer::Type as TypeEnum;
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{Annotation, AnnotationError},
        attribute_type::AttributeType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager,
        KindAPI, Ordering, TypeAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    query::schema::Redefine,
    schema::definable::{
        function::Function,
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Relates as TypeQLRelates},
            Capability, CapabilityBase,
        },
        Struct, Type,
    },
    Definable,
};

use crate::{
    definable_resolution::{
        filter_variants, get_struct_field_value_type_optionality, resolve_attribute_type, resolve_relates,
        resolve_role_type, resolve_struct_definition_key, resolve_typeql_type, resolve_value_type, try_unwrap,
        type_ref_to_label_and_ordering, type_to_object_type, SymbolResolutionError,
    },
    definable_status::{
        get_capability_annotation_status, get_owns_status, get_plays_status, get_relates_status,
        get_struct_field_status, get_sub_status, get_type_annotation_status, get_value_type_status, DefinableStatus,
    },
};

macro_rules! verify_no_annotations_for_capability {
    ($capability:ident, $annotation_error:path) => {
        if let Some(typeql_annotation) = &$capability.annotations.first() {
            let annotation = translate_annotation(typeql_annotation)
                .map_err(|source| RedefineError::LiteralParseError { source })?;
            Err(RedefineError::IllegalCapabilityAnnotation {
                declaration: $capability.clone(),
                source: $annotation_error(annotation.category()),
                annotation: annotation,
            })
        } else {
            Ok(())
        }
    };
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefine: Redefine,
) -> Result<(), RedefineError> {
    let redefined_structs = process_struct_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    let redefined_types = process_type_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    let redefined_functions = process_function_redefinitions(snapshot, type_manager, &redefine.definables)?;

    if !redefined_structs && !redefined_types && !redefined_functions {
        Err(RedefineError::NothingRedefined {})
    } else {
        Ok(())
    }
}

fn process_struct_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut anything_redefined = false;
    filter_variants!(Definable::Struct : definables).try_for_each(|struct_| {
        redefine_struct_fields(snapshot, type_manager, thing_manager, &mut anything_redefined, struct_)
    })?;
    Ok(anything_redefined)
}

fn process_type_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut anything_redefined = false;
    let declarations = filter_variants!(Definable::TypeDeclaration : definables);
    declarations
        .clone()
        .try_for_each(|declaration| redefine_alias(snapshot, type_manager, &mut anything_redefined, declaration))?;
    declarations.clone().try_for_each(|declaration| {
        redefine_value_type(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    declarations.clone().try_for_each(|declaration| {
        redefine_type_annotations(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    declarations.clone().try_for_each(|declaration| {
        redefine_sub(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    declarations.clone().try_for_each(|declaration| {
        redefine_relates(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    declarations.clone().try_for_each(|declaration| {
        redefine_owns(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    declarations.clone().try_for_each(|declaration| {
        redefine_plays(snapshot, type_manager, thing_manager, &mut anything_redefined, declaration)
    })?;
    Ok(anything_redefined)
}

fn process_function_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut anything_redefined = false;
    filter_variants!(Definable::Function : definables)
        .try_for_each(|function| redefine_function(snapshot, type_manager, &mut anything_redefined, function))?;
    Ok(anything_redefined)
}

fn redefine_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    struct_definable: &Struct,
) -> Result<(), RedefineError> {
    let name = struct_definable.ident.as_str();
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let definition_status = get_struct_field_status(
            snapshot,
            type_manager,
            struct_key.clone(),
            field.key.as_str(),
            value_type.clone(),
            optional,
        )
        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::StructFieldDoesNotExist { declaration: field.to_owned() });
            }
            DefinableStatus::ExistsSame(_) => {
                return Err(RedefineError::StructFieldRemainsSame { declaration: field.to_owned() });
            }
            DefinableStatus::ExistsDifferent(_) => {}
        }

        error_if_anything_redefined_else_set_true(anything_redefined)?;
        type_manager.delete_struct_field(snapshot, thing_manager, struct_key.clone(), field.key.as_str()).map_err(
            |err| RedefineError::StructFieldDeleteError {
                struct_name: name.to_owned(),
                declaration: field.clone(),
                typedb_source: err,
            },
        )?;

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
            .map_err(|err| RedefineError::StructFieldCreateError {
                struct_name: name.to_owned(),
                declaration: field.clone(),
                typedb_source: err,
            })?;
    }
    Ok(())
}

fn redefine_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for typeql_annotation in &type_declaration.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        match type_.clone() {
            TypeEnum::Entity(entity) => {
                if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
                    snapshot,
                    type_manager,
                    &label,
                    entity.clone(),
                    annotation.clone(),
                    type_declaration,
                )? {
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    entity.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            label: label.to_owned(),
                            annotation,
                            declaration: type_declaration.clone(),
                            typedb_source: source,
                        }
                    })?;
                }
            }
            TypeEnum::Relation(relation) => {
                if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
                    snapshot,
                    type_manager,
                    &label,
                    relation.clone(),
                    annotation.clone(),
                    type_declaration,
                )? {
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    relation.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            label: label.to_owned(),
                            annotation,
                            declaration: type_declaration.clone(),
                            typedb_source: source,
                        }
                    })?;
                }
            }
            TypeEnum::Attribute(attribute) => {
                if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
                    snapshot,
                    type_manager,
                    &label,
                    attribute.clone(),
                    annotation.clone(),
                    type_declaration,
                )? {
                    if converted.is_value_type_annotation() {
                        return Err(RedefineError::IllegalTypeAnnotation {
                            label: label.to_owned(),
                            annotation: annotation.clone(),
                            declaration: type_declaration.clone(),
                            source: AnnotationError::UnsupportedAnnotationForAttributeType(annotation.category()),
                        });
                    }
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    attribute.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            label: label.to_owned(),
                            annotation,
                            declaration: type_declaration.clone(),
                            typedb_source: source,
                        }
                    })?;
                }
            }
            TypeEnum::RoleType(_) => unreachable!("Role annotations are syntactically on relates"),
        }
    }
    Ok(())
}

fn redefine_alias(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(RedefineError::Unimplemented { description: "Aliases redefinition.".to_string() }))?;

    // TODO: Uncomment when alias is implemented
    // redefine_alias_annotations(capability)?;

    Ok(())
}

fn redefine_alias_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    Err(RedefineError::Unimplemented { description: "Alias redefinition is not yet implmented.".to_string() })
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
}

fn redefine_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };

        let supertype_label = Label::parse_from(sub.supertype_label.ident.as_str());
        let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
        if type_.kind() != supertype.kind() {
            Err(err_capability_kind_mismatch(&label, &supertype_label, capability, type_.kind(), supertype.kind()))?;
        }

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                    RedefineError::SetSupertype { declaration: sub.clone(), typedb_source: source }
                })?;
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                    RedefineError::SetSupertype { declaration: sub.clone(), typedb_source: source }
                })?;
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                    RedefineError::SetSupertype { declaration: sub.clone(), typedb_source: source }
                })?;
            }
            (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => {
                return Err(err_unsupported_capability(&label, Kind::Role, capability));
            }
            _ => unreachable!(),
        }

        redefine_sub_annotations(capability)?;
    }
    Ok(())
}

fn redefine_sub_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    verify_no_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
}

fn redefine_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::ValueType(value_type_statement) = &capability.base else {
            continue;
        };
        let TypeEnum::Attribute(attribute_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let value_type = resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
            .map_err(|source| RedefineError::ValueTypeSymbolResolution { typedb_source: source })?;

        let definition_status =
            get_value_type_status(snapshot, type_manager, attribute_type.clone(), value_type.clone())
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        let redefine_needed = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::AttributeTypeValueTypeIsNotDefined { label: label.to_owned(), value_type });
            }
            DefinableStatus::ExistsSame(_) => false, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent(_) => true,
        };

        if redefine_needed {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            attribute_type.set_value_type(snapshot, type_manager, thing_manager, value_type.clone()).map_err(
                |source| RedefineError::SetValueType { label: label.to_owned(), value_type, typedb_source: source },
            )?;
        }

        redefine_value_type_annotations(
            snapshot,
            type_manager,
            thing_manager,
            anything_redefined,
            attribute_type.clone(),
            &label,
            capability,
            type_declaration,
        )?;
    }
    Ok(())
}

fn redefine_value_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    attribute_type: AttributeType<'a>,
    attribute_type_label: &Label<'a>,
    typeql_capability: &Capability,
    typeql_type_declaration: &Type,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            attribute_type_label,
            attribute_type.clone(),
            annotation.clone(),
            typeql_type_declaration,
        )? {
            if !converted.is_value_type_annotation() {
                return Err(RedefineError::IllegalCapabilityAnnotation {
                    declaration: typeql_capability.clone(),
                    source: AnnotationError::UnsupportedAnnotationForValueType(annotation.category()),
                    annotation,
                });
            }

            error_if_anything_redefined_else_set_true(anything_redefined)?;
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    declaration: typeql_capability.clone(),
                    annotation,
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn redefine_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(typeql_relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_relates.related)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let definition_status =
            get_relates_status(snapshot, type_manager, relation_type.clone(), &role_label, ordering)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        let relates = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::RelatesIsNotDefined {
                    label: label.clone(),
                    role: role_label.name.to_string(),
                    declaration: capability.to_owned(),
                    ordering,
                });
            }
            DefinableStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinableStatus::ExistsSame(Some((existing_relates, _))) => existing_relates, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent((existing_relates, _)) => {
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                existing_relates.role().set_ordering(snapshot, type_manager, thing_manager, ordering).map_err(
                    |source| RedefineError::SetRelatesOrdering {
                        label: label.clone().into_owned(),
                        declaration: typeql_relates.to_owned(),
                        typedb_source: source,
                    },
                )?;
                existing_relates
            }
        };

        redefine_relates_annotations(
            snapshot,
            type_manager,
            thing_manager,
            anything_redefined,
            &label,
            relates.clone(),
            capability,
        )?;
        redefine_relates_specialise(
            snapshot,
            type_manager,
            thing_manager,
            anything_redefined,
            &label,
            relates,
            typeql_relates,
        )?;
    }
    Ok(())
}

fn redefine_relates_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    relation_label: &Label<'_>,
    relates: Relates<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            relation_label,
            relates.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            relates.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    annotation,
                    declaration: typeql_capability.clone(),
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn redefine_relates_specialise<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    relation_label: &Label<'_>,
    relates: Relates<'static>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), RedefineError> {
    if let Some(specialised_label) = &typeql_relates.specialised {
        let specialised_relates =
            resolve_relates(snapshot, type_manager, relates.relation(), &specialised_label.ident.as_str())
                .map_err(|typedb_source| RedefineError::DefinitionResolution { typedb_source })?;

        let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::RelatesSpecialiseIsNotDefined {
                    label: relation_label.clone().into_owned(),
                    new_specialise_type: relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                        .clone()
                        .into_owned(),
                    declaration: typeql_relates.clone(),
                })
            }
            DefinableStatus::ExistsSame(_) => {
                return Err(RedefineError::RelatesSpecialiseRemainsSame {
                    label: relation_label.clone().into_owned(),
                    specialised_label: specialised_relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                        .clone()
                        .into_owned(),
                    declaration: typeql_relates.clone(),
                })
            }
            DefinableStatus::ExistsDifferent(_) => {}
        };

        error_if_anything_redefined_else_set_true(anything_redefined)?;
        relates.set_specialise(snapshot, type_manager, thing_manager, specialised_relates).map_err(|source| {
            RedefineError::SetRelatesSpecialise {
                label: relation_label.clone().into_owned(),
                declaration: typeql_relates.clone(),
                typedb_source: source,
            }
        })?;
    }
    Ok(())
}

fn redefine_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Owns(typeql_owns) = &capability.base else {
            continue;
        };

        let (attr_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_owns.owned)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
        let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status =
            get_owns_status(snapshot, type_manager, object_type.clone(), attribute_type.clone(), ordering)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        let owns = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::OwnsIsNotDefined {
                    label: label.clone(),
                    attribute: attribute_type
                        .get_label(snapshot, type_manager)
                        .map_err(|err| RedefineError::UnexpectedConceptRead { source: err })?
                        .as_reference()
                        .into_owned(),
                    declaration: capability.to_owned(),
                    ordering,
                });
            }
            DefinableStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinableStatus::ExistsSame(Some((existing_owns, _))) => existing_owns, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent((existing_owns, _)) => {
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                existing_owns.set_ordering(snapshot, type_manager, thing_manager, ordering).map_err(|source| {
                    RedefineError::SetOwnsOrdering {
                        label: label.clone().into_owned(),
                        declaration: typeql_owns.clone(),
                        typedb_source: source,
                    }
                })?;
                existing_owns
            }
        };

        redefine_owns_annotations(snapshot, type_manager, thing_manager, anything_redefined, &label, owns, capability)?;
    }
    Ok(())
}

fn redefine_owns_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    owner_label: &Label<'_>,
    owns: Owns<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            owner_label,
            owns.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    declaration: typeql_capability.clone(),
                    annotation,
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn redefine_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(typeql_plays) = &capability.base else {
            continue;
        };

        let role_label =
            Label::build_scoped(typeql_plays.role.name.ident.as_str(), typeql_plays.role.scope.ident.as_str());
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status = get_plays_status(snapshot, type_manager, object_type.clone(), role_type.clone())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        let plays = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::PlaysIsNotDefined {
                    label: label.clone(),
                    role_label: role_type
                        .get_label(snapshot, type_manager)
                        .map_err(|err| RedefineError::UnexpectedConceptRead { source: err })?
                        .as_reference()
                        .into_owned(),
                    declaration: capability.to_owned(),
                });
            }
            DefinableStatus::ExistsSame(Some(existing_plays)) => existing_plays,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing plays concept expected"),
            DefinableStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
        };

        redefine_plays_annotations(
            snapshot,
            type_manager,
            thing_manager,
            anything_redefined,
            &label,
            plays,
            capability,
        )?;
    }
    Ok(())
}

fn redefine_plays_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    player_label: &Label<'_>,
    plays: Plays<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            player_label,
            plays.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation { declaration: todo!(), annotation, typedb_source: source }
            })?;
        }
    }
    Ok(())
}

fn redefine_function(
    _snapshot: &impl WritableSnapshot,
    _type_manager: &TypeManager,
    _anything_redefined: &mut bool,
    _function_declaration: &Function,
) -> Result<(), RedefineError> {
    Err(RedefineError::Unimplemented { description: "Function redefinition.".to_string() })
}

fn check_can_redefine_sub<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    new_supertype: T,
) -> Result<(), RedefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype.clone())
        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Err(RedefineError::TypeSubIsNotDefined {
            label: label.clone().into_owned(),
            new_supertype_label: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
        }),
        DefinableStatus::ExistsSame(_) => Err(RedefineError::TypeSubRemainsSame {
            label: label.clone().into_owned(),
            supertype_label: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
        }),
        DefinableStatus::ExistsDifferent(_) => Ok(()),
    }
}

fn type_convert_and_validate_annotation_redefinition_need<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    annotation: Annotation,
    typeql_declaration: &Type,
) -> Result<Option<T::AnnotationType>, RedefineError> {
    error_if_not_redefinable(label, annotation.clone())?;

    let converted =
        T::AnnotationType::try_from(annotation.clone()).map_err(|source| RedefineError::IllegalTypeAnnotation {
            label: label.as_reference().into_owned(),
            annotation: annotation.clone(),
            declaration: typeql_declaration.clone(),
            source,
        })?;

    let definition_status =
        get_type_annotation_status(snapshot, type_manager, type_, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => {
            Err(RedefineError::TypeAnnotationIsNotDefined { label: label.clone().into_owned(), annotation })
        }
        DefinableStatus::ExistsSame(_) => {
            Err(RedefineError::TypeAnnotationRemainsSame { label: label.clone().into_owned(), annotation })
        }
        DefinableStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

fn capability_convert_and_validate_annotation_redefinition_need<'a, CAP: concept::type_::Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    annotation: Annotation,
    typeql_capability: &Capability,
) -> Result<Option<CAP::AnnotationType>, RedefineError> {
    error_if_not_redefinable(label, annotation.clone())?;

    let converted = CAP::AnnotationType::try_from(annotation.clone()).map_err(|source| {
        RedefineError::IllegalCapabilityAnnotation {
            declaration: typeql_capability.clone(),
            annotation: annotation.clone(),
            source,
        }
    })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, &capability, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => {
            Err(RedefineError::CapabilityAnnotationIsNotDefined { declaration: typeql_capability.clone(), annotation })
        }
        DefinableStatus::ExistsSame(_) => {
            Err(RedefineError::CapabilityAnnotationRemainsSame { declaration: typeql_capability.clone(), annotation })
        }
        DefinableStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

fn error_if_not_redefinable(label: &Label<'_>, annotation: Annotation) -> Result<(), RedefineError> {
    match annotation.category().has_parameter() {
        false => Err(RedefineError::ParameterFreeAnnotationCannotBeRedefined {
            label: label.clone().into_owned(),
            annotation,
        }),
        true => Ok(()),
    }
}

fn err_capability_kind_mismatch(
    left: &Label<'_>,
    right: &Label<'_>,
    capability: &Capability,
    left_kind: Kind,
    right_kind: Kind,
) -> RedefineError {
    RedefineError::CapabilityKindMismatch {
        left: left.clone().into_owned(),
        right: right.clone().into_owned(),
        left_kind,
        right_kind,
        declaration: capability.clone(),
    }
}

fn error_if_anything_redefined_else_set_true(anything_redefined: &mut bool) -> Result<(), RedefineError> {
    match anything_redefined {
        true => Err(RedefineError::CanOnlyRedefineOneThingPerQuery {}),
        false => {
            *anything_redefined = true;
            Ok(())
        }
    }
}

typedb_error!(
    pub RedefineError(component = "Redefine execution", prefix = "REX") {
        Unimplemented(1, "Unimplemented redefine functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error during redefine query execution.", ( source: ConceptReadError )),
        NothingRedefined(3, "Nothing was redefined."),
        DefinitionResolution(4, "Could not find symbol in redefine query.", ( typedb_source: SymbolResolutionError )),
        LiteralParseError(5, "Error parsing literal in redefine query.", ( source : LiteralParseError )),
        CanOnlyRedefineOneThingPerQuery(6, "Redefine queries can currently only mutate exactly one schema element per query."),
        StructFieldDoesNotExist(7, "Struct field used in redefine query does not exist.\nSource:\n{declaration}", declaration: Field),
        StructFieldRemainsSame(8, "Struct field in redefine was not changed. Redefine queries are required to update the schema.\nSource:\n{declaration}", declaration: Field),
        StructFieldDeleteError(
            9,
            "Error removing field from struct type '{struct_name}'.\nSource:\n{declaration}",
            struct_name: String,
            declaration: Field,
            ( typedb_source: ConceptWriteError )
        ),
        StructFieldCreateError(
            10,
            "Error creating new field in struct type '{struct_name}'.\nSource:\n{declaration}",
            struct_name: String,
            declaration: Field,
            ( typedb_source: ConceptWriteError )
        ),
        SetSupertype(
            11,
            "Error setting supertype during redefine.\nSource:\n{declaration}",
            declaration: typeql::schema::definable::type_::capability::Sub,
            ( typedb_source: ConceptWriteError )
        ),
        TypeCannotHaveCapability(
            12,
            "Invalid redefine - the type '{label}' of kind '{kind}', which is not allowed to declare:\n'{declaration}'",
            label: Label<'static>,
            kind: Kind,
            declaration: Capability
        ),
        ValueTypeSymbolResolution(
            13,
            "Error resolving value type in redefine query.",
            ( typedb_source: SymbolResolutionError )
        ),
        TypeSubIsNotDefined(
            14,
            "Redefining the supertype of '{label}' to '{new_supertype_label}' failed, since there is no supertype to replace. Use define to set instead of replace the supertype.",
            label: Label<'static>,
            new_supertype_label: Label<'static>
        ),
        TypeSubRemainsSame(
            15,
            "Redefining the supertype of '{label}' to '{supertype_label}' failed, since this is already the supertype. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            supertype_label: Label<'static>
        ),
        RelatesIsNotDefined(
            16,
            "On type '{label}', redefining 'relates {role}{ordering}' failed since it this 'relates' doesn't yet exist. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            label: Label<'static>,
            role: String,
            ordering: Ordering,
            declaration: Capability
        ),
        OwnsIsNotDefined(
            17,
            "On type '{label}', redefining 'owns {attribute}{ordering}' failed since this 'owns' doesn't exist yet. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            label: Label<'static>,
            attribute: Label<'static>,
            declaration: Capability,
            ordering: Ordering
        ),
        PlaysIsNotDefined(
            18,
            "On type '{label}', redefining 'plays {role_label}' failed since this 'plays' doesn't exist yet. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            label: Label<'static>,
            role_label: Label<'static>,
            declaration: Capability
        ),
        RelatesSpecialiseIsNotDefined(
            19,
            "For type '{label}', redefining specialise 'as {new_specialise_type}' failed as no specialise exists yet. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            label: Label<'static>,
            new_specialise_type: Label<'static>,
            declaration: TypeQLRelates
        ),
        RelatesSpecialiseRemainsSame(
            20,
            "For type '{label}', redefining specialise 'as {specialised_label}' failed since this specialise already exists. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            label: Label<'static>,
            specialised_label: Label<'static>,
            declaration: TypeQLRelates
        ),
        AttributeTypeValueTypeIsNotDefined(
            21,
            "For attribute type '{label}', redefining value type '{value_type}' failed since no value type exists yet. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            value_type: ValueType
        ),
        AttributeTypeValueTypeRemainsSame(
            22,
            "For attribute type '{label}', redefining value type '{value_type}' failed since this value type already exists. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            value_type: ValueType
        ),
        TypeAnnotationIsNotDefined(
            23,
            "For type '{label}', redefining annotation '{annotation}' failed since this annotation doesn't exist yet. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            annotation: Annotation
        ),
        TypeAnnotationRemainsSame(
            24,
            "For type '{label}', redefining annotation '{annotation}' failed since this annotation already exist identically. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            annotation: Annotation
        ),
        CapabilityAnnotationIsNotDefined(
            25,
            "Redefining annotation '{annotation}' failed since this annotation doesn't exist yet. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            declaration: Capability,
            annotation: Annotation
        ),
        CapabilityAnnotationRemainsSame(
            26,
            "Redefining annotation '{annotation}' failed since this annotation exists identically. Redefine can only replace existing schema elements.\nSource:\n{declaration}",
            declaration: Capability,
            annotation: Annotation
        ),
        ParameterFreeAnnotationCannotBeRedefined(
            27,
            "For type '{label}', annotation '{annotation}' can never be redefined as it carries no parameters. Redefine can only replace existing schema elements.",
            label: Label<'static>,
            annotation: Annotation
        ),
        SetValueType(
            28,
            "Redefining '{label}' to have value type '{value_type}' failed.",
            label: Label<'static>,
            value_type: ValueType,
            ( typedb_source: ConceptWriteError )
        ),
        SetRelatesOrdering(
            29,
            "Redefining '{label}' to have an updated 'relates' ordering failed.\nSource:\n{declaration}",
            label: Label<'static>,
            declaration: TypeQLRelates,
            ( typedb_source: ConceptWriteError )
        ),
        SetOwnsOrdering(
            30,
            "Redefining '{label}' to have an updated 'owns' ordering failed.\nSource:\n{declaration}",
            label: Label<'static>,
            declaration: TypeQLOwns,
            ( typedb_source: ConceptWriteError )
        ),
        IllegalTypeAnnotation(
            31,
            "Redefining '{label}' to have annotation '{annotation}' failed as this is an illegal annotation.\nSource:\n:{declaration}",
            label: Label<'static>,
            annotation: Annotation,
            declaration: Type,
            ( source: AnnotationError )
        ),
        IllegalCapabilityAnnotation(
            32,
            "Redefining to have annotation '{annotation}' failed.\nSource:\n{declaration}",
            annotation: Annotation,
            declaration: Capability,
            ( source: AnnotationError )
        ),
        SetTypeAnnotation(
            33,
            "Redefining '{label}' to have annotation '{annotation}' failed.\nSource:\n{declaration}",
            label: Label<'static>,
            annotation: Annotation,
            declaration: Type,
            ( typedb_source: ConceptWriteError )
        ),
        SetCapabilityAnnotation(
            34,
            "Redefining '{annotation}' failed.\nSource:\n{declaration}",
            annotation: Annotation,
            declaration: Capability,
            ( typedb_source: ConceptWriteError )
        ),
        SetRelatesSpecialise(
            35,
            "For relation type '{label}', redefining 'relates' failed.\nSource:\n{declaration}",
            label: Label<'static>,
            declaration: TypeQLRelates,
            ( typedb_source: ConceptWriteError )
        ),
        CapabilityKindMismatch(
            36,
            "Redefine failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'.\nSource:\n{declaration}",
            left: Label<'static>,
            right: Label<'static>,
            left_kind: Kind,
            right_kind: Kind,
            declaration: Capability
        ),
    }
);

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &Capability) -> RedefineError {
    RedefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, declaration: capability.clone() }
}

impl fmt::Display for RedefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for RedefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
