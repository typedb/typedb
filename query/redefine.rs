/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
use function::{function::SchemaFunction, function_manager::FunctionManager, FunctionError};
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::{error::TypeQLError, Span, Spanned},
    query::schema::Redefine,
    schema::definable::{
        function::Function,
        type_::{capability::Relates as TypeQLRelates, Capability, CapabilityBase},
        Struct, Type,
    },
    token::Keyword,
    Definable,
};

use crate::{
    definable_resolution::{
        filter_variants, get_struct_field_value_type_optionality, resolve_attribute_type, resolve_relates,
        resolve_role_type, resolve_struct_definition_key, resolve_typeql_type, resolve_value_type,
        type_ref_to_label_and_ordering, type_to_object_type, SymbolResolutionError,
    },
    definable_status::{
        get_capability_annotation_status, get_owns_status, get_plays_status, get_relates_status,
        get_struct_field_status, get_sub_status, get_type_annotation_status, get_value_type_status, DefinableStatus,
        DefinableStatusMode,
    },
};

macro_rules! verify_no_annotations_for_capability {
    ($capability:ident, $annotation_error:path, $error_arg_name:ident) => {
        if let Some(typeql_annotation) = &$capability.annotations.first() {
            let annotation = translate_annotation(typeql_annotation)
                .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
            let error = { $annotation_error { $error_arg_name: annotation.category() } };
            Err(RedefineError::IllegalCapabilityAnnotation {
                source_span: $capability.span(),
                typedb_source: error,
                annotation,
            })
        } else {
            Ok(())
        }
    };
}

fn checked_identifier(identifier: &typeql::Identifier) -> Result<&str, RedefineError> {
    identifier.as_str_unreserved().map_err(|_source| {
        let TypeQLError::ReservedKeywordAsIdentifier { identifier } = _source else { unreachable!() };
        RedefineError::IllegalKeywordAsIdentifier { source_span: identifier.span(), identifier }
    })
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    function_manager: &FunctionManager,
    redefine: Redefine,
) -> Result<(), RedefineError> {
    let redefined_structs = process_struct_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    let redefined_types = process_type_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    let redefined_functions = process_function_redefinitions(snapshot, function_manager, &redefine.definables)?;
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
    function_manager: &FunctionManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut anything_redefined = false;
    for function in filter_variants!(Definable::Function : definables) {
        redefine_function(snapshot, function_manager, &mut anything_redefined, function)?;
    }
    Ok(anything_redefined)
}

fn redefine_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    struct_definable: &Struct,
) -> Result<(), RedefineError> {
    let name = checked_identifier(&struct_definable.ident)?;
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let definition_status = get_struct_field_status(
            snapshot,
            type_manager,
            struct_key.clone(),
            checked_identifier(&field.key)?,
            value_type.clone(),
            optional,
        )
        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::StructFieldDoesNotExist { source_span: field.span() });
            }
            DefinableStatus::ExistsSame(_) => {
                return Err(RedefineError::StructFieldRemainsSame { source_span: field.span() });
            }
            DefinableStatus::ExistsDifferent(_) => {}
        }

        error_if_anything_redefined_else_set_true(anything_redefined)?;
        type_manager
            .delete_struct_field(snapshot, thing_manager, struct_key.clone(), checked_identifier(&field.key)?)
            .map_err(|err| RedefineError::StructFieldDeleteError {
                struct_name: name.to_owned(),
                source_span: field.span(),
                typedb_source: err,
            })?;

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), checked_identifier(&field.key)?, value_type, optional)
            .map_err(|err| RedefineError::StructFieldCreateError {
                struct_name: name.to_owned(),
                source_span: field.span(),
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
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for typeql_annotation in &type_declaration.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
        match type_ {
            TypeEnum::Entity(entity) => {
                if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
                    snapshot,
                    type_manager,
                    &label,
                    entity,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    entity.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            type_: label.to_owned(),
                            annotation,
                            source_span: type_declaration.span(),
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
                    relation,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    relation.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            type_: label.to_owned(),
                            annotation,
                            source_span: type_declaration.span(),
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
                    attribute,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    if converted.is_value_type_annotation() {
                        return Err(RedefineError::IllegalTypeAnnotation {
                            type_: label.to_owned(),
                            annotation: annotation.clone(),
                            source_span: type_declaration.span(),
                            typedb_source: AnnotationError::UnsupportedAnnotationForAttributeType {
                                category: annotation.category(),
                            },
                        });
                    }
                    error_if_anything_redefined_else_set_true(anything_redefined)?;
                    attribute.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetTypeAnnotation {
                            type_: label.to_owned(),
                            annotation,
                            source_span: type_declaration.span(),
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
    _snapshot: &mut impl WritableSnapshot,
    _type_manager: &TypeManager,
    _anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Alias(_alias) = &capability.base else {
            continue;
        };
        return Err(RedefineError::Unimplemented { description: "Alias definition.".to_string() });
        // redefine_alias_annotations(capability) // Uncomment when implemented
    }

    Ok(())
}

fn redefine_alias_annotations(_typeql_capability: &Capability) -> Result<(), RedefineError> {
    Err(RedefineError::Unimplemented { description: "Alias redefinition is not yet implemented.".to_string() })
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
}

fn redefine_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };

        let supertype_label =
            Label::parse_from(checked_identifier(&sub.supertype_label.ident)?, sub.supertype_label.span());
        let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
        if type_.kind() != supertype.kind() {
            Err(err_capability_kind_mismatch(&label, &supertype_label, capability, type_.kind(), supertype.kind()))?;
        }

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_
                    .set_supertype(snapshot, type_manager, thing_manager, supertype)
                    .map_err(|source| RedefineError::SetSupertype { source_span: sub.span(), typedb_source: source })?;
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_
                    .set_supertype(snapshot, type_manager, thing_manager, supertype)
                    .map_err(|source| RedefineError::SetSupertype { source_span: sub.span(), typedb_source: source })?;
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                check_can_redefine_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                type_
                    .set_supertype(snapshot, type_manager, thing_manager, supertype)
                    .map_err(|source| RedefineError::SetSupertype { source_span: sub.span(), typedb_source: source })?;
            }
            (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => {
                unreachable!("RoleType's sub is controlled by specialise")
            } // Turn into an error
            _ => unreachable!(),
        }

        redefine_sub_annotations(capability)?;
    }
    Ok(())
}

fn redefine_sub_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    verify_no_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub, category)
}

fn redefine_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
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

        let definition_status = get_value_type_status(
            snapshot,
            type_manager,
            *attribute_type,
            value_type.clone(),
            DefinableStatusMode::Declared,
        )
        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        let redefine_needed = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::AttributeTypeValueTypeNotDefined {
                    type_: label.to_owned(),
                    key: Keyword::Value,
                    value_type,
                    source_span: capability.span(),
                });
            }
            DefinableStatus::ExistsSame(_) => false, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent(_) => true,
        };

        if redefine_needed {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            attribute_type.set_value_type(snapshot, type_manager, thing_manager, value_type.clone()).map_err(
                |source| RedefineError::SetValueType {
                    type_: label.to_owned(),
                    value_type,
                    source_span: value_type_statement.span(),
                    typedb_source: source,
                },
            )?;
        }

        redefine_value_type_annotations(
            snapshot,
            type_manager,
            thing_manager,
            anything_redefined,
            *attribute_type,
            &label,
            capability,
            type_declaration,
        )?;
    }
    Ok(())
}

fn redefine_value_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    attribute_type: AttributeType,
    attribute_type_label: &Label,
    typeql_capability: &Capability,
    typeql_type_declaration: &Type,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            attribute_type_label,
            attribute_type,
            annotation.clone(),
            typeql_annotation,
            typeql_type_declaration,
        )? {
            if !converted.is_value_type_annotation() {
                return Err(RedefineError::IllegalCapabilityAnnotation {
                    source_span: typeql_capability.span(),
                    typedb_source: AnnotationError::UnsupportedAnnotationForValueType {
                        category: annotation.category(),
                    },
                    annotation,
                });
            }

            error_if_anything_redefined_else_set_true(anything_redefined)?;
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    source_span: typeql_capability.span(),
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
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
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

        let definition_status = get_relates_status(
            snapshot,
            type_manager,
            *relation_type,
            &role_label,
            ordering,
            DefinableStatusMode::Declared,
        )
        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        let relates = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::RelatesNotDefined {
                    type_: label.clone(),
                    key: Keyword::Relates,
                    role: role_label.name.to_string(),
                    source_span: capability.span(),
                    ordering,
                });
            }
            DefinableStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinableStatus::ExistsSame(Some((existing_relates, _))) => existing_relates, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent((existing_relates, _)) => {
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                existing_relates.role().set_ordering(snapshot, type_manager, thing_manager, ordering).map_err(
                    |source| RedefineError::SetRelatesOrdering {
                        type_: label.clone(),
                        key: Keyword::Relates,
                        source_span: typeql_relates.span(),
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
            relates,
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
    relation_label: &Label,
    relates: Relates,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            relation_label,
            relates,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            relates.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    annotation,
                    source_span: typeql_capability.span(),
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn redefine_relates_specialise(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    anything_redefined: &mut bool,
    relation_label: &Label,
    relates: Relates,
    typeql_relates: &TypeQLRelates,
) -> Result<(), RedefineError> {
    if let Some(specialised_label) = &typeql_relates.specialised {
        let specialised_relates =
            resolve_relates(snapshot, type_manager, relates.relation(), checked_identifier(&specialised_label.ident)?)
                .map_err(|typedb_source| RedefineError::DefinitionResolution { typedb_source })?;

        let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
            .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::RelatesSpecialiseNotDefined {
                    type_: relation_label.clone(),
                    relates_key: Keyword::Relates,
                    as_key: Keyword::As,
                    specialised_role_name: specialised_relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    specialising_role_name: relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    source_span: typeql_relates.span(),
                })
            }
            DefinableStatus::ExistsSame(_) => {
                return Err(RedefineError::RelatesSpecialiseRemainsSame {
                    type_: relation_label.clone(),
                    relates_key: Keyword::Relates,
                    as_key: Keyword::As,
                    specialised_role_name: specialised_relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    specialising_role_name: relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    source_span: typeql_relates.span(),
                })
            }
            DefinableStatus::ExistsDifferent(_) => {}
        };

        error_if_anything_redefined_else_set_true(anything_redefined)?;
        relates.set_specialise(snapshot, type_manager, thing_manager, specialised_relates).map_err(|source| {
            RedefineError::SetRelatesSpecialise {
                type_: relation_label.clone(),
                relates_key: Keyword::Relates,
                as_key: Keyword::As,
                source_span: typeql_relates.span(),
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
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
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

        let definition_status = get_owns_status(
            snapshot,
            type_manager,
            object_type,
            attribute_type,
            ordering,
            DefinableStatusMode::Declared,
        )
        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        let owns = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::OwnsNotDefined {
                    type_: label.clone(),
                    key: Keyword::Owns,
                    attribute: attribute_type
                        .get_label(snapshot, type_manager)
                        .map_err(|err| RedefineError::UnexpectedConceptRead { typedb_source: err })?
                        .clone(),
                    source_span: capability.span(),
                    ordering,
                });
            }
            DefinableStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinableStatus::ExistsSame(Some((existing_owns, _))) => existing_owns, // is not a leaf of redefine query, don't error
            DefinableStatus::ExistsDifferent((existing_owns, _)) => {
                error_if_anything_redefined_else_set_true(anything_redefined)?;
                existing_owns.set_ordering(snapshot, type_manager, thing_manager, ordering).map_err(|source| {
                    RedefineError::SetOwnsOrdering {
                        type_: label.clone(),
                        key: Keyword::Owns,
                        source_span: typeql_owns.span(),
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
    owner_label: &Label,
    owns: Owns,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            owner_label,
            owns,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    source_span: typeql_capability.span(),
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
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(typeql_plays) = &capability.base else {
            continue;
        };

        let role_label = Label::build_scoped(
            checked_identifier(&typeql_plays.role.name.ident)?,
            checked_identifier(&typeql_plays.role.scope.ident)?,
            typeql_plays.role.span(),
        );
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status =
            get_plays_status(snapshot, type_manager, object_type, role_type, DefinableStatusMode::Declared)
                .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
        let plays = match definition_status {
            DefinableStatus::DoesNotExist => {
                return Err(RedefineError::PlaysNotDefined {
                    type_: label.clone(),
                    key: Keyword::Plays,
                    role: role_type
                        .get_label(snapshot, type_manager)
                        .map_err(|err| RedefineError::UnexpectedConceptRead { typedb_source: err })?
                        .clone(),
                    source_span: capability.span(),
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
    player_label: &Label,
    plays: Plays,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| RedefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            player_label,
            plays,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            error_if_anything_redefined_else_set_true(anything_redefined)?;
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetCapabilityAnnotation {
                    annotation,
                    source_span: typeql_capability.span(),
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn redefine_function(
    snapshot: &mut impl WritableSnapshot,
    function_manager: &FunctionManager,
    anything_redefined: &mut bool,
    function_declaration: &Function,
) -> Result<SchemaFunction, RedefineError> {
    let function = function_manager.redefine_function(snapshot, function_declaration).map_err(|source| {
        RedefineError::FunctionRedefinition {
            name: function_declaration.signature.ident.as_str_unchecked().to_owned(),
            source_span: function_declaration.span(),
            typedb_source: Box::new(source),
        }
    })?;
    *anything_redefined = true;
    Ok(function)
}

fn check_can_redefine_sub<T: TypeAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    new_supertype: T,
    capability: &Capability,
) -> Result<(), RedefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype)
        .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Err(RedefineError::TypeSubNotDefined {
            type_: label.clone(),
            key: Keyword::Sub,
            new_supertype: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            source_span: capability.span(),
        }),
        DefinableStatus::ExistsSame(_) => Err(RedefineError::TypeSubRemainsSame {
            type_: label.clone(),
            key: Keyword::Sub,
            supertype: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            source_span: capability.span(),
        }),
        DefinableStatus::ExistsDifferent(_) => Ok(()),
    }
}

fn type_convert_and_validate_annotation_redefinition_need<T: KindAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    annotation: Annotation,
    typeql_annotation: &typeql::Annotation,
    typeql_declaration: &Type,
) -> Result<Option<T::AnnotationType>, RedefineError> {
    error_if_not_redefinable(label, annotation.clone(), typeql_annotation)?;

    let converted = T::AnnotationType::try_from(annotation.clone()).map_err(|typedb_source| {
        RedefineError::IllegalTypeAnnotation {
            type_: label.clone(),
            annotation: annotation.clone(),
            source_span: typeql_declaration.span(),
            typedb_source,
        }
    })?;

    let definition_status =
        get_type_annotation_status(snapshot, type_manager, type_, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Err(RedefineError::TypeAnnotationNotDefined {
            type_: label.clone(),
            annotation,
            source_span: typeql_declaration.span(),
        }),
        DefinableStatus::ExistsSame(_) => Err(RedefineError::TypeAnnotationRemainsSame {
            type_: label.clone(),
            annotation,
            source_span: typeql_declaration.span(),
        }),
        DefinableStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

fn capability_convert_and_validate_annotation_redefinition_need<CAP: concept::type_::Capability>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    capability: CAP,
    annotation: Annotation,
    typeql_annotation: &typeql::Annotation,
    typeql_capability: &Capability,
) -> Result<Option<CAP::AnnotationType>, RedefineError> {
    error_if_not_redefinable(label, annotation.clone(), typeql_annotation)?;

    let converted = CAP::AnnotationType::try_from(annotation.clone()).map_err(|typedb_source| {
        RedefineError::IllegalCapabilityAnnotation {
            source_span: typeql_capability.span(),
            annotation: annotation.clone(),
            typedb_source,
        }
    })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, &capability, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => {
            Err(RedefineError::CapabilityAnnotationNotDefined { source_span: typeql_capability.span(), annotation })
        }
        DefinableStatus::ExistsSame(_) => {
            Err(RedefineError::CapabilityAnnotationRemainsSame { source_span: typeql_capability.span(), annotation })
        }
        DefinableStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

fn error_if_not_redefinable(
    label: &Label,
    annotation: Annotation,
    typeql_annotation: &typeql::Annotation,
) -> Result<(), RedefineError> {
    match annotation.category().has_parameter() {
        false => Err(RedefineError::ParameterFreeAnnotationCannotBeRedefined {
            type_: label.clone(),
            annotation,
            source_span: typeql_annotation.span(),
        }),
        true => Ok(()),
    }
}

fn err_capability_kind_mismatch(
    left: &Label,
    right: &Label,
    capability: &Capability,
    left_kind: Kind,
    right_kind: Kind,
) -> RedefineError {
    RedefineError::CapabilityKindMismatch {
        left: left.clone(),
        right: right.clone(),
        left_kind,
        right_kind,
        source_span: capability.span(),
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

typedb_error! {
    pub RedefineError(component = "Redefine execution", prefix = "REX") {
        Unimplemented(1, "Unimplemented redefine functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error during redefine query execution.", typedb_source: Box<ConceptReadError>),
        NothingRedefined(3, "Nothing was redefined."),
        DefinitionResolution(4, "Could not find symbol in redefine query.", typedb_source: Box<SymbolResolutionError>),
        LiteralParseError(5, "Error parsing literal in redefine query.", typedb_source: LiteralParseError),
        CanOnlyRedefineOneThingPerQuery(6, "Redefine queries can currently only mutate exactly one schema element per query."),
        StructFieldDoesNotExist(7, "Struct field used in redefine query does not exist.", source_span: Option<Span>),
        StructFieldRemainsSame(8, "Struct field in redefine was not changed. Redefine queries are required to update the schema", source_span: Option<Span>),
        StructFieldDeleteError(
            9,
            "Error removing field from struct type '{struct_name}'.",
            struct_name: String,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        StructFieldCreateError(
            10,
            "Error creating new field in struct type '{struct_name}'.",
            struct_name: String,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetSupertype(
            11,
            "Error setting supertype during redefine.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        TypeCannotHaveCapability(
            12,
            "Invalid redefine - the type '{type_}' of kind '{kind}', which is not allowed to make this declaration.'",
            type_: Label,
            kind: Kind,
            source_span: Option<Span>,
        ),
        ValueTypeSymbolResolution(
            13,
            "Error resolving value type in redefine query.",
            typedb_source: Box<SymbolResolutionError>
        ),
        TypeSubNotDefined(
            14,
            "Redefining '{key}' to '{new_supertype}' for type '{type_}' failed since there is no previously defined '{type_} {key}' to replace. Try define instead?",
            type_: Label,
            key: Keyword,
            new_supertype: Label,
            source_span: Option<Span>,
        ),
        TypeSubRemainsSame(
            15,
            "Redefining '{key}' to '{supertype}' for type '{type_}' failed since '{type_} {key} {supertype}' is already defined.",
            type_: Label,
            key: Keyword,
            supertype: Label,
            source_span: Option<Span>,
        ),
        RelatesNotDefined(
            16,
            "Redefining '{key}' to '{role}{ordering}' for type '{type_}' failed since there is no previously defined '{type_} {key}' to replace. Try define instead?",
            type_: Label,
            key: Keyword,
            role: String,
            ordering: Ordering,
            source_span: Option<Span>,
        ),
        OwnsNotDefined(
            17,
            "Redefining '{key}' to '{attribute}{ordering}' for type '{type_}' failed since there is no previously defined '{type_} {key}' to replace. Try define instead?",
            type_: Label,
            key: Keyword,
            attribute: Label,
            ordering: Ordering,
            source_span: Option<Span>,
        ),
        PlaysNotDefined(
            18,
            "Redefining '{key}' to '{role}' for type '{type_}' failed since there is no previously defined '{type_} {key}' to replace. Try define instead?",
            type_: Label,
            key: Keyword,
            role: Label,
            source_span: Option<Span>,
        ),
        RelatesSpecialiseNotDefined(
            19,
            "Redefining '{relates_key} {specialising_role_name} {as_key}' to '{specialised_role_name}' for type '{type_}' failed since there is no previously defined '{relates_key} {specialising_role_name} {as_key}' to replace. Try define instead?",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            specialised_role_name: String,
            specialising_role_name: String,
            source_span: Option<Span>,
        ),
        RelatesSpecialiseRemainsSame(
            20,
            "Redefining '{relates_key} {specialising_role_name} {as_key}' to '{specialised_role_name}' for type '{type_}' failed since '{type_} {relates_key} {specialising_role_name} {as_key} {specialised_role_name}' is already defined.",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            specialised_role_name: String,
            specialising_role_name: String,
            source_span: Option<Span>,
        ),
        AttributeTypeValueTypeNotDefined(
            21,
            "Redefining '{key}' to '{value_type}' for type '{type_}' failed since there is no previously defined '{type_} {key}' to replace. Try define instead?",
            type_: Label,
            key: Keyword,
            value_type: ValueType,
            source_span: Option<Span>,
        ),
        TypeAnnotationNotDefined(
            22,
            "Redefining annotation '{annotation}' for type '{type_}' failed since there is no previously defined annotation of this category to replace. Try define instead?",
            type_: Label,
            annotation: Annotation,
            source_span: Option<Span>,
        ),
        TypeAnnotationRemainsSame(
            23,
            "Redefining annotation '{annotation}' for type '{type_}' failed since '{type_} {annotation}' is already defined.",
            type_: Label,
            annotation: Annotation,
            source_span: Option<Span>,
        ),
        CapabilityAnnotationNotDefined(
            24,
            "Redefining annotation '{annotation}' for a capability failed since there is no previously defined annotation of this category to replace. Try define instead?",
            annotation: Annotation,
            source_span: Option<Span>,
        ),
        CapabilityAnnotationRemainsSame(
            25,
            "Redefining annotation '{annotation}' for a capability failed since is already defined identically.",
            annotation: Annotation,
            source_span: Option<Span>,
        ),
        ParameterFreeAnnotationCannotBeRedefined(
            26,
            "For type '{type_}', annotation '{annotation}' can never be redefined as it carries no parameters. Redefine can only replace existing schema elements.",
            type_: Label,
            annotation: Annotation,
            source_span: Option<Span>,
        ),
        SetValueType(
            27,
            "Redefining '{type_}' to have value type '{value_type}' failed.",
            type_: Label,
            value_type: ValueType,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetRelatesOrdering(
            28,
            "Redefining '{type_}' to have an updated '{key}' ordering failed.",
            type_: Label,
            key: Keyword,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetOwnsOrdering(
            29,
            "Redefining '{type_}' to have an updated '{key}' ordering failed.",
            type_: Label,
            key: Keyword,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        IllegalTypeAnnotation(
            30,
            "Redefining '{type_}' to have annotation '{annotation}' failed as this is an illegal annotation.",
            type_: Label,
            annotation: Annotation,
            source_span: Option<Span>,
            typedb_source: AnnotationError
        ),
        IllegalCapabilityAnnotation(
            31,
            "Redefining to have annotation '{annotation}' failed.",
            annotation: Annotation,
            source_span: Option<Span>,
            typedb_source: AnnotationError
        ),
        SetTypeAnnotation(
            32,
            "Redefining '{type_}' to have annotation '{annotation}' failed.",
            type_: Label,
            annotation: Annotation,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetCapabilityAnnotation(
            33,
            "Redefining '{annotation}' failed.",
            annotation: Annotation,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetRelatesSpecialise(
            34,
            "For relation type '{type_}', redefining '{relates_key} {as_key}' failed.",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        CapabilityKindMismatch(
            35,
            "Redefine failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'.",
            left: Label,
            right: Label,
            left_kind: Kind,
            right_kind: Kind,
            source_span: Option<Span>,
        ),
        FunctionRedefinition(
            36,
            "Redefining the function '{name}' failed",
            name: String,
            source_span: Option<Span>,
            typedb_source: Box<FunctionError>
        ),
        IllegalKeywordAsIdentifier(
            37,
            "The reserved keyword '{identifier}' cannot be used as an identifier.",
            identifier: typeql::Identifier,
            source_span: Option<Span>,
        ),
    }
}

fn err_unsupported_capability(label: &Label, kind: Kind, capability: &Capability) -> RedefineError {
    RedefineError::TypeCannotHaveCapability { type_: label.to_owned(), kind, source_span: capability.span() }
}
