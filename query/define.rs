/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

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
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::{definition::r#struct::StructDefinitionField, type_::Kind},
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use function::{function_manager::FunctionManager, FunctionError};
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::{error::TypeQLError, Span, Spanned},
    query::schema::Define,
    schema::definable::{
        struct_::Field,
        type_::{capability::Relates as TypeQLRelates, Capability as TypeQLCapability, CapabilityBase},
        Struct, Type,
    },
    token,
    token::Keyword,
    Definable,
};

use crate::{
    definable_resolution::{
        filter_variants, get_struct_field_value_type_optionality, resolve_attribute_type, resolve_relates,
        resolve_relates_declared, resolve_role_type, resolve_struct_definition_key, resolve_typeql_type,
        resolve_value_type, try_resolve_typeql_type, type_ref_to_label_and_ordering, type_to_object_type,
        SymbolResolutionError,
    },
    definable_status::{
        get_capability_annotation_status, get_owns_status, get_plays_status, get_relates_status,
        get_struct_field_status, get_struct_status, get_sub_status, get_type_annotation_status, get_value_type_status,
        DefinableStatus, DefinableStatusMode,
    },
};

fn checked_identifier(identifier: &typeql::Identifier) -> Result<&str, DefineError> {
    identifier.as_str_unreserved().map_err(|_source| {
        let TypeQLError::ReservedKeywordAsIdentifier { identifier } = _source else { unreachable!() };
        DefineError::IllegalKeywordAsIdentifier { identifier }
    })
}

macro_rules! verify_empty_annotations_for_capability {
    ($capability:ident, $annotation_error:path, $error_arg_name:ident) => {
        if let Some(typeql_annotation) = &$capability.annotations.first() {
            let annotation = translate_annotation(typeql_annotation)
                .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
            let error = { $annotation_error { $error_arg_name: annotation.category() } };
            Err(DefineError::IllegalAnnotation { source_span: typeql_annotation.span(), typedb_source: error })
        } else {
            Ok(())
        }
    };
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    function_manager: &FunctionManager,
    define: Define,
) -> Result<(), DefineError> {
    process_struct_definitions(snapshot, type_manager, &define.definables)?;
    process_type_definitions(snapshot, type_manager, thing_manager, &define.definables)?;
    process_function_definitions(snapshot, function_manager, &define.definables)?;
    Ok(())
}

fn process_struct_definitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    filter_variants!(Definable::Struct : definables)
        .try_for_each(|struct_| define_struct(snapshot, type_manager, struct_))?;
    filter_variants!(Definable::Struct : definables)
        .try_for_each(|struct_| define_struct_fields(snapshot, type_manager, struct_))?;
    Ok(())
}

fn process_type_definitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    let declarations = filter_variants!(Definable::TypeDeclaration : definables);
    define_types(snapshot, type_manager, declarations.clone())?;
    declarations.clone().try_for_each(|declaration| define_alias(snapshot, type_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_value_type(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_type_annotations(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| define_sub(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| {
        define_relates_with_annotations(snapshot, type_manager, thing_manager, declaration)
    })?;
    declarations
        .clone()
        .try_for_each(|declaration| define_relates_specialises(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_owns_with_annotations(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| {
        define_plays_with_annotations(snapshot, type_manager, thing_manager, declaration)
    })?;
    Ok(())
}

fn process_function_definitions(
    snapshot: &mut impl WritableSnapshot,
    function_manager: &FunctionManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    let functions = filter_variants!(Definable::Function: definables);
    if functions.clone().next().is_some() {
        function_manager
            .define_functions(snapshot, functions)
            .map_err(|typedb_source| DefineError::FunctionDefinition { typedb_source })?;
    }
    Ok(())
}

fn define_types<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    mut declarations: impl Iterator<Item = &'a Type>,
) -> Result<(), DefineError> {
    let mut undefined_labels: HashSet<Label> = HashSet::new();

    declarations.try_for_each(|declaration| define_type(snapshot, type_manager, declaration, &mut undefined_labels))?;

    for label in undefined_labels {
        let existing =
            try_resolve_typeql_type(snapshot, type_manager, &label).map_err(|err| DefineError::SymbolResolution {
                typedb_source: Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: err }),
            })?;
        if existing.is_none() {
            return Err(DefineError::SymbolResolution {
                typedb_source: Box::new(SymbolResolutionError::TypeNotFound { label }),
            });
        }
    }

    Ok(())
}

fn define_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &Struct,
) -> Result<(), DefineError> {
    let name = checked_identifier(&struct_definable.ident)?;

    let definition_status = get_struct_status(snapshot, type_manager, name)
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => {}
        DefinableStatus::ExistsSame(_) => return Ok(()),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Structs cannot differ"),
    }

    type_manager
        .create_struct(snapshot, name.to_owned())
        .map_err(|err| DefineError::StructCreateError { typedb_source: err, source_span: struct_definable.span() })?;
    Ok(())
}

fn define_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &Struct,
) -> Result<(), DefineError> {
    let name = checked_identifier(&struct_definable.ident)?;
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let field_key = checked_identifier(&field.key)?;
        let definition_status = get_struct_field_status(
            snapshot,
            type_manager,
            struct_key.clone(),
            field_key,
            value_type.clone(),
            optional,
        )
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        match definition_status {
            DefinableStatus::DoesNotExist => {}
            DefinableStatus::ExistsSame(_) => return Ok(()),
            DefinableStatus::ExistsDifferent(existing_field) => {
                return Err(DefineError::StructFieldAlreadyDefinedButDifferent {
                    struct_name: name.to_owned(),
                    existing_field,
                    source_span: field.span(),
                    declaration: field.to_owned(),
                });
            }
        }

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), checked_identifier(&field.key)?, value_type, optional)
            .map_err(|err| DefineError::StructFieldCreateError {
                typedb_source: err,
                struct_name: name.to_owned(),
                source_span: field.span(),
                struct_field: field.clone(),
            })?;
    }
    Ok(())
}

fn define_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
    undefined_labels: &mut HashSet<Label>,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let existing =
        try_resolve_typeql_type(snapshot, type_manager, &label).map_err(|err| DefineError::SymbolResolution {
            typedb_source: Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: err }),
        })?;
    match type_declaration.kind {
        None => {
            if existing.is_none() {
                undefined_labels.insert(label);
            }
        }
        Some(token::Kind::Role) => {
            return Err(DefineError::RoleTypeDirectCreate { source_span: type_declaration.span() });
        }
        Some(token::Kind::Entity) => {
            if matches!(existing, Some(TypeEnum::Entity(_))) {
                return Ok(());
            }
            type_manager.create_entity_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                source_span: type_declaration.span(),
            })?;
        }
        Some(token::Kind::Relation) => {
            if matches!(existing, Some(TypeEnum::Relation(_))) {
                return Ok(());
            }
            type_manager.create_relation_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                source_span: type_declaration.span(),
            })?;
        }
        Some(token::Kind::Attribute) => {
            if matches!(existing, Some(TypeEnum::Attribute(_))) {
                return Ok(());
            }
            type_manager.create_attribute_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                source_span: type_declaration.span(),
            })?;
        }
    }
    Ok(())
}

fn define_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for typeql_annotation in &type_declaration.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
        match type_ {
            TypeEnum::Entity(entity) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    entity,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    entity.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            type_: label.to_owned(),
                            source_span: typeql_annotation.span(),
                        }
                    })?;
                }
            }
            TypeEnum::Relation(relation) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    relation,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    relation.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            type_: label.to_owned(),
                            source_span: typeql_annotation.span(),
                        }
                    })?;
                }
            }
            TypeEnum::Attribute(attribute) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    attribute,
                    annotation.clone(),
                    typeql_annotation,
                    type_declaration,
                )? {
                    if converted.is_value_type_annotation() {
                        return Err(DefineError::IllegalAnnotation {
                            source_span: typeql_annotation.span(),
                            typedb_source: AnnotationError::UnsupportedAnnotationForAttributeType {
                                category: annotation.category(),
                            },
                        });
                    }
                    attribute.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            type_: label.to_owned(),
                            source_span: typeql_annotation.span(),
                        }
                    })?;
                }
            }
            TypeEnum::RoleType(_) => unreachable!("Role annotations are syntactically on relates"),
        }
    }
    Ok(())
}

fn define_alias(
    _snapshot: &mut impl WritableSnapshot,
    _type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Alias(_alias) = &capability.base else {
            continue;
        };
        return Err(DefineError::Unimplemented { description: "Alias definition.".to_string() });
        // define_alias_annotations(capability) // Uncomment when implemented
    }

    Ok(())
}

fn define_alias_annotations(typeql_capability: &TypeQLCapability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(
        typeql_capability,
        AnnotationError::UnsupportedAnnotationForAlias,
        category
    )
}

fn define_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };
        let supertype_label =
            Label::parse_from(checked_identifier(&sub.supertype_label.ident)?, type_declaration.label.span());
        let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                if need_define {
                    type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                        DefineError::SetSupertype { source_span: sub.span(), typedb_source: source }
                    })?;
                }
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                if need_define {
                    type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                        DefineError::SetSupertype { source_span: sub.span(), typedb_source: source }
                    })?;
                }
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, *type_, supertype, capability)?;
                if need_define {
                    type_.set_supertype(snapshot, type_manager, thing_manager, supertype).map_err(|source| {
                        DefineError::SetSupertype { source_span: sub.span(), typedb_source: source }
                    })?;
                }
            }
            (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => {
                unreachable!("RoleType's sub is controlled by specialise")
            } // Turn into an error
            (type_, supertype) => {
                return Err(err_capability_kind_mismatch(
                    &label,
                    &supertype_label,
                    capability,
                    type_.kind(),
                    supertype.kind(),
                ))
            }
        }

        define_sub_annotations(capability)?;
    }
    Ok(())
}

fn define_sub_annotations(typeql_capability: &TypeQLCapability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub, category)
}

fn define_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::ValueType(value_type_statement) = &capability.base else {
            continue;
        };
        let TypeEnum::Attribute(attribute_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };
        let value_type = resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
            .map_err(|typedb_source| DefineError::ValueTypeSymbolResolution { typedb_source })?;

        let definition_status = get_value_type_status(
            snapshot,
            type_manager,
            *attribute_type,
            value_type.clone(),
            DefinableStatusMode::Declared,
        )
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        let define_needed = match definition_status {
            DefinableStatus::DoesNotExist => true,
            DefinableStatus::ExistsSame(_) => false,
            DefinableStatus::ExistsDifferent(existing_value_type) => {
                return Err(DefineError::AttributeTypeValueTypeAlreadyDefinedButDifferent {
                    type_: label.to_owned(),
                    key: Keyword::Value,
                    value_type,
                    existing_value_type,
                    source_span: capability.span(),
                });
            }
        };

        if define_needed {
            attribute_type.set_value_type(snapshot, type_manager, thing_manager, value_type.clone()).map_err(
                |source| DefineError::SetValueType {
                    type_: label.to_owned(),
                    value_type,
                    source_span: value_type_statement.value_type.span(),
                    typedb_source: source,
                },
            )?;
        }

        define_value_type_annotations(
            snapshot,
            type_manager,
            thing_manager,
            *attribute_type,
            &label,
            capability,
            type_declaration,
        )?;
    }
    Ok(())
}

fn define_value_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute_type: AttributeType,
    attribute_type_label: &Label,
    typeql_capability: &TypeQLCapability,
    typeql_type: &Type,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = type_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            attribute_type_label,
            attribute_type,
            annotation.clone(),
            typeql_annotation,
            typeql_type,
        )? {
            if !converted.is_value_type_annotation() {
                return Err(DefineError::IllegalAnnotation {
                    source_span: typeql_annotation.span(),
                    typedb_source: AnnotationError::UnsupportedAnnotationForValueType {
                        category: annotation.category(),
                    },
                });
            }
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    typedb_source: source,
                    type_: attribute_type_label.clone(),
                    source_span: typeql_annotation.span(),
                }
            })?;
        }
    }
    Ok(())
}

fn define_relates_with_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &relates.related)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let definition_status = get_relates_status(
            snapshot,
            type_manager,
            *relation_type,
            &role_label,
            ordering,
            DefinableStatusMode::Declared,
        )
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => relation_type
                .create_relates(snapshot, type_manager, thing_manager, role_label.name.as_str(), ordering)
                .map_err(|source| DefineError::CreateRelates {
                    key: Keyword::Relates,
                    source_span: relates.span(),
                    typedb_source: source,
                })?,
            DefinableStatus::ExistsSame(Some((existing_relates, _))) => existing_relates,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinableStatus::ExistsDifferent((_, existing_ordering)) => {
                return Err(DefineError::RelatesAlreadyDefinedButDifferent {
                    type_: label.clone(),
                    key: Keyword::Relates,
                    role: role_label.name.to_string(),
                    ordering,
                    existing_ordering,
                    source_span: capability.span(),
                });
            }
        };

        define_relates_annotations(snapshot, type_manager, thing_manager, &label, defined, capability)?;
    }
    Ok(())
}

fn define_relates_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label,
    relates: Relates,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            relates,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            relates.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    type_: relation_label.clone(),
                    typedb_source: source,
                    source_span: typeql_annotation.span(),
                }
            })?;
        }
    }
    Ok(())
}

fn define_relates_specialises(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(typeql_relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let (role_label, _ordering) = type_ref_to_label_and_ordering(&label, &typeql_relates.related)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
        let relates = resolve_relates_declared(snapshot, type_manager, *relation_type, role_label.name.as_str())
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        define_relates_specialise(snapshot, type_manager, thing_manager, &label, relates, typeql_relates)?;
    }
    Ok(())
}

fn define_relates_specialise(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label,
    relates: Relates,
    typeql_relates: &TypeQLRelates,
) -> Result<(), DefineError> {
    if let Some(specialised_label) = &typeql_relates.specialised {
        let checked_specialised = checked_identifier(&specialised_label.ident)?;
        let specialised_relates = resolve_relates(snapshot, type_manager, relates.relation(), checked_specialised)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
            .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        let need_define = match definition_status {
            DefinableStatus::DoesNotExist => Ok(true),
            DefinableStatus::ExistsSame(_) => Ok(false),
            DefinableStatus::ExistsDifferent(existing_superrole) => {
                Err(DefineError::RelatesSpecialiseAlreadyDefinedButDifferent {
                    type_: relation_label.clone(),
                    relates_key: Keyword::Relates,
                    as_key: Keyword::As,
                    specialised_role_name: specialised_relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    specialising_role_name: relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    existing_specialised_role_name: existing_superrole
                        .get_label(snapshot, type_manager)
                        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?
                        .name()
                        .to_string(),
                    source_span: typeql_relates.span(),
                })
            }
        }?;

        if need_define {
            relates.set_specialise(snapshot, type_manager, thing_manager, specialised_relates).map_err(|source| {
                DefineError::SetSpecialise {
                    type_: relation_label.clone(),
                    source_span: typeql_relates.span(),
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn define_owns_with_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Owns(owns) = &capability.base else {
            continue;
        };
        let (attr_label, ordering) = type_ref_to_label_and_ordering(&label, &owns.owned)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
        let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

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
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => object_type
                .set_owns(snapshot, type_manager, thing_manager, attribute_type, ordering)
                .map_err(|source| DefineError::CreateOwns {
                    key: Keyword::Owns,
                    source_span: owns.span(),
                    typedb_source: source,
                })?,
            DefinableStatus::ExistsSame(Some((existing_owns, _))) => existing_owns,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinableStatus::ExistsDifferent((_, existing_ordering)) => {
                return Err(DefineError::OwnsAlreadyDefinedButDifferent {
                    type_: label.clone(),
                    key: Keyword::Owns,
                    attribute: attr_label,
                    ordering,
                    existing_ordering,
                    source_span: capability.span(),
                });
            }
        };

        define_owns_annotations(snapshot, type_manager, thing_manager, &label, defined, capability)?;
    }
    Ok(())
}

fn define_owns_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label,
    owns: Owns,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            owns,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    type_: owner_label.clone(),
                    typedb_source: source,
                    source_span: typeql_annotation.span(),
                }
            })?;
        }
    }
    Ok(())
}

fn define_plays_with_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(checked_identifier(&type_declaration.label.ident)?, type_declaration.label.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(plays) = &capability.base else {
            continue;
        };
        let role_label = Label::build_scoped(
            checked_identifier(&plays.role.name.ident)?,
            checked_identifier(&plays.role.scope.ident)?,
            plays.role.span(),
        );
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status =
            get_plays_status(snapshot, type_manager, object_type, role_type, DefinableStatusMode::Declared)
                .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => {
                object_type.set_plays(snapshot, type_manager, thing_manager, role_type).map_err(|source| {
                    DefineError::CreatePlays { key: Keyword::Plays, source_span: plays.span(), typedb_source: source }
                })?
            }
            DefinableStatus::ExistsSame(Some(existing_plays)) => existing_plays,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing plays concept expected"),
            DefinableStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
        };

        define_plays_annotations(snapshot, type_manager, thing_manager, &label, defined, capability)?;
    }
    Ok(())
}

fn define_plays_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label,
    plays: Plays,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation = translate_annotation(typeql_annotation)
            .map_err(|typedb_source| DefineError::LiteralParseError { typedb_source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            plays,
            annotation.clone(),
            typeql_annotation,
            typeql_capability,
        )? {
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    type_: player_label.clone(),
                    source_span: typeql_annotation.span(),
                    typedb_source: source,
                }
            })?;
        }
    }
    Ok(())
}

fn check_can_and_need_define_sub<T: TypeAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    new_supertype: T,
    capability: &TypeQLCapability,
) -> Result<bool, DefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype)
        .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(true),
        DefinableStatus::ExistsSame(_) => Ok(false),
        DefinableStatus::ExistsDifferent(existing) => Err(DefineError::TypeSubAlreadyDefinedButDifferent {
            type_: label.clone(),
            key: Keyword::Sub,
            supertype: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            existing_supertype: existing
                .get_label(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            source_span: capability.span(),
        }),
    }
}

fn type_convert_and_validate_annotation_definition_need<T: KindAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    annotation: Annotation,
    typeql_annotation: &typeql::Annotation,
    typeql_declaration: &Type,
) -> Result<Option<T::AnnotationType>, DefineError> {
    let converted = T::AnnotationType::try_from(annotation.clone()).map_err(|typedb_source| {
        DefineError::IllegalAnnotation { source_span: typeql_annotation.span(), typedb_source }
    })?;

    let definition_status =
        get_type_annotation_status(snapshot, type_manager, type_, &converted, annotation.category())
            .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(Some(converted)),
        DefinableStatus::ExistsSame(_) => Ok(None),
        DefinableStatus::ExistsDifferent(existing) => Err(DefineError::TypeAnnotationAlreadyDefinedButDifferent {
            type_: label.clone(),
            annotation,
            existing_annotation: existing.clone().into(),
            source_span: typeql_declaration.span(),
        }),
    }
}

fn capability_convert_and_validate_annotation_definition_need<CAP: Capability>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: CAP,
    annotation: Annotation,
    typeql_annotation: &typeql::Annotation,
    typeql_capability: &TypeQLCapability,
) -> Result<Option<CAP::AnnotationType>, DefineError> {
    let converted = CAP::AnnotationType::try_from(annotation.clone()).map_err(|typedb_source| {
        DefineError::IllegalAnnotation { source_span: typeql_annotation.span(), typedb_source }
    })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, &capability, &converted, annotation.category())
            .map_err(|source| DefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(Some(converted)),
        DefinableStatus::ExistsSame(_) => Ok(None),
        DefinableStatus::ExistsDifferent(existing) => {
            Err(DefineError::CapabilityAnnotationAlreadyDefinedButDifferent {
                annotation,
                existing_annotation: existing.clone().into(),
                source_span: typeql_capability.span(),
            })
        }
    }
}

fn err_capability_kind_mismatch(
    left: &Label,
    right: &Label,
    capability: &TypeQLCapability,
    left_kind: Kind,
    right_kind: Kind,
) -> DefineError {
    DefineError::CapabilityKindMismatch {
        left: left.clone(),
        right: right.clone(),
        source_span: capability.span(),
        left_kind,
        right_kind,
    }
}

fn err_unsupported_capability(label: &Label, kind: Kind, capability: &TypeQLCapability) -> DefineError {
    DefineError::TypeCannotHaveCapability {
        type_: label.to_owned(),
        kind,
        capability: capability.clone(),
        source_span: capability.span(),
    }
}

typedb_error! {
    pub DefineError(component = "Define execution", prefix = "DEX") {
        Unimplemented(1, "Unimplemented define functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error. ", typedb_source: Box<ConceptReadError>),
        SymbolResolution(3, "Failed to find symbol.", typedb_source: Box<SymbolResolutionError>),
        LiteralParseError(4, "Failed to parse literal.", typedb_source: LiteralParseError),
        TypeCreateError(
            5,
            "Failed to create type.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        RoleTypeDirectCreate(
            6,
            "Role types cannot be created directly, but only through 'relates' declarations.",
            source_span: Option<Span>,
        ),
        StructCreateError(
            7,
            "Failed to create struct.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        StructFieldCreateError(
            8,
            "Failed to create struct field '{struct_field}' in struct type '{struct_name}'.",
            struct_field: Field,
            struct_name: String,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        StructFieldAlreadyDefinedButDifferent(
            9,
            "Defining struct field for struct '{struct_name}' failed since it already has a different definition: {existing_field}.",
            struct_name: String,
            existing_field: StructDefinitionField,
            source_span: Option<Span>,
            declaration: Field
        ),
        SetSupertype(
            10,
            "Setting supertype failed.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        TypeCannotHaveCapability(
            11,
            "Invalid define - the type '{type_}' of kind '{kind}', which cannot have: '{capability}'",
            type_: Label,
            kind: Kind,
            capability: TypeQLCapability,
            source_span: Option<Span>,
        ),
        ValueTypeSymbolResolution(
            12,
            "Error resolving value type in define query.",
            typedb_source: Box<SymbolResolutionError>
        ),
        TypeSubAlreadyDefinedButDifferent(
            13,
            "Defining '{key} {supertype}' for type '{type_}' failed since a different '{type_} {key} {existing_supertype}' is already defined. Try redefine instead?",
            type_: Label,
            key: Keyword,
            supertype: Label,
            existing_supertype: Label,
            source_span: Option<Span>,
        ),
        AttributeTypeValueTypeAlreadyDefinedButDifferent(
            14,
            "Defining '{key} {value_type}' for type '{type_}' failed since a different '{type_} {key} {existing_value_type}' is already defined. Try redefine instead?",
            type_: Label,
            key: Keyword,
            value_type: ValueType,
            existing_value_type: ValueType,
            source_span: Option<Span>,
        ),
        TypeAnnotationAlreadyDefinedButDifferent(
            15,
            "Defining annotation '{annotation}' for type '{type_}' failed since a different '{type_} {existing_annotation}' is already defined. Try redefine instead?",
            type_: Label,
            annotation: Annotation,
            existing_annotation: Annotation,
            source_span: Option<Span>,
        ),
        CapabilityAnnotationAlreadyDefinedButDifferent(
            16,
            "Defining annotation '{annotation}' for a capability failed since a different annotation '{existing_annotation}' is already defined. Try redefine instead?",
            annotation: Annotation,
            existing_annotation: Annotation,
            source_span: Option<Span>,

        ),
        RelatesAlreadyDefinedButDifferent(
            17,
            "Defining '{key} {role}{ordering}' for type '{type_}' failed since a different '{type_} {key} {role}{existing_ordering}' is already defined. Try redefine instead?",
            type_: Label,
            key: Keyword,
            role: String,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        OwnsAlreadyDefinedButDifferent(
            18,
            "Defining '{key} {attribute}{ordering}' for type '{type_}' failed since a different '{type_} {key} {attribute}{existing_ordering}' is already defined. Try redefine instead?",
            type_: Label,
            key: Keyword,
            attribute: Label,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        RelatesSpecialiseAlreadyDefinedButDifferent(
            19,
            "Defining '{as_key} {specialised_role_name}' for '{type_} {relates_key} {specialising_role_name}' failed since a different '{type_} {relates_key} {specialising_role_name} {as_key} {existing_specialised_role_name}' is already defined. Try redefine instead?",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            specialised_role_name: String,
            specialising_role_name: String,
            existing_specialised_role_name: String,
            source_span: Option<Span>,
        ),
        SetValueType(
            20,
            "Defining '{type_}' to have value type '{value_type}' failed.",
            type_: Label,
            value_type: ValueType,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        CreateRelates(
            21,
            "Defining new '{key}' failed.",
            key: Keyword,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        CreatePlays(
            22,
            "Defining new '{key}' failed.",
            source_span: Option<Span>,
            key: Keyword,
            typedb_source: Box<ConceptWriteError>
        ),
        CreateOwns(
            23,
            "Defining new '{key}' failed.",
            source_span: Option<Span>,
            key: Keyword,
            typedb_source: Box<ConceptWriteError>
        ),
        IllegalAnnotation(
            24,
            "Illegal annotation",
            source_span: Option<Span>,
            typedb_source: AnnotationError
        ),
        SetAnnotation(
            25,
            "Defining annotation failed for type '{type_}'.",
            type_: Label,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        SetSpecialise(
            26,
            "Defining specialise failed for type '{type_}'.",
            type_: Label,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        CapabilityKindMismatch(
            27,
            "Declaration failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'.",
            left: Label,
            right: Label,
            source_span: Option<Span>,
            left_kind: Kind,
            right_kind: Kind
        ),
        FunctionDefinition(
            28,
            "An error occurred by defining the function",
            typedb_source: FunctionError
        ),
        IllegalKeywordAsIdentifier(29, "The reserved keyword \"{identifier}\" cannot be used as an identifier.", identifier: typeql::Identifier),
    }
}
