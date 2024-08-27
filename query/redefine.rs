/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};
use std::cmp::PartialEq;
use std::collections::HashMap;

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
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    query::schema::Redefine,
    schema::definable::{
        function::Function,
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates},
            Capability, CapabilityBase,
        },
        Struct, Type,
    },
    Definable, TypeRefAny,
};

use crate::{
    definition_resolution::{
        filter_variants, get_struct_field_value_type_optionality, named_type_to_label, resolve_attribute_type,
        resolve_owns, resolve_owns_declared, resolve_plays_declared, resolve_plays_role_label, resolve_relates,
        resolve_relates_declared, resolve_role_type, resolve_struct_definition_key, resolve_typeql_type,
        resolve_value_type, try_unwrap, type_ref_to_label_and_ordering, type_to_object_type, SymbolResolutionError,
    },
    definition_status::{
        get_capability_annotation_status, get_override_status, get_owns_status, get_plays_status, get_relates_status,
        get_struct_field_status, get_sub_status, get_type_annotation_status, get_value_type_status, DefinitionStatus,
    },
};

macro_rules! verify_empty_annotations_for_capability {
    ($capability:ident, $annotation_error:path) => {
        if let Some(typeql_annotation) = &$capability.annotations.first() {
            let annotation = translate_annotation(typeql_annotation)
                .map_err(|source| RedefineError::LiteralParseError { source })?;
            Err(RedefineError::IllegalAnnotation { source: $annotation_error(annotation.category()) })
        } else {
            Ok(())
        }
    };
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum RedefinitionLevel {
    TypeAxioms,
    ConstraintsAndTriggers,
    Structs,
    Functions,
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

    // TODO: In the current implementation, it returns error for `redefine entity person`,
    // but does not for `redefine entity person owns name` (because `name` could be ordered and we could change it)`.
    // We want to error only if the query is built the way there is no possibility to redefine anything.
    // However, if we just
    if !redefined_structs && !redefined_types && !redefined_functions {
        Err(RedefineError::RedefinedNothing {})
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
    let mut redefinition_levels: HashMap<String, RedefinitionLevel> = HashMap::new();
    filter_variants!(Definable::Struct : definables)
        .try_for_each(|struct_| redefine_struct_fields(snapshot, type_manager, thing_manager, &mut redefinition_levels, struct_))?;
    Ok(!redefinition_levels.is_empty())
}

fn process_type_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut redefinition_levels: HashMap<String, RedefinitionLevel> = HashMap::new();
    let declarations = filter_variants!(Definable::TypeDeclaration : definables);
    declarations
        .clone()
        .try_for_each(|declaration| redefine_value_type(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_type_annotations(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_sub(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations.clone().try_for_each(|declaration| redefine_alias(snapshot, type_manager, &mut redefinition_levels, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_relates(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations.clone().try_for_each(|declaration| {
        redefine_relates_capabilities(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration)
    })?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_owns(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_owns_capabilities(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    declarations.clone().try_for_each(|declaration| redefine_plays(snapshot, type_manager, &mut redefinition_levels, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_plays_capabilities(snapshot, type_manager, thing_manager, &mut redefinition_levels, declaration))?;
    Ok(!redefinition_levels.is_empty())
}

fn process_function_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<bool, RedefineError> {
    let mut redefinition_levels: HashMap<String, RedefinitionLevel> = HashMap::new();
    filter_variants!(Definable::Function : definables)
        .try_for_each(|function| redefine_functions(snapshot, type_manager, &mut redefinition_levels, function))?;
    Ok(!redefinition_levels.is_empty())
}

fn redefine_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    struct_definable: &Struct,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::Structs;

    let name = struct_definable.ident.as_str();
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;

    for field in &struct_definable.fields {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, name, REDEFINITION_LEVEL)?;

        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

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
            DefinitionStatus::DoesNotExist => {
                return Err(RedefineError::StructFieldDoesNotExist { field: field.to_owned() });
            }
            DefinitionStatus::ExistsSame(_) => {
                // TODO: Should ExistsSame error Redefine? If yes, change everywhere!
                return Ok(());
            }
            DefinitionStatus::ExistsDifferent(_) => {}
        }

        type_manager.delete_struct_field(snapshot, thing_manager, struct_key.clone(), field.key.as_str()).map_err(
            |err| RedefineError::StructFieldDeleteError {
                source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            },
        )?;

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
            .map_err(|err| RedefineError::StructFieldCreateError {
                source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            })?;
    }
    Ok(())
}

fn redefine_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for typeql_annotation in &type_declaration.annotations {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

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
                )? {
                    entity.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetAnnotation { source, label: label.to_owned(), annotation }
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
                )? {
                    relation.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetAnnotation { source, label: label.to_owned(), annotation }
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
                )? {
                    if converted.is_value_type_annotation() {
                        return Err(RedefineError::IllegalAnnotation {
                            source: AnnotationError::UnsupportedAnnotationForAttributeType(annotation.category()),
                        });
                    }
                    attribute.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        RedefineError::SetAnnotation { source, label: label.to_owned(), annotation }
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
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    &type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(RedefineError::Unimplemented))?;

    // TODO: Uncomment when alias is implemented
    // redefine_alias_annotations(capability)?;

    Ok(())
}

fn redefine_alias_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
}

fn redefine_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let supertype_label = Label::parse_from(&sub.supertype_label.ident.as_str());
        let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        if type_.kind() != supertype.kind() {
            return Err(err_capability_kind_mismatch(
                &label,
                &supertype_label,
                capability,
                type_.kind(),
                supertype.kind(),
            ))?;
        }

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                let need_define =
                    check_can_and_need_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| RedefineError::SetSupertype { sub: sub.clone(), source })?;
                }
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                let need_define =
                    check_can_and_need_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| RedefineError::SetSupertype { sub: sub.clone(), source })?;
                }
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                let need_define =
                    check_can_and_need_redefine_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| RedefineError::SetSupertype { sub: sub.clone(), source })?;
                }
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
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
}

fn redefine_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::ValueType(value_type_statement) = &capability.base else {
            continue;
        };
        let TypeEnum::Attribute(attribute_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let value_type = resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
            .map_err(|source| RedefineError::AttributeTypeBadValueType { source })?;

        let definition_status =
            get_value_type_status(snapshot, type_manager, attribute_type.clone(), value_type.clone())
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        let redefine_needed = match definition_status {
            DefinitionStatus::DoesNotExist => {
                return Err(RedefineError::AttributeTypeValueTypeIsNotDefined { label: label.to_owned(), value_type });
            }
            DefinitionStatus::ExistsSame(_) => false,
            DefinitionStatus::ExistsDifferent(_) => true,
        };

        if redefine_needed {
            attribute_type
                .set_value_type(snapshot, type_manager, thing_manager, value_type.clone())
                .map_err(|source| RedefineError::SetValueType { label: label.to_owned(), value_type, source })?;
        }

        redefine_value_type_annotations(
            snapshot,
            type_manager,
            thing_manager, 
            redefinition_levels,
            attribute_type.clone(),
            &label,
            capability,
        )?;
    }
    Ok(())
}

fn redefine_value_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    attribute_type: AttributeType<'a>,
    attribute_type_label: &Label<'a>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    for typeql_annotation in &typeql_capability.annotations {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, attribute_type_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = type_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            &attribute_type_label,
            attribute_type.clone(),
            annotation.clone(),
        )? {
            if !converted.is_value_type_annotation() {
                return Err(RedefineError::IllegalAnnotation {
                    source: AnnotationError::UnsupportedAnnotationForValueType(annotation.category()),
                });
            }
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetAnnotation { source, label: attribute_type_label.clone().into_owned(), annotation }
            })?;
        }
    }
    Ok(())
}

fn redefine_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &relates.related)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let definition_status =
            get_relates_status(snapshot, type_manager, relation_type.clone(), &role_label, ordering)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinitionStatus::DoesNotExist => {
                return Err(RedefineError::RelatesIsNotDefined {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                    ordering,
                })
            }
            DefinitionStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinitionStatus::ExistsSame(Some(_)) => {}
            DefinitionStatus::ExistsDifferent((existing_relates, _)) => {
                existing_relates
                    .role()
                    .set_ordering(snapshot, type_manager, thing_manager, ordering)
                    .map_err(|source| RedefineError::CreateRelates { source, relates: relates.to_owned() })?;
            }
        }
    }
    Ok(())
}

fn redefine_relates_capabilities<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(typeql_relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_relates.related)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        let relates =
            resolve_relates_declared(snapshot, type_manager, relation_type.clone(), &role_label.name.as_str())
                .map_err(|source| RedefineError::DefinitionResolution { source })?;

        redefine_relates_annotations(snapshot, type_manager, thing_manager, redefinition_levels, &label, relates.clone(), capability)?;
        redefine_relates_override(snapshot, type_manager, thing_manager, redefinition_levels, &label, relates, typeql_relates)?;
    }
    Ok(())
}

fn redefine_relates_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    relation_label: &Label<'a>,
    relates: Relates<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    for typeql_annotation in &typeql_capability.annotations {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, relation_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        let converted_for_relates = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            &relation_label,
            relates.clone(),
            annotation.clone(),
        );
        match converted_for_relates {
            Ok(Some(relates_annotation)) => {
                relates.set_annotation(snapshot, type_manager, thing_manager, relates_annotation).map_err(
                    |source| RedefineError::SetAnnotation {
                        label: relation_label.clone().into_owned(),
                        source,
                        annotation,
                    },
                )?;
            }
            Ok(None) => {}
            Err(_) => {
                if let Some(converted_for_role) = type_convert_and_validate_annotation_redefinition_need(
                    snapshot,
                    type_manager,
                    &relation_label,
                    relates.role(),
                    annotation.clone(),
                )? {
                    relates.role().set_annotation(snapshot, type_manager, thing_manager, converted_for_role).map_err(
                        |source| RedefineError::SetAnnotation {
                            label: relation_label.clone().into_owned(),
                            source,
                            annotation,
                        },
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn redefine_relates_override<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    relation_label: &Label<'a>,
    relates: Relates<'static>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    if let Some(overridden_label) = &typeql_relates.overridden {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, relation_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let overridden_relates =
            resolve_relates(snapshot, type_manager, relates.relation(), &overridden_label.ident.as_str())
                .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let need_redefine = check_can_and_need_redefine_override(
            snapshot,
            type_manager,
            &relation_label,
            relates.clone(),
            overridden_relates.clone(),
        )?;
        if need_redefine {
            relates
                .set_override(snapshot, type_manager, thing_manager, overridden_relates)
                .map_err(|source| RedefineError::SetOverride { label: relation_label.clone().into_owned(), source })?;
        }
    }
    Ok(())
}

fn redefine_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Owns(owns) = &capability.base else {
            continue;
        };
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?; // TODO: Set it only when we change something!

        let (attr_label, ordering) = type_ref_to_label_and_ordering(&label, &owns.owned)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status =
            get_owns_status(snapshot, type_manager, object_type.clone(), attribute_type.clone(), ordering)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinitionStatus::DoesNotExist => {
                return Err(RedefineError::OwnsIsNotDefined {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                    ordering,
                })
            }
            DefinitionStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinitionStatus::ExistsSame(Some(_)) => {}
            DefinitionStatus::ExistsDifferent((existing_owns, _)) => {
                existing_owns
                    .set_ordering(snapshot, type_manager, thing_manager, ordering)
                    .map_err(|source| RedefineError::SetOwnsOrdering { owns: owns.clone(), source })?;
            }
        }
    }
    Ok(())
}

fn redefine_owns_capabilities(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Owns(typeql_owns) = &capability.base else {
            continue;
        };
        let (attr_label, _) = type_ref_to_label_and_ordering(&label, &typeql_owns.owned)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;
        let owns = resolve_owns_declared(snapshot, type_manager, object_type.clone(), attribute_type.clone())
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        redefine_owns_annotations(snapshot, type_manager, thing_manager, redefinition_levels, &label, owns.clone(), capability)?;
        redefine_owns_override(snapshot, type_manager, thing_manager, redefinition_levels, &label, owns, typeql_owns)?;
    }
    Ok(())
}

fn redefine_owns_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    owner_label: &Label<'a>,
    owns: Owns<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    for typeql_annotation in &typeql_capability.annotations {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, owner_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            &owner_label,
            owns.clone(),
            annotation.clone(),
        )? {
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetAnnotation { label: owner_label.clone().into_owned(), source, annotation }
            })?;
        }
    }
    Ok(())
}

fn redefine_owns_override<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    owner_label: &Label<'a>,
    owns: Owns<'static>,
    typeql_owns: &TypeQLOwns,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    if let Some(overridden_label) = &typeql_owns.overridden {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, owner_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let overridden_label = Label::parse_from(overridden_label.ident.as_str());
        let overridden_attribute_type = resolve_attribute_type(snapshot, type_manager, &overridden_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        let overridden_owns = resolve_owns(snapshot, type_manager, owns.owner(), overridden_attribute_type)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let need_redefine = check_can_and_need_redefine_override(
            snapshot,
            type_manager,
            &owner_label,
            owns.clone(),
            overridden_owns.clone(),
        )?;
        if need_redefine {
            owns.set_override(snapshot, type_manager, thing_manager, overridden_owns)
                .map_err(|source| RedefineError::SetOverride { label: owner_label.clone().into_owned(), source })?
        }
    }
    Ok(())
}

fn redefine_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::TypeAxioms;

    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(plays) = &capability.base else {
            continue;
        };
        set_redefinition_level_and_error_if_mixed(redefinition_levels, label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let role_label = Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status = get_plays_status(snapshot, type_manager, object_type.clone(), role_type.clone())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinitionStatus::DoesNotExist => {
                return Err(RedefineError::PlaysIsNotDefined {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                })
            }
            DefinitionStatus::ExistsSame(Some(_)) => {}
            DefinitionStatus::ExistsSame(None) => unreachable!("Existing plays concept expected"),
            DefinitionStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
        }
    }
    Ok(())
}

fn redefine_plays_capabilities(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| RedefineError::DefinitionResolution { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(typeql_plays) = &capability.base else {
            continue;
        };
        let role_label =
            Label::build_scoped(typeql_plays.role.name.ident.as_str(), typeql_plays.role.scope.ident.as_str());
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;
        let plays = resolve_plays_declared(snapshot, type_manager, object_type.clone(), role_type.clone())
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        redefine_plays_annotations(snapshot, type_manager, thing_manager, redefinition_levels, &label, plays.clone(), capability)?;
        redefine_plays_override(snapshot, type_manager, thing_manager, redefinition_levels, &label, plays, typeql_plays)?;
    }
    Ok(())
}

fn redefine_plays_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    player_label: &Label<'a>,
    plays: Plays<'static>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    for typeql_annotation in &typeql_capability.annotations {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, player_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| RedefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_redefinition_need(
            snapshot,
            type_manager,
            &player_label,
            plays.clone(),
            annotation.clone(),
        )? {
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                RedefineError::SetAnnotation { label: player_label.clone().into_owned(), source, annotation }
            })?;
        }
    }
    Ok(())
}

fn redefine_plays_override<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    player_label: &Label<'a>,
    plays: Plays<'static>,
    typeql_plays: &TypeQLPlays,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::ConstraintsAndTriggers;

    if let Some(overridden_type_name) = &typeql_plays.overridden {
        set_redefinition_level_and_error_if_mixed(redefinition_levels, player_label.scoped_name().as_str(), REDEFINITION_LEVEL)?;

        let overridden_label = named_type_to_label(overridden_type_name)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;
        let overridden_plays = resolve_plays_role_label(snapshot, type_manager, plays.player(), &overridden_label)
            .map_err(|source| RedefineError::DefinitionResolution { source })?;

        let need_redefine = check_can_and_need_redefine_override(
            snapshot,
            type_manager,
            &player_label,
            plays.clone(),
            overridden_plays.clone(),
        )?;
        if need_redefine {
            plays
                .set_override(snapshot, type_manager, thing_manager, overridden_plays)
                .map_err(|source| RedefineError::SetOverride { label: player_label.clone().into_owned(), source })?
        }
    }
    Ok(())
}

fn redefine_functions(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    function_declaration: &Function,
) -> Result<(), RedefineError> {
    static REDEFINITION_LEVEL: RedefinitionLevel = RedefinitionLevel::Functions;
    Err(RedefineError::Unimplemented)
}

pub(crate) fn check_can_and_need_redefine_sub<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    new_supertype: T,
) -> Result<bool, RedefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype.clone())
        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Err(RedefineError::TypeSubIsNotDefined {
            label: label.clone().into_owned(),
            supertype: new_supertype
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                .into_owned(),
        }),
        DefinitionStatus::ExistsSame(_) => Ok(false),
        DefinitionStatus::ExistsDifferent(_) => Ok(true),
    }
}

pub(crate) fn check_can_and_need_redefine_override<'a, CAP: concept::type_::Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    new_override: CAP,
) -> Result<bool, RedefineError> {
    let definition_status = get_override_status(snapshot, type_manager, capability, new_override.clone())
        .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Err(RedefineError::CapabilityOverrideIsNotDefined {
            label: label.clone().into_owned(),
            overridden_interface: new_override
                .interface()
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| RedefineError::UnexpectedConceptRead { source })?
                .into_owned(),
        }),
        DefinitionStatus::ExistsSame(_) => Ok(false),
        DefinitionStatus::ExistsDifferent(_) => Ok(true),
    }
}

pub(crate) fn type_convert_and_validate_annotation_redefinition_need<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    annotation: Annotation,
) -> Result<Option<T::AnnotationType>, RedefineError> {
    let converted = T::AnnotationType::try_from(annotation.clone())
        .map_err(|source| RedefineError::IllegalAnnotation { source })?;

    let definition_status =
        get_type_annotation_status(snapshot, type_manager, type_, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => {
            Err(RedefineError::TypeAnnotationIsNotDefined { label: label.clone().into_owned(), annotation })
        }
        DefinitionStatus::ExistsSame(_) => Ok(None),
        DefinitionStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

pub(crate) fn capability_convert_and_validate_annotation_redefinition_need<'a, CAP: concept::type_::Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    annotation: Annotation,
) -> Result<Option<CAP::AnnotationType>, RedefineError> {
    let converted = CAP::AnnotationType::try_from(annotation.clone())
        .map_err(|source| RedefineError::IllegalAnnotation { source })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, capability, &converted, annotation.category())
            .map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => {
            Err(RedefineError::CapabilityAnnotationIsNotDefined { label: label.clone().into_owned(), annotation })
        }
        DefinitionStatus::ExistsSame(_) => Ok(None),
        DefinitionStatus::ExistsDifferent(_) => Ok(Some(converted)),
    }
}

fn err_capability_kind_mismatch(
    capability_receiver: &Label<'_>,
    capability_provider: &Label<'_>,
    capability: &Capability,
    expected_kind: Kind,
    actual_kind: Kind,
) -> RedefineError {
    RedefineError::CapabilityKindMismatch {
        capability_receiver: capability_receiver.clone().into_owned(),
        capability_provider: capability_provider.clone().into_owned(),
        capability: capability.clone(),
        expected_kind,
        actual_kind,
    }
}

fn set_redefinition_level_and_error_if_mixed(
    current_redefinition_levels: &mut HashMap<String, RedefinitionLevel>,
    target: &str,
    target_level: RedefinitionLevel,
) -> Result<(), RedefineError> {
    match current_redefinition_levels.get(target) {
        Some(current_level) if current_level != &target_level => Err(RedefineError::RedefiningMixedRedefinitionLevelsInOneQuery {
            target: target.to_string(),
            first_redefinition_level: current_level.clone(),
            second_redefinition_level: target_level.clone(),
        }),
        Some(_) => Ok(()),
        None => {
            current_redefinition_levels.insert(target.to_string(), target_level);
            Ok(())
        },
        
    }
}

#[derive(Debug)]
pub enum RedefineError {
    Unimplemented,
    UnexpectedConceptRead {
        source: ConceptReadError,
    },
    DefinitionResolution {
        source: SymbolResolutionError,
    },
    RedefiningMixedRedefinitionLevelsInOneQuery {
        target: String,
        first_redefinition_level: RedefinitionLevel,
        second_redefinition_level: RedefinitionLevel,
    },
    RedefinedNothing {},
    TypeCreateRequiresKind {
        type_declaration: Type,
    },
    TypeCreateError {
        source: ConceptWriteError,
        type_declaration: Type,
    },
    RoleTypeDirectCreate {
        type_declaration: Type,
    },
    StructRedefinitionIsNotSupported {
        struct_declaration: Struct,
    },
    StructFieldDoesNotExist {
        field: Field,
    },
    StructFieldDeleteError {
        source: ConceptWriteError,
        struct_name: String,
        struct_field: Field,
    },
    StructFieldCreateError {
        source: ConceptWriteError,
        struct_name: String,
        struct_field: Field,
    },
    SetSupertype {
        sub: typeql::schema::definable::type_::capability::Sub,
        source: ConceptWriteError,
    },
    TypeCannotHaveCapability {
        label: Label<'static>,
        kind: Kind,
        capability: Capability,
    },
    AttributeTypeBadValueType {
        source: SymbolResolutionError,
    },
    TypeSubIsNotDefined {
        label: Label<'static>,
        supertype: Label<'static>,
    },
    StructIsNotDefined {
        declaration: Struct,
    },
    RelatesIsNotDefined {
        label: Label<'static>,
        declaration: Capability,
        ordering: Ordering,
    },
    OwnsIsNotDefined {
        label: Label<'static>,
        declaration: Capability,
        ordering: Ordering,
    },
    PlaysIsNotDefined {
        label: Label<'static>,
        declaration: Capability,
    },
    CapabilityOverrideIsNotDefined {
        label: Label<'static>,
        overridden_interface: Label<'static>,
    },
    AttributeTypeValueTypeIsNotDefined {
        label: Label<'static>,
        value_type: ValueType,
    },
    // Careful with the error message as it is also used for value types (stored on their attribute type)!
    TypeAnnotationIsNotDefined {
        label: Label<'static>,
        annotation: Annotation,
    },
    CapabilityAnnotationIsNotDefined {
        label: Label<'static>,
        annotation: Annotation,
    },
    SetValueType {
        label: Label<'static>,
        value_type: ValueType,
        source: ConceptWriteError,
    },
    RelatesRoleMustBeLabelAndNotOptional {
        relation: Label<'static>,
        role_label: TypeRefAny,
    },
    CreateRelates {
        relates: TypeQLRelates,
        source: ConceptWriteError,
    },
    OverriddenRelatesNotFound {
        relates: TypeQLRelates,
    },
    CreatePlays {
        plays: TypeQLPlays,
        source: ConceptWriteError,
    },
    PlaysRoleTypeNotFound {
        plays: TypeQLPlays,
    },
    OverriddenPlaysNotFound {
        plays: TypeQLPlays,
    },
    OverriddenPlaysRoleTypeNotFound {
        plays: TypeQLPlays,
    },
    OwnsAttributeMustBeLabelOrList {
        owns: TypeQLOwns,
    },
    CreateOwns {
        owns: TypeQLOwns,
        source: ConceptWriteError,
    },
    SetOwnsOrdering {
        owns: TypeQLOwns,
        source: ConceptWriteError,
    },
    OwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    CreateRelatesModifiesExistingOrdering {
        label: Label<'static>,
        existing_ordering: Ordering,
        new_ordering: Ordering,
    },
    IllegalAnnotation {
        source: AnnotationError,
    },
    SetAnnotation {
        source: ConceptWriteError,
        label: Label<'static>,
        annotation: Annotation,
    },
    SetOverride {
        source: ConceptWriteError,
        label: Label<'static>,
    },
    CapabilityKindMismatch {
        capability_receiver: Label<'static>,
        capability_provider: Label<'static>,
        capability: Capability,
        expected_kind: Kind,
        actual_kind: Kind,
    },
    LiteralParseError {
        source: LiteralParseError,
    },
}

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &Capability) -> RedefineError {
    RedefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
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
