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
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::{Relates, RelatesAnnotation},
        type_manager::TypeManager,
        KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::{definition::r#struct::StructDefinitionField, type_::Kind},
    value::{label::Label, value_type::ValueType},
};
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    annotation::Annotation as TypeQLAnnotation,
    common::token,
    query::schema::Define,
    schema::definable::{
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates},
            Capability, CapabilityBase,
        },
        Struct, Type,
    },
    type_::Optional,
    Definable, ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    definition_status::{
        get_attribute_type_status, get_capability_annotation_status, get_entity_type_status, get_override_status,
        get_owns_status, get_plays_status, get_relates_status, get_relation_type_status, get_struct_field_status,
        get_struct_status, get_sub_status, get_type_annotation_status, get_value_type_status, DefinitionStatus,
    },
    util::{
        filter_variants, resolve_type, resolve_value_type, try_unwrap, type_ref_to_label_and_ordering,
        type_to_object_type,
    },
    SymbolResolutionError,
};

macro_rules! verify_empty_annotations_for_capability {
    ($capability:ident, $annotation_error:path) => {
        if let Some(typeql_annotation) = &$capability.annotations.first() {
            let annotation =
                translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
            Err(DefineError::IllegalAnnotation { source: $annotation_error(annotation.category()) })
        } else {
            Ok(())
        }
    };
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    define: Define,
) -> Result<(), DefineError> {
    process_struct_definitions(snapshot, type_manager, &define.definables)?;
    process_type_definitions(snapshot, type_manager, thing_manager, &define.definables)?;
    process_function_definitions(snapshot, type_manager, &define.definables)?;
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
    declarations.clone().try_for_each(|declaration| define_types(snapshot, type_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_type_annotations(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| define_sub(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_value_type(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| define_alias(snapshot, type_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_relates(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| define_owns(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| define_plays(snapshot, type_manager, thing_manager, declaration))?;
    Ok(())
}

fn process_function_definitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    filter_variants!(Definable::Function : definables)
        .try_for_each(|declaration| define_functions(snapshot, type_manager, definables))?;
    Ok(())
}

fn define_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &Struct,
) -> Result<(), DefineError> {
    let name = struct_definable.ident.as_str();

    let definition_status = get_struct_status(snapshot, type_manager, name)
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => {}
        DefinitionStatus::ExistsSame(_) => return Ok(()),
        DefinitionStatus::ExistsDifferent(_) => unreachable!("Structs cannot differ"),
    }

    type_manager
        .create_struct(snapshot, name.to_owned())
        .map_err(|err| DefineError::StructCreateError { source: err, struct_declaration: struct_definable.clone() })?;
    Ok(())
}

fn define_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &Struct,
) -> Result<(), DefineError> {
    let name = struct_definable.ident.as_str();
    let struct_key = type_manager
        .get_struct_definition_key(snapshot, name)
        .map_err(|err| DefineError::UnexpectedConceptRead { source: err })?
        .unwrap();

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)?;

        let definition_status = get_struct_field_status(
            snapshot,
            type_manager,
            struct_key.clone(),
            field.key.as_str(),
            value_type.clone(),
            optional,
        )
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        match definition_status {
            DefinitionStatus::DoesNotExist => {}
            DefinitionStatus::ExistsSame(_) => return Ok(()),
            DefinitionStatus::ExistsDifferent(existing_field) => {
                return Err(DefineError::StructFieldAlreadyDefinedButDifferent {
                    field: field.to_owned(),
                    existing_field,
                })
            }
        }

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
            .map_err(|err| DefineError::StructFieldCreateError {
                source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            })?;
    }
    Ok(())
}

fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), DefineError> {
    let optional = matches!(&field.type_, TypeRefAny::Optional(_));
    match &field.type_ {
        TypeRefAny::Type(TypeRef::Named(named))
        | TypeRefAny::Optional(Optional { inner: TypeRef::Named(named), .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)
                .map_err(|source| DefineError::StructFieldCouldNotResolveValueType { source })?;
            Ok((value_type, optional))
        }
        TypeRefAny::Type(TypeRef::Variable(_)) | TypeRefAny::Optional(Optional { inner: TypeRef::Variable(_), .. }) => {
            return Err(DefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
        }
        TypeRefAny::List(_) => {
            return Err(DefineError::StructFieldIllegalList { field_declaration: field.clone() });
        }
    }
}

fn define_types(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    match type_declaration.kind {
        None => {
            resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
        }
        Some(token::Kind::Role) => {
            return Err(DefineError::RoleTypeDirectCreate { type_declaration: type_declaration.clone() })?;
        }
        Some(token::Kind::Entity) => {
            let definition_status = get_entity_type_status(snapshot, type_manager, &label)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
            match definition_status {
                DefinitionStatus::DoesNotExist => {}
                DefinitionStatus::ExistsSame(_) => return Ok(()),
                DefinitionStatus::ExistsDifferent(_) => unreachable!("Entity types cannot differ"),
            }
            type_manager.create_entity_type(snapshot, &label).map(|x| answer::Type::Entity(x)).map_err(|err| {
                DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() }
            })?;
        }
        Some(token::Kind::Relation) => {
            let definition_status = get_relation_type_status(snapshot, type_manager, &label)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
            match definition_status {
                DefinitionStatus::DoesNotExist => {}
                DefinitionStatus::ExistsSame(_) => return Ok(()),
                DefinitionStatus::ExistsDifferent(_) => unreachable!("Relation types cannot differ"),
            }
            type_manager.create_relation_type(snapshot, &label).map(|x| answer::Type::Relation(x)).map_err(|err| {
                DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() }
            })?;
        }
        Some(token::Kind::Attribute) => {
            let definition_status = get_attribute_type_status(snapshot, type_manager, &label)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
            match definition_status {
                DefinitionStatus::DoesNotExist => {}
                DefinitionStatus::ExistsSame(_) => return Ok(()),
                DefinitionStatus::ExistsDifferent(_) => unreachable!("Attribute types cannot differ"),
            }
            type_manager.create_attribute_type(snapshot, &label).map(|x| answer::Type::Attribute(x)).map_err(
                |err| DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() },
            )?;
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
    for typeql_annotation in &type_declaration.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        match type_.clone() {
            TypeEnum::Entity(entity) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    entity.clone(),
                    annotation.clone(),
                )? {
                    entity
                        .set_annotation(snapshot, type_manager, thing_manager, converted)
                        .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation })?;
                }
            }
            TypeEnum::Relation(relation) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    relation.clone(),
                    annotation.clone(),
                )? {
                    relation
                        .set_annotation(snapshot, type_manager, thing_manager, converted)
                        .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation })?;
                }
            }
            TypeEnum::Attribute(attribute) => {
                if let Some(converted) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &label,
                    attribute.clone(),
                    annotation.clone(),
                )? {
                    if converted.is_value_type_annotation() {
                        return Err(DefineError::IllegalAnnotation {
                            source: AnnotationError::UnsupportedAnnotationForAttributeType(annotation.category()),
                        });
                    }
                    attribute
                        .set_annotation(snapshot, type_manager, thing_manager, converted)
                        .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation })?;
                }
            }
            TypeEnum::RoleType(_) => unreachable!("Role annotations are syntactically on relates"),
        }
    }
    Ok(())
}

fn define_alias(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    &type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(DefineError::Unimplemented))?;

    // TODO: Uncomment when alias is implemented
    // define_alias_annotations(capability)?;

    Ok(())
}

fn define_alias_annotations(typeql_capability: &Capability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
}

fn define_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };
        let supertype_label = Label::parse_from(&sub.supertype_label.ident.as_str());
        let supertype = resolve_type(snapshot, type_manager, &supertype_label)
            .map_err(|source| DefineError::TypeLookup { source })?;
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
                let need_define = check_can_and_need_define_supertype(
                    snapshot,
                    type_manager,
                    &label,
                    type_.clone(),
                    supertype.clone(),
                )?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                }
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                let need_define = check_can_and_need_define_supertype(
                    snapshot,
                    type_manager,
                    &label,
                    type_.clone(),
                    supertype.clone(),
                )?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                }
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                let need_define = check_can_and_need_define_supertype(
                    snapshot,
                    type_manager,
                    &label,
                    type_.clone(),
                    supertype.clone(),
                )?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                }
            }
            (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => {
                return Err(err_unsupported_capability(&label, Kind::Role, capability));
            }
            _ => unreachable!(),
        }

        define_sub_annotations(capability)?;
    }
    Ok(())
}

fn define_sub_annotations(typeql_capability: &Capability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
}

fn define_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::ValueType(value_type_statement) = &capability.base else {
            continue;
        };
        let TypeEnum::Attribute(attribute_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };
        let value_type = resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
            .map_err(|source| DefineError::AttributeTypeBadValueType { source })?;

        let definition_status =
            get_value_type_status(snapshot, type_manager, attribute_type.clone(), value_type.clone())
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let define_needed = match definition_status {
            DefinitionStatus::DoesNotExist => true,
            DefinitionStatus::ExistsSame(_) => false,
            DefinitionStatus::ExistsDifferent(existing_value_type) => {
                return Err(DefineError::AttributeTypeValueTypeAlreadyDefinedButDifferent {
                    label: label.to_owned(),
                    value_type,
                    existing_value_type,
                })
            }
        };

        if define_needed {
            attribute_type
                .set_value_type(snapshot, type_manager, thing_manager, value_type.clone())
                .map_err(|source| DefineError::SetValueType { label: label.to_owned(), value_type, source })?;
        }

        define_value_type_annotations(
            snapshot,
            type_manager,
            thing_manager,
            attribute_type.clone(),
            &label,
            capability,
        )?;
    }
    Ok(())
}

fn define_value_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute_type: AttributeType<'a>,
    attribute_type_label: &Label<'a>,
    typeql_capability: &Capability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = type_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            &attribute_type_label,
            attribute_type.clone(),
            annotation.clone(),
        )? {
            if !converted.is_value_type_annotation() {
                return Err(DefineError::IllegalAnnotation {
                    source: AnnotationError::UnsupportedAnnotationForValueType(annotation.category()),
                });
            }
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation { source, label: attribute_type_label.clone().into_owned(), annotation }
            })?;
        }
    }
    Ok(())
}

fn define_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Relates(relates) = &capability.base else {
            continue;
        };
        let TypeEnum::Relation(relation_type) = &type_ else {
            return Err(err_unsupported_capability(&label, type_.kind(), capability));
        };

        let (role_label, ordering) = type_ref_to_label_and_ordering(&relates.related).map_err(|_| {
            DefineError::RelatesRoleMustBeLabelAndNotOptional {
                relation: label.to_owned(),
                role_label: relates.related.clone(),
            }
        })?;

        let definition_status = get_relates_status(snapshot, type_manager, relation_type.clone(), &label, ordering)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let (defined, is_new) = match definition_status {
            DefinitionStatus::DoesNotExist => {
                let init_cardinality = if let Some(typeql_cardinality) = capability
                    .annotations
                    .iter()
                    .find(|annotation| matches!(annotation, TypeQLAnnotation::Cardinality(_)))
                {
                    let annotation = translate_annotation(typeql_cardinality)
                        .map_err(|source| DefineError::LiteralParseError { source })?;
                    match annotation {
                        Annotation::Cardinality(card) => Some(card),
                        other => {
                            debug_assert!(false, "Expected to translate found typeql annotation for relates to Cardinality. Got {:?} instead", other);
                            None
                        }
                    }
                } else {
                    None
                };

                let relates = relation_type
                    .create_relates(
                        snapshot,
                        type_manager,
                        thing_manager,
                        role_label.name.as_str(),
                        ordering,
                        init_cardinality,
                    )
                    .map_err(|source| DefineError::CreateRelates { source, relates: relates.to_owned() })?;
                (relates, true)
            }
            DefinitionStatus::ExistsSame(Some((existing_relates, _))) => (existing_relates, false),
            DefinitionStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinitionStatus::ExistsDifferent((existing_relates, existing_ordering)) => {
                return Err(DefineError::RelatesAlreadyDefinedButDifferent {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                    ordering,
                    existing_relates,
                    existing_ordering,
                })
            }
        };

        define_relates_annotations(
            snapshot,
            type_manager,
            thing_manager,
            &label,
            defined.clone(),
            &capability,
            is_new,
        )?;
        define_relates_overridden(snapshot, type_manager, thing_manager, &label, defined, relates)?;
    }
    Ok(())
}

fn define_relates_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), DefineError> {
    if let Some(overridden_label) = &typeql_relates.overridden {
        let overridden_relates_opt = relates
            .relation()
            .get_relates_role_name_with_overridden(snapshot, type_manager, overridden_label.ident.as_str())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let overridden_relates = if let Some(overridden_relates) = overridden_relates_opt {
            overridden_relates
        } else {
            return Err(DefineError::OverriddenRelatesNotFound { relates: typeql_relates.clone() });
        };

        let need_define = check_can_and_need_define_override(
            snapshot,
            type_manager,
            &relation_label,
            relates.clone(),
            overridden_relates.clone(),
        )?;
        if need_define {
            relates
                .set_override(snapshot, type_manager, thing_manager, overridden_relates)
                .map_err(|source| DefineError::SetOverride { label: relation_label.clone().into_owned(), source })?;
        }
    }
    Ok(())
}

fn define_relates_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_capability: &Capability,
    is_new: bool,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        let converted_for_relates = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            &relation_label,
            relates.clone(),
            annotation.clone(),
        );
        match converted_for_relates {
            Ok(Some(relates_annotation)) => {
                // New relates should set Cardinality on initialization
                if matches!(relates_annotation, RelatesAnnotation::Cardinality(_)) && is_new {
                    continue;
                }
                relates.set_annotation(snapshot, type_manager, thing_manager, relates_annotation).map_err(
                    |source| DefineError::SetAnnotation {
                        label: relation_label.clone().into_owned(),
                        source,
                        annotation,
                    },
                )?;
            }
            Ok(None) => {}
            Err(_) => {
                if let Some(converted_for_role) = type_convert_and_validate_annotation_definition_need(
                    snapshot,
                    type_manager,
                    &relation_label,
                    relates.role(),
                    annotation.clone(),
                )? {
                    relates.role().set_annotation(snapshot, type_manager, thing_manager, converted_for_role).map_err(
                        |source| DefineError::SetAnnotation {
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

fn define_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Owns(owns) = &capability.base else {
            continue;
        };
        let (attr_label, ordering) = type_ref_to_label_and_ordering(&owns.owned)
            .map_err(|_| DefineError::OwnsAttributeMustBeLabelOrList { owns: owns.clone() })?;
        let attribute_type_opt = type_manager
            .get_attribute_type(snapshot, &attr_label)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let attribute_type = if let Some(attribute_type) = attribute_type_opt {
            attribute_type
        } else {
            return Err(DefineError::CreateOwnsAttributeTypeNotFound { owns: owns.clone() })?;
        };

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status =
            get_owns_status(snapshot, type_manager, object_type.clone(), attribute_type.clone(), ordering)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let (defined, is_new) = match definition_status {
            DefinitionStatus::DoesNotExist => {
                let owns = object_type
                    .set_owns(snapshot, type_manager, thing_manager, attribute_type)
                    .map_err(|source| DefineError::CreateOwns { owns: owns.clone(), source })?;
                (owns, true)
            }
            DefinitionStatus::ExistsSame(Some((existing_owns, _))) => (existing_owns, false),
            DefinitionStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinitionStatus::ExistsDifferent((existing_owns, existing_ordering)) => {
                return Err(DefineError::OwnsAlreadyDefinedButDifferent {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                    ordering,
                    existing_owns,
                    existing_ordering,
                })
            }
        };

        if is_new {
            defined.set_ordering(snapshot, type_manager, thing_manager, ordering);
        }

        define_owns_annotations(snapshot, type_manager, thing_manager, &label, defined.clone(), &capability)?;
        define_owns_overridden(snapshot, type_manager, thing_manager, &label, defined, owns)?;
    }
    Ok(())
}

fn define_owns_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_owns: &TypeQLOwns,
) -> Result<(), DefineError> {
    if let Some(overridden_label) = &typeql_owns.overridden {
        let overridden_label = Label::parse_from(overridden_label.ident.as_str());
        let overridden_attribute_type_opt = type_manager
            .get_attribute_type(snapshot, &overridden_label)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let overridden_attribute_type = if let Some(type_) = overridden_attribute_type_opt {
            type_
        } else {
            return Err(DefineError::OverriddenOwnsAttributeTypeNotFound { owns: typeql_owns.clone() })?;
        };

        let overridden_owns_opt = owns
            .owner()
            .get_owns_attribute_with_overridden(snapshot, type_manager, overridden_attribute_type.clone())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let overridden_owns = if let Some(overridden_owns) = overridden_owns_opt {
            overridden_owns
        } else {
            return Err(DefineError::OverriddenOwnsNotFound { owns: typeql_owns.clone() });
        };

        let need_define = check_can_and_need_define_override(
            snapshot,
            type_manager,
            &owner_label,
            owns.clone(),
            overridden_owns.clone(),
        )?;
        if need_define {
            owns.set_override(snapshot, type_manager, thing_manager, overridden_owns)
                .map_err(|source| DefineError::SetOverride { label: owner_label.clone().into_owned(), source })?
        }
    }
    Ok(())
}

fn define_owns_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_capability: &Capability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            &owner_label,
            owns.clone(),
            annotation.clone(),
        )? {
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation { label: owner_label.clone().into_owned(), source, annotation }
            })?;
        }
    }
    Ok(())
}

fn define_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label).map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(plays) = &capability.base else {
            continue;
        };
        let role_label = Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
        let role_type_opt = type_manager
            .get_role_type(snapshot, &role_label)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let role_type = if let Some(role_type) = role_type_opt {
            role_type
        } else {
            return Err(DefineError::CreatePlaysRoleTypeNotFound { plays: plays.clone() })?;
        };

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status = get_plays_status(snapshot, type_manager, object_type.clone(), role_type.clone())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let defined =
            match definition_status {
                DefinitionStatus::DoesNotExist => object_type
                    .set_plays(snapshot, type_manager, thing_manager, role_type)
                    .map_err(|source| DefineError::CreatePlays { plays: plays.clone(), source })?,
                DefinitionStatus::ExistsSame(Some(existing_plays)) => existing_plays,
                DefinitionStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
                DefinitionStatus::ExistsDifferent(existing_owns) => unreachable!("Plays cannot differ"),
            };

        define_plays_annotations(snapshot, type_manager, thing_manager, &label, defined.clone(), &capability)?;
        define_plays_overridden(snapshot, type_manager, thing_manager, &label, defined, plays)?;
    }
    Ok(())
}

fn define_plays_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_plays: &TypeQLPlays,
) -> Result<(), DefineError> {
    if let Some(overridden_label) = &typeql_plays.overridden {
        let overridden_label = Label::parse_from(overridden_label.ident.as_str());
        let overridden_role_type_opt = type_manager
            .get_role_type(snapshot, &overridden_label)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let overridden_role_type = if let Some(type_) = overridden_role_type_opt {
            type_
        } else {
            return Err(DefineError::OverriddenPlaysRoleTypeNotFound { plays: typeql_plays.clone() })?;
        };

        let overridden_plays_opt = plays
            .player()
            .get_plays_role_with_overridden(snapshot, type_manager, overridden_role_type)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let overridden_plays = if let Some(overridden_plays) = overridden_plays_opt {
            overridden_plays
        } else {
            return Err(DefineError::OverriddenPlaysNotFound { plays: typeql_plays.clone() });
        };

        let need_define = check_can_and_need_define_override(
            snapshot,
            type_manager,
            &player_label,
            plays.clone(),
            overridden_plays.clone(),
        )?;
        if need_define {
            plays
                .set_override(snapshot, type_manager, thing_manager, overridden_plays)
                .map_err(|source| DefineError::SetOverride { label: player_label.clone().into_owned(), source })?
        }
    }
    Ok(())
}

fn define_plays_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_capability: &Capability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            &player_label,
            plays.clone(),
            annotation.clone(),
        )? {
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation { label: player_label.clone().into_owned(), source, annotation }
            })?;
        }
    }
    Ok(())
}

fn define_functions(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    Ok(())
}

pub(crate) fn check_can_and_need_define_supertype<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    new_supertype: T,
) -> Result<bool, DefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype.clone())
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Ok(true),
        DefinitionStatus::ExistsSame(_) => Ok(false),
        DefinitionStatus::ExistsDifferent(existing) => Err(DefineError::TypeSubAlreadyDefinedButDifferent {
            label: label.clone().into_owned(),
            supertype: new_supertype
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .into_owned(),
            existing_supertype: existing
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .into_owned(),
        }),
    }
}

pub(crate) fn check_can_and_need_define_override<'a, CAP: concept::type_::Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    new_override: CAP,
) -> Result<bool, DefineError> {
    let definition_status = get_override_status(snapshot, type_manager, capability, new_override.clone())
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Ok(true),
        DefinitionStatus::ExistsSame(_) => Ok(false),
        DefinitionStatus::ExistsDifferent(existing) => Err(DefineError::CapabilityOverrideAlreadyDefinedButDifferent {
            label: label.clone().into_owned(),
            overridden_interface: new_override
                .interface()
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .into_owned(),
            existing_overridden_interface: existing
                .interface()
                .get_label_cloned(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .into_owned(),
        }),
    }
}

pub(crate) fn type_convert_and_validate_annotation_definition_need<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    annotation: Annotation,
) -> Result<Option<T::AnnotationType>, DefineError> {
    let converted =
        T::AnnotationType::try_from(annotation.clone()).map_err(|source| DefineError::IllegalAnnotation { source })?;

    let definition_status =
        get_type_annotation_status(snapshot, type_manager, type_, &converted, annotation.category())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Ok(Some(converted)),
        DefinitionStatus::ExistsSame(_) => Ok(None),
        DefinitionStatus::ExistsDifferent(existing) => Err(DefineError::TypeAnnotationAlreadyDefinedButDifferent {
            label: label.clone().into_owned(),
            annotation,
            existing_annotation: existing.clone().into(),
        }),
    }
}

pub(crate) fn capability_convert_and_validate_annotation_definition_need<'a, CAP: concept::type_::Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    annotation: Annotation,
) -> Result<Option<CAP::AnnotationType>, DefineError> {
    let converted = CAP::AnnotationType::try_from(annotation.clone())
        .map_err(|source| DefineError::IllegalAnnotation { source })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, capability, &converted, annotation.category())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinitionStatus::DoesNotExist => Ok(Some(converted)),
        DefinitionStatus::ExistsSame(_) => Ok(None),
        DefinitionStatus::ExistsDifferent(existing) => {
            Err(DefineError::CapabilityAnnotationAlreadyDefinedButDifferent {
                label: label.clone().into_owned(),
                annotation,
                existing_annotation: existing.clone().into(),
            })
        }
    }
}

fn err_capability_kind_mismatch(
    capability_receiver: &Label<'_>,
    capability_provider: &Label<'_>,
    capability: &Capability,
    expected_kind: Kind,
    actual_kind: Kind,
) -> DefineError {
    DefineError::CapabilityKindMismatch {
        capability_receiver: capability_receiver.clone().into_owned(),
        capability_provider: capability_provider.clone().into_owned(),
        capability: capability.clone(),
        expected_kind,
        actual_kind,
    }
}

#[derive(Debug)]
pub enum DefineError {
    Unimplemented,
    UnexpectedConceptRead {
        source: ConceptReadError,
    },
    TypeLookup {
        source: SymbolResolutionError,
    },
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
    StructCreateError {
        source: ConceptWriteError,
        struct_declaration: Struct,
    },
    StructFieldCreateError {
        source: ConceptWriteError,
        struct_name: String,
        struct_field: Field,
    },
    StructFieldIllegalList {
        field_declaration: Field,
    },
    StructFieldIllegalVariable {
        field_declaration: Field,
    },
    StructFieldIllegalNotValueType {
        scoped_label: ScopedLabel,
    },
    StructFieldCouldNotResolveValueType {
        source: SymbolResolutionError,
    },
    StructFieldAlreadyDefinedButDifferent {
        field: Field,
        existing_field: StructDefinitionField,
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
    TypeSubAlreadyDefinedButDifferent {
        label: Label<'static>,
        supertype: Label<'static>,
        existing_supertype: Label<'static>,
    },
    CapabilityOverrideAlreadyDefinedButDifferent {
        label: Label<'static>,
        overridden_interface: Label<'static>,
        existing_overridden_interface: Label<'static>,
    },
    AttributeTypeValueTypeAlreadyDefinedButDifferent {
        label: Label<'static>,
        value_type: ValueType,
        existing_value_type: ValueType,
    },
    // Careful with the error message as it is also used for value types (stored on their attribute type)!
    TypeAnnotationAlreadyDefinedButDifferent {
        label: Label<'static>,
        annotation: Annotation,
        existing_annotation: Annotation,
    },
    CapabilityAnnotationAlreadyDefinedButDifferent {
        label: Label<'static>,
        annotation: Annotation,
        existing_annotation: Annotation,
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
    CreatePlaysRoleTypeNotFound {
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
    CreateOwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    RelatesAlreadyDefinedButDifferent {
        label: Label<'static>,
        declaration: Capability,
        ordering: Ordering,
        existing_relates: Relates<'static>,
        existing_ordering: Ordering,
    },
    OwnsAlreadyDefinedButDifferent {
        label: Label<'static>,
        declaration: Capability,
        ordering: Ordering,
        existing_owns: Owns<'static>,
        existing_ordering: Ordering,
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

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &Capability) -> DefineError {
    DefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
}

impl fmt::Display for DefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
