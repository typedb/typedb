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
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::{definition::r#struct::StructDefinitionField, type_::Kind},
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    query::schema::Define,
    schema::definable::{
        function::Function,
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates},
            Capability as TypeQLCapability, CapabilityBase,
        },
        Struct, Type,
    },
    token, Definable,
};

use crate::{
    definable_resolution::{
        filter_variants, get_struct_field_value_type_optionality, resolve_attribute_type, resolve_relates,
        resolve_relates_declared, resolve_role_type, resolve_struct_definition_key, resolve_typeql_type,
        resolve_value_type, try_resolve_typeql_type, try_unwrap, type_ref_to_label_and_ordering, type_to_object_type,
        SymbolResolutionError,
    },
    definable_status::{
        get_capability_annotation_status, get_owns_status, get_plays_status, get_relates_status,
        get_struct_field_status, get_struct_status, get_sub_status, get_type_annotation_status, get_value_type_status,
        DefinableStatus,
    },
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
    declarations.clone().try_for_each(|declaration| define_type(snapshot, type_manager, declaration))?;
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
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), DefineError> {
    filter_variants!(Definable::Function : definables)
        .try_for_each(|function| define_function(snapshot, type_manager, function))?;
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
        DefinableStatus::DoesNotExist => {}
        DefinableStatus::ExistsSame(_) => return Ok(()),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Structs cannot differ"),
    }

    type_manager.create_struct(snapshot, name.to_owned()).map_err(|err| DefineError::StructCreateError {
        typedb_source: err,
        struct_declaration: struct_definable.clone(),
    })?;
    Ok(())
}

fn define_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &Struct,
) -> Result<(), DefineError> {
    let name = struct_definable.ident.as_str();
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

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
            DefinableStatus::DoesNotExist => {}
            DefinableStatus::ExistsSame(_) => return Ok(()),
            DefinableStatus::ExistsDifferent(existing_field) => {
                return Err(DefineError::StructFieldAlreadyDefinedButDifferent {
                    field: field.to_owned(),
                    existing_field,
                });
            }
        }

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
            .map_err(|err| DefineError::StructFieldCreateError {
                typedb_source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            })?;
    }
    Ok(())
}

fn define_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let existing = try_resolve_typeql_type(snapshot, type_manager, &label).map_err(|err| {
        DefineError::SymbolResolution { typedb_source: SymbolResolutionError::UnexpectedConceptRead { source: err } }
    })?;
    match type_declaration.kind {
        None => {
            if existing.is_none() {
                return Err(DefineError::SymbolResolution {
                    typedb_source: SymbolResolutionError::TypeNotFound { label },
                });
            }
        }
        Some(token::Kind::Role) => {
            return Err(DefineError::RoleTypeDirectCreate { type_declaration: type_declaration.clone() });
        }
        Some(token::Kind::Entity) => {
            if matches!(existing, Some(TypeEnum::Entity(_))) {
                return Ok(());
            }
            type_manager.create_entity_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                type_declaration: type_declaration.clone(),
            })?;
        }
        Some(token::Kind::Relation) => {
            if matches!(existing, Some(TypeEnum::Relation(_))) {
                return Ok(());
            }
            type_manager.create_relation_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                type_declaration: type_declaration.clone(),
            })?;
        }
        Some(token::Kind::Attribute) => {
            if matches!(existing, Some(TypeEnum::Attribute(_))) {
                return Ok(());
            }
            type_manager.create_attribute_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                typedb_source: err,
                type_declaration: type_declaration.clone(),
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
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
                    entity.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            label: label.to_owned(),
                            annotation_declaration: typeql_annotation.clone(),
                        }
                    })?;
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
                    relation.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            label: label.to_owned(),
                            annotation_declaration: typeql_annotation.clone(),
                        }
                    })?;
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
                    attribute.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                        DefineError::SetAnnotation {
                            typedb_source: source,
                            label: label.to_owned(),
                            annotation_declaration: typeql_annotation.clone(),
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
    type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(DefineError::Unimplemented { description: "Alias definition.".to_string() }))?;

    // TODO: Uncomment when alias is implemented
    // define_alias_annotations(capability)?;

    Ok(())
}

fn define_alias_annotations(typeql_capability: &TypeQLCapability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
}

fn define_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

    for capability in &type_declaration.capabilities {
        let CapabilityBase::Sub(sub) = &capability.base else {
            continue;
        };
        let supertype_label = Label::parse_from(sub.supertype_label.ident.as_str());
        let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
        if type_.kind() != supertype.kind() {
            return Err(err_capability_kind_mismatch(
                &label,
                &supertype_label,
                capability,
                type_.kind(),
                supertype.kind(),
            ));
        }

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), typedb_source: source })?;
                }
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), typedb_source: source })?;
                }
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                let need_define =
                    check_can_and_need_define_sub(snapshot, type_manager, &label, type_.clone(), supertype.clone())?;
                if need_define {
                    type_
                        .set_supertype(snapshot, type_manager, thing_manager, supertype)
                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), typedb_source: source })?;
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

fn define_sub_annotations(typeql_capability: &TypeQLCapability) -> Result<(), DefineError> {
    verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
}

fn define_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
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

        let definition_status =
            get_value_type_status(snapshot, type_manager, attribute_type.clone(), value_type.clone())
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let define_needed = match definition_status {
            DefinableStatus::DoesNotExist => true,
            DefinableStatus::ExistsSame(_) => false,
            DefinableStatus::ExistsDifferent(existing_value_type) => {
                return Err(DefineError::AttributeTypeValueTypeAlreadyDefinedButDifferent {
                    label: label.to_owned(),
                    value_type,
                    existing_value_type,
                });
            }
        };

        if define_needed {
            attribute_type.set_value_type(snapshot, type_manager, thing_manager, value_type.clone()).map_err(
                |source| DefineError::SetValueType { label: label.to_owned(), value_type, typedb_source: source },
            )?;
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
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = type_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            attribute_type_label,
            attribute_type.clone(),
            annotation.clone(),
        )? {
            if !converted.is_value_type_annotation() {
                return Err(DefineError::IllegalAnnotation {
                    source: AnnotationError::UnsupportedAnnotationForValueType(annotation.category()),
                });
            }
            attribute_type.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    typedb_source: source,
                    label: attribute_type_label.clone().into_owned(),
                    annotation_declaration: typeql_annotation.clone(),
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
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

        let definition_status =
            get_relates_status(snapshot, type_manager, relation_type.clone(), &role_label, ordering)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => {
                let relates = relation_type
                    .create_relates(snapshot, type_manager, thing_manager, role_label.name.as_str(), ordering)
                    .map_err(|source| DefineError::CreateRelates {
                        typedb_source: source,
                        relates: relates.to_owned(),
                    })?;
                relates
            }
            DefinableStatus::ExistsSame(Some((existing_relates, _))) => existing_relates,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing relates concept expected"),
            DefinableStatus::ExistsDifferent((existing_relates, existing_ordering)) => {
                return Err(DefineError::RelatesAlreadyDefinedButDifferent {
                    label: label.clone(),
                    role: role_label.name.to_string(),
                    ordering,
                    existing_relates: format!(
                        "{} relates {}",
                        existing_relates.relation().get_label(snapshot, type_manager).unwrap().name.to_string(),
                        existing_relates.role().get_label(snapshot, type_manager).unwrap().name.to_string()
                    ),
                    existing_ordering,
                    declaration: capability.to_owned(),
                });
            }
        };

        define_relates_annotations(snapshot, type_manager, thing_manager, &label, defined.clone(), capability)?;
    }
    Ok(())
}

fn define_relates_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            relates.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            relates.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    label: relation_label.clone().into_owned(),
                    typedb_source: source,
                    annotation_declaration: typeql_annotation.clone(),
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
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
        let relates = resolve_relates_declared(snapshot, type_manager, relation_type.clone(), role_label.name.as_str())
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        define_relates_specialise(snapshot, type_manager, thing_manager, &label, relates, typeql_relates)?;
    }
    Ok(())
}

fn define_relates_specialise<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'static>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), DefineError> {
    if let Some(specialised_label) = &typeql_relates.specialised {
        let specialised_relates =
            resolve_relates(snapshot, type_manager, relates.relation(), specialised_label.ident.as_str())
                .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let need_define = match definition_status {
            DefinableStatus::DoesNotExist => Ok(true),
            DefinableStatus::ExistsSame(_) => Ok(false),
            DefinableStatus::ExistsDifferent(existing_superrole) => {
                Err(DefineError::RelatesSpecialiseAlreadyDefinedButDifferent {
                    label: relation_label.clone().into_owned(),
                    specialised_interface: specialised_relates
                        .role()
                        .get_label(snapshot, type_manager)
                        .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                        .clone()
                        .into_owned(),
                    existing_specialised_interface: existing_superrole
                        .get_label(snapshot, type_manager)
                        .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                        .clone()
                        .into_owned(),
                })
            }
        }?;

        if need_define {
            relates.set_specialise(snapshot, type_manager, thing_manager, specialised_relates).map_err(|source| {
                DefineError::SetSpecialise { label: relation_label.clone().into_owned(), typedb_source: source }
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
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

        let definition_status =
            get_owns_status(snapshot, type_manager, object_type.clone(), attribute_type.clone(), ordering)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => {
                let owns = object_type
                    .set_owns(snapshot, type_manager, thing_manager, attribute_type, ordering)
                    .map_err(|source| DefineError::CreateOwns { owns: owns.clone(), typedb_source: source })?;
                owns
            }
            DefinableStatus::ExistsSame(Some((existing_owns, _))) => existing_owns,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing owns concept expected"),
            DefinableStatus::ExistsDifferent((existing_owns, existing_ordering)) => {
                return Err(DefineError::OwnsAlreadyDefinedButDifferent {
                    label: label.clone(),
                    declaration: capability.to_owned(),
                    ordering,
                    existing_owns: format!(
                        "{} owns {}",
                        existing_owns.owner().get_label(snapshot, type_manager).unwrap().name.to_string(),
                        existing_owns.attribute().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    ),
                    existing_ordering,
                });
            }
        };

        define_owns_annotations(snapshot, type_manager, thing_manager, &label, defined, capability)?;
    }
    Ok(())
}

fn define_owns_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            owns.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            owns.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    label: owner_label.clone().into_owned(),
                    typedb_source: source,
                    annotation_declaration: typeql_annotation.clone(),
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
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;
    for capability in &type_declaration.capabilities {
        let CapabilityBase::Plays(plays) = &capability.base else {
            continue;
        };
        let role_label = Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
        let role_type = resolve_role_type(snapshot, type_manager, &role_label)
            .map_err(|typedb_source| DefineError::SymbolResolution { typedb_source })?;

        let object_type =
            type_to_object_type(&type_).map_err(|_| err_unsupported_capability(&label, type_.kind(), capability))?;

        let definition_status = get_plays_status(snapshot, type_manager, object_type.clone(), role_type.clone())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        let defined = match definition_status {
            DefinableStatus::DoesNotExist => object_type
                .set_plays(snapshot, type_manager, thing_manager, role_type)
                .map_err(|source| DefineError::CreatePlays { plays: plays.clone(), typedb_source: source })?,
            DefinableStatus::ExistsSame(Some(existing_plays)) => existing_plays,
            DefinableStatus::ExistsSame(None) => unreachable!("Existing plays concept expected"),
            DefinableStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
        };

        define_plays_annotations(snapshot, type_manager, thing_manager, &label, defined.clone(), capability)?;
    }
    Ok(())
}

fn define_plays_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_capability: &TypeQLCapability,
) -> Result<(), DefineError> {
    for typeql_annotation in &typeql_capability.annotations {
        let annotation =
            translate_annotation(typeql_annotation).map_err(|source| DefineError::LiteralParseError { source })?;
        if let Some(converted) = capability_convert_and_validate_annotation_definition_need(
            snapshot,
            type_manager,
            plays.clone(),
            annotation.clone(),
            typeql_capability,
        )? {
            plays.set_annotation(snapshot, type_manager, thing_manager, converted).map_err(|source| {
                DefineError::SetAnnotation {
                    label: player_label.clone().into_owned(),
                    typedb_source: source,
                    annotation_declaration: typeql_annotation.clone(),
                }
            })?;
        }
    }
    Ok(())
}

fn define_function(
    _snapshot: &impl WritableSnapshot,
    _type_manager: &TypeManager,
    _declaration: &Function,
) -> Result<(), DefineError> {
    Err(DefineError::Unimplemented { description: "Function definition.".to_string() })
}

fn check_can_and_need_define_sub<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    new_supertype: T,
) -> Result<bool, DefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, new_supertype.clone())
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(true),
        DefinableStatus::ExistsSame(_) => Ok(false),
        DefinableStatus::ExistsDifferent(existing) => Err(DefineError::TypeSubAlreadyDefinedButDifferent {
            label: label.clone().into_owned(),
            supertype_label: new_supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
            existing_supertype_label: existing
                .get_label(snapshot, type_manager)
                .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
        }),
    }
}

fn type_convert_and_validate_annotation_definition_need<'a, T: KindAPI<'a>>(
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
        DefinableStatus::DoesNotExist => Ok(Some(converted)),
        DefinableStatus::ExistsSame(_) => Ok(None),
        DefinableStatus::ExistsDifferent(existing) => Err(DefineError::TypeAnnotationAlreadyDefinedButDifferent {
            label: label.clone().into_owned(),
            annotation,
            existing_annotation: existing.clone().into(),
        }),
    }
}

fn capability_convert_and_validate_annotation_definition_need<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: CAP,
    annotation: Annotation,
    typeql_capability: &TypeQLCapability,
) -> Result<Option<CAP::AnnotationType>, DefineError> {
    let converted = CAP::AnnotationType::try_from(annotation.clone())
        .map_err(|source| DefineError::IllegalAnnotation { source })?;

    let definition_status =
        get_capability_annotation_status(snapshot, type_manager, &capability, &converted, annotation.category())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(Some(converted)),
        DefinableStatus::ExistsSame(_) => Ok(None),
        DefinableStatus::ExistsDifferent(existing) => {
            Err(DefineError::CapabilityAnnotationAlreadyDefinedButDifferent {
                declaration: typeql_capability.clone(),
                annotation,
                existing_annotation: existing.clone().into(),
            })
        }
    }
}

fn err_capability_kind_mismatch(
    left: &Label<'_>,
    right: &Label<'_>,
    capability: &TypeQLCapability,
    left_kind: Kind,
    right_kind: Kind,
) -> DefineError {
    DefineError::CapabilityKindMismatch {
        left: left.clone().into_owned(),
        right: right.clone().into_owned(),
        declaration: capability.clone(),
        left_kind,
        right_kind,
    }
}

typedb_error!(
    pub DefineError(component = "Define execution", prefix = "DEX") {
        Unimplemented(1, "Unimplemented define functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error. ", ( source: ConceptReadError )),
        SymbolResolution(3, "Failed to find symbol.", ( typedb_source: SymbolResolutionError )),
        LiteralParseError(4, "Failed to parse literal.", ( source: LiteralParseError )),
        TypeCreateError(
            5,
            "Failed to create type.\nSource:\n{type_declaration}",
            type_declaration: Type,
            ( typedb_source: ConceptWriteError )
        ),
        RoleTypeDirectCreate(
            6,
            "Role types cannot be created directly, but only through 'relates' declarations.\nSource:\n{type_declaration}",
            type_declaration: Type
        ),
        StructCreateError(
            7,
            "Failed to create struct.\nSource:\n{struct_declaration}",
            struct_declaration: Struct,
            ( typedb_source: ConceptWriteError )
        ),
        StructFieldCreateError(
            8,
            "Failed to create struct field '{struct_field}' in struct type '{struct_name}'.",
            struct_field: Field,
            struct_name: String,
            ( typedb_source: ConceptWriteError )
        ),
        StructFieldAlreadyDefinedButDifferent(
            9,
            "Struct field '{field}' already exists with a different definition {existing_field}.",
            field: Field,
            existing_field: StructDefinitionField
        ),
        SetSupertype(
            10,
            "Setting supertype failed.\nSource: {sub}",
            sub: typeql::schema::definable::type_::capability::Sub,
            ( typedb_source: ConceptWriteError )
        ),
        TypeCannotHaveCapability(
            11,
            "Invalid define - the type '{label}' of kind '{kind}', which cannot have: '{capability}'",
            label: Label<'static>,
            kind: Kind,
            capability: TypeQLCapability
        ),
        ValueTypeSymbolResolution(
            12,
            "Error resolving value type in define query.",
            ( typedb_source: SymbolResolutionError )
        ),
        // TODO: add source TypeQL fragment here so we get line number, ideally!
        TypeSubAlreadyDefinedButDifferent(
            13,
            "Defining supertype of type '{label}' to '{supertype_label}' failed since it already has supertype '{existing_supertype_label}'. Try redefine instead?",
            label: Label<'static>,
            supertype_label: Label<'static>,
            existing_supertype_label: Label<'static>
        ),
        // TODO: need the label that is the specialising type should be provided - need all 4: <x> relates <y> as <z> failed because <q> exists
        RelatesSpecialiseAlreadyDefinedButDifferent(
            14,
            "On type '{label}', the define of specialise using '{specialised_interface}' failed since it already has specialise '{existing_specialised_interface}'. Try redefine instead?",
            label: Label<'static>,
            specialised_interface: Label<'static>,
            existing_specialised_interface: Label<'static>
        ),
        AttributeTypeValueTypeAlreadyDefinedButDifferent(
            15,
            "Attribute type '{label}' cannot be defined to have value type '{value_type}' since it already has value type '{existing_value_type}'. Try redefine instead?",
            label: Label<'static>,
            value_type: ValueType,
            existing_value_type: ValueType
        ),
        TypeAnnotationAlreadyDefinedButDifferent(
            16,
            "Annotation on type '{label}' cannot be defined as '{annotation}' since it already has '{existing_annotation}'. Try redefine instead?",
            label: Label<'static>,
            annotation: Annotation,
            existing_annotation: Annotation
        ),
        CapabilityAnnotationAlreadyDefinedButDifferent(
            17,
            "Annotation '{annotation}' already exists as '{existing_annotation}'. Try redefine instead?\nSource:\n:{declaration}",
            declaration: TypeQLCapability,
            annotation: Annotation,
            existing_annotation: Annotation
        ),
        SetValueType(
            18,
            "Defining '{label}' to have value type '{value_type}' failed.",
            label: Label<'static>,
            value_type: ValueType,
            ( typedb_source: ConceptWriteError )
        ),
        CreateRelates(
            19,
            "Defining new 'relates' failed.\nSource:\n{relates}",
            relates: TypeQLRelates,
            ( typedb_source: ConceptWriteError )
        ),
        CreatePlays(
            20,
            "Defining new 'plays' failed.\nSource:\n{plays}",
            plays: TypeQLPlays,
            ( typedb_source: ConceptWriteError )
        ),
        CreateOwns(
            21,
            "Defining new 'owns' failed.\nSource:\n{owns}",
            owns: TypeQLOwns,
            ( typedb_source: ConceptWriteError )
        ),
        SetOwnsOrdering(
            22,
            "Defining 'owns' ordering failed.\nSource:\n{owns}",
            owns: TypeQLOwns,
            ( typedb_source: ConceptWriteError )
        ),
        RelatesAlreadyDefinedButDifferent(
            23,
            "Relation type '{label}' define 'relates' failed because it already has existing relates '{existing_relates}{existing_ordering}'. Try redefine instead?\nSource:\n{declaration}",
            label: Label<'static>,
            role: String,
            ordering: Ordering,
            existing_relates: String,
            existing_ordering: Ordering,
            declaration: TypeQLCapability
        ),
        OwnsAlreadyDefinedButDifferent(
            24,
            "Type '{label}' define 'owns{ordering}' failed because it already has existing owns '{existing_owns}{existing_ordering}'. Try redefine instead?\nSource:\n{declaration}.",
            label: Label<'static>,
            declaration: TypeQLCapability,
            existing_owns: String,
            existing_ordering: Ordering,
            ordering: Ordering
        ),
        IllegalAnnotation(
            25,
            "Illegal annotation",
            ( source: AnnotationError )
        ),
        SetAnnotation(
            26,
            "On type '{label}', defining annotation failed.\nSource:\n{annotation_declaration}",
            label: Label<'static>,
            annotation_declaration: typeql::annotation::Annotation,
            ( typedb_source: ConceptWriteError )
        ),
        SetSpecialise(
            27,
            "On type '{label}', defining specialise failed.",
            label: Label<'static>,
            ( typedb_source: ConceptWriteError )
        ),
        CapabilityKindMismatch(
            28,
            "Declaration failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'.\nSource:\n{declaration}",
            left: Label<'static>,
            right: Label<'static>,
            declaration: TypeQLCapability,
            left_kind: Kind,
            right_kind: Kind
        ),
    }
);

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &TypeQLCapability) -> DefineError {
    DefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
}
