/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use answer::Type as TypeEnum;
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::token,
    query::schema::Define,
    schema::definable::{
        struct_::Field,
        type_::{
            capability::{Owns, Plays, Relates, Sub},
            Capability, CapabilityBase,
        },
        Struct, Type,
    },
    type_::{NamedType, Optional},
    Definable, ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    util::{resolve_type, resolve_value_type, UnwrapTypeAs},
    SymbolResolutionError,
};

macro_rules! unwrap_type {
    ($type_enum:ident, $unwrapped_type:ident, $expr:expr) => {
        match &$type_enum {
            TypeEnum::Entity($unwrapped_type) => $block,
            TypeEnum::Relation($unwrapped_type) => $block,
            TypeEnum::Attribute($unwrapped_type) => $block,
            TypeEnum::RoleType($unwrapped_type) => $block,
        }
    };
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    define: Define,
) -> Result<(), DefineError> {
    define_types(snapshot, type_manager, &define.definables)?;
    define_structs(snapshot, type_manager, &define.definables)?;
    define_struct_fields(snapshot, type_manager, &define.definables)?;
    define_capabilities(snapshot, type_manager, &define.definables)?;
    define_functions(snapshot, type_manager, &define.definables)?;
    Ok(())
}

fn define_types<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            let label = Label::parse_from(type_declaration.label.ident.as_str());
            match type_declaration.kind {
                None => {
                    return Err(DefineError::TypeCreateRequiresKind { type_declaration: type_declaration.clone() });
                }
                Some(token::Kind::Entity) => {
                    type_manager.create_entity_type(snapshot, &label).map_err(|err| DefineError::TypeCreateError {
                        source: err,
                        type_declaration: type_declaration.clone(),
                    })?;
                }
                Some(token::Kind::Relation) => {
                    type_manager.create_relation_type(snapshot, &label).map_err(|err| {
                        DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() }
                    })?;
                }
                Some(token::Kind::Attribute) => {
                    type_manager.create_attribute_type(snapshot, &label).map_err(|err| {
                        DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() }
                    })?;
                }
                Some(token::Kind::Role) => {
                    return Err(DefineError::RoleTypeDirectCreate { type_declaration: type_declaration.clone() })?;
                }
            }
        }
    }
    Ok(())
}

fn define_structs<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::Struct(struct_definable) = definable {
            let name = struct_definable.ident.as_str();
            type_manager.create_struct(snapshot, name.to_owned()).map_err(|err| DefineError::StructCreateError {
                source: err,
                struct_declaration: struct_definable.clone(),
            })?;
        }
    }
    Ok(())
}

fn define_struct_fields<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::Struct(struct_definable) = definable {
            let name = struct_definable.ident.as_str();
            let struct_key = type_manager
                .get_struct_definition_key(snapshot, name)
                .map_err(|err| DefineError::UnexpectedConceptRead { source: err })?
                .unwrap();
            for field in &struct_definable.fields {
                let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)?;
                type_manager
                    .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
                    .map_err(|err| DefineError::StructFieldCreateError {
                        source: err,
                        struct_name: name.to_owned(),
                        struct_field: field.clone(),
                    })?;
            }
        }
    }
    Ok(())
}

fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), DefineError> {
    match &field.type_ {
        TypeRefAny::Type(type_ref) => match type_ref {
            TypeRef::Named(named) => {
                let value_type = resolve_value_type(snapshot, type_manager, named)
                    .map_err(|source| DefineError::StructFieldCouldNotResolveValueType { source })?;
                Ok((value_type, false))
            }
            TypeRef::Variable(_) => {
                return Err(DefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
            }
        },
        TypeRefAny::Optional(Optional { inner: type_ref, .. }) => match type_ref {
            TypeRef::Named(named) => {
                let value_type = resolve_value_type(snapshot, type_manager, named)
                    .map_err(|source| DefineError::StructFieldCouldNotResolveValueType { source })?;
                Ok((value_type, false))
            }
            TypeRef::Variable(_) => {
                return Err(DefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
            }
        },
        TypeRefAny::List(_) => {
            return Err(DefineError::StructFieldIllegalList { field_declaration: field.clone() });
        }
    }
}

fn define_capabilities<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    define_capabilities_sub(snapshot, type_manager, definables)?;
    define_capabilities_alias(snapshot, type_manager, definables)?;
    define_capabilities_value_type(snapshot, type_manager, definables)?;
    define_capabilities_relates(snapshot, type_manager, definables)?;
    define_capabilities_owns(snapshot, type_manager, definables)?;
    define_capabilities_plays(snapshot, type_manager, definables)?;
    Ok(())
}

fn define_capabilities_alias<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::Alias(_) => {
                            Err(DefineError::Unimplemented)?;
                        }
                        CapabilityBase::Sub(_)
                        | CapabilityBase::ValueType(_)
                        | CapabilityBase::Relates(_)
                        | CapabilityBase::Plays(_)
                        | CapabilityBase::Owns(_) => {
                            // Done in another function
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_capabilities_sub<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                let label = Label::parse_from(type_declaration.label.ident.as_str());
                let type_ = resolve_type(snapshot, type_manager, &label)
                    .map_err(|source| DefineError::TypeLookup { source })?;
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::Alias(_) => {
                            Err(DefineError::Unimplemented)?;
                        }
                        CapabilityBase::Sub(sub) => {
                            match &type_ {
                                TypeEnum::Entity(type_) => {
                                    let supertype = EntityType::resolve_for(
                                        snapshot,
                                        type_manager,
                                        &sub.supertype_label,
                                        capability,
                                    )
                                    .map_err(|source| DefineError::TypeLookup { source })?;
                                    type_
                                        .set_supertype(snapshot, type_manager, supertype)
                                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                                }
                                TypeEnum::Relation(type_) => {
                                    let supertype = RelationType::resolve_for(
                                        snapshot,
                                        type_manager,
                                        &sub.supertype_label,
                                        capability,
                                    )
                                    .map_err(|source| DefineError::TypeLookup { source })?;
                                    type_
                                        .set_supertype(snapshot, type_manager, supertype)
                                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                                }
                                TypeEnum::Attribute(type_) => {
                                    let supertype = AttributeType::resolve_for(
                                        snapshot,
                                        type_manager,
                                        &sub.supertype_label,
                                        capability,
                                    )
                                    .map_err(|source| DefineError::TypeLookup { source })?;
                                    type_
                                        .set_supertype(snapshot, type_manager, supertype)
                                        .map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
                                }
                                TypeEnum::RoleType(_) => {
                                    Err(DefineError::TypeCannotHaveCapability {
                                        label: label.to_owned(),
                                        kind: Kind::Role,
                                        capability: capability.clone(),
                                    })?;
                                }
                            };
                        }

                        CapabilityBase::ValueType(_)
                        | CapabilityBase::Relates(_)
                        | CapabilityBase::Plays(_)
                        | CapabilityBase::Owns(_) => {
                            // Done in another function
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_capabilities_value_type<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                let label = Label::parse_from(type_declaration.label.ident.as_str());
                let type_ = resolve_type(snapshot, type_manager, &label)
                    .map_err(|source| DefineError::TypeLookup { source })?;
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::ValueType(value_type_statement) => match &type_ {
                            TypeEnum::Attribute(attribute_type) => {
                                let value_type =
                                    resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
                                        .map_err(|source| DefineError::AttributeTypeBadValueType { source })?;
                                attribute_type.set_value_type(snapshot, type_manager, value_type.clone()).map_err(
                                    |source| DefineError::SetValueType { label: label.to_owned(), value_type, source },
                                )?;
                            }
                            _ => {
                                Err(DefineError::TypeCannotHaveCapability {
                                    label: label.to_owned(),
                                    kind: type_.kind(),
                                    capability: capability.clone(),
                                })?;
                            }
                        },
                        CapabilityBase::Sub(_)
                        | CapabilityBase::Alias(_)
                        | CapabilityBase::Relates(_)
                        | CapabilityBase::Plays(_)
                        | CapabilityBase::Owns(_) => {
                            // Done in another function
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_capabilities_relates<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                let label = Label::parse_from(type_declaration.label.ident.as_str());
                let type_ = resolve_type(snapshot, type_manager, &label)
                    .map_err(|source| DefineError::TypeLookup { source })?;
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::Alias(_)
                        | CapabilityBase::Sub(_)
                        | CapabilityBase::ValueType(_)
                        | CapabilityBase::Plays(_)
                        | CapabilityBase::Owns(_) => {
                            // Done elsewhere
                        }
                        CapabilityBase::Relates(relates) => match &type_ {
                            TypeEnum::Relation(relation_type) => {
                                println!("Relates is {}", &relates.related);
                                if let Some((role_label, is_list)) = type_ref_to_label_and_is_list(&relates.related) {
                                    let ordering = if is_list { Ordering::Ordered } else { Ordering::Unordered };
                                    relation_type
                                        .create_relates(snapshot, type_manager, role_label.ident.as_str(), ordering)
                                        .map_err(|source| DefineError::CreateRelates {
                                            source,
                                            relates: relates.to_owned(),
                                        })?;
                                } else {
                                    Err(DefineError::RelatesRoleMustBeLabelAndNotOptional {
                                        relation: label.to_owned(),
                                        role_label: relates.related.clone(),
                                    })?;
                                }
                            }
                            _ => {
                                Err(DefineError::TypeCannotHaveCapability {
                                    label: label.to_owned(),
                                    kind: type_.kind(),
                                    capability: capability.clone(),
                                })?;
                            }
                        },
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_capabilities_owns<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                let label = Label::parse_from(type_declaration.label.ident.as_str());
                let type_ = resolve_type(snapshot, type_manager, &label)
                    .map_err(|source| DefineError::TypeLookup { source })?;
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::Alias(_)
                        | CapabilityBase::Sub(_)
                        | CapabilityBase::ValueType(_)
                        | CapabilityBase::Relates(_)
                        | CapabilityBase::Plays(_) => {
                            // Done elsewhere
                        }
                        CapabilityBase::Owns(owns) => {
                            let (attr_label, is_list) = match type_ref_to_label_and_is_list(&owns.owned) {
                                None => Err(DefineError::OwnsAttributeMustBeLabelOrList { owns: owns.clone() })?,
                                Some(name_list) => name_list,
                            };
                            let attribute_type =
                                AttributeType::resolve_for(snapshot, type_manager, &attr_label, capability)
                                    .map_err(|source| DefineError::TypeLookup { source })?;
                            let ordering = if is_list { Ordering::Ordered } else { Ordering::Unordered };
                            match &type_ {
                                TypeEnum::Entity(entity_type) => {
                                    ObjectType::Entity(entity_type.clone())
                                        .set_owns(snapshot, type_manager, attribute_type, ordering)
                                        .map_err(|source| DefineError::CreateOwns { owns: owns.clone(), source })?;
                                }
                                TypeEnum::Relation(relation_type) => {
                                    ObjectType::Relation(relation_type.clone())
                                        .set_owns(snapshot, type_manager, attribute_type, ordering)
                                        .map_err(|source| DefineError::CreateOwns { owns: owns.clone(), source })?;
                                }
                                _ => {
                                    Err(DefineError::TypeCannotHaveCapability {
                                        label: label.to_owned(),
                                        kind: type_.kind(),
                                        capability: capability.clone(),
                                    })?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_capabilities_plays<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            if !type_declaration.capabilities.is_empty() {
                let label = Label::parse_from(type_declaration.label.ident.as_str());
                let type_ = resolve_type(snapshot, type_manager, &label)
                    .map_err(|source| DefineError::TypeLookup { source })?;
                for capability in &type_declaration.capabilities {
                    match &capability.base {
                        CapabilityBase::Alias(_)
                        | CapabilityBase::Sub(_)
                        | CapabilityBase::ValueType(_)
                        | CapabilityBase::Relates(_)
                        | CapabilityBase::Owns(_) => {
                            // Done elsewhere
                        }
                        CapabilityBase::Plays(plays) => {
                            let label =
                                Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
                            let role_type_opt = type_manager
                                .get_role_type(snapshot, &label)
                                .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
                            if let Some(role_type) = role_type_opt {
                                match &type_ {
                                    TypeEnum::Entity(entity_type) => {
                                        ObjectType::Entity(entity_type.clone())
                                            .set_plays(snapshot, type_manager, role_type)
                                            .map_err(|source| DefineError::CreatePlays {
                                                plays: plays.clone(),
                                                source,
                                            })?;
                                    }
                                    TypeEnum::Relation(relation_type) => {
                                        ObjectType::Relation(relation_type.clone())
                                            .set_plays(snapshot, type_manager, role_type)
                                            .map_err(|source| DefineError::CreatePlays {
                                                plays: plays.clone(),
                                                source,
                                            })?;
                                    }
                                    _ => {
                                        Err(DefineError::TypeCannotHaveCapability {
                                            label: label.to_owned(),
                                            kind: type_.kind(),
                                            capability: capability.clone(),
                                        })?;
                                    }
                                }
                            } else {
                                Err(DefineError::CreatePlaysRoleNotFound { plays: plays.clone() })?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn define_functions<'a>(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    Ok(())
}

fn type_ref_to_label_and_is_list(type_ref: &TypeRefAny) -> Option<(typeql::Label, bool)> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => Some((label.clone(), false)),
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Some((label.clone(), true))
        }
        _ => None,
    }
}

fn type_ref_to_scoped_label_and_is_list(type_ref: &TypeRefAny) -> Option<(ScopedLabel, bool)> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Role(label))) => Some((label.clone(), false)),
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Role(label)), .. }) => {
            Some((label.clone(), true))
        }
        _ => None,
    }
}

#[derive(Debug)]
pub enum DefineError {
    UnexpectedConceptRead { source: ConceptReadError },
    TypeLookup { source: SymbolResolutionError },
    TypeCreateRequiresKind { type_declaration: Type },
    TypeCreateError { source: ConceptWriteError, type_declaration: Type },
    RoleTypeDirectCreate { type_declaration: Type },
    StructCreateError { source: ConceptWriteError, struct_declaration: Struct },
    StructFieldCreateError { source: ConceptWriteError, struct_name: String, struct_field: Field },
    StructFieldIllegalList { field_declaration: Field },
    StructFieldIllegalVariable { field_declaration: Field },
    StructFieldIllegalNotValueType { scoped_label: ScopedLabel },
    StructFieldCouldNotResolveValueType { source: SymbolResolutionError },

    Unimplemented,

    SetSupertype { sub: Sub, source: ConceptWriteError },
    TypeCannotHaveCapability { label: Label<'static>, kind: Kind, capability: Capability },
    AttributeTypeBadValueType { source: SymbolResolutionError },
    SetValueType { label: Label<'static>, value_type: ValueType, source: ConceptWriteError },
    RelatesRoleMustBeLabelAndNotOptional { relation: Label<'static>, role_label: TypeRefAny },
    CreateRelates { relates: Relates, source: ConceptWriteError },
    CreatePlays { plays: Plays, source: ConceptWriteError },
    CreatePlaysRoleNotFound { plays: Plays },
    OwnsAttributeMustBeLabelOrList { owns: Owns },
    CreateOwns { owns: Owns, source: ConceptWriteError },
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
