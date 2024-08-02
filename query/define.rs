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

use concept::type_::annotation::{Annotation, AnnotationError};
use concept::type_::attribute_type::AttributeTypeAnnotation;
use concept::type_::entity_type::EntityTypeAnnotation;
use concept::type_::relation_type::RelationTypeAnnotation;

use crate::{
    util::{resolve_type, resolve_value_type, UnwrapTypeAs},
    SymbolResolutionError,
};
use crate::util::translate_annotation;


macro_rules! unwrap_or_else {
    ($variant:path = $to_unwrap:expr ; $else_:block) => {
        if let $variant(inner) = $to_unwrap { inner } else { $else_ };
    };
}

macro_rules! try_unwrap {
    ($variant:path = $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    }
}
macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| {
            if let $variant(inner) = item {
                Some(inner)
            } else {
                None
            }
        })
    }
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    define: Define,
) -> Result<(), DefineError> {
    process_struct_definitions(snapshot, type_manager, &define.definables)?;
    process_type_declarations(snapshot, type_manager, &define.definables)?;
    process_functions(snapshot, type_manager, &define.definables)?;
    Ok(())
}
pub(crate) fn process_struct_definitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    filter_variants!(Definable::Struct : definables).try_for_each(|struct_|
        define_structs(snapshot, type_manager, struct_)
    )?;
    filter_variants!(Definable::Struct : definables).try_for_each(|struct_|
        define_struct_fields(snapshot, type_manager, &struct_)
    )?;
    Ok(())
}

pub(crate) fn process_type_declarations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_types(snapshot, type_manager, declaration)
    })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_type_annotations(snapshot, type_manager, declaration)
    })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_sub(snapshot, type_manager, declaration)
    })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_alias(snapshot, type_manager, declaration)
    })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_value_type(snapshot, type_manager, declaration)
    })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_relates(snapshot, type_manager, declaration)
    })?;
    // filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
    //     define_capabilities_relates_overrides(snapshot, type_manager, definables)
    // })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_owns(snapshot, type_manager, declaration)
    })?;
    // filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
    //     define_capabilities_owns_overrides(snapshot, type_manager, definables)
    // })?;
    filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
        define_capabilities_plays(snapshot, type_manager, declaration)
    })?;
    // filter_variants!(Definable::TypeDeclaration : definables).try_for_each(|declaration| {
    //     define_capabilities_plays_overrides(snapshot, type_manager, definables)
    // })?;
    Ok(())
}

pub(crate) fn process_functions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    filter_variants!(Definable::Function : definables).try_for_each(|declaration| {
        define_functions(snapshot, type_manager, definables)
    })?;
    Ok(())
}

fn define_structs<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &typeql::schema::definable::Struct
) -> Result<(), DefineError> {
    let name = struct_definable.ident.as_str();
    type_manager.create_struct(snapshot, name.to_owned()).map_err(|err| DefineError::StructCreateError {
        source: err,
        struct_declaration: struct_definable.clone(),
    })?;
    Ok(())
}

fn define_struct_fields<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    struct_definable: &typeql::schema::definable::Struct
) -> Result<(), DefineError> {
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

fn define_types<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    match type_declaration.kind {
        None => {
            // Refers to an existing type.
            resolve_type(snapshot, type_manager, &label)
                .map_err(|source| DefineError::TypeLookup { source })?;
            Ok(())
        }
        Some(token::Kind::Role) => Err(DefineError::RoleTypeDirectCreate { type_declaration: type_declaration.clone() })?,
        Some(token::Kind::Entity) => type_manager.create_entity_type(snapshot, &label).map(|_|()),
        Some(token::Kind::Relation) => type_manager.create_relation_type(snapshot, &label).map(|_|()),
        Some(token::Kind::Attribute) => type_manager.create_attribute_type(snapshot, &label).map(|_|()),
    }.map_err(|err| {
        DefineError::TypeCreateError { source: err, type_declaration: type_declaration.clone() }
    })?;
    Ok(())
}

fn define_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| DefineError::TypeLookup { source })?;
    for typeql_annotation in &type_declaration.annotations {
        let annotation = translate_annotation(typeql_annotation);
        match type_.clone() {
            TypeEnum::Entity(entity) => {
                let converted = <Result<EntityTypeAnnotation, AnnotationError> as From<Annotation>>::from(annotation.clone())
                    .map_err(|source| DefineError::IllegalAnnotation { source })?;
                entity.set_annotation(snapshot, type_manager, converted)
                    .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation: annotation } )?;
            }
            TypeEnum::Relation(relation) => {
                let converted : RelationTypeAnnotation = <Result<RelationTypeAnnotation, AnnotationError> as From<Annotation>>::from(annotation.clone())
                    .map_err(|source| DefineError::IllegalAnnotation { source })?;
                relation.set_annotation(snapshot, type_manager, converted)
                    .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation: annotation } )?;
            }
            TypeEnum::Attribute(attribute) => {
                let converted : AttributeTypeAnnotation = <Result<AttributeTypeAnnotation, AnnotationError> as From<Annotation>>::from(annotation.clone())
                    .map_err(|source| DefineError::IllegalAnnotation { source })?;
                attribute.set_annotation(snapshot, type_manager, converted)
                    .map_err(|source| DefineError::SetAnnotation { source, label: label.to_owned(), annotation: annotation } )?;
            }
            TypeEnum::RoleType(role) => unreachable!("Role annotations are syntactically on relates")
        }
    }
    Ok(())
}

fn define_capabilities_alias<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    &type_declaration.capabilities.iter().filter_map(|capability| {
        try_unwrap!(CapabilityBase::Alias = &capability.base)
    }).try_for_each(|_| {
        Err(DefineError::Unimplemented)
    })?;
    Ok(())
}

fn define_capabilities_sub<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| DefineError::TypeLookup { source })?;

    for capability in &type_declaration.capabilities{
        let sub = unwrap_or_else!(CapabilityBase::Sub = &capability.base ; {continue;});
        let supertype_label = Label::parse_from(&sub.supertype_label.ident.as_str());
        let supertype = resolve_type(snapshot, type_manager, &supertype_label)
            .map_err(|source| DefineError::TypeLookup { source })?;;
        if type_.kind() != supertype.kind() {
            return Err(DefineError::SetSupertypeKindMismatch {
                label: label.clone(), supertype_label: supertype_label.clone(),
                capability: capability.clone(),
                subtype_kind:  type_.kind(), supertype_kind: supertype.kind()
            })?;
        }

        match (&type_, supertype) {
            (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
                type_.set_supertype(snapshot, type_manager, supertype)
            }
            (TypeEnum::Relation(type_), TypeEnum::Relation(supertype) ) => {
                type_.set_supertype(snapshot, type_manager, supertype)
            }
            (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
                type_.set_supertype(snapshot, type_manager, supertype)
            }
            (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => {
                return Err(DefineError::TypeCannotHaveCapability {
                    label: label.clone(), kind: Kind::Role, capability: capability.clone(),
                })?;
            }
            _ => unreachable!(),
        }.map_err(|source| DefineError::SetSupertype { sub: sub.clone(), source })?;
    }
    Ok(())
}

fn define_capabilities_value_type<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let value_type_statement = unwrap_or_else!(CapabilityBase::ValueType = &capability.base; {continue;});
        let attribute_type =  unwrap_or_else!(TypeEnum::Attribute = &type_; {
            return Err(DefineError::TypeCannotHaveCapability {
                label: label.to_owned(),
                kind: type_.kind(),
                capability: capability.clone(),
            })?;
        });
        let value_type =
            resolve_value_type(snapshot, type_manager, &value_type_statement.value_type)
                .map_err(|source| DefineError::AttributeTypeBadValueType { source })?;
        attribute_type.set_value_type(snapshot, type_manager, value_type.clone()).map_err(
            |source| DefineError::SetValueType { label: label.to_owned(), value_type, source },
        )?;
    }
    Ok(())
}

fn define_capabilities_relates<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let relates = unwrap_or_else!(CapabilityBase::Relates = &capability.base ;{continue;});
        let relation_type = unwrap_or_else!(TypeEnum::Relation = &type_ ;{
            return Err(DefineError::TypeCannotHaveCapability {
                label: label.to_owned(),
                kind: type_.kind(),
                capability: capability.clone(),
            })?;
        });

        let (role_label, is_list) = type_ref_to_label_and_is_list(&relates.related)
            .map_err(|_| DefineError::RelatesRoleMustBeLabelAndNotOptional {
                relation: label.to_owned(),
                role_label: relates.related.clone(),
            })?;
        let ordering = if is_list { Ordering::Ordered } else { Ordering::Unordered };
        if let Some(relates) = relation_type.get_relates_of_role(snapshot, type_manager, role_label.ident.as_str())
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?
        {
            let existing_ordering = relates.role().get_ordering(snapshot, type_manager).map_err(|source| DefineError::UnexpectedConceptRead { source })?;
            if ordering != existing_ordering {
                Err(DefineError::CreateRelatesModifiesExistingOrdering { label: label.clone(), existing_ordering, new_ordering: ordering })?;
            }
        } else {
            relation_type
                .create_relates(snapshot, type_manager, role_label.ident.as_str(), ordering)
                .map_err(|source| DefineError::CreateRelates {
                    source,
                    relates: relates.to_owned(),
                })?;
        }
    }
    Ok(())
}
//
// fn define_capabilities_relates_overrides<'a>(
//     snapshot: &mut impl WritableSnapshot,
//     type_manager: &TypeManager,
//     definables: &Vec<Definable>,
// ) -> Result<(), DefineError> {
//     for definable in definables {
//         if let Definable::TypeDeclaration(type_declaration) = definable {
//             if type_declaration.capabilities.is_empty() {
//                 continue;
//             }
//             let label = Label::parse_from(type_declaration.label.ident.as_str());
//             let type_ = resolve_type(snapshot, type_manager, &label)
//                 .map_err(|source| DefineError::TypeLookup { source })?;
//             for capability in &type_declaration.capabilities {
//                 if let CapabilityBase::Relates(relates) = &capability.base {
//                     match &type_ {
//                         TypeEnum::Relation(relation_type) => {
//                             let (overriding_label, overriding_is_list) = type_ref_to_label_and_is_list(&relates.related)
//                                 .map_err(|_| {
//                                     Err(DefineError::RelatesRoleMustBeLabelAndNotOptional {
//                                         relation: label.to_owned(),
//                                         role_label: relates.related.clone(),
//                                     })
//                                 })?;
//                             let (overridden_label, overridden_is_list) = type_ref_to_label_and_is_list(&relates.related)
//                                 .map_err(|_| {
//                                     Err(DefineError::RelatesRoleMustBeLabelAndNotOptional {
//                                         relation: label.to_owned(),
//                                         role_label: relates.related.clone(),
//                                     })
//                                 })?;
//
//                             let overriding_ordering = if overriding_is_list { Ordering::Ordered } else { Ordering::Unordered };
//                             let overridden_ordering = if overridden_is_list { Ordering::Ordered } else { Ordering::Unordered };
//                             let overridding_relates = relation_type.get_relates_of_role(snapshot, type_manager, role_label.ident.as_str())
//                                 .map_err(|source| DefineError::UnexpectedConceptRead { source })?
//                             {
//
//                             }
//                             if let Some(overriding_relates) =
//                             {
//                                 if ordering != existing_ordering {
//                                     Err(DefineError::CreateRelatesModifiesExistingOrdering { label: label.clone(), existing_ordering, new_ordering: ordering })?;
//                                 }
//                             } else {
//                                 relation_type
//                                     .create_relates(snapshot, type_manager, role_label.ident.as_str(), ordering)
//                                     .map_err(|source| DefineError::CreateRelates {
//                                         source,
//                                         relates: relates.to_owned(),
//                                     })?;
//                             }
//
//                         }
//                         _ => {
//                             Err(DefineError::TypeCannotHaveCapability {
//                                 label: label.to_owned(),
//                                 kind: type_.kind(),
//                                 capability: capability.clone(),
//                             })?;
//                         }
//                     },
//                 }
//             }
//         }
//     }
//     Ok(())
// }

fn define_capabilities_owns<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| crate::define::DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let owns = unwrap_or_else!(CapabilityBase::Owns = &capability.base ;{continue;});
        let (attr_label, is_list) = type_ref_to_label_and_is_list(&owns.owned)
            .map_err(|_| DefineError::OwnsAttributeMustBeLabelOrList { owns: owns.clone() })?;
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
    Ok(())
}

fn define_capabilities_plays<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &typeql::schema::definable::Type,
) -> Result<(), DefineError> {
    let label = Label::parse_from(type_declaration.label.ident.as_str());
    let type_ = resolve_type(snapshot, type_manager, &label)
        .map_err(|source| DefineError::TypeLookup { source })?;
    for capability in &type_declaration.capabilities {
        let plays = unwrap_or_else!(CapabilityBase::Plays = &capability.base ;{continue;});
        let role_label =
            Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
        let role_type_opt = type_manager
            .get_role_type(snapshot, &role_label)
            .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
        if let Some(role_type) = role_type_opt {
            let as_object_type = match &type_ {
                TypeEnum::Entity(entity_type) => ObjectType::Entity(entity_type.clone()),
                TypeEnum::Relation(relation_type) => ObjectType::Relation(relation_type.clone()),
                _ => {
                    return Err(DefineError::TypeCannotHaveCapability {
                        label: role_label.to_owned(),
                        kind: type_.kind(),
                        capability: capability.clone(),
                    })?;
                }
            };
            as_object_type.set_plays(snapshot, type_manager, role_type).map_err(|source|
                DefineError::CreatePlays { plays: plays.clone(), source, }
            )?;
        } else {
            Err(DefineError::CreatePlaysRoleNotFound { plays: plays.clone() })?;
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

fn type_ref_to_label_and_is_list(type_ref: &TypeRefAny) -> Result<(typeql::Label, bool), ()> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => Ok((label.clone(), false)),
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Ok((label.clone(), true))
        }
        _ => Err(())
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
    CreateRelatesModifiesExistingOrdering { label: Label<'static>, existing_ordering: Ordering, new_ordering: Ordering },
    IllegalAnnotation { source: AnnotationError },
    SetAnnotation { source: ConceptWriteError, label: Label<'static>, annotation: Annotation },
    SetSupertypeKindMismatch { label: Label<'static>, supertype_label: Label<'static>, capability: Capability, subtype_kind: Kind, supertype_kind: Kind },
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
