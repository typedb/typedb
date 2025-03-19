/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use answer::Type;
use concept::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, owns::Owns, plays::Plays,
        relates::Relates, relation_type::RelationType, role_type::RoleType, type_manager::TypeManager, ObjectTypeAPI,
        Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::definition::definition_key::DefinitionKey,
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::{error::TypeQLError, Spanned},
    schema::definable::struct_::Field,
    type_::{BuiltinValueType, NamedType, NamedTypeAny, NamedTypeOptional},
    TypeRef, TypeRefAny,
};

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
pub(crate) use filter_variants;

fn checked_identifier(ident: &typeql::Identifier) -> Result<&str, Box<SymbolResolutionError>> {
    ident.as_str_unreserved().map_err(|_source| {
        let TypeQLError::ReservedKeywordAsIdentifier { identifier } = _source else { unreachable!() };
        Box::new(SymbolResolutionError::IllegalKeywordAsIdentifier { identifier })
    })
}

pub(crate) fn type_ref_to_label_and_ordering(
    _label: &Label,
    type_ref: &TypeRefAny,
) -> Result<(Label, Ordering), Box<SymbolResolutionError>> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Label(label)) => {
            Ok((Label::parse_from(checked_identifier(&label.ident)?, label.span()), Ordering::Unordered))
        }
        TypeRefAny::List(typeql::type_::TypeRefList { inner: TypeRef::Label(label), .. }) => {
            Ok((Label::parse_from(checked_identifier(&label.ident)?, label.span()), Ordering::Ordered))
        }
        _ => Err(Box::new(SymbolResolutionError::ExpectedNonVariableAndNonScopedTypeSymbol {
            declaration: type_ref.clone(),
        })),
    }
}

pub(crate) fn type_to_object_type(type_: &Type) -> Result<ObjectType, ()> {
    Ok(match &type_ {
        Type::Entity(entity_type) => ObjectType::Entity(*entity_type),
        Type::Relation(relation_type) => ObjectType::Relation(*relation_type),
        _ => return Err(()),
    }
    .into_object_type())
}

pub(crate) fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), Box<SymbolResolutionError>> {
    let optional = matches!(&field.type_, NamedTypeAny::Optional(_));
    match &field.type_ {
        NamedTypeAny::Simple(named) | NamedTypeAny::Optional(NamedTypeOptional { inner: named, .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)?;
            Ok((value_type, optional))
        }
        NamedTypeAny::List(_) => {
            Err(Box::new(SymbolResolutionError::StructFieldIllegalList { declaration: field.clone() }))
        }
    }
}

pub(crate) fn resolve_typeql_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Type, Box<SymbolResolutionError>> {
    match try_resolve_typeql_type(snapshot, type_manager, label) {
        Ok(Some(type_)) => Ok(type_),
        Ok(None) => Err(Box::new(SymbolResolutionError::TypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_typeql_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<Type>, Box<ConceptReadError>> {
    // TODO: Introduce a method on type_manager that does this in one step
    let type_ = if let Some(object_type) = try_resolve_object_type(snapshot, type_manager, label)? {
        match object_type {
            ObjectType::Entity(entity_type) => Some(Type::Entity(entity_type)),
            ObjectType::Relation(relation_type) => Some(Type::Relation(relation_type)),
        }
    } else if let Some(attribute_type) = try_resolve_attribute_type(snapshot, type_manager, label)? {
        Some(Type::Attribute(attribute_type))
    } else {
        try_resolve_role_type(snapshot, type_manager, label)?.map(Type::RoleType)
    };
    Ok(type_)
}

pub(crate) fn resolve_value_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field_name: &NamedType,
) -> Result<ValueType, Box<SymbolResolutionError>> {
    match field_name {
        NamedType::Label(label) => {
            let key = try_resolve_struct_definition_key(snapshot, type_manager, checked_identifier(&label.ident)?);
            match key {
                Ok(Some(key)) => Ok(ValueType::Struct(key)),
                Ok(None) => Err(Box::new(SymbolResolutionError::ValueTypeNotFound {
                    name: label.ident.as_str_unchecked().to_owned(),
                })),
                Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
            }
        }
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ir::translation::tokens::translate_value_type(token))
        }
    }
}

pub(crate) fn resolve_struct_definition_key(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<DefinitionKey, Box<SymbolResolutionError>> {
    match try_resolve_struct_definition_key(snapshot, type_manager, name) {
        Ok(Some(key)) => Ok(key),
        Ok(None) => Err(Box::new(SymbolResolutionError::StructNotFound { name: name.to_owned() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_struct_definition_key(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<Option<DefinitionKey>, Box<ConceptReadError>> {
    type_manager.get_struct_definition_key(snapshot, name)
}

pub(crate) fn resolve_object_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<ObjectType, Box<SymbolResolutionError>> {
    match try_resolve_object_type(snapshot, type_manager, label) {
        Ok(Some(object_type)) => Ok(object_type),
        Ok(None) => Err(Box::new(SymbolResolutionError::ObjectTypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_object_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<ObjectType>, Box<ConceptReadError>> {
    type_manager.get_object_type(snapshot, label)
}

pub(crate) fn resolve_entity_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<EntityType, Box<SymbolResolutionError>> {
    match try_resolve_entity_type(snapshot, type_manager, label) {
        Ok(Some(entity_type)) => Ok(entity_type),
        Ok(None) => Err(Box::new(SymbolResolutionError::EntityTypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_entity_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<EntityType>, Box<ConceptReadError>> {
    type_manager.get_entity_type(snapshot, label)
}

pub(crate) fn resolve_relation_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<RelationType, Box<SymbolResolutionError>> {
    match try_resolve_relation_type(snapshot, type_manager, label) {
        Ok(Some(relation_type)) => Ok(relation_type),
        Ok(None) => Err(Box::new(SymbolResolutionError::RelationTypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_relation_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<RelationType>, Box<ConceptReadError>> {
    type_manager.get_relation_type(snapshot, label)
}

pub(crate) fn resolve_attribute_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<AttributeType, Box<SymbolResolutionError>> {
    match try_resolve_attribute_type(snapshot, type_manager, label) {
        Ok(Some(attribute_type)) => Ok(attribute_type),
        Ok(None) => Err(Box::new(SymbolResolutionError::AttributeTypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_attribute_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<AttributeType>, Box<ConceptReadError>> {
    type_manager.get_attribute_type(snapshot, label)
}

pub(crate) fn resolve_role_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<RoleType, Box<SymbolResolutionError>> {
    match try_resolve_role_type(snapshot, type_manager, label) {
        Ok(Some(role_type)) => Ok(role_type),
        Ok(None) => Err(Box::new(SymbolResolutionError::RoleTypeNotFound { label: label.clone() })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_role_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
) -> Result<Option<RoleType>, Box<ConceptReadError>> {
    type_manager.get_role_type(snapshot, label)
}

pub(crate) fn resolve_relates_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Relates, Box<SymbolResolutionError>> {
    match try_resolve_relates_declared(snapshot, type_manager, relation_type, role_name) {
        Ok(Some(relates)) => Ok(relates),
        Ok(None) => Err(Box::new(SymbolResolutionError::RelatesNotFound {
            label: relation_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_name: role_name.to_string(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_relates_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Option<Relates>, Box<ConceptReadError>> {
    relation_type.get_relates_role_name_explicit_declared(snapshot, type_manager, role_name)
}

pub(crate) fn resolve_relates(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Relates, Box<SymbolResolutionError>> {
    match try_resolve_relates(snapshot, type_manager, relation_type, role_name) {
        Ok(Some(relates)) => Ok(relates),
        Ok(None) => Err(Box::new(SymbolResolutionError::RelatesNotFound {
            label: relation_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_name: role_name.to_string(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_relates(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Option<Relates>, Box<ConceptReadError>> {
    relation_type.get_relates_role_name_explicit(snapshot, type_manager, role_name)
}

pub(crate) fn resolve_owns_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    attribute_type: AttributeType,
) -> Result<Owns, Box<SymbolResolutionError>> {
    match try_resolve_owns_declared(snapshot, type_manager, object_type, attribute_type) {
        Ok(Some(owns)) => Ok(owns),
        Ok(None) => Err(Box::new(SymbolResolutionError::OwnsNotFound {
            owner_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            attribute_label: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_owns_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    attribute_type: AttributeType,
) -> Result<Option<Owns>, Box<ConceptReadError>> {
    object_type.get_owns_attribute_declared(snapshot, type_manager, attribute_type)
}

pub(crate) fn resolve_owns(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    attribute_type: AttributeType,
) -> Result<Owns, Box<SymbolResolutionError>> {
    match try_resolve_owns(snapshot, type_manager, object_type, attribute_type) {
        Ok(Some(owns)) => Ok(owns),
        Ok(None) => Err(Box::new(SymbolResolutionError::OwnsNotFound {
            owner_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            attribute_label: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_owns(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    attribute_type: AttributeType,
) -> Result<Option<Owns>, Box<ConceptReadError>> {
    object_type.get_owns_attribute(snapshot, type_manager, attribute_type)
}

pub(crate) fn resolve_plays_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    role_type: RoleType,
) -> Result<Plays, Box<SymbolResolutionError>> {
    match try_resolve_plays_declared(snapshot, type_manager, object_type, role_type) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(Box::new(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_plays_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    role_type: RoleType,
) -> Result<Option<Plays>, Box<ConceptReadError>> {
    object_type.get_plays_role_declared(snapshot, type_manager, role_type)
}

pub(crate) fn resolve_plays(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    role_type: RoleType,
) -> Result<Plays, Box<SymbolResolutionError>> {
    match try_resolve_plays(snapshot, type_manager, object_type, role_type) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(Box::new(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_plays(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    role_type: RoleType,
) -> Result<Option<Plays>, Box<ConceptReadError>> {
    object_type.get_plays_role(snapshot, type_manager, role_type)
}

pub(crate) fn resolve_plays_role_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    label: &Label,
) -> Result<Plays, Box<SymbolResolutionError>> {
    match try_resolve_plays_role_label(snapshot, type_manager, object_type, label) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(Box::new(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: label.clone(),
        })),
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { typedb_source: source })),
    }
}

pub(crate) fn try_resolve_plays_role_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    label: &Label,
) -> Result<Option<Plays>, Box<ConceptReadError>> {
    match label.scope {
        Some(_) => {
            let role_type = try_resolve_role_type(snapshot, type_manager, label)?;
            match role_type {
                Some(role_type) => try_resolve_plays(snapshot, type_manager, object_type, role_type),
                None => Ok(None),
            }
        }
        None => object_type.get_plays_role_name(snapshot, type_manager, label.name.as_str()),
    }
}

// TODO: ideally these all have TypeQL declarations, so we can pinpoint line number in these errors!
typedb_error! {
    pub SymbolResolutionError(component = "Symbol resolution", prefix = "SYR") {
        TypeNotFound(1, "The type '{label}' was not found.", label: Label),
        StructNotFound(2, "The struct value type '{name}' was not found.", name: String),
        StructFieldIllegalList(3, "Struct fields cannot be lists.\nSource:\n{declaration}", declaration: Field),
        ValueTypeNotFound(4, "The value type '{name}' was not found.", name: String),
        ObjectTypeNotFound(5, "The entity or relation type '{label}' was not found.", label: Label),
        EntityTypeNotFound(6, "The entity type '{label}' was not found.", label: Label),
        RelationTypeNotFound(7, "The relation type '{label}' was not found.", label: Label),
        AttributeTypeNotFound(8, "The attribute type '{label}' was not found.", label: Label),
        RoleTypeNotFound(9, "The role type '{label}' was not found.", label: Label),
        RelatesNotFound(10, "The relation type '{label}' does not relate role '{role_name}'", label: Label, role_name: String),
        OwnsNotFound(11, "The type '{owner_label}' does not own attribute type '{attribute_label}'.", owner_label: Label, attribute_label: Label),
        PlaysNotFound(12, "The type '{player_label}' does not play the role '{role_label}'.", player_label: Label, role_label: Label),
        ExpectedNonVariableAndNonScopedTypeSymbol(14, "Expected a type label or a type[] label, but not a variable or scoped label.\nSource:\n{declaration}", declaration: TypeRefAny),
        UnexpectedConceptRead(15, "Unexpected concept read error.", typedb_source: Box<ConceptReadError>),
        IllegalKeywordAsIdentifier(16, "The reserved keyword '{identifier}' cannot be used as an identifier.", identifier: typeql::Identifier),
    }
}
