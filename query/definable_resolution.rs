/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use typeql::{
    common::error::TypeQLError,
    schema::definable::struct_::Field,
    type_::{BuiltinValueType, NamedType, Optional},
    TypeRef, TypeRefAny,
};

use answer::Type;
use concept::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, ObjectTypeAPI, Ordering,
        OwnerAPI, owns::Owns, PlayerAPI, plays::Plays, relates::Relates,
        relation_type::RelationType, role_type::RoleType, type_manager::TypeManager, TypeAPI,
    },
};
use encoding::{
    graph::definition::definition_key::DefinitionKey,
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

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
    label: &Label,
    type_ref: &TypeRefAny,
) -> Result<(Label, Ordering), Box<SymbolResolutionError>> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => {
            Ok((Label::parse_from(checked_identifier(&label.ident)?), Ordering::Unordered))
        }
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Ok((Label::parse_from(checked_identifier(&label.ident)?), Ordering::Ordered))
        }
        _ => Err(Box::new(SymbolResolutionError::ExpectedNonOptionalTypeSymbol { declaration: type_ref.clone() })),
    }
}

pub(crate) fn named_type_to_label(named_type: &NamedType) -> Result<Label, Box<SymbolResolutionError>> {
    match named_type {
        NamedType::Label(label) => Ok(Label::build(checked_identifier(&label.ident)?)),
        NamedType::Role(scoped_label) => Ok(Label::build_scoped(
            checked_identifier(&scoped_label.name.ident)?,
            checked_identifier(&scoped_label.scope.ident)?,
        )),
        NamedType::BuiltinValueType(_) => Err(Box::new(SymbolResolutionError::ExpectedLabelButGotBuiltinValueType {
            declaration: named_type.clone(),
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
    let optional = matches!(&field.type_, TypeRefAny::Optional(_));
    match &field.type_ {
        TypeRefAny::Type(TypeRef::Named(named))
        | TypeRefAny::Optional(Optional { inner: TypeRef::Named(named), .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)?;
            Ok((value_type, optional))
        }
        TypeRefAny::Type(TypeRef::Variable(_)) | TypeRefAny::Optional(Optional { inner: TypeRef::Variable(_), .. }) => {
            Err(Box::new(SymbolResolutionError::StructFieldIllegalVariable { declaration: field.clone() }))
        }
        TypeRefAny::List(_) => {
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
                Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
            }
        }
        NamedType::Role(scoped_label) => Err(Box::new(SymbolResolutionError::ScopedValueTypeName {
            scope: scoped_label.scope.ident.as_str_unchecked().to_owned(),
            name: scoped_label.name.ident.as_str_unchecked().to_owned(),
        })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
    }
}

pub(crate) fn try_resolve_relates_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Option<Relates>, Box<ConceptReadError>> {
    relation_type.get_relates_role_name_declared(snapshot, type_manager, role_name)
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
    }
}

pub(crate) fn try_resolve_relates(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType,
    role_name: &str,
) -> Result<Option<Relates>, Box<ConceptReadError>> {
    relation_type.get_relates_role_name_with_specialised(snapshot, type_manager, role_name)
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
    }
}

pub(crate) fn try_resolve_owns(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    attribute_type: AttributeType,
) -> Result<Option<Owns>, Box<ConceptReadError>> {
    object_type.get_owns_attribute_with_specialised(snapshot, type_manager, attribute_type)
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
    }
}

pub(crate) fn try_resolve_plays(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType,
    role_type: RoleType,
) -> Result<Option<Plays>, Box<ConceptReadError>> {
    object_type.get_plays_role_with_specialised(snapshot, type_manager, role_type)
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
        Err(source) => Err(Box::new(SymbolResolutionError::UnexpectedConceptRead { source })),
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
        None => object_type.get_plays_role_name_with_specialised(snapshot, type_manager, label.name.as_str()),
    }
}

// TODO: ideally these all have TypeQL declarations, so we can pinpoint line number in these errors!
typedb_error!(
    pub(crate) SymbolResolutionError(component = "Symbol resolution", prefix = "SYR") {
        TypeNotFound(1, "The type '{label}' was not found.", label: Label),
        StructNotFound(2, "The struct value type '{name}' was not found.", name: String),
        StructFieldIllegalList(3, "Struct fields cannot be lists.\nSource:\n{declaration}", declaration: Field),
        StructFieldIllegalVariable(4, "Encountered variable in struct field declaration.\nSource:\n{declaration}", declaration: Field),
        ValueTypeNotFound(5, "The value type '{name}' was not found.", name: String),
        ObjectTypeNotFound(6, "The entity or relation type '{label}' was not found.", label: Label),
        EntityTypeNotFound(7, "The entity type '{label}' was not found.", label: Label),
        RelationTypeNotFound(8, "The relation type '{label}' was not found.", label: Label),
        AttributeTypeNotFound(9, "The attribute type '{label}' was not found.", label: Label),
        RoleTypeNotFound(10, "The role type '{label}' was not found.", label: Label),
        RelatesNotFound(11, "The relation type '{label}' does not relate role '{role_name}'", label: Label, role_name: String),
        OwnsNotFound(12, "The type '{owner_label}' does not own attribute type '{attribute_label}'.", owner_label: Label, attribute_label: Label ),
        PlaysNotFound(13, "The type '{player_label}' does not play the role '{role_label}'.", player_label: Label, role_label: Label),
        ScopedValueTypeName(14, "Value type names cannot have scopes. Provided illegal name: '{scope}:{name}'.", scope: String, name: String),
        ExpectedNonOptionalTypeSymbol(15, "Expected a type label or a type[] label, but not an optional type? label.\nSource:\n{declaration}", declaration: TypeRefAny ),
        ExpectedLabelButGotBuiltinValueType(16, "Expected type label got built-in value type name:\nSource:\n{declaration}", declaration: NamedType),
        UnexpectedConceptRead(17, "Unexpected concept read error.", ( source: Box<ConceptReadError> ) ),
        IllegalKeywordAsIdentifier(18, "The reserved keyword \"{identifier}\" cannot be used as an identifier.", identifier: typeql::Identifier),
    }
);
