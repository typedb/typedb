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
    schema::definable::struct_::Field,
    type_::{BuiltinValueType, NamedType, Optional},
    TypeRef, TypeRefAny,
};

macro_rules! try_unwrap {
    ($variant:path = $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    };
}
pub(crate) use try_unwrap;

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
pub(crate) use filter_variants;

pub(crate) fn type_ref_to_label_and_ordering(
    label: &Label,
    type_ref: &TypeRefAny,
) -> Result<(Label<'static>, Ordering), SymbolResolutionError> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Unordered))
        }
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Ordered))
        }
        _ => Err(SymbolResolutionError::ExpectedNonOptionalTypeSymbol { declaration: type_ref.clone() }),
    }
}

pub(crate) fn named_type_to_label(named_type: &NamedType) -> Result<Label<'static>, SymbolResolutionError> {
    match named_type {
        NamedType::Label(label) => Ok(Label::build(label.ident.as_str())),
        NamedType::Role(scoped_label) => {
            Ok(Label::build_scoped(scoped_label.name.ident.as_str(), scoped_label.scope.ident.as_str()))
        }
        NamedType::BuiltinValueType(_) => {
            Err(SymbolResolutionError::ExpectedLabelButGotBuiltinValueType { declaration: named_type.clone() })
        }
    }
}

pub(crate) fn type_to_object_type(type_: &Type) -> Result<ObjectType<'static>, ()> {
    Ok(match &type_ {
        Type::Entity(entity_type) => ObjectType::Entity(entity_type.clone()),
        Type::Relation(relation_type) => ObjectType::Relation(relation_type.clone()),
        _ => return Err(()),
    }
    .into_owned_object_type())
}

pub(crate) fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), SymbolResolutionError> {
    let optional = matches!(&field.type_, TypeRefAny::Optional(_));
    match &field.type_ {
        TypeRefAny::Type(TypeRef::Named(named))
        | TypeRefAny::Optional(Optional { inner: TypeRef::Named(named), .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)?;
            Ok((value_type, optional))
        }
        TypeRefAny::Type(TypeRef::Variable(_)) | TypeRefAny::Optional(Optional { inner: TypeRef::Variable(_), .. }) => {
            Err(SymbolResolutionError::StructFieldIllegalVariable { declaration: field.clone() })
        }
        TypeRefAny::List(_) => Err(SymbolResolutionError::StructFieldIllegalList { declaration: field.clone() }),
    }
}

pub(crate) fn resolve_typeql_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Type, SymbolResolutionError> {
    match try_resolve_typeql_type(snapshot, type_manager, label) {
        Ok(Some(type_)) => Ok(type_),
        Ok(None) => Err(SymbolResolutionError::TypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_typeql_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<Type>, ConceptReadError> {
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
) -> Result<ValueType, SymbolResolutionError> {
    match field_name {
        NamedType::Label(label) => {
            let key = try_resolve_struct_definition_key(snapshot, type_manager, label.ident.as_str());
            match key {
                Ok(Some(key)) => Ok(ValueType::Struct(key)),
                Ok(None) => Err(SymbolResolutionError::ValueTypeNotFound { name: label.ident.as_str().to_owned() }),
                Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
            }
        }
        NamedType::Role(scoped_label) => Err(SymbolResolutionError::ScopedValueTypeName {
            scope: scoped_label.scope.ident.as_str().to_owned(),
            name: scoped_label.name.ident.as_str().to_owned(),
        }),
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ir::translation::tokens::translate_value_type(token))
        }
    }
}

pub(crate) fn resolve_struct_definition_key<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<DefinitionKey<'static>, SymbolResolutionError> {
    match try_resolve_struct_definition_key(snapshot, type_manager, name) {
        Ok(Some(key)) => Ok(key),
        Ok(None) => Err(SymbolResolutionError::StructNotFound { name: name.to_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_struct_definition_key(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<Option<DefinitionKey<'static>>, ConceptReadError> {
    type_manager.get_struct_definition_key(snapshot, name)
}

pub(crate) fn resolve_object_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<ObjectType<'static>, SymbolResolutionError> {
    match try_resolve_object_type(snapshot, type_manager, label) {
        Ok(Some(object_type)) => Ok(object_type),
        Ok(None) => Err(SymbolResolutionError::ObjectTypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_object_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<ObjectType<'static>>, ConceptReadError> {
    type_manager.get_object_type(snapshot, label)
}

pub(crate) fn resolve_entity_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<EntityType<'static>, SymbolResolutionError> {
    match try_resolve_entity_type(snapshot, type_manager, label) {
        Ok(Some(entity_type)) => Ok(entity_type),
        Ok(None) => Err(SymbolResolutionError::EntityTypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_entity_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<EntityType<'static>>, ConceptReadError> {
    type_manager.get_entity_type(snapshot, label)
}

pub(crate) fn resolve_relation_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<RelationType<'static>, SymbolResolutionError> {
    match try_resolve_relation_type(snapshot, type_manager, label) {
        Ok(Some(relation_type)) => Ok(relation_type),
        Ok(None) => Err(SymbolResolutionError::RelationTypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_relation_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<RelationType<'static>>, ConceptReadError> {
    type_manager.get_relation_type(snapshot, label)
}

pub(crate) fn resolve_attribute_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<AttributeType<'static>, SymbolResolutionError> {
    match try_resolve_attribute_type(snapshot, type_manager, label) {
        Ok(Some(attribute_type)) => Ok(attribute_type),
        Ok(None) => Err(SymbolResolutionError::AttributeTypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_attribute_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<AttributeType<'static>>, ConceptReadError> {
    type_manager.get_attribute_type(snapshot, label)
}

pub(crate) fn resolve_role_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<RoleType<'static>, SymbolResolutionError> {
    match try_resolve_role_type(snapshot, type_manager, label) {
        Ok(Some(role_type)) => Ok(role_type),
        Ok(None) => Err(SymbolResolutionError::RoleTypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_role_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<RoleType<'static>>, ConceptReadError> {
    type_manager.get_role_type(snapshot, label)
}

pub(crate) fn resolve_relates_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_name: &str,
) -> Result<Relates<'static>, SymbolResolutionError> {
    match try_resolve_relates_declared(snapshot, type_manager, relation_type.clone(), role_name) {
        Ok(Some(relates)) => Ok(relates),
        Ok(None) => Err(SymbolResolutionError::RelatesNotFound {
            label: relation_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_name: role_name.to_string(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_relates_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_name: &str,
) -> Result<Option<Relates<'static>>, ConceptReadError> {
    relation_type.get_relates_role_name_declared(snapshot, type_manager, role_name)
}

pub(crate) fn resolve_relates(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_name: &str,
) -> Result<Relates<'static>, SymbolResolutionError> {
    match try_resolve_relates(snapshot, type_manager, relation_type.clone(), role_name) {
        Ok(Some(relates)) => Ok(relates),
        Ok(None) => Err(SymbolResolutionError::RelatesNotFound {
            label: relation_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_name: role_name.to_string(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_relates(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_name: &str,
) -> Result<Option<Relates<'static>>, ConceptReadError> {
    relation_type.get_relates_role_name_with_specialised(snapshot, type_manager, role_name)
}

pub(crate) fn resolve_owns_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
) -> Result<Owns<'static>, SymbolResolutionError> {
    match try_resolve_owns_declared(snapshot, type_manager, object_type.clone(), attribute_type.clone()) {
        Ok(Some(owns)) => Ok(owns),
        Ok(None) => Err(SymbolResolutionError::OwnsNotFound {
            owner_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            attribute_label: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_owns_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
) -> Result<Option<Owns<'static>>, ConceptReadError> {
    object_type.get_owns_attribute_declared(snapshot, type_manager, attribute_type.clone())
}

pub(crate) fn resolve_owns(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
) -> Result<Owns<'static>, SymbolResolutionError> {
    match try_resolve_owns(snapshot, type_manager, object_type.clone(), attribute_type.clone()) {
        Ok(Some(owns)) => Ok(owns),
        Ok(None) => Err(SymbolResolutionError::OwnsNotFound {
            owner_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            attribute_label: attribute_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_owns(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
) -> Result<Option<Owns<'static>>, ConceptReadError> {
    object_type.get_owns_attribute_with_specialised(snapshot, type_manager, attribute_type.clone())
}

pub(crate) fn resolve_plays_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<Plays<'static>, SymbolResolutionError> {
    match try_resolve_plays_declared(snapshot, type_manager, object_type.clone(), role_type.clone()) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_plays_declared(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<Option<Plays<'static>>, ConceptReadError> {
    object_type.get_plays_role_declared(snapshot, type_manager, role_type.clone())
}

pub(crate) fn resolve_plays(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<Plays<'static>, SymbolResolutionError> {
    match try_resolve_plays(snapshot, type_manager, object_type.clone(), role_type.clone()) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: role_type.get_label(snapshot, type_manager).unwrap().to_owned(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_plays(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<Option<Plays<'static>>, ConceptReadError> {
    object_type.get_plays_role_with_specialised(snapshot, type_manager, role_type.clone())
}

pub(crate) fn resolve_plays_role_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    label: &Label<'_>,
) -> Result<Plays<'static>, SymbolResolutionError> {
    match try_resolve_plays_role_label(snapshot, type_manager, object_type.clone(), label) {
        Ok(Some(plays)) => Ok(plays),
        Ok(None) => Err(SymbolResolutionError::PlaysNotFound {
            player_label: object_type.get_label(snapshot, type_manager).unwrap().to_owned(),
            role_label: label.clone().into_owned(),
        }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_plays_role_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    label: &Label<'_>,
) -> Result<Option<Plays<'static>>, ConceptReadError> {
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
        TypeNotFound(1, "The type '{label}' was not found.", label: Label<'static>),
        StructNotFound(2, "The struct value type '{name}' was not found.", name: String),
        StructFieldIllegalList(3, "Struct fields cannot be lists.\nSource:\n{declaration}", declaration: Field),
        StructFieldIllegalVariable(4, "Encountered variable in struct field declaration.\nSource:\n{declaration}", declaration: Field),
        ValueTypeNotFound(5, "The value type '{name}' was not found.", name: String),
        ObjectTypeNotFound(6, "The entity or relation type '{label}' was not found.", label: Label<'static>),
        EntityTypeNotFound(7, "The entity type '{label}' was not found.", label: Label<'static>),
        RelationTypeNotFound(8, "The relation type '{label}' was not found.", label: Label<'static>),
        AttributeTypeNotFound(9, "The attribute type '{label}' was not found.", label: Label<'static>),
        RoleTypeNotFound(10, "The role type '{label}' was not found.", label: Label<'static>),
        RelatesNotFound(11, "The relation type '{label}' does not relate role '{role_name}'", label: Label<'static>, role_name: String),
        OwnsNotFound(12, "The type '{owner_label}' does not own attribute type '{attribute_label}'.", owner_label: Label<'static>, attribute_label: Label<'static> ),
        PlaysNotFound(13, "The type '{player_label}' does not play the role '{role_label}'.", player_label: Label<'static>, role_label: Label<'static>),
        ScopedValueTypeName(14, "Value type names cannot have scopes. Provided illegal name: '{scope}:{name}'.", scope: String, name: String),
        ExpectedNonOptionalTypeSymbol(15, "Expected a type label or a type[] label, but not an optional type? label.\nSource:\n{declaration}", declaration: TypeRefAny ),
        ExpectedLabelButGotBuiltinValueType(16, "Expected type label got built-in value type name:\nSource:\n{declaration}", declaration: NamedType),
        UnexpectedConceptRead(17, "Unexpected concept read error.", ( source: ConceptReadError ) ),
    }
);
