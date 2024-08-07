/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use concept::{
    error::ConceptReadError,
    type_::{
        object_type::ObjectType,
        type_manager::TypeManager,
    },
};
use encoding::{
    value::label::Label,
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{type_::{NamedType}, TypeRef, TypeRefAny};
use typeql::type_::BuiltinValueType;
use concept::type_::Ordering;

use crate::{define::DefineError, SymbolResolutionError};


pub(crate) fn type_ref_to_label_and_ordering(type_ref: &TypeRefAny) -> Result<(Label<'static>, Ordering), ()> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Unordered))
        }
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Ordered))
        }
        _ => Err(()),
    }
}


pub(crate) fn resolve_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<answer::Type, SymbolResolutionError> {
    match try_resolve_type(snapshot, type_manager, label) {
        Ok(Some(type_)) => Ok(type_),
        Ok(None) => Err(SymbolResolutionError::TypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<answer::Type>, ConceptReadError> {
    // TODO: Introduce a method on type_manager that does this in one step
    let type_ = if let Some(object_type) = type_manager.get_object_type(snapshot, label)? {
        match object_type {
            ObjectType::Entity(entity_type) => Some(answer::Type::Entity(entity_type)),
            ObjectType::Relation(relation_type) => Some(answer::Type::Relation(relation_type)),
        }
    } else if let Some(attribute_type) = type_manager.get_attribute_type(snapshot, label)? {
        Some(answer::Type::Attribute(attribute_type))
    } else if let Some(role_type) = type_manager.get_role_type(snapshot, label)? {
        Some(answer::Type::RoleType(role_type))
    } else {
        None
    };
    Ok(type_)
}

pub(crate) fn resolve_value_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field_name: &NamedType,
) -> Result<encoding::value::value_type::ValueType, SymbolResolutionError> {
    match field_name {
        NamedType::Label(label) => {
            let key = type_manager.get_struct_definition_key(snapshot, label.ident.as_str());
            match key {
                Ok(Some(key)) => Ok(encoding::value::value_type::ValueType::Struct(key)),
                Ok(None) | Err(_) => {
                    Err(SymbolResolutionError::ValueTypeNotFound { name: label.ident.as_str().to_owned() })
                }
            }
        }
        NamedType::Role(scoped_label) => Err(SymbolResolutionError::IllegalValueTypeName {
            scope: scoped_label.scope.ident.as_str().to_owned(),
            name: scoped_label.name.ident.as_str().to_owned(),
        }),
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ir::translation::tokens::translate_value_type(token))
        }
    }
}
