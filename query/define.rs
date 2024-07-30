/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use concept::{
    error::{ConceptReadError, ConceptWriteError},
    type_::type_manager::TypeManager,
};
use encoding::value::{label::Label, value_type::ValueType};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::token,
    query::schema::Define,
    schema::definable::{struct_::Field, Struct, Type},
    type_::{BuiltinValueType, NamedType, Optional},
    Definable, ScopedLabel, TypeRef, TypeRefAny,
};

use crate::util::as_value_type_category;

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
                .map_err(|err| DefineError::ConceptRead { source: err })?
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
            TypeRef::Named(named) => Ok((get_named_value_type(snapshot, type_manager, named)?, false)),
            TypeRef::Variable(_) => {
                return Err(DefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
            }
        },
        TypeRefAny::Optional(Optional { inner: type_ref, .. }) => match type_ref {
            TypeRef::Named(named) => Ok((get_named_value_type(snapshot, type_manager, named)?, false)),
            TypeRef::Variable(_) => {
                return Err(DefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
            }
        },
        TypeRefAny::List(list) => {
            return Err(DefineError::StructFieldIllegalList { field_declaration: field.clone() });
        }
    }
}

fn get_named_value_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field_name: &NamedType,
) -> Result<ValueType, DefineError> {
    match field_name {
        NamedType::Label(label) => {
            let key = type_manager.get_struct_definition_key(snapshot, label.ident.as_str());
            match key {
                Ok(Some(key)) => Ok(ValueType::Struct(key)),
                Ok(None) | Err(_) => Err(DefineError::StructFieldValueTypeNotFound { label: label.clone() }),
            }
        }
        NamedType::Role(scoped_label) => {
            Err(DefineError::StructFieldIllegalNotValueType { scoped_label: scoped_label.clone() })
        }
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            let category = as_value_type_category(token);
            let value_type = category.try_into_value_type().unwrap(); // unwrap is safe: builtins are never struct
            Ok(value_type)
        }
    }
}

fn define_capabilities<'a>(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    Ok(())
}

fn define_functions<'a>(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &Vec<Definable>,
) -> Result<(), DefineError> {
    Ok(())
}

#[derive(Debug)]
pub enum DefineError {
    ConceptRead { source: ConceptReadError },
    TypeCreateRequiresKind { type_declaration: Type },
    TypeCreateError { source: ConceptWriteError, type_declaration: Type },
    RoleTypeDirectCreate { type_declaration: Type },
    StructCreateError { source: ConceptWriteError, struct_declaration: Struct },
    StructFieldCreateError { source: ConceptWriteError, struct_name: String, struct_field: Field },
    StructFieldIllegalList { field_declaration: Field },
    StructFieldIllegalVariable { field_declaration: Field },
    StructFieldIllegalNotValueType { scoped_label: ScopedLabel },
    StructFieldValueTypeNotFound { label: typeql::Label },
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
