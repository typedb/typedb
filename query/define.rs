/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;

use typeql::common::token;
use typeql::Definable;
use typeql::query::schema::Define;
use typeql::schema::definable::{Struct, Type};

use concept::error::ConceptWriteError;
use concept::type_::type_manager::TypeManager;
use encoding::value::label::Label;
use storage::snapshot::WritableSnapshot;

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    define: Define,
) -> Result<(), DefineError> {
    define_types(snapshot, type_manager, define.definables.iter())?;
    define_structs(snapshot, type_manager, define.definables.iter())?;
    define_capabilities(snapshot, type_manager, define.definables.iter())?;
    define_functions(snapshot, type_manager, define.definables.iter())?;
    Ok(())
}

fn define_types(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: impl Iterator<Item=&Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::TypeDeclaration(type_declaration) = definable {
            let label = Label::parse_from(type_declaration.label.ident.as_str());
            match type_declaration.kind {
                None => {
                    return Err(DefineError0::TypeCreateRequiresKind { type_declaration: type_declaration.clone() });
                }
                Some(token::Kind::Entity) => {
                    type_manager.create_entity_type(snapshot, &label)
                        .map_err(|err| DefineError::TypeCreateError {
                            source: err,
                            type_declaration: type_declaration.clone(),
                        })?
                }
                Some(token::Kind::Relation) => {
                    type_manager.create_relation_type(snapshot, &label)
                        .map_err(|err| DefineError::TypeCreateError {
                            source: err,
                            type_declaration: type_declaration.clone(),
                        })?
                }
                Some(token::Kind::Attribute) => {
                    type_manager.create_attribute_type(snapshot, &label)
                        .map_err(|err| DefineError::TypeCreateError {
                            source: err,
                            type_declaration: type_declaration.clone(),
                        })?
                }
                Some(token::Kind::Role) => return Err(DefineError::RoleTypeDirectCreate {
                    type_declaration: type_declaration.clone()
                })?
            }
        }
    };
    Ok(())
}

fn define_structs(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: impl Iterator<Item=&Definable>,
) -> Result<(), DefineError> {
    for definable in definables {
        if let Definable::Struct(struct_definable) = definable {
            let name = struct_definable.ident.as_str();
            let struct_key = type_manager.create_struct(snapshot, name.to_owned())
                .map_err(|err| DefineError::StructCreateError { source: err, struct_declaration: struct_definable.clone() })?;

            for field in &struct_definable.fields {
                type_manager.create_struct_field().unwrap()
            }
        }
    }
    Ok(())
}

fn define_capabilities(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: impl Iterator<Item=&Definable>,
) -> Result<(), DefineError> {
    Ok(())
}

fn define_functions(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: impl Iterator<Item=&Definable>,
) -> Result<(), DefineError> {
    Ok(())
}

#[derive(Debug)]
pub enum DefineError {
    TypeCreateRequiresKind { type_declaration: Type },
    TypeCreateError { source: ConceptWriteError, type_declaration: Type },
    RoleTypeDirectCreate { type_declaration: Type },
    StructCreateError { source: ConceptWriteError, struct_declaration: Struct },
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
