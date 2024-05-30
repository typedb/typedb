/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use encoding::graph::Typed;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use storage::snapshot::ReadableSnapshot;
use crate::error::ConceptReadError;
use crate::type_::attribute_type::AttributeType;
use crate::type_::object_type::ObjectType;
use crate::type_::role_type::RoleType;
use crate::type_::TypeAPI;

pub mod annotation_compatibility;
pub mod commit_time_validation;
pub mod operation_time_validation;


#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    RootModification,
    LabelUniqueness(Label<'static>),
    CyclicTypeHierarchy(Label<'static>, Label<'static>), // TODO: Add details of what caused it
    RelatesNotInherited(RoleType<'static>),
    OwnsNotInherited(AttributeType<'static>),
    PlaysNotInherited(ObjectType<'static>, RoleType<'static>),
    OverriddenTypeNotSupertype(Label<'static>, Label<'static>),
    PlaysNotDeclared(ObjectType<'static>, RoleType<'static>),
    TypeIsNotAbstract(Label<'static>),
    IncompatibleValueTypes(Option<ValueType>, Option<ValueType>),
    DeletingTypeWithSubtypes(Label<'static>),
    DeletingTypeWithInstances(Label<'static>),
}

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SchemaValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead(source) => Some(source),
            Self::LabelUniqueness(_) => None,
            SchemaValidationError::RootModification => None,
            SchemaValidationError::CyclicTypeHierarchy(_,_) => None,
            SchemaValidationError::RelatesNotInherited(_) => None,
            SchemaValidationError::OwnsNotInherited(_) => None,
            SchemaValidationError::PlaysNotInherited(_, _) => None,
            SchemaValidationError::OverriddenTypeNotSupertype(_, _) => None,
            SchemaValidationError::PlaysNotDeclared(_, _) => None,
            SchemaValidationError::TypeIsNotAbstract(_) => None,
            SchemaValidationError::IncompatibleValueTypes(_, _) => None,
            SchemaValidationError::DeletingTypeWithSubtypes(_) => None,
            SchemaValidationError::DeletingTypeWithInstances(_) => None,
        }
    }
}
