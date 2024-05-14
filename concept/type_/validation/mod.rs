/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use encoding::value::label::Label;
use crate::error::ConceptReadError;
use crate::type_::attribute_type::AttributeType;
use crate::type_::role_type::RoleType;

pub mod validation;


#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    LabelUniqueness(Label<'static>),
    CyclicTypeHierarchy, // TODO: Add details of what caused it
    RootModification,
    RelatesNotInherited(RoleType<'static>),
    OwnsNotInherited(AttributeType<'static>),
    PlaysNotInherited(RoleType<'static>),
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
            SchemaValidationError::CyclicTypeHierarchy => None,
            SchemaValidationError::RootModification => None,
            SchemaValidationError::RelatesNotInherited(_) => None,
            SchemaValidationError::OwnsNotInherited(_) => None,
            SchemaValidationError::PlaysNotInherited(_) => None,
        }
    }
}
