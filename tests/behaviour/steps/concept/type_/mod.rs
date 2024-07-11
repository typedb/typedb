/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use concept::error::ConceptReadError;
use concept::type_::annotation::{AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues};
use concept::type_::attribute_type::AttributeType;
use concept::type_::object_type::ObjectType;
use concept::type_::relation_type::RelationType;
use concept::type_::role_type::RoleType;
use concept::type_::type_manager::validation::SchemaValidationError;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;

pub mod attribute_type;
mod owns;
mod plays;
mod relation_type;
mod struct_definition;
mod thing_type;

#[derive(Debug, Clone)]
pub enum BehaviourConceptTestExecutionError {
    CannotFindRoleToOverride,
}

impl Display for BehaviourConceptTestExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for BehaviourConceptTestExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CannotFindRoleToOverride => None,
        }
    }
}
