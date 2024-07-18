/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt,
    fmt::{Display, Formatter},
};

use concept::{
    error::ConceptReadError,
    type_::{
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues},
        attribute_type::AttributeType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::validation::SchemaValidationError,
    },
};
use encoding::value::{label::Label, value_type::ValueType};

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
