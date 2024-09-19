/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

pub mod attribute_type;
mod owns;
mod plays;
mod relation_type;
mod struct_definition;
mod thing_type;

#[derive(Debug, Clone)]
pub enum BehaviourConceptTestExecutionError {
    CannotFindRelationTypeRoleTypeToSpecialise,
    CannotFindRoleToAddPlayerTo,
    CannotFindStructDefinition,
}

impl fmt::Display for BehaviourConceptTestExecutionError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for BehaviourConceptTestExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CannotFindRelationTypeRoleTypeToSpecialise => None,
            Self::CannotFindRoleToAddPlayerTo => None,
            Self::CannotFindStructDefinition => None,
        }
    }
}
