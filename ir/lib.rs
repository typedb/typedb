/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use std::fmt::Display;
use crate::pattern::constraint::Constraint;

use crate::pattern::variable::{Variable, VariableCategory};

pub mod pattern;
mod inference;
pub mod program;


#[derive(Debug)]
pub enum PatternDefinitionError {
    DisjointVariableReuse { variable_name: String },
    VariableCategoryMismatch {
        variable: Variable,
        variable_name: Option<String>,
        category_1: VariableCategory,
        category_1_source: Constraint,
        category_2: VariableCategory,
        category_2_source: Constraint,
    },
}

impl fmt::Display for PatternDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for PatternDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DisjointVariableReuse { .. } => None,
            Self::VariableCategoryMismatch { .. } => None,
        }
    }
}
