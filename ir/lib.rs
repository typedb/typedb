/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};

use crate::variable::Variable;

pub mod variable;
pub mod optional;
pub mod negation;
pub mod constraint;
pub mod conjunction;

pub mod disjunction;
mod expression;
mod function;
pub mod context;
mod pattern;


trait Scope {
    fn scope_id(&self) -> ScopeId;

    // fn add_parent_variable(&mut self, variable: Variable);
}


#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct ScopeId {
    id: u16,
    // TODO: retain line/character from original query at which point this scope started
}

impl ScopeId {
    pub const ROOT: ScopeId = ScopeId { id: 0 };

    fn new(id: u16) -> Self {
        ScopeId { id }
    }
}

impl Display for ScopeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.id)
    }
}


#[derive(Debug)]
pub enum PatternDefinitionError {
    DisjointVariableReuse { variable_name: String },
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
        }
    }
}
