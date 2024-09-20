/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, hash::Hash};

use answer::variable::Variable;
use encoding::value::label::Label;

pub mod conjunction;
pub mod constraint;
pub mod negation;
pub mod optional;
pub mod variable_category;

pub mod disjunction;
pub mod expression;
pub mod function_call;
pub mod nested_pattern;

pub trait Scope {
    fn scope_id(&self) -> ScopeId;
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct ScopeId {
    id: u16,
    // TODO: retain line/character from original query at which point this scope started
}

impl ScopeId {
    pub const INPUT: ScopeId = ScopeId { id: 0 };
    pub const ROOT: ScopeId = ScopeId { id: 1 };

    pub(crate) fn new(id: u16) -> Self {
        ScopeId { id }
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.id)
    }
}

pub trait IrID: Copy + fmt::Display + Hash + Eq + PartialEq + Ord + PartialOrd + 'static {}

impl IrID for Variable {}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Vertex<ID> {
    Variable(ID),
    Label(Label<'static>),
    Parameter(ParameterID),
}

impl<ID: IrID> Vertex<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Vertex<T> {
        match self {
            Self::Variable(var) => Vertex::Variable(mapping[&var]),
            Self::Label(label) => Vertex::Label(label),
            Self::Parameter(param) => Vertex::Parameter(param),
        }
    }

    pub fn as_variable(&self) -> Option<ID> {
        if let &Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the vertex is [`Variable`].
    ///
    /// [`Variable`]: Vertex::Variable
    #[must_use]
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(..))
    }
}

impl<ID> From<ID> for Vertex<ID> {
    fn from(var: ID) -> Self {
        Self::Variable(var)
    }
}

impl<ID: fmt::Display> fmt::Display for Vertex<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(var) => fmt::Display::fmt(var, f),
            Self::Label(label) => write!(f, "{}", label.scoped_name().as_str()),
            Self::Parameter(param) => fmt::Display::fmt(param, f),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ParameterID {
    pub id: usize,
}

impl fmt::Display for ParameterID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parameter[{}]", self.id)
    }
}
