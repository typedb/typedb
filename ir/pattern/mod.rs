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

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum TypeSource<ID> {
    Variable(ID),
    Label(Label<'static>),
}

impl<ID: IrID> TypeSource<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> TypeSource<T> {
        match self {
            TypeSource::Variable(var) => TypeSource::Variable(mapping[&var]),
            TypeSource::Label(label) => TypeSource::Label(label),
        }
    }

    pub fn as_variable(&self) -> Option<ID> {
        if let &Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl<ID: fmt::Display> fmt::Display for TypeSource<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeSource::Variable(var) => fmt::Display::fmt(var, f),
            TypeSource::Label(label) => write!(f, "{}", label.scoped_name().as_str()),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ValueSource<ID> {
    Variable(ID),
    Parameter(ParameterID),
}

impl<ID: IrID> ValueSource<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ValueSource<T> {
        match self {
            ValueSource::Variable(var) => ValueSource::Variable(mapping[&var]),
            ValueSource::Parameter(parameter) => ValueSource::Parameter(parameter),
        }
    }

    pub fn as_variable(&self) -> Option<ID> {
        if let &Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl<ID: fmt::Display> fmt::Display for ValueSource<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(var) => fmt::Display::fmt(var, f),
            Self::Parameter(param) => fmt::Display::fmt(param, f),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ParameterID {
    pub id: usize,
}

impl fmt::Display for ParameterID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parameter[{}]", self.id)
    }
}
