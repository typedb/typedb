/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, fmt::Formatter, hash::Hash, mem, ops::BitXor};

use answer::variable::Variable;
use encoding::value::label::Label;
use structural_equality::StructuralEquality;

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

pub trait IrID: Copy + fmt::Display + fmt::Debug + Hash + Eq + PartialEq + Ord + PartialOrd + 'static {
    fn map<T: IrID>(&self, mapping: &HashMap<Self, T>) -> T {
        mapping.get(self).unwrap().clone()
    }
}

impl IrID for Variable {}

// TODO: rename to 'Identifier' in lieu of a better name
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Vertex<ID> {
    Variable(ID),
    Label(Label),
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

    pub fn as_label(&self) -> Option<&Label> {
        if let Self::Label(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_parameter(&self) -> Option<ParameterID> {
        if let &Self::Parameter(v) = self {
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

    /// Returns `true` if the vertex is [`Label`].
    ///
    /// [`Label`]: Vertex::Label
    #[must_use]
    pub fn is_label(&self) -> bool {
        matches!(self, Self::Label(..))
    }

    /// Returns `true` if the vertex is [`Parameter`].
    ///
    /// [`Parameter`]: Vertex::Parameter
    #[must_use]
    pub fn is_parameter(&self) -> bool {
        matches!(self, Self::Parameter(..))
    }
}

impl<ID> From<ID> for Vertex<ID> {
    fn from(var: ID) -> Self {
        Self::Variable(var)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Vertex<ID> {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self)).bitxor(match self {
            Vertex::Variable(var) => StructuralEquality::hash(var),
            Vertex::Label(label) => StructuralEquality::hash(label),
            Vertex::Parameter(parameter) => StructuralEquality::hash(parameter),
        })
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Variable(var), Self::Variable(other_var)) => var.equals(other_var),
            (Self::Label(label), Self::Label(other_label)) => label.equals(other_label),
            (Self::Parameter(parameter), Self::Parameter(other_parameter)) => parameter.equals(other_parameter),
            // note: this style forces updating the match when the variants change
            (Self::Variable { .. }, _) | (Self::Parameter { .. }, _) | (Self::Label { .. }, _) => false,
        }
    }
}

impl<ID: fmt::Debug> fmt::Debug for Vertex<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(var) => fmt::Debug::fmt(var, f),
            Self::Label(label) => write!(f, "{}", label.scoped_name().as_str()),
            Self::Parameter(param) => fmt::Debug::fmt(param, f),
        }
    }
}

impl<ID: fmt::Debug> fmt::Display for Vertex<ID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParameterID {
    Value(usize),
    Iid(usize),
    FetchKey(usize),
}

impl StructuralEquality for ParameterID {
    fn hash(&self) -> u64 {
        let id = match *self {
            ParameterID::Value(id) => id,
            ParameterID::Iid(id) => id,
            ParameterID::FetchKey(id) => id,
        };
        StructuralEquality::hash(&id)
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Debug for ParameterID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Param[")?;
        match self {
            ParameterID::Value(id) => write!(f, "Value({id})")?,
            ParameterID::Iid(id) => write!(f, "IID({id})")?,
            ParameterID::FetchKey(id) => write!(f, "FetchKey({id})")?,
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl fmt::Display for ParameterID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ValueType {
    Builtin(encoding::value::value_type::ValueType),
    Struct(String),
}

impl ValueType {
    pub fn as_builtin(&self) -> Option<encoding::value::value_type::ValueType> {
        if let Self::Builtin(v) = self {
            Some(v.clone())
        } else {
            None
        }
    }

    pub fn as_struct(&self) -> Option<&str> {
        if let Self::Struct(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value type is [`Builtin`].
    ///
    /// [`Builtin`]: ValueType::Builtin
    #[must_use]
    pub fn is_builtin(&self) -> bool {
        matches!(self, Self::Builtin(..))
    }

    /// Returns `true` if the value type is [`Struct`].
    ///
    /// [`Struct`]: ValueType::Struct
    #[must_use]
    pub fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(..))
    }
}

impl StructuralEquality for ValueType {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self)).bitxor(match self {
            ValueType::Builtin(value_type) => StructuralEquality::hash(value_type),
            ValueType::Struct(name) => StructuralEquality::hash(name.as_str()),
        })
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Builtin(inner), Self::Builtin(other_inner)) => inner.equals(other_inner),
            (Self::Struct(inner), Self::Struct(other_inner)) => inner.as_str().equals(other_inner.as_str()),
            // note: this style forces updating the match when the variants change
            (Self::Builtin { .. }, _) | (Self::Struct { .. }, _) => false,
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Builtin(var) => fmt::Display::fmt(var, f),
            Self::Struct(name) => fmt::Display::fmt(name, f),
        }
    }
}
