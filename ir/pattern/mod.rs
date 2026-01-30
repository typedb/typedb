/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::{Ordering, PartialEq},
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    mem,
    ops::{BitAndAssign, BitOrAssign, BitXor},
};

use answer::variable::Variable;
use constraint::Constraint;
use encoding::value::label::Label;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::pipeline::{block::BlockContext, VariableRegistry};

pub mod conjunction;
pub mod constraint;
pub mod negation;
pub mod optional;
pub mod variable_category;

pub mod disjunction;
pub mod expression;
pub mod function_call;
pub mod nested_pattern;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct BranchID(pub u16);

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
    fn map<T: Clone>(&self, mapping: &HashMap<Self, T>) -> T {
        mapping.get(self).unwrap().clone()
    }
}

impl IrID for Variable {}

pub trait Pattern {
    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_;

    fn required_inputs<'a>(&'a self, _block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| mode.is_require_prebound().then_some(v))
    }

    fn named_visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.visible_binding_variables(block_context).filter(Variable::is_named)
    }

    fn visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes()
            .into_iter()
            .filter_map(|(v, mode)| (mode.is_always_binding() || mode.is_optionally_binding()).then_some(v))
    }

    fn variable_binding_modes(&self) -> HashMap<Variable, VariableBindingMode<'_>>;
}

// TODO: rename to 'Identifier' in lieu of a better name
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Vertex<ID> {
    Variable(ID),
    Label(Label),
    Parameter(ParameterID),
}

impl<ID: Hash + Eq> Vertex<ID> {
    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Vertex<T> {
        match self {
            Self::Variable(var) => Vertex::Variable(mapping[&var].clone()),
            Self::Label(label) => Vertex::Label(label),
            Self::Parameter(param) => Vertex::Parameter(param),
        }
    }
}

impl<ID: IrID> Vertex<ID> {
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

    pub fn as_parameter(&self) -> Option<&ParameterID> {
        if let Self::Parameter(v) = self {
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

impl Vertex<Variable> {
    pub fn source_span(&self, variable_registry: &VariableRegistry) -> Option<Span> {
        match self {
            Vertex::Variable(id) => variable_registry.source_span(*id),
            Vertex::Label(label) => label.source_span(),
            Vertex::Parameter(param) => Some(param.source_span()),
        }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone)]
pub enum ParameterID {
    Value(usize, ValueType, Span),
    Iid(usize, Span),
    FetchKey(usize, Span),
}

impl ParameterID {
    fn source_span(&self) -> Span {
        match self {
            ParameterID::Value(_, _, span) | ParameterID::Iid(_, span) | ParameterID::FetchKey(_, span) => *span,
        }
    }
}

impl Eq for ParameterID {}

impl PartialEq for ParameterID {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Value(v1, ty1, _), Self::Value(v2, ty2, _)) => (v1, ty1) == (v2, ty2),
            (Self::Iid(v1, _), Self::Iid(v2, _)) => v1 == v2,
            (Self::FetchKey(v1, _), Self::FetchKey(v2, _)) => v1 == v2,
            (_, _) => false,
        }
    }
}

impl PartialOrd for ParameterID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for ParameterID {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            ParameterID::Value(v1, ty1, _) => match other {
                ParameterID::Value(v2, ty2, _) => v1.cmp(v2).then(ty1.cmp(ty2)),
                ParameterID::Iid(_, _) | ParameterID::FetchKey(_, _) => Ordering::Less,
            },
            ParameterID::Iid(v1, _) => match other {
                ParameterID::Value { .. } => Ordering::Greater,
                ParameterID::Iid(v2, _) => v1.cmp(v2),
                ParameterID::FetchKey(_, _) => Ordering::Less,
            },
            ParameterID::FetchKey(v1, _) => match other {
                ParameterID::Value { .. } | ParameterID::Iid(_, _) => Ordering::Greater,
                ParameterID::FetchKey(v2, _) => v1.cmp(v2),
            },
        }
    }
}

impl Hash for ParameterID {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ParameterID::Value(v, ty, _) => {
                state.write_u64(*v as u64);
                Hash::hash(&ty, state);
            }
            ParameterID::Iid(v, _) => state.write_u64(*v as u64),
            ParameterID::FetchKey(v, _) => state.write_u64(*v as u64),
        }
    }
}

impl StructuralEquality for ParameterID {
    fn hash(&self) -> u64 {
        match self {
            ParameterID::Value(id, ty, _) => StructuralEquality::hash(&(id, ty)),
            ParameterID::Iid(id, _) => StructuralEquality::hash(id),
            ParameterID::FetchKey(id, _) => StructuralEquality::hash(id),
        }
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Debug for ParameterID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Param[")?;
        match self {
            ParameterID::Value(id, ty, _) => write!(f, "Value({ty}:{id})")?,
            ParameterID::Iid(id, _) => write!(f, "IID({id})")?,
            ParameterID::FetchKey(id, _) => write!(f, "FetchKey({id})")?,
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl fmt::Display for ParameterID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl PartialOrd for ValueType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for ValueType {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ValueType::Builtin(_), ValueType::Struct(_)) => Ordering::Less,
            (ValueType::Builtin(lhs), ValueType::Builtin(rhs)) => lhs.cmp(rhs),
            (ValueType::Struct(_), ValueType::Builtin(_)) => Ordering::Greater,
            (ValueType::Struct(lhs), ValueType::Struct(rhs)) => lhs.cmp(rhs),
        }
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

// TODO: consider if this makes Scopes entirely redundant
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum BindingMode {
    RequirePrebound,
    AlwaysBinding,
    LocallyBindingInChild,
    OptionallyBinding,
}

impl BitAndAssign for BindingMode {
    fn bitand_assign(&mut self, rhs: Self) {
        match (*self, rhs) {
            (Self::AlwaysBinding, _) | (_, Self::AlwaysBinding) => *self = Self::AlwaysBinding,
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => *self = Self::RequirePrebound,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => *self = Self::LocallyBindingInChild,
            (Self::OptionallyBinding, Self::OptionallyBinding) => (),
        }
    }
}

impl BitOrAssign for BindingMode {
    fn bitor_assign(&mut self, rhs: Self) {
        match (*self, rhs) {
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => *self = Self::RequirePrebound,
            (Self::OptionallyBinding, _) | (_, Self::OptionallyBinding) => *self = Self::OptionallyBinding,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => *self = Self::LocallyBindingInChild,
            (Self::AlwaysBinding, Self::AlwaysBinding) => (),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VariableBindingMode<'a> {
    mode: BindingMode,
    referencing_constraints: Vec<&'a Constraint<Variable>>,
}

impl<'a> VariableBindingMode<'a> {
    pub fn require_prebound(constraint: &'a Constraint<Variable>) -> Self {
        Self { mode: BindingMode::RequirePrebound, referencing_constraints: vec![constraint] }
    }

    pub fn always_binding(constraint: &'a Constraint<Variable>) -> Self {
        Self { mode: BindingMode::AlwaysBinding, referencing_constraints: vec![constraint] }
    }

    pub fn set_require_prebound(&mut self) {
        self.mode = BindingMode::RequirePrebound;
    }

    pub fn set_locally_binding_in_child(&mut self) {
        self.mode = BindingMode::LocallyBindingInChild;
    }

    pub fn set_optionally_binding(&mut self) {
        self.mode = BindingMode::OptionallyBinding;
    }

    pub fn is_require_prebound(&self) -> bool {
        self.mode == BindingMode::RequirePrebound
    }

    pub fn is_always_binding(&self) -> bool {
        self.mode == BindingMode::AlwaysBinding
    }

    pub fn is_locally_binding_in_child(&self) -> bool {
        self.mode == BindingMode::LocallyBindingInChild
    }

    pub fn is_optionally_binding(&self) -> bool {
        self.mode == BindingMode::OptionallyBinding
    }

    pub fn referencing_constraints(&self) -> &[&Constraint<Variable>] {
        &self.referencing_constraints
    }
}

impl BitAndAssign for VariableBindingMode<'_> {
    fn bitand_assign(&mut self, rhs: Self) {
        self.referencing_constraints.extend_from_slice(&rhs.referencing_constraints);
        self.mode &= rhs.mode;
    }
}

impl BitOrAssign for VariableBindingMode<'_> {
    fn bitor_assign(&mut self, rhs: Self) {
        self.referencing_constraints.extend_from_slice(&rhs.referencing_constraints);
        self.mode |= rhs.mode;
    }
}
