/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::{Ordering, PartialEq},
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    mem,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor},
};

use answer::variable::Variable;
use encoding::value::label::Label;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{pattern::variable_category::VariableOptionality, pipeline::VariableRegistry};

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
    fn named_visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.visible_referenced_variables().filter(Variable::is_named)
    }

    fn is_variable_visible_referenced(&self, variable: &Variable) -> bool;
    // A referenced variable is "visible" if it's not local to some subpattern.
    // includes all variables from constraints and subpatterns. Does not include stage inputs if unused.
    fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_;

    fn required_inputs(&self) -> impl Iterator<Item = Variable> + '_;

    fn bound_by_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_;

    fn bound_outside_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_;

    fn optionality(&self, variable: &Variable) -> VariableOptionality;

    fn input_optionalities(&self) -> impl Iterator<Item = (Variable, VariableOptionality)> + '_;

    fn bound_optionalities(&self) -> impl Iterator<Item = (Variable, VariableOptionality)> + '_;
}

macro_rules! impl_pattern_from_pattern_variables {
    ($pattern:ty) => {
        impl Pattern for $pattern {
            fn is_variable_visible_referenced(&self, variable: &Variable) -> bool {
                self.pattern_variables.is_variable_visible_referenced(variable)
            }

            fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
                self.pattern_variables.visible_referenced_variables()
            }

            fn required_inputs(&self) -> impl Iterator<Item = Variable> + '_ {
                self.pattern_variables.required_inputs()
            }

            fn bound_outside_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_ {
                self.pattern_variables.bound_outside_try_in_pattern()
            }

            fn bound_by_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_ {
                self.pattern_variables.bound_by_try_in_pattern()
            }

            fn optionality(&self, variable: &Variable) -> crate::pattern::VariableOptionality {
                self.pattern_variables.optionality(&variable)
            }

            fn input_optionalities(
                &self,
            ) -> impl Iterator<Item = (Variable, crate::pattern::VariableOptionality)> + '_ {
                self.pattern_variables.input_optionalities()
            }

            fn bound_optionalities(
                &self,
            ) -> impl Iterator<Item = (Variable, crate::pattern::VariableOptionality)> + '_ {
                self.pattern_variables.bound_optionalities()
            }
        }
    };
}
pub(self) use impl_pattern_from_pattern_variables;

use crate::pattern::{
    conjunction::{ConjunctionBuilder, NestedPatternBuilder},
    constraint::Constraint,
    disjunction::DisjunctionBuilder,
};

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
        if let &Self::Variable(v) = self { Some(v) } else { None }
    }

    pub fn as_label(&self) -> Option<&Label> {
        if let Self::Label(v) = self { Some(v) } else { None }
    }

    pub fn as_parameter(&self) -> Option<&ParameterID> {
        if let Self::Parameter(v) = self { Some(v) } else { None }
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
        if let Self::Builtin(v) = self { Some(v.clone()) } else { None }
    }

    pub fn as_struct(&self) -> Option<&str> {
        if let Self::Struct(v) = self { Some(v) } else { None }
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum PatternVariableMode {
    RequiredInput(VariableOptionality),
    Binding(VariableOptionality),
    BoundByTry,
}

#[derive(Debug, Clone)]
pub(crate) struct PatternVariables(HashMap<Variable, PatternVariableMode>);

impl PatternVariables {
    pub(crate) fn for_block(
        block_binding_modes: HashMap<Variable, BindingMode>,
        input_variables: impl Iterator<Item = (Variable, VariableOptionality)>,
        root_unwrapped_vars: impl Iterator<Item = Variable>,
    ) -> Self {
        let input_modes = input_variables
            .map(|(variable, optionality)| (variable, PatternVariableMode::RequiredInput(optionality)))
            .collect();
        PatternVariables::build(block_binding_modes, &PatternVariables(input_modes), root_unwrapped_vars)
    }

    pub(crate) fn build(
        pattern_modes: HashMap<Variable, BindingMode>,
        parent_pattern_variables: &PatternVariables,
        unwrapped_vars: impl IntoIterator<Item = Variable>,
    ) -> Self {
        let mut pattern_variables: HashMap<Variable, PatternVariableMode> = pattern_modes
            .into_iter()
            .filter_map(|(var, mode)| {
                let mode = if let Some(parent_mode) = parent_pattern_variables.0.get(&var).copied() {
                    match (parent_mode, mode) {
                        (_, BindingMode::Absent) => None?,
                        (PatternVariableMode::RequiredInput(o), _) => PatternVariableMode::RequiredInput(o),
                        (PatternVariableMode::Binding(o), BindingMode::LocallyBindingInChild)
                        | (PatternVariableMode::Binding(o), BindingMode::BoundInTry) => {
                            PatternVariableMode::RequiredInput(o)
                        }
                        (PatternVariableMode::Binding(o), BindingMode::RequirePrebound) => {
                            PatternVariableMode::RequiredInput(o)
                        }
                        (PatternVariableMode::Binding(o1), BindingMode::AlwaysBinding(o2)) => {
                            let o = match (o1, o2) {
                                (VariableOptionality::Optional, _) | (_, PatternVariableOptionality::MaybeNone) => {
                                    VariableOptionality::Optional
                                }
                                (VariableOptionality::Required, PatternVariableOptionality::NotNone) => {
                                    VariableOptionality::Required
                                }
                            };
                            PatternVariableMode::Binding(o)
                        }
                        (PatternVariableMode::BoundByTry, BindingMode::LocallyBindingInChild)
                        | (PatternVariableMode::BoundByTry, BindingMode::RequirePrebound) => {
                            debug_assert!(false, "Unreachable: Illegal optional reuse");
                            PatternVariableMode::RequiredInput(VariableOptionality::Optional)
                        }
                        (PatternVariableMode::BoundByTry, BindingMode::AlwaysBinding(o)) => {
                            // Happens in the transition from optional to inner
                            PatternVariableMode::Binding(o.into())
                        }
                        (PatternVariableMode::BoundByTry, BindingMode::BoundInTry) => {
                            // There's a nested optional even deeper.
                            PatternVariableMode::BoundByTry
                        }
                    }
                } else {
                    match mode {
                        BindingMode::RequirePrebound => {
                            debug_assert!(
                                false,
                                "Unreachable: checked in validate_all_required_variables_can_be_bound"
                            );
                            PatternVariableMode::RequiredInput(VariableOptionality::Required)
                        }
                        BindingMode::BoundInTry => PatternVariableMode::BoundByTry,
                        BindingMode::AlwaysBinding(o) => PatternVariableMode::Binding(o.into()),
                        BindingMode::LocallyBindingInChild => None?,
                        BindingMode::Absent => None?,
                    }
                };
                Some((var, mode))
            })
            .collect();
        unwrapped_vars.into_iter().for_each(|id| {
            if let Some(mode) = pattern_variables.get_mut(&id) {
                match mode {
                    PatternVariableMode::Binding(o) => *o = VariableOptionality::Required,
                    PatternVariableMode::RequiredInput(o) => *o = VariableOptionality::Required,
                    PatternVariableMode::BoundByTry => {
                        debug_assert!(false, "A try-bound variable can't be referenced in the same stage")
                    }
                }
            }
        });
        Self(pattern_variables)
    }

    pub(crate) fn is_variable_visible_referenced(&self, variable: &Variable) -> bool {
        self.0.contains_key(variable)
    }

    pub(crate) fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.0.keys().copied()
    }

    pub(crate) fn required_inputs(&self) -> impl Iterator<Item = Variable> + '_ {
        self.0
            .iter()
            .filter_map(|(v, required)| matches!(required, PatternVariableMode::RequiredInput(_)).then_some(*v))
    }

    pub(crate) fn bound_by_pattern(&self) -> impl Iterator<Item = Variable> + '_ {
        self.0.iter().filter_map(|(v, required)| {
            (matches!(*required, PatternVariableMode::BoundByTry | PatternVariableMode::Binding(_))).then_some(*v)
        })
    }

    pub(crate) fn bound_by_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_ {
        self.0.iter().filter_map(|(v, required)| (*required == PatternVariableMode::BoundByTry).then_some(*v))
    }

    pub(crate) fn bound_outside_try_in_pattern(&self) -> impl Iterator<Item = Variable> + '_ {
        self.0.iter().filter_map(|(v, required)| (matches!(*required, PatternVariableMode::Binding(_))).then_some(*v))
    }

    pub(crate) fn optionality(&self, variable: &Variable) -> VariableOptionality {
        match self.0[variable] {
            PatternVariableMode::Binding(o) | PatternVariableMode::RequiredInput(o) => o,
            PatternVariableMode::BoundByTry => VariableOptionality::Optional,
        }
    }

    pub(crate) fn input_optionalities(&self) -> impl Iterator<Item = (Variable, VariableOptionality)> + '_ {
        self.0.iter().filter_map(|(v, required)| match required {
            PatternVariableMode::RequiredInput(optionality) => Some((*v, *optionality)),
            PatternVariableMode::BoundByTry => None,
            PatternVariableMode::Binding(_) => None,
        })
    }

    pub(crate) fn bound_optionalities(&self) -> impl Iterator<Item = (Variable, VariableOptionality)> + '_ {
        self.0.iter().filter_map(|(v, required)| match required {
            PatternVariableMode::RequiredInput(_) => None,
            PatternVariableMode::BoundByTry => Some((*v, VariableOptionality::Optional)),
            PatternVariableMode::Binding(optionality) => Some((*v, *optionality)),
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PatternVariableOptionality {
    NotNone, // InAtleastOneBranch
    MaybeNone,
}

impl BitOr for PatternVariableOptionality {
    type Output = Self;
    fn bitor(self, other: Self) -> Self {
        match (self, other) {
            (Self::MaybeNone, _) | (_, Self::MaybeNone) => Self::MaybeNone,
            (Self::NotNone, Self::NotNone) => Self::NotNone,
        }
    }
}

impl From<VariableOptionality> for PatternVariableOptionality {
    fn from(optionality: VariableOptionality) -> Self {
        match optionality {
            VariableOptionality::Required => Self::NotNone,
            VariableOptionality::Optional => Self::MaybeNone,
        }
    }
}

impl Into<VariableOptionality> for PatternVariableOptionality {
    fn into(self) -> VariableOptionality {
        match self {
            Self::MaybeNone => VariableOptionality::Optional,
            Self::NotNone => VariableOptionality::Required,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum BindingMode {
    RequirePrebound,
    AlwaysBinding(PatternVariableOptionality),
    LocallyBindingInChild, // Bound in some, but not all branches
    BoundInTry,            // Try blocks, but not assignments. Assignments are AlwaysBinding regardless.
    #[default]
    Absent,
}

impl BindingMode {
    pub(super) fn is_require_prebound(&self) -> bool {
        *self == BindingMode::RequirePrebound
    }

    pub(super) fn is_always_binding(&self) -> bool {
        matches!(self, BindingMode::AlwaysBinding(_))
    }

    pub(super) fn is_locally_binding_in_child(&self) -> bool {
        *self == BindingMode::LocallyBindingInChild
    }

    pub(super) fn is_optionally_binding(&self) -> bool {
        *self == BindingMode::BoundInTry
    }
}

impl BitAnd for BindingMode {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        // We upgrade (Optionally|LocallyBinding) & (Optionally|LocallyBinding) to RequirePrebound
        match (self, rhs) {
            (Self::Absent, x) | (x, Self::Absent) => x,
            (Self::AlwaysBinding(a), Self::AlwaysBinding(b)) => Self::AlwaysBinding(a | b), // Yes, or.
            (Self::AlwaysBinding(a), _) | (_, Self::AlwaysBinding(a)) => Self::AlwaysBinding(a),
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => Self::RequirePrebound,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => Self::RequirePrebound,
            (Self::BoundInTry, Self::BoundInTry) => Self::RequirePrebound,
        }
    }
}

impl BitOr for BindingMode {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::BoundInTry, Self::BoundInTry) => Self::BoundInTry,
            (Self::AlwaysBinding(a), Self::AlwaysBinding(b)) => Self::AlwaysBinding(a | b),
            (Self::Absent, Self::Absent) => Self::Absent,
            (Self::Absent, Self::AlwaysBinding(_)) | (Self::AlwaysBinding(_), Self::Absent) => {
                Self::LocallyBindingInChild
            }
            (Self::Absent, Self::LocallyBindingInChild) | (Self::LocallyBindingInChild, Self::Absent) => {
                Self::LocallyBindingInChild
            }
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => Self::RequirePrebound,
            (Self::BoundInTry, _) | (_, Self::BoundInTry) => Self::RequirePrebound,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => {
                // This preserves associativity, but doesn't correctly escalate to RequirePrebound.
                // ((AlwaysBinding | AlwaysBinding) | Absent) should be required
                // That's corrected in disjunction
                Self::LocallyBindingInChild
            }
        }
    }
}

impl BitAndAssign for BindingMode {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

impl BitOrAssign for BindingMode {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

pub(crate) type LocationNote = Option<Span>;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum AssignmentStatus {
    #[default]
    NotAssigned,
    AtMostOncePerBranch(LocationNote),
    ErrorMultipleAssignments(LocationNote, LocationNote),
}

impl AssignmentStatus {
    pub(crate) fn for_conjunction(conjunction: &ConjunctionBuilder) -> HashMap<Variable, AssignmentStatus> {
        let mut assignment_statuses = HashMap::new();
        conjunction.constraints().iter().for_each(|constraint| {
            let assigned_ids = match constraint {
                Constraint::FunctionCallBinding(binding) => binding.ids_assigned().for_each(|id| {
                    *assignment_statuses.entry(id).or_default() &=
                        AssignmentStatus::AtMostOncePerBranch(constraint.source_span());
                }),
                Constraint::ExpressionBinding(binding) => binding.ids_assigned().for_each(|id| {
                    *assignment_statuses.entry(id).or_default() &=
                        AssignmentStatus::AtMostOncePerBranch(constraint.source_span());
                }),
                _ => return,
            };
        });
        for nested in conjunction.nested_patterns() {
            let nested_statuses = match nested {
                NestedPatternBuilder::Negation(negation) => Self::for_conjunction(negation.conjunction()),
                NestedPatternBuilder::Optional(optional) => Self::for_conjunction(optional.conjunction()),
                NestedPatternBuilder::Disjunction(disjunction) => Self::for_disjunction(disjunction),
            };
            for (id, status) in nested_statuses {
                *assignment_statuses.entry(id).or_default() &= status;
            }
        }
        assignment_statuses
    }

    pub(crate) fn for_disjunction(disjunction: &DisjunctionBuilder) -> HashMap<Variable, AssignmentStatus> {
        let mut assignment_statuses = HashMap::new();
        for conjunction in disjunction.conjunctions() {
            for (id, status) in Self::for_conjunction(conjunction) {
                *assignment_statuses.entry(id).or_default() |= status;
            }
        }
        assignment_statuses
    }
}

impl BitAnd for AssignmentStatus {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
            (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
                Self::ErrorMultipleAssignments(s1, s2)
            }
            (Self::AtMostOncePerBranch(s1), Self::AtMostOncePerBranch(s2)) => Self::ErrorMultipleAssignments(s1, s2),
        }
    }
}

impl BitOr for AssignmentStatus {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
            (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
                Self::ErrorMultipleAssignments(s1, s2)
            }
            (Self::AtMostOncePerBranch(s), Self::AtMostOncePerBranch(_)) => Self::AtMostOncePerBranch(s),
        }
    }
}

impl BitAndAssign for AssignmentStatus {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

impl BitOrAssign for AssignmentStatus {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

#[derive(Clone, Debug, Copy)]
pub struct AssignedVariable {
    pub(crate) variable: Variable,
    pub(crate) optionality: VariableOptionality,
}

impl AssignedVariable {
    pub fn new_optional(variable: Variable) -> Self {
        Self { variable, optionality: VariableOptionality::Optional }
    }

    pub fn new_required(variable: Variable) -> Self {
        Self { variable, optionality: VariableOptionality::Required }
    }
}
