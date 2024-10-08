/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    iter,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::{constraint::Comparison, Vertex};
use itertools::chain;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    match_::planner::{
        plan::PlanBuilder,
        vertex::{constraint::ConstraintVertex, variable::VariableVertex},
    },
};

pub(super) mod constraint;
pub(super) mod variable;

const OPEN_ITERATOR_RELATIVE_COST: f64 = 5.0;
const ADVANCE_ITERATOR_RELATIVE_COST: f64 = 1.0;

const _REGEX_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;
const _CONTAINS_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;

// FIXME name
#[derive(Debug)]
pub(super) enum PlannerVertex<'a> {
    Fixed(FixedVertex),
    Variable(VariableVertex),
    Constraint(ConstraintVertex),

    Comparison(ComparisonPlanner),
    Expression(()),

    FunctionCall(FunctionCallPlanner),
    Negation(NegationPlanner),
    Disjunction(DisjunctionPlanner<'a>),
}

impl PlannerVertex<'_> {
    pub(super) fn is_valid(&self, index: usize, ordered: &[usize], adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        match self {
            Self::Fixed(_) => true, // always valid: comes from query
            Self::Variable(inner) => inner.is_valid(index, ordered, adjacency),
            Self::Constraint(inner) => inner.is_valid(index, ordered, adjacency),

            Self::FunctionCall(FunctionCallPlanner { arguments, .. }) => {
                arguments.iter().all(|arg| ordered.contains(arg))
            }

            Self::Comparison(ComparisonPlanner { lhs, rhs }) => {
                if let Input::Variable(lhs) = lhs {
                    if !ordered.contains(lhs) {
                        return false;
                    }
                }
                if let Input::Variable(rhs) = rhs {
                    if !ordered.contains(rhs) {
                        return false;
                    }
                }
                true
            }

            Self::Expression(_) => todo!("validate expression"), // may be invalid: inputs must be bound

            Self::FunctionCall(_) => todo!(),
            Self::Negation(inner) => inner.is_valid(index, ordered, adjacency),
            Self::Disjunction(inner) => inner.is_valid(index, ordered, adjacency),
        }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Fixed(inner) => inner.variables(),
            Self::Variable(_) => Box::new(iter::empty()),
            Self::Constraint(inner) => inner.variables(),
            Self::FunctionCall(inner) => Box::new(inner.variables()),
            Self::Comparison(inner) => Box::new(inner.variables()),
            Self::Expression(_inner) => todo!(),
            Self::Negation(inner) => Box::new(inner.variables()),
            Self::Disjunction(inner) => Box::new(inner.variables()),
        }
    }

    /// Returns `true` if the planner vertex is [`Value`].
    ///
    /// [`Value`]: PlannerVertex::Value
    #[must_use]
    pub(crate) fn is_value(&self) -> bool {
        self.as_variable().is_some_and(VariableVertex::is_value)
    }

    /// Returns `true` if the planner vertex is [`Input`].
    ///
    /// [`Input`]: PlannerVertex::Input
    #[must_use]
    pub(crate) fn is_input(&self) -> bool {
        self.as_variable().is_some_and(VariableVertex::is_input)
    }

    pub(super) fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(_))
    }

    pub(super) fn as_variable(&self) -> Option<&VariableVertex> {
        match self {
            Self::Variable(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_variable_mut(&mut self) -> Option<&mut VariableVertex> {
        match self {
            Self::Variable(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_constraint(&self) -> Option<&ConstraintVertex> {
        match self {
            Self::Constraint(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ElementCost {
    pub per_input: f64,
    pub per_output: f64,
    pub branching_factor: f64,
}

impl ElementCost {
    fn free_with_branching(branching_factor: f64) -> Self {
        Self { per_input: 0.0, per_output: 0.0, branching_factor }
    }

    pub(crate) fn chain(self, other: Self) -> Self {
        Self {
            per_input: self.per_input + other.per_input * self.branching_factor,
            per_output: self.per_output / other.branching_factor + other.per_output,
            branching_factor: self.branching_factor * other.branching_factor,
        }
    }
}

impl Default for ElementCost {
    fn default() -> Self {
        Self { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

pub(super) trait Costed {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost;
}

impl Costed for PlannerVertex<'_> {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        match self {
            Self::Fixed(inner) => inner.cost(inputs, elements),
            Self::Variable(inner) => inner.cost(inputs, elements),
            Self::Constraint(inner) => inner.cost(inputs, elements),

            Self::FunctionCall(inner) => inner.cost(inputs, elements),
            Self::Comparison(inner) => inner.cost(inputs, elements),
            Self::Expression(_) => todo!("expression cost"),

            Self::Negation(inner) => inner.cost(inputs, elements),
            Self::Disjunction(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Canonical,
    Reverse,
}

#[derive(Debug)]
pub(super) enum FixedVertex {
    Constant,
    TypeList(TypeListPlanner),
}

impl FixedVertex {
    fn variables(&self) -> Box<dyn Iterator<Item = usize>> {
        match self {
            FixedVertex::Constant => Box::new(iter::empty()),
            FixedVertex::TypeList(inner) => Box::new(inner.variables()),
        }
    }
}

impl Costed for FixedVertex {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        match self {
            FixedVertex::Constant => ElementCost::default(),
            FixedVertex::TypeList(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Debug)]
pub(super) struct TypeListPlanner {
    var: usize,
    num_types: f64,
}

impl TypeListPlanner {
    fn variables(&self) -> impl Iterator<Item = usize> {
        iter::once(self.var)
    }

    pub(crate) fn from_label_constraint(
        label: &ir::pattern::constraint::Label<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
    ) -> TypeListPlanner {
        let num_types = type_annotations.vertex_annotations_of(label.type_()).map(|annos| annos.len()).unwrap_or(0);
        Self { var: variable_index[&label.type_().as_variable().unwrap()], num_types: num_types as f64 }
    }

    pub(crate) fn from_role_name_constraint(
        role_name: &ir::pattern::constraint::RoleName<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
    ) -> TypeListPlanner {
        let num_types = type_annotations.vertex_annotations_of(role_name.type_()).map(|annos| annos.len()).unwrap_or(0);
        Self { var: variable_index[&role_name.type_().as_variable().unwrap()], num_types: num_types as f64 }
    }

    pub(crate) fn from_kind_constraint(
        kind: &ir::pattern::constraint::Kind<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
    ) -> TypeListPlanner {
        let num_types = type_annotations.vertex_annotations_of(kind.type_()).map(|annos| annos.len()).unwrap_or(0);
        Self { var: variable_index[&kind.type_().as_variable().unwrap()], num_types: num_types as f64 }
    }
}

impl Costed for TypeListPlanner {
    fn cost(&self, _: &[usize], _: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost::free_with_branching(self.num_types)
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Input {
    Fixed,
    Variable(usize),
}

impl Input {
    pub(crate) fn from_vertex(vertex: &Vertex<Variable>, variable_index: &HashMap<Variable, usize>) -> Self {
        match vertex {
            Vertex::Variable(var) => Input::Variable(variable_index[var]),
            Vertex::Label(_) | Vertex::Parameter(_) => Input::Fixed,
        }
    }

    pub(crate) fn as_variable(self) -> Option<usize> {
        if let Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(crate) struct FunctionCallPlanner {
    arguments: Vec<usize>,
    assigned: Vec<usize>,
    cost: ElementCost,
}

impl FunctionCallPlanner {
    pub(crate) fn new(arguments: Vec<usize>, assigned: Vec<usize>, cost: ElementCost) -> Self {
        Self { arguments, assigned, cost }
    }

    fn variables(&self) -> impl Iterator<Item = usize> + '_ {
        self.arguments.iter().chain(self.assigned.iter()).copied()
    }
}

impl Costed for FunctionCallPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        self.cost
    }
}

#[derive(Debug)]
pub(crate) struct ComparisonPlanner {
    pub lhs: Input,
    pub rhs: Input,
}

impl ComparisonPlanner {
    pub(crate) fn from_constraint(
        constraint: &Comparison<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        Self {
            lhs: Input::from_vertex(constraint.lhs(), variable_index),
            rhs: Input::from_vertex(constraint.rhs(), variable_index),
        }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }
}

impl Costed for ComparisonPlanner {
    fn cost(&self, _: &[usize], _: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(super) struct NegationPlanner {
    variables: Vec<usize>,
    cost: ElementCost,
}

impl NegationPlanner {
    pub(super) fn new(variables: Vec<usize>, cost: ElementCost) -> Self {
        Self { variables, cost }
    }

    fn is_valid(&self, _index: usize, ordered: &[usize], _adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        self.variables().all(|var| ordered.contains(&var))
    }

    fn variables(&self) -> impl Iterator<Item = usize> + '_ {
        self.variables.iter().copied()
    }
}

impl Costed for NegationPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        // TODO cost can be adjusted for disjunctions
        self.cost
    }
}

#[derive(Debug)]
pub(super) struct DisjunctionPlanner<'a> {
    input_variables: Vec<usize>,
    shared_variables: Vec<usize>,
    branch_builders: Vec<PlanBuilder<'a>>,
}

impl<'a> DisjunctionPlanner<'a> {
    pub(super) fn from_builders(branch_builders: Vec<PlanBuilder<'a>>) -> Self {
        Self { input_variables: Vec::new(), shared_variables: Vec::new(), branch_builders }
    }

    fn is_valid(&self, _index: usize, ordered: &[usize], _adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        self.variables().all(|var| ordered.contains(&var))
    }

    fn variables(&self) -> impl Iterator<Item = usize> + '_ {
        chain!(&self.input_variables, &self.shared_variables).copied()
    }
}

impl Costed for DisjunctionPlanner<'_> {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        todo!()
    }
}
