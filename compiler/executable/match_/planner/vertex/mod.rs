/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::{constraint::Comparison, Vertex};
use itertools::chain;

use super::plan::{ConjunctionPlan, Graph, VariableVertexId, VertexId};
use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::planner::{
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
    Variable(VariableVertex),
    Constraint(ConstraintVertex<'a>),

    Comparison(ComparisonPlanner<'a>),
    Expression(()),

    FunctionCall(FunctionCallPlanner),
    Negation(NegationPlanner<'a>),
    Disjunction(DisjunctionPlanner<'a>),
}

impl PlannerVertex<'_> {
    pub(super) fn is_valid(&self, index: VertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        match self {
            Self::Variable(inner) => inner.is_valid(index, ordered, graph),
            Self::Constraint(inner) => inner.is_valid(index, ordered, graph),

            Self::FunctionCall(FunctionCallPlanner { arguments, .. }) => {
                arguments.iter().all(|arg| ordered.contains(arg))
            }

            Self::Comparison(ComparisonPlanner { lhs, rhs, .. }) => {
                if let &Input::Variable(lhs) = lhs {
                    if !ordered.contains(&VertexId::Variable(lhs)) {
                        return false;
                    }
                }
                if let &Input::Variable(rhs) = rhs {
                    if !ordered.contains(&VertexId::Variable(rhs)) {
                        return false;
                    }
                }
                true
            }

            Self::Expression(_) => todo!("validate expression"), // may be invalid: inputs must be bound

            Self::FunctionCall(_) => todo!(),
            Self::Negation(inner) => inner.is_valid(index, ordered, graph),
            Self::Disjunction(inner) => inner.is_valid(index, ordered, graph),
        }
    }

    pub(super) fn variables(&self) -> Box<dyn Iterator<Item = VariableVertexId> + '_> {
        match self {
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

    pub(super) fn as_constraint(&self) -> Option<&ConstraintVertex<'_>> {
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
    fn cost(&self, inputs: &[VertexId], graph: &Graph<'_>) -> ElementCost;
}

impl Costed for PlannerVertex<'_> {
    fn cost(&self, inputs: &[VertexId], graph: &Graph<'_>) -> ElementCost {
        match self {
            Self::Variable(inner) => inner.cost(inputs, graph),
            Self::Constraint(inner) => inner.cost(inputs, graph),

            Self::FunctionCall(inner) => inner.cost(inputs, graph),
            Self::Comparison(inner) => inner.cost(inputs, graph),
            Self::Expression(_) => todo!("expression cost"),

            Self::Negation(inner) => inner.cost(inputs, graph),
            Self::Disjunction(inner) => inner.cost(inputs, graph),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Canonical,
    Reverse,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Input {
    Fixed,
    Variable(VariableVertexId),
}

impl Input {
    pub(super) fn from_vertex(vertex: &Vertex<Variable>, variable_index: &HashMap<Variable, VariableVertexId>) -> Self {
        match vertex {
            Vertex::Variable(var) => Input::Variable(variable_index[var]),
            Vertex::Label(_) | Vertex::Parameter(_) => Input::Fixed,
        }
    }

    pub(super) fn as_variable(self) -> Option<VariableVertexId> {
        match self {
            Self::Variable(v) => Some(v),
            _ => None,
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
pub(crate) struct ComparisonPlanner<'a> {
    comparison: &'a Comparison<Variable>,
    pub lhs: Input,
    pub rhs: Input,
}

impl<'a> ComparisonPlanner<'a> {
    pub(crate) fn from_constraint(
        comparison: &'a Comparison<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        Self {
            comparison,
            lhs: Input::from_vertex(comparison.lhs(), variable_index),
            rhs: Input::from_vertex(comparison.rhs(), variable_index),
        }
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }

    pub(super) fn comparison(&self) -> &Comparison<Variable> {
        self.comparison
    }
}

impl Costed for ComparisonPlanner<'_> {
    fn cost(&self, _: &[VertexId], _: &Graph<'_>) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(super) struct NegationPlanner<'a> {
    plan: ConjunctionPlan<'a>,
}

impl<'a> NegationPlanner<'a> {
    pub(super) fn new(plan: ConjunctionPlan<'a>) -> Self {
        Self { plan }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.variables().all(|var| ordered.contains(&VertexId::Variable(var)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.plan.shared_variables().iter().copied()
    }

    pub(super) fn plan(&self) -> &ConjunctionPlan<'a> {
        &self.plan
    }
}

impl Costed for NegationPlanner<'_> {
    fn cost(&self, _inputs: &[VertexId], _: &Graph<'_>) -> ElementCost {
        self.plan.cost()
    }
}

#[derive(Debug)]
pub(super) struct DisjunctionPlanner<'a> {
    input_variables: Vec<VariableVertexId>,
    shared_variables: Vec<VariableVertexId>,
    branch_builders: Vec<PlanBuilder<'a>>,
}

impl<'a> DisjunctionPlanner<'a> {
    pub(super) fn from_builders(branch_builders: Vec<PlanBuilder<'a>>) -> Self {
        Self { input_variables: Vec::new(), shared_variables: Vec::new(), branch_builders }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.input_variables.iter().all(|&var| ordered.contains(&VertexId::Variable(var)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        chain!(&self.input_variables, &self.shared_variables).copied()
    }
}

impl Costed for DisjunctionPlanner<'_> {
    fn cost(&self, _inputs: &[VertexId], _: &Graph<'_>) -> ElementCost {
        todo!()
    }
}
