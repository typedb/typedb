/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    iter,
};

use answer::{variable::Variable, Type};
use concept::thing::statistics::Statistics;
use ir::pattern::{
    constraint::{Comparison, FunctionCallBinding, Is},
    Vertex,
};
use itertools::chain;

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::planner::{
        plan::{ConjunctionPlan, DisjunctionPlanBuilder, Graph, VariableVertexId, VertexId},
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
#[derive(Clone, Debug)]
pub(super) enum PlannerVertex<'a> {
    Variable(VariableVertex),
    Constraint(ConstraintVertex<'a>),

    Is(IsPlanner<'a>),
    Comparison(ComparisonPlanner<'a>),

    Expression(ExpressionPlanner<'a>),
    FunctionCall(FunctionCallPlanner<'a>),

    Negation(NegationPlanner<'a>),
    Disjunction(DisjunctionPlanner<'a>),
}

impl PlannerVertex<'_> {
    pub(super) fn is_valid(&self, index: VertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        let is_valid = match self {
            Self::Variable(inner) => inner.is_valid(index, ordered, graph),
            Self::Is(inner) => inner.is_valid(index, ordered, graph),
            Self::Constraint(inner) => inner.is_valid(index, ordered, graph),

            Self::Comparison(inner) => inner.is_valid(index, ordered, graph),

            Self::Expression(inner) => inner.is_valid(index, ordered, graph),

            Self::FunctionCall(FunctionCallPlanner { arguments, .. }) => {
                arguments.iter().all(|&arg| ordered.contains(&VertexId::Variable(arg)))
            }

            Self::Negation(inner) => inner.is_valid(index, ordered, graph),
            Self::Disjunction(inner) => inner.is_valid(index, ordered, graph),
        };
        if !is_valid {
            return false;
        }
        let mut ordered = ordered.to_owned();
        ordered.push(index);
        self.variables().all(|var| {
            ordered.contains(&VertexId::Variable(var))
                || graph.elements()[&VertexId::Variable(var)].is_valid(VertexId::Variable(var), &ordered, graph)
        })
    }

    pub(super) fn variables(&self) -> Box<dyn Iterator<Item = VariableVertexId> + '_> {
        match self {
            Self::Variable(_) => Box::new(iter::empty()),
            Self::Is(inner) => Box::new(inner.variables()),
            Self::Constraint(inner) => inner.variables(),
            Self::Comparison(inner) => Box::new(inner.variables()),
            Self::Expression(inner) => Box::new(inner.variables()),
            Self::FunctionCall(inner) => Box::new(inner.variables()),
            Self::Negation(inner) => Box::new(inner.variables()),
            Self::Disjunction(inner) => Box::new(inner.variables()),
        }
    }

    pub(super) fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(_))
    }

    pub(super) fn is_constraint(&self) -> bool {
        matches!(self, Self::Constraint(_))
    }

    pub(super) fn as_variable(&self) -> Option<&VariableVertex> {
        match self {
            Self::Variable(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_variable_mut(&mut self) -> Option<&mut VariableVertex> {
        match self {
            Self::Variable(var) => Some(var),
            _ => None,
        }
    }

    pub(super) fn as_constraint(&self) -> Option<&ConstraintVertex<'_>> {
        match self {
            Self::Constraint(constraint) => Some(constraint),
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
    pub const EMPTY: Self = Self { per_input: 0.0, per_output: 0.0, branching_factor: 0.0 };
    pub const FREE_BRANCH_1: Self = Self { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 };

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

    pub(crate) fn combine_parallel(self, other: Self) -> Self {
        fn weighted_mean((lhs_value, lhs_weight): (f64, f64), (rhs_value, rhs_weight): (f64, f64)) -> f64 {
            (lhs_value * lhs_weight + rhs_value * rhs_weight) / (lhs_weight + rhs_weight)
        }
        Self {
            per_input: self.per_input + other.per_input,
            per_output: weighted_mean(
                (self.per_output, self.branching_factor),
                (other.per_output, other.branching_factor),
            ),
            branching_factor: self.branching_factor + other.branching_factor,
        }
    }
}

pub(super) trait Costed {
    fn cost(
        &self,
        inputs: &[VertexId],
        intersection_variable: Option<VariableVertexId>,
        graph: &Graph<'_>,
    ) -> ElementCost;
}

impl Costed for PlannerVertex<'_> {
    fn cost(&self, inputs: &[VertexId], intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        match self {
            Self::Variable(inner) => inner.cost(inputs, intersection, graph),
            Self::Constraint(inner) => inner.cost(inputs, intersection, graph),

            Self::Is(inner) => inner.cost(inputs, intersection, graph),
            Self::Comparison(inner) => inner.cost(inputs, intersection, graph),

            Self::Expression(inner) => inner.cost(inputs, intersection, graph),
            Self::FunctionCall(inner) => inner.cost(inputs, intersection, graph),

            Self::Negation(inner) => inner.cost(inputs, intersection, graph),
            Self::Disjunction(inner) => inner.cost(inputs, intersection, graph),
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

#[derive(Clone, Debug)]
pub(crate) struct ExpressionPlanner<'a> {
    pub expression: &'a ExecutableExpression<Variable>,
    inputs: Vec<VariableVertexId>,
    pub output: VariableVertexId,
    cost: ElementCost,
}

impl<'a> ExpressionPlanner<'a> {
    pub(crate) fn from_expression(
        expression: &'a ExecutableExpression<Variable>,
        inputs: Vec<VariableVertexId>,
        output: VariableVertexId,
    ) -> Self {
        let cost = ElementCost::FREE_BRANCH_1;
        Self { inputs, output, cost, expression }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.inputs.iter().all(|&input| ordered.contains(&VertexId::Variable(input)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.inputs.iter().chain(iter::once(&self.output)).copied()
    }
}

impl Costed for ExpressionPlanner<'_> {
    fn cost(&self, _inputs: &[VertexId], _intersection: Option<VariableVertexId>, _graph: &Graph<'_>) -> ElementCost {
        self.cost
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FunctionCallPlanner<'a> {
    pub call_binding: &'a FunctionCallBinding<Variable>,
    pub(super) arguments: Vec<VariableVertexId>,
    pub(super) assigned: Vec<VariableVertexId>,
    cost: ElementCost,
}

impl<'a> FunctionCallPlanner<'a> {
    pub(crate) fn from_constraint(
        call_binding: &'a FunctionCallBinding<Variable>,
        arguments: Vec<VariableVertexId>,
        assigned: Vec<VariableVertexId>,
        cost: ElementCost,
    ) -> Self {
        Self { call_binding, arguments, assigned, cost }
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.arguments.iter().chain(self.assigned.iter()).copied()
    }
}

impl<'a> Costed for FunctionCallPlanner<'a> {
    fn cost(&self, _inputs: &[VertexId], _intersection: Option<VariableVertexId>, _graph: &Graph<'_>) -> ElementCost {
        self.cost
    }
}

#[derive(Clone, Debug)]
pub(super) struct IsPlanner<'a> {
    is: &'a Is<Variable>,
    pub lhs: VariableVertexId,
    pub rhs: VariableVertexId,
}

impl<'a> IsPlanner<'a> {
    pub(crate) fn from_constraint(
        is: &'a Is<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let lhs = is.lhs().as_variable().unwrap();
        let rhs = is.rhs().as_variable().unwrap();
        Self { is, lhs: variable_index[&lhs], rhs: variable_index[&rhs] }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        ordered.contains(&VertexId::Variable(self.lhs)) || ordered.contains(&VertexId::Variable(self.rhs))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.lhs, self.rhs].into_iter()
    }

    pub(super) fn is(&self) -> &Is<Variable> {
        self.is
    }
}

impl Costed for IsPlanner<'_> {
    fn cost(&self, _: &[VertexId], _: Option<VariableVertexId>,  _: &Graph<'_>) -> ElementCost {
        ElementCost::FREE
    }
}

#[derive(Clone, Debug)]
pub(super) struct ComparisonPlanner<'a> {
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

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        if let Input::Variable(lhs) = self.lhs {
            if !ordered.contains(&VertexId::Variable(lhs)) {
                return false;
            }
        }
        if let Input::Variable(rhs) = self.rhs {
            if !ordered.contains(&VertexId::Variable(rhs)) {
                return false;
            }
        }
        true
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }

    pub(super) fn comparison(&self) -> &Comparison<Variable> {
        self.comparison
    }
}

impl Costed for ComparisonPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::FREE_BRANCH_1
    }
}

#[derive(Clone, Debug)]
pub(super) struct NegationPlanner<'a> {
    plan: ConjunctionPlan<'a>,
    shared_variables: Vec<VariableVertexId>,
}

impl<'a> NegationPlanner<'a> {
    pub(super) fn new(plan: ConjunctionPlan<'a>, variable_index: &HashMap<Variable, VariableVertexId>) -> Self {
        let shared_variables = plan.shared_variables().iter().map(|v| variable_index[v]).collect();
        Self { plan, shared_variables }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.variables().all(|var| ordered.contains(&VertexId::Variable(var)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.shared_variables.iter().copied()
    }

    pub(super) fn plan(&self) -> &ConjunctionPlan<'a> {
        &self.plan
    }
}

impl Costed for NegationPlanner<'_> {
    fn cost(&self, _inputs: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        self.plan.cost()
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlanner<'a> {
    input_variables: Vec<VariableVertexId>,
    shared_variables: HashSet<VariableVertexId>,
    builder: DisjunctionPlanBuilder<'a>,
}

impl<'a> DisjunctionPlanner<'a> {
    pub(super) fn from_builder(
        builder: DisjunctionPlanBuilder<'a>,
        variable_index: &HashMap<Variable, VariableVertexId>,
    ) -> Self {
        let shared_variables =
            builder.branches().iter().flat_map(|pb| pb.shared_variables()).map(|v| variable_index[v]).collect();
        Self { input_variables: Vec::new(), shared_variables, builder }
    }

    fn is_valid(&self, _index: VertexId, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.input_variables.iter().all(|&var| ordered.contains(&VertexId::Variable(var)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        chain!(&self.input_variables, &self.shared_variables).copied()
    }

    pub(super) fn builder(&self) -> &DisjunctionPlanBuilder<'a> {
        &self.builder
    }
}

impl Costed for DisjunctionPlanner<'_> {
    fn cost(&self, inputs: &[VertexId], _intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        let input_variables =
            inputs.iter().filter_map(|id| graph.elements()[id].as_variable()).map(|var| var.variable());
        self.builder()
            .branches()
            .iter()
            .map(|branch| branch.clone().with_inputs(input_variables.clone()).plan().cost())
            .fold(ElementCost::EMPTY, ElementCost::combine_parallel)
    }
}

pub(super) fn type_count(type_: &Type, statistics: &Statistics) -> u64 {
    match type_ {
        Type::Entity(entity) => *statistics.entity_counts.get(entity).unwrap_or(&0),
        Type::Relation(relation) => *statistics.relation_counts.get(relation).unwrap_or(&0),
        Type::Attribute(attribute) => *statistics.attribute_counts.get(attribute).unwrap_or(&0),
        Type::RoleType(_) => 0,
    }
}
