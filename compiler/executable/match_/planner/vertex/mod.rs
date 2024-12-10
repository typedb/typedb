/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::{HashMap, HashSet}, fmt, iter};
use std::fmt::Formatter;
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

pub(super) const OPEN_ITERATOR_RELATIVE_COST: f64 = 5.0;
pub(super) const ADVANCE_ITERATOR_RELATIVE_COST: f64 = 1.0;

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
    pub(super) fn is_valid(&self, id: VertexId, vertex_plan: &[VertexId], graph: &Graph<'_>) -> bool {
        for var in self.variables().filter(|&var| !vertex_plan.contains(&VertexId::Variable(var))) {
            let variable_vertex = graph.elements()[&VertexId::Variable(var)].as_variable().unwrap();
            if variable_vertex.binding().is_some_and(|binding| VertexId::Pattern(binding) != id) {
                return false;
            }
        }

        match self {
            Self::Variable(_) => false,
            Self::Constraint(inner) => inner.is_valid(vertex_plan, graph),
            Self::Is(inner) => inner.is_valid(vertex_plan, graph),
            Self::Comparison(inner) => inner.is_valid(vertex_plan, graph),
            Self::Expression(inner) => inner.is_valid(vertex_plan, graph),
            Self::FunctionCall(FunctionCallPlanner { arguments, .. }) => {
                arguments.iter().all(|&arg| vertex_plan.contains(&VertexId::Variable(arg)))
            }
            Self::Negation(inner) => inner.is_valid(vertex_plan, graph),
            Self::Disjunction(inner) => inner.is_valid(vertex_plan, graph),
        }
    }

    pub(super) fn variables(&self) -> Box<dyn Iterator<Item = VariableVertexId> + '_> {
        match self {
            Self::Variable(_) => Box::new(iter::empty()),
            Self::Constraint(inner) => inner.variables(),
            Self::Is(inner) => Box::new(inner.variables()),
            Self::Comparison(inner) => Box::new(inner.variables()),
            Self::Expression(inner) => Box::new(inner.variables()),
            Self::FunctionCall(inner) => Box::new(inner.variables()),
            Self::Negation(inner) => Box::new(inner.variables()),
            Self::Disjunction(inner) => Box::new(inner.variables()),
        }
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Cost {
    pub cost: f64, // per input
    pub io_ratio: f64,
}

impl<'a> fmt::Display for PlannerVertex<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            PlannerVertex::Variable(v) => { write!(f, "|Var {}|", v.variable())}
            PlannerVertex::Constraint(v) => { write!(f, "{}", v) }
            PlannerVertex::Is(_) => { write!(f, "|Is|") } //TODO
            PlannerVertex::Comparison(v) => { write!(f, "|{:?} {:?} {:?}|", v.lhs, v.comparison, v.rhs) }
            PlannerVertex::Expression(v) => { write!(f, "|Expr of {:?}|", v.expression.variables) }
            PlannerVertex::FunctionCall(_) => { write!(f, "|Fun Call|") } //TODO
            PlannerVertex::Negation(_) => { write!(f, "|Negation|") } //TODO
            PlannerVertex::Disjunction(_) => { write!(f, "|Disjunction|") } //TODO
        }
    }
}

impl Cost {
    const MIN_IO_RATIO: f64 = 0.000000001;
    const IN_MEM_COST_SIMPLE: f64 = 0.02;
    const IN_MEM_COST_COMPLEX: f64 = Cost::IN_MEM_COST_SIMPLE * 2.0;
    pub const NOOP: Self = Self { cost: 0.0, io_ratio: 1.0 };
    pub const EMPTY: Self = Self { cost: 0.0, io_ratio: 0.0 };
    pub const INFINITY: Self = Self { cost: f64::INFINITY, io_ratio: 0.0 };
    pub const MEM_SIMPLE_BRANCH_1: Self = Self { cost: Cost::IN_MEM_COST_SIMPLE, io_ratio: 1.0 };
    pub const MEM_COMPLEX_BRANCH_1: Self = Self { cost: Cost::IN_MEM_COST_COMPLEX, io_ratio: 1.0 };

    fn in_mem_complex_with_ratio(io_ratio: f64) -> Self {
        Self { cost: Cost::IN_MEM_COST_COMPLEX, io_ratio }
    }

    fn in_mem_simple_with_ratio(io_ratio: f64) -> Self {
        Self { cost: Cost::IN_MEM_COST_SIMPLE, io_ratio }
    }

    pub(crate) fn chain(self, other: Self) -> Self {
        Self {
            cost: self.cost + other.cost * self.io_ratio,
            io_ratio: f64::max(self.io_ratio * other.io_ratio, Cost::MIN_IO_RATIO),
        }
    }

    pub(crate) fn join(self, other: Self, join_size: f64) -> Self {
        Self {
            cost: self.cost + other.cost, // Cost is additive, both scans are performed separately // TODO: fix cartesian product situation in Rocks
            io_ratio: f64::max(self.io_ratio * other.io_ratio / join_size, Cost::MIN_IO_RATIO), // Probabilty of join = 1 / total_join_size
        }
    }

    pub(crate) fn combine_parallel(self, other: Self) -> Self {
        Self { cost: self.cost + other.cost, io_ratio: self.io_ratio + other.io_ratio }
    }
}

pub(super) trait Costed {
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData);
}

impl Costed for PlannerVertex<'_> {
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        match self {
            Self::Variable(_) => (Cost::NOOP, CostMetaData::None),
            Self::Constraint(vertex) => vertex.cost_and_metadata(vertex_ordering, graph),

            Self::Is(planner) => planner.cost_and_metadata(vertex_ordering, graph),
            Self::Comparison(planner) => planner.cost_and_metadata(vertex_ordering, graph),

            Self::Expression(planner) => planner.cost_and_metadata(vertex_ordering, graph),
            Self::FunctionCall(planner) => planner.cost_and_metadata(vertex_ordering, graph),

            Self::Negation(planner) => planner.cost_and_metadata(vertex_ordering, graph),
            Self::Disjunction(planner) => planner.cost_and_metadata(vertex_ordering, graph),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostMetaData {
    Direction(Direction), // Cheapest direction of individual constraints
    // Pushdown(Pushdown), // Pushdown constraints from function calls if they are very selective
    // Split(Split), // Split negation into disjunctions if one part expensive and low selectivity
    // Sort(Binding), // Produce sorted iterator for var with binding (easy e.g. for monotone functions)
    None,
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
    cost: Cost,
}

impl<'a> ExpressionPlanner<'a> {
    pub(crate) fn from_expression(
        expression: &'a ExecutableExpression<Variable>,
        inputs: Vec<VariableVertexId>,
        output: VariableVertexId,
    ) -> Self {
        let cost = Cost::MEM_COMPLEX_BRANCH_1;
        Self { inputs, output, cost, expression }
    }

    fn is_valid(&self, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
        self.inputs.iter().all(|&input| ordered.contains(&VertexId::Variable(input)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.inputs.iter().chain(iter::once(&self.output)).copied()
    }
}

impl Costed for ExpressionPlanner<'_> {
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (self.cost, CostMetaData::None)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FunctionCallPlanner<'a> {
    pub call_binding: &'a FunctionCallBinding<Variable>,
    pub(super) arguments: Vec<VariableVertexId>,
    pub(super) assigned: Vec<VariableVertexId>,
    cost: Cost,
}

impl<'a> FunctionCallPlanner<'a> {
    pub(crate) fn from_constraint(
        call_binding: &'a FunctionCallBinding<Variable>,
        arguments: Vec<VariableVertexId>,
        assigned: Vec<VariableVertexId>,
        cost: Cost,
    ) -> Self {
        Self { call_binding, arguments, assigned, cost }
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.arguments.iter().chain(self.assigned.iter()).copied()
    }
}

impl Costed for FunctionCallPlanner<'_> {
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (self.cost, CostMetaData::None)
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

    fn is_valid(&self, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
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
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::MEM_COMPLEX_BRANCH_1, CostMetaData::None)
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

    fn is_valid(&self, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
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
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::MEM_COMPLEX_BRANCH_1, CostMetaData::None)
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

    fn is_valid(&self, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
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
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (self.plan.cost(), CostMetaData::None)
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

    fn is_valid(&self, ordered: &[VertexId], _graph: &Graph<'_>) -> bool {
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
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let input_variables =
            vertex_ordering.iter().filter_map(|id| graph.elements()[id].as_variable()).map(|var| var.variable());
        let cost = self
            .builder()
            .branches()
            .iter()
            .map(|branch| branch.clone().with_inputs(input_variables.clone()).plan().cost())
            .fold(Cost::EMPTY, |acc_cost, cost| acc_cost.combine_parallel(cost));
        (cost, CostMetaData::None)
    }
}

pub(super) fn instance_count(type_: &Type, statistics: &Statistics) -> u64 {
    match type_ {
        Type::Entity(entity) => *statistics.entity_counts.get(entity).unwrap_or(&0),
        Type::Relation(relation) => *statistics.relation_counts.get(relation).unwrap_or(&0),
        Type::Attribute(attribute) => *statistics.attribute_counts.get(attribute).unwrap_or(&0),
        Type::RoleType(_) => unreachable!("Cannot count role instances"),
    }
}
