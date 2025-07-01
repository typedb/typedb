/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt, iter,
};

use answer::{variable::Variable, Type};
use concept::thing::statistics::Statistics;
use ir::pattern::{
    constraint::{Comparison, FunctionCallBinding, Is, LinksDeduplication, Unsatisfiable},
    Vertex,
};
use itertools::chain;

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::planner::{
        plan::{
            ConjunctionPlan, DisjunctionPlan, DisjunctionPlanBuilder, Graph, OptionalPlan, QueryPlanningError,
            VariableVertexId, VertexId,
        },
        vertex::{constraint::ConstraintVertex, variable::VariableVertex},
    },
};

pub(super) mod constraint;
pub(super) mod variable;

pub(super) const OPEN_ITERATOR_RELATIVE_COST: f64 = 5.0;
pub(super) const SEEK_ITERATOR_RELATIVE_COST: f64 = 5.0;
pub(super) const ADVANCE_ITERATOR_RELATIVE_COST: f64 = 1.0;

const _REGEX_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;
const _CONTAINS_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;

#[derive(Clone, Debug)]
pub(super) enum PlannerVertex<'a> {
    Variable(VariableVertex),
    Constraint(ConstraintVertex<'a>),

    Is(IsPlanner<'a>),
    LinksDeduplication(LinksDeduplicationPlanner<'a>),
    Comparison(ComparisonPlanner<'a>),
    Unsatisfiable(UnsatisfiablePlanner<'a>),

    Expression(ExpressionPlanner<'a>),
    FunctionCall(FunctionCallPlanner<'a>),

    Negation(NestedNegationPlan<'a>),
    Disjunction(NestedDisjunctionPlanner<'a>),
    Optional(NestedOptionalPlan<'a>),
}

impl PlannerVertex<'_> {
    pub(super) fn is_valid(&self, _id: VertexId, vertex_plan: &[VertexId]) -> bool {
        match self {
            Self::Variable(_) => false,
            Self::Constraint(inner) => inner.is_valid(vertex_plan),
            Self::Is(inner) => inner.is_valid(vertex_plan),
            Self::LinksDeduplication(inner) => inner.is_valid(vertex_plan),
            Self::Comparison(inner) => inner.is_valid(vertex_plan),
            Self::Expression(inner) => inner.is_valid(vertex_plan),
            Self::FunctionCall(inner) => inner.is_valid(vertex_plan),
            Self::Negation(inner) => inner.is_valid(vertex_plan),
            Self::Disjunction(inner) => inner.is_valid(vertex_plan),
            Self::Optional(inner) => inner.is_valid(vertex_plan),
            Self::Unsatisfiable(inner) => inner.is_valid(vertex_plan),
        }
    }

    pub(super) fn visible_variable_vertex_ids<'a>(&'a self) -> Box<dyn Iterator<Item = VariableVertexId> + 'a> {
        match self {
            Self::Variable(_) => Box::new(iter::empty()),
            Self::Constraint(inner) => Box::new(inner.variables()),
            Self::Is(inner) => Box::new(inner.variables()),
            Self::LinksDeduplication(inner) => Box::new(inner.variables()),
            Self::Comparison(inner) => Box::new(inner.variables()),
            Self::Expression(inner) => Box::new(inner.variables()),
            Self::FunctionCall(inner) => Box::new(inner.variables()),
            Self::Negation(inner) => Box::new(inner.referenced_parent_vertex_ids()),
            Self::Disjunction(inner) => Box::new(inner.referenced_parent_vertex_ids()),
            Self::Optional(inner) => Box::new(inner.referenced_parent_and_optional_ids()),
            Self::Unsatisfiable(inner) => Box::new(inner.variables()),
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

    pub(super) fn can_be_trivial(&self) -> bool {
        matches!(
            self,
            Self::Comparison(_)
                | Self::Expression(_)
                | Self::Constraint(ConstraintVertex::TypeList(_))
                | Self::Constraint(ConstraintVertex::Isa(_))
        )
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            PlannerVertex::Variable(v) => {
                write!(f, "|Var {}|", v.variable())
            }
            PlannerVertex::Constraint(v) => {
                write!(f, "{}", v)
            }
            PlannerVertex::Is(_) => {
                write!(f, "|Is|")
            } //TODO
            PlannerVertex::LinksDeduplication(_) => {
                write!(f, "|LinksDeduplication|")
            }
            PlannerVertex::Comparison(v) => {
                write!(f, "|{:?} comp {:?}|", v.comparison.lhs(), v.comparison.rhs())
            }
            PlannerVertex::Expression(v) => {
                write!(f, "|Expr of {:?}|", v.expression.variables)
            }
            PlannerVertex::FunctionCall(_) => {
                write!(f, "|Fun Call|")
            } //TODO
            PlannerVertex::Negation(_) => {
                write!(f, "|Negation|")
            } //TODO
            PlannerVertex::Disjunction(_) => {
                write!(f, "|Disjunction|")
            } //TODO
            PlannerVertex::Optional(_) => {
                write!(f, "|Optional|")
            } //TODO
            PlannerVertex::Unsatisfiable(_) => {
                write!(f, "|Unsatisfiable|")
            }
        }
    }
}

impl Cost {
    const MIN_IO_RATIO: f64 = 0.000000001;
    const IN_MEM_COST_SIMPLE: f64 = 0.02;
    const IN_MEM_COST_COMPLEX: f64 = Cost::IN_MEM_COST_SIMPLE * 1.0; // TODO: revisit based on final usage of trivial patterns (see TRIVIAL_COST)
    pub const NOOP: Self = Self { cost: 0.0, io_ratio: 1.0 };
    pub const EMPTY: Self = Self { cost: 0.0, io_ratio: 0.0 };
    pub const INFINITY: Self = Self { cost: f64::INFINITY, io_ratio: 0.0 };
    pub const MEM_SIMPLE_OUTPUT_1: Self = Self { cost: Cost::IN_MEM_COST_SIMPLE, io_ratio: 1.0 };
    pub const MEM_COMPLEX_OUTPUT_1: Self = Self { cost: Cost::IN_MEM_COST_COMPLEX, io_ratio: 1.0 };
    pub const TRIVIAL_COST_THRESHOLD: f64 = 0.05;
    pub const TRIVIAL_IO_THRESHOLD: f64 = 1.0;
    pub const TRIVIAL_COST: f64 = Cost::IN_MEM_COST_SIMPLE;

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
        let io_ratio = f64::max(self.io_ratio * other.io_ratio / join_size, Cost::MIN_IO_RATIO);
        let num_seeks_each = f64::min(self.io_ratio, other.io_ratio); // FIXME detect when seeks can be replaced by advancing
        let self_out_cost = self.cost / self.io_ratio; // if cost = Ci + Co * io, then cost / io ~ Co
        let other_out_cost = other.cost / other.io_ratio;
        let cost_self = SEEK_ITERATOR_RELATIVE_COST + self_out_cost * num_seeks_each;
        let cost_other = SEEK_ITERATOR_RELATIVE_COST + other_out_cost * num_seeks_each;
        Self { cost: cost_self + cost_other, io_ratio }
    }

    pub(crate) fn combine_parallel(self, other: Self) -> Self {
        Self { cost: self.cost + other.cost, io_ratio: self.io_ratio + other.io_ratio }
    }

    pub(crate) fn is_trivial(&self) -> bool {
        self.cost < Self::TRIVIAL_COST_THRESHOLD && self.io_ratio <= Self::TRIVIAL_IO_THRESHOLD
    }
}

pub(super) trait Costed {
    fn cost_and_metadata(
        &self,
        vertex_ordering: &[VertexId],
        fix_dir: Option<Direction>,
        graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError>;
}

impl Costed for PlannerVertex<'_> {
    fn cost_and_metadata(
        &self,
        vertex_ordering: &[VertexId],
        fix_dir: Option<Direction>,
        graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        match self {
            Self::Variable(_) => Ok((Cost::NOOP, CostMetaData::None)),
            Self::Constraint(vertex) => vertex.cost_and_metadata(vertex_ordering, fix_dir, graph),

            Self::Is(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::LinksDeduplication(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::Comparison(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),

            Self::Expression(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::FunctionCall(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),

            Self::Negation(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::Disjunction(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::Optional(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
            Self::Unsatisfiable(planner) => planner.cost_and_metadata(vertex_ordering, fix_dir, graph),
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
pub(crate) enum Direction {
    Canonical,
    Reverse,
}

impl Direction {
    pub(crate) fn canonical_if(b: bool) -> Direction {
        match b {
            true => Direction::Canonical,
            false => Direction::Reverse,
        }
    }
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
        let cost = Cost::MEM_COMPLEX_OUTPUT_1;
        Self { inputs, output, cost, expression }
    }

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
        self.inputs.iter().all(|&input| ordered.contains(&VertexId::Variable(input)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.inputs.iter().chain(iter::once(&self.output)).copied()
    }
}

impl Costed for ExpressionPlanner<'_> {
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((self.cost, CostMetaData::None))
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

    fn is_valid(&self, vertex_plan: &[VertexId]) -> bool {
        self.arguments.iter().all(|&arg| vertex_plan.contains(&VertexId::Variable(arg)))
    }
}

impl Costed for FunctionCallPlanner<'_> {
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((self.cost, CostMetaData::None))
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

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
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
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((Cost::MEM_COMPLEX_OUTPUT_1, CostMetaData::None))
    }
}
#[derive(Clone, Debug)]
pub(super) struct LinksDeduplicationPlanner<'a> {
    links_deduplication: &'a LinksDeduplication<Variable>,
    pub role1: VariableVertexId,
    pub player1: VariableVertexId,
    pub role2: VariableVertexId,
    pub player2: VariableVertexId,
}

impl<'a> LinksDeduplicationPlanner<'a> {
    pub(crate) fn from_constraint(
        links_deduplication: &'a LinksDeduplication<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let role1 = links_deduplication.links1().role_type().as_variable().unwrap();
        let player1 = links_deduplication.links1().player().as_variable().unwrap();
        let role2 = links_deduplication.links2().role_type().as_variable().unwrap();
        let player2 = links_deduplication.links2().player().as_variable().unwrap();
        Self {
            links_deduplication,
            role1: variable_index[&role1],
            player1: variable_index[&player1],
            role2: variable_index[&role2],
            player2: variable_index[&player2],
        }
    }

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
        self.variables().all(|v| ordered.contains(&VertexId::Variable(v)))
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.role1, self.player1, self.role2, self.player2].into_iter()
    }

    pub(super) fn links_deduplication(&self) -> &LinksDeduplication<Variable> {
        self.links_deduplication
    }
}

impl Costed for LinksDeduplicationPlanner<'_> {
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((Cost::MEM_COMPLEX_OUTPUT_1, CostMetaData::None))
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

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
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
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((Cost::MEM_COMPLEX_OUTPUT_1, CostMetaData::None))
    }
}

#[derive(Clone, Debug)]
pub(super) struct UnsatisfiablePlanner<'a> {
    _unsatisfiable: &'a Unsatisfiable,
}

impl<'a> UnsatisfiablePlanner<'a> {
    pub(crate) fn from_constraint(
        _unsatisfiable: &'a Unsatisfiable,
        _variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        Self { _unsatisfiable }
    }

    fn is_valid(&self, _ordered: &[VertexId]) -> bool {
        true
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        iter::empty()
    }
}

impl Costed for UnsatisfiablePlanner<'_> {
    fn cost_and_metadata(
        &self,
        _: &[VertexId],
        _: Option<Direction>,
        _: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((Cost::in_mem_simple_with_ratio(Cost::MIN_IO_RATIO), CostMetaData::None))
    }
}

#[derive(Clone, Debug)]
pub(super) struct NestedNegationPlan<'a> {
    plan: ConjunctionPlan<'a>,
    referenced_parent_vertex_ids: HashSet<VariableVertexId>,
}

impl<'a> NestedNegationPlan<'a> {
    pub(super) fn new(plan: ConjunctionPlan<'a>, parent_variable_index: &HashMap<Variable, VariableVertexId>) -> Self {
        let referenced_parent_vertex_ids =
            plan.referenced_input_variables().map(|v| parent_variable_index[&v]).collect();
        Self { plan, referenced_parent_vertex_ids }
    }

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
        self.referenced_parent_vertex_ids.iter().all(|var| ordered.contains(&VertexId::Variable(*var)))
    }

    pub(crate) fn referenced_parent_vertex_ids(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.referenced_parent_vertex_ids.iter().copied()
    }

    pub(super) fn plan(&self) -> &ConjunctionPlan<'a> {
        &self.plan
    }
}

impl Costed for NestedNegationPlan<'_> {
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((self.plan.planner_statistics.query_cost, CostMetaData::None))
    }
}

#[derive(Clone, Debug)]
pub(super) struct NestedOptionalPlan<'a> {
    pub(super) plan: OptionalPlan<'a>,
    pub(super) referenced_parent_vertex_ids: HashSet<VariableVertexId>,
    pub(super) optional_vertex_ids: HashSet<VariableVertexId>,
}

impl<'a> NestedOptionalPlan<'a> {
    pub(super) fn new(plan: OptionalPlan<'a>, parent_variable_index: &HashMap<Variable, VariableVertexId>) -> Self {
        let referenced_parent_vertex_ids: HashSet<_> =
            plan.referenced_input_variables().map(|v| parent_variable_index[&v]).collect();
        let optional_vertex_ids = plan.optional_variables().map(|v| parent_variable_index[&v]).collect();
        Self { plan, referenced_parent_vertex_ids, optional_vertex_ids }
    }

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
        self.referenced_parent_vertex_ids.iter().all(|var| ordered.contains(&VertexId::Variable(*var)))
    }

    pub(crate) fn referenced_parent_and_optional_ids(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        chain!(self.referenced_parent_vertex_ids.iter(), self.optional_vertex_ids.iter()).copied()
    }

    pub(super) fn plan(&self) -> &OptionalPlan<'a> {
        &self.plan
    }
}

impl Costed for NestedOptionalPlan<'_> {
    fn cost_and_metadata(
        &self,
        _vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        _graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        Ok((self.plan.plan().planner_statistics.query_cost, CostMetaData::None))
    }
}

#[derive(Clone, Debug)]
pub(super) struct NestedDisjunctionPlanner<'a> {
    referenced_parent_vertex_ids: HashSet<VariableVertexId>,
    required_parent_vertex_ids: HashSet<VariableVertexId>,
    builder: DisjunctionPlanBuilder<'a>,
}

impl<'a> NestedDisjunctionPlanner<'a> {
    pub(super) fn from_builder(
        builder: DisjunctionPlanBuilder<'a>,
        parent_variable_index: &HashMap<Variable, VariableVertexId>,
    ) -> Self {
        // note: not every referenced variable is in the parent index & mapped
        let referenced_parent_vertex_ids = builder
            .branches()
            .iter()
            .flat_map(|branch| branch.referenced_variables().filter_map(|v| parent_variable_index.get(&v).copied()))
            .collect();
        let required_parent_vertex_ids = builder
            .branches()
            .iter()
            .flat_map(|branch| branch.required_input_variables())
            .map(|v| parent_variable_index[&v])
            .collect();
        Self { referenced_parent_vertex_ids, required_parent_vertex_ids, builder }
    }

    fn is_valid(&self, ordered: &[VertexId]) -> bool {
        self.required_parent_vertex_ids.iter().all(|&var| ordered.contains(&VertexId::Variable(var)))
    }

    pub(crate) fn referenced_parent_vertex_ids(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.referenced_parent_vertex_ids.iter().copied()
    }

    pub(crate) fn plan(
        &self,
        input_variables: impl Iterator<Item = Variable> + Clone,
    ) -> Result<DisjunctionPlan<'a>, QueryPlanningError> {
        // TODO: would be nice if we can do this without cloning
        //  however, we do need to manipulate each branch's graph based on the available input
        let DisjunctionPlanBuilder { branch_ids, branches, .. } = self.builder.clone();
        let branches = branches
            .into_iter()
            .map(|branch| branch.with_inputs(input_variables.clone()).plan())
            .collect::<Result<Vec<_>, _>>()?;
        let cost = branches.iter().map(ConjunctionPlan::cost).fold(Cost::EMPTY, Cost::combine_parallel);
        Ok(DisjunctionPlan::new(branch_ids, branches, cost))
    }
}

impl Costed for NestedDisjunctionPlanner<'_> {
    fn cost_and_metadata(
        &self,
        vertex_ordering: &[VertexId],
        _fix_dir: Option<Direction>,
        graph: &Graph<'_>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        let input_variables =
            vertex_ordering.iter().filter_map(|id| graph.elements()[id].as_variable()).map(|var| var.variable());
        let cost = self
            .builder
            .branches()
            .iter()
            .map(|branch| branch.clone().with_inputs(input_variables.clone()).plan().map(|plan| plan.cost()))
            .collect::<Result<Vec<_>, _>>()
            .map(|costs| costs.into_iter().fold(Cost::EMPTY, |acc_cost, cost| acc_cost.combine_parallel(cost)))?;
        Ok((cost, CostMetaData::None))
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
