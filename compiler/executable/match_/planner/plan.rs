/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::type_name_of_val,
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fmt,
    fmt::Debug,
    time::Instant,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{
            Comparator, Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Iid, Is, Isa, Kind, Label,
            Links, Owns, Plays, Relates, RoleName, Sub, Value,
        },
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Vertex,
    },
    pipeline::{block::BlockContext, VariableRegistry},
};
use itertools::Itertools;

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::{
        instructions::{
            thing::{
                HasInstruction, HasReverseInstruction, IidInstruction, IsaInstruction, IsaReverseInstruction,
                LinksInstruction, LinksReverseInstruction,
            },
            type_::{
                OwnsInstruction, OwnsReverseInstruction, PlaysInstruction, PlaysReverseInstruction, RelatesInstruction,
                RelatesReverseInstruction, SubInstruction, SubReverseInstruction,
            },
            CheckInstruction, CheckVertex, ConstraintInstruction, Inputs, IsInstruction,
        },
        planner::{
            vertex::{
                constraint::{
                    ConstraintVertex, HasPlanner, IidPlanner, IsaPlanner, LinksPlanner, OwnsPlanner, PlaysPlanner,
                    RelatesPlanner, SubPlanner, TypeListPlanner,
                },
                variable::{InputPlanner, ThingPlanner, TypePlanner, ValuePlanner, VariableVertex},
                CombinedCost, ComparisonPlanner, CostMetaData, Costed, Direction, DisjunctionPlanner, ElementCost,
                ExpressionPlanner, FunctionCallPlanner, Input, IsPlanner, NegationPlanner, PlannerVertex,
            },
            DisjunctionBuilder, ExpressionBuilder, FunctionCallBuilder, IntersectionBuilder, MatchExecutableBuilder,
            NegationBuilder, StepBuilder, StepInstructionsBuilder,
        },
    },
    ExecutorVariable, VariablePosition,
};

pub const BEAM_WIDTH: usize = 1;
pub const EXTENSION_WIDTH: usize = 1;

pub(crate) fn plan_conjunction<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    variable_positions: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a TypeAnnotations,
    variable_registry: &VariableRegistry,
    expressions: &'a HashMap<Variable, ExecutableExpression<Variable>>,
    statistics: &'a Statistics,
) -> ConjunctionPlan<'a> {
    make_builder(
        conjunction,
        block_context,
        variable_positions,
        type_annotations,
        variable_registry,
        expressions,
        statistics,
    )
    .plan()
}

fn make_builder<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    variable_positions: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a TypeAnnotations,
    variable_registry: &VariableRegistry,
    expressions: &'a HashMap<Variable, ExecutableExpression<Variable>>,
    statistics: &'a Statistics,
) -> ConjunctionPlanBuilder<'a> {
    let mut negation_subplans = Vec::new();
    let mut disjunction_planners = Vec::new();
    for pattern in conjunction.nested_patterns() {
        match pattern {
            NestedPattern::Disjunction(disjunction) => disjunction_planners.push(DisjunctionPlanBuilder::new(
                disjunction
                    .conjunctions()
                    .iter()
                    .map(|conj| {
                        make_builder(
                            conj,
                            block_context,
                            variable_positions,
                            type_annotations,
                            variable_registry,
                            expressions,
                            statistics,
                        )
                    })
                    .collect_vec(),
            )),
            NestedPattern::Negation(negation) => negation_subplans.push(
                make_builder(
                    negation.conjunction(),
                    block_context,
                    variable_positions,
                    type_annotations,
                    variable_registry,
                    expressions,
                    statistics,
                )
                .with_inputs(negation.conjunction().captured_variables(block_context))
                .plan(),
            ),
            NestedPattern::Optional(_) => todo!(),
        }
    }

    let mut plan_builder = ConjunctionPlanBuilder::new(type_annotations, statistics);
    plan_builder.register_variables(
        variable_positions.keys().copied(),
        conjunction.captured_variables(block_context),
        conjunction.declared_variables(block_context),
        variable_registry,
    );
    plan_builder.register_constraints(conjunction, expressions);
    plan_builder.register_negations(negation_subplans);
    plan_builder.register_disjunctions(disjunction_planners);
    plan_builder
}

#[derive(Clone, Copy, Default, Hash, PartialEq, Eq)]
pub(super) struct VariableVertexId(usize);

impl fmt::Debug for VariableVertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V[{}]", self.0)
    }
}

#[derive(Clone, Copy, Default, Hash, PartialEq, Eq)]
pub(super) struct PatternVertexId(usize);

impl fmt::Debug for PatternVertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "P[{}]", self.0)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) enum VertexId {
    Variable(VariableVertexId),
    Pattern(PatternVertexId),
}

impl fmt::Debug for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(id) => fmt::Debug::fmt(id, f),
            Self::Pattern(id) => fmt::Debug::fmt(id, f),
        }
    }
}

impl VertexId {
    pub(super) fn as_variable_id(&self) -> Option<VariableVertexId> {
        match *self {
            Self::Variable(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_pattern_id(&self) -> Option<PatternVertexId> {
        match *self {
            Self::Pattern(v) => Some(v),
            _ => None,
        }
    }
}

/*
 * 1. Named variables that are not returned or reused beyond a step can simply be counted, and not output
 * 2. Anonymous variables that are not reused beyond a step can just be checked for a single answer
 *
 * Planner outputs an ordering over variables, with directions over which edges should be traversed.
 * If we know this we can:
 *   1. group edges intersecting into the same variable as one step.
 *   2. if the ordering implies it, we may need to perform Storage/Comparison checks, if the variables are visited,
 *      disconnected and then joined
 *   3. some checks are fully bound, while others are not... when do we decide? What is a Check versus an Iterate
 *      instructions? Do we need to differentiate?
 */

#[derive(Clone)]
pub(super) struct ConjunctionPlanBuilder<'a> {
    shared_variables: Vec<Variable>,
    graph: Graph<'a>,
    type_annotations: &'a TypeAnnotations,
    statistics: &'a Statistics,
}

impl fmt::Debug for ConjunctionPlanBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBuilder")
            .field("shared_variables", &self.shared_variables)
            .field("graph", &self.graph)
            .finish()
    }
}

impl<'a> ConjunctionPlanBuilder<'a> {
    fn new(type_annotations: &'a TypeAnnotations, statistics: &'a Statistics) -> Self {
        Self { shared_variables: Vec::new(), graph: Graph::default(), type_annotations, statistics }
    }

    pub(super) fn shared_variables(&self) -> &[Variable] {
        &self.shared_variables
    }

    fn input_variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.graph
            .variable_index
            .values()
            .copied()
            .filter(|&v| self.graph.elements[&VertexId::Variable(v)].as_variable().is_some_and(|v| v.is_input()))
    }

    pub(super) fn with_inputs(mut self, input_variables: impl Iterator<Item = Variable>) -> Self {
        for var in input_variables {
            if let Some(&id) = self.graph.variable_index.get(&var) {
                self.graph.elements.insert(
                    VertexId::Variable(id),
                    PlannerVertex::Variable(VariableVertex::Input(InputPlanner::from_variable(var))),
                );
            }
        }
        self
    }

    fn register_variables(
        &mut self,
        input_variables: impl Iterator<Item = Variable>,
        shared_variables: impl Iterator<Item = Variable>,
        local_variables: impl Iterator<Item = Variable>,
        variable_registry: &VariableRegistry,
    ) {
        self.shared_variables.reserve(input_variables.size_hint().0 + shared_variables.size_hint().0);

        for variable in input_variables {
            self.register_input_var(variable);
        }

        for variable in shared_variables {
            if self.graph.variable_index.contains_key(&variable) {
                continue;
            }
            self.shared_variables.push(variable);
            let category = variable_registry.get_variable_category(variable).unwrap();
            match category {
                | VariableCategory::Type
                | VariableCategory::ThingType
                | VariableCategory::AttributeType
                | VariableCategory::RoleType => self.register_type_var(variable),

                | VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                    self.register_thing_var(variable)
                }

                VariableCategory::Value => self.register_value_var(variable),

                | VariableCategory::ObjectList
                | VariableCategory::ThingList
                | VariableCategory::AttributeList
                | VariableCategory::ValueList => todo!("list variable planning"),
                VariableCategory::AttributeOrValue => {
                    unreachable!("Insufficiently bound variable should have been flagged earlier")
                }
            }
        }

        for variable in local_variables {
            if self.graph.variable_index.contains_key(&variable) {
                continue;
            }
            let category = variable_registry.get_variable_category(variable).unwrap();
            match category {
                | VariableCategory::Type
                | VariableCategory::ThingType
                | VariableCategory::AttributeType
                | VariableCategory::RoleType => self.register_type_var(variable),

                VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                    self.register_thing_var(variable)
                }

                VariableCategory::Value => self.register_value_var(variable),

                | VariableCategory::ObjectList
                | VariableCategory::ThingList
                | VariableCategory::AttributeList
                | VariableCategory::ValueList => todo!("list variable planning"),
                VariableCategory::AttributeOrValue => {
                    unreachable!("Insufficiently bound variable would have been flagged earlier")
                }
            }
        }
    }

    fn register_input_var(&mut self, variable: Variable) {
        self.shared_variables.push(variable);
        let planner = InputPlanner::from_variable(variable);
        self.graph.push_variable(variable, VariableVertex::Input(planner));
    }

    fn register_type_var(&mut self, variable: Variable) {
        let planner = TypePlanner::from_variable(variable, self.type_annotations);
        self.graph.push_variable(variable, VariableVertex::Type(planner));
    }

    fn register_thing_var(&mut self, variable: Variable) {
        let planner = ThingPlanner::from_variable(variable, self.type_annotations, self.statistics);
        self.graph.push_variable(variable, VariableVertex::Thing(planner));
    }

    fn register_value_var(&mut self, variable: Variable) {
        let planner = ValuePlanner::from_variable(variable);
        self.graph.push_variable(variable, VariableVertex::Value(planner));
    }

    fn register_constraints(
        &mut self,
        conjunction: &'a Conjunction,
        expressions: &'a HashMap<Variable, ExecutableExpression<Variable>>,
    ) {
        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::Kind(kind) => self.register_kind(kind),
                Constraint::RoleName(role_name) => self.register_role_name(role_name),
                Constraint::Label(label) => self.register_label(label),
                Constraint::Value(value) => self.register_value(value),

                Constraint::Sub(sub) => self.register_sub(sub),
                Constraint::Owns(owns) => self.register_owns(owns),
                Constraint::Relates(relates) => self.register_relates(relates),
                Constraint::Plays(plays) => self.register_plays(plays),

                Constraint::Isa(isa) => self.register_isa(isa),
                Constraint::Iid(iid) => self.register_iid(iid),
                Constraint::Has(has) => self.register_has(has),
                Constraint::Links(links) => self.register_links(links),

                Constraint::ExpressionBinding(expression) => self.register_expression_binding(expression, expressions),
                Constraint::FunctionCallBinding(call) => self.register_function_call_binding(call),

                Constraint::Is(is) => self.register_is(is),
                Constraint::Comparison(comparison) => self.register_comparison(comparison),
            }
        }
    }

    fn register_label(&mut self, label: &'a Label<Variable>) {
        let planner = TypeListPlanner::from_label_constraint(label, &self.graph.variable_index, self.type_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_role_name(&mut self, role_name: &'a RoleName<Variable>) {
        let planner =
            TypeListPlanner::from_role_name_constraint(role_name, &self.graph.variable_index, self.type_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_kind(&mut self, kind: &'a Kind<Variable>) {
        let planner = TypeListPlanner::from_kind_constraint(kind, &self.graph.variable_index, self.type_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_sub(&mut self, sub: &'a Sub<Variable>) {
        let planner = SubPlanner::from_constraint(sub, &self.graph.variable_index, self.type_annotations);
        self.graph.push_constraint(ConstraintVertex::Sub(planner));
    }

    fn register_owns(&mut self, owns: &'a Owns<Variable>) {
        let planner =
            OwnsPlanner::from_constraint(owns, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Owns(planner));
    }

    fn register_relates(&mut self, relates: &'a Relates<Variable>) {
        let planner = RelatesPlanner::from_constraint(
            relates,
            &self.graph.variable_index,
            self.type_annotations,
            self.statistics,
        );
        self.graph.push_constraint(ConstraintVertex::Relates(planner));
    }

    fn register_plays(&mut self, plays: &'a Plays<Variable>) {
        let planner =
            PlaysPlanner::from_constraint(plays, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Plays(planner));
    }

    fn register_value(&mut self, value: &'a Value<Variable>) {
        let planner = TypeListPlanner::from_value_constraint(value, &self.graph.variable_index, self.type_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_isa(&mut self, isa: &'a Isa<Variable>) {
        let planner =
            IsaPlanner::from_constraint(isa, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Isa(planner));
    }

    fn register_iid(&mut self, iid: &'a Iid<Variable>) {
        let planner =
            IidPlanner::from_constraint(iid, &self.graph.variable_index, self.type_annotations, self.statistics);
        // TODO not setting exact bound for the var here as the checker can't currently take advantage of that
        //      so the cost would be misleading the planner
        self.graph.push_constraint(ConstraintVertex::Iid(planner));
    }

    fn register_has(&mut self, has: &'a Has<Variable>) {
        let planner =
            HasPlanner::from_constraint(has, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Has(planner));
    }

    fn register_links(&mut self, links: &'a Links<Variable>) {
        let planner =
            LinksPlanner::from_constraint(links, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Links(planner));
    }

    fn register_expression_binding(
        &mut self,
        expression: &ExpressionBinding<Variable>,
        expressions: &'a HashMap<Variable, ExecutableExpression<Variable>>,
    ) {
        let variable = expression.left().as_variable().unwrap();
        let output = self.graph.variable_index[&variable];
        let expression = &expressions[&variable];
        let inputs = expression.variables().iter().map(|&var| self.graph.variable_index[&var]).unique().collect_vec();
        self.graph.push_expression(output, ExpressionPlanner::from_expression(expression, inputs, output));
    }

    fn register_function_call_binding(&mut self, call_binding: &'a FunctionCallBinding<Variable>) {
        let arguments =
            call_binding.function_call().argument_ids().map(|variable| self.graph.variable_index[&variable]).collect();
        let return_vars = call_binding
            .assigned()
            .iter()
            .map(|vertex| {
                let Vertex::Variable(variable) = vertex else { unreachable!() };
                self.graph.variable_index[variable]
            })
            .collect();
        // TODO: Use the real cost when we have function planning
        let element_cost = ElementCost { per_input: 1.0, per_output: 1.0, io_ratio: 1.0 };
        let combined_cost = CombinedCost { cost: 1.0, io_ratio: 1.0 };
        self.graph.push_function_call(FunctionCallPlanner::from_constraint(
            call_binding,
            arguments,
            return_vars,
            element_cost,
            combined_cost,
        ));
    }

    fn register_is(&mut self, is: &'a Is<Variable>) {
        let lhs = self.graph.variable_index[&is.lhs().as_variable().unwrap()];
        let rhs = self.graph.variable_index[&is.rhs().as_variable().unwrap()];
        self.graph.elements.get_mut(&VertexId::Variable(lhs)).unwrap().as_variable_mut().unwrap().add_is(rhs);
        self.graph.elements.get_mut(&VertexId::Variable(rhs)).unwrap().as_variable_mut().unwrap().add_is(lhs);
        self.graph.push_is(IsPlanner::from_constraint(
            is,
            &self.graph.variable_index,
            self.type_annotations,
            self.statistics,
        ));
    }

    fn register_comparison(&mut self, comparison: &'a Comparison<Variable>) {
        let lhs = Input::from_vertex(comparison.lhs(), &self.graph.variable_index);
        let rhs = Input::from_vertex(comparison.rhs(), &self.graph.variable_index);
        if let Input::Variable(lhs) = lhs {
            let lhs = self.graph.elements.get_mut(&VertexId::Variable(lhs)).unwrap().as_variable_mut().unwrap();
            match comparison.comparator() {
                Comparator::Equal => lhs.add_equal(rhs),
                Comparator::NotEqual => (), // no tangible impact on traversal costs
                Comparator::Less | Comparator::LessOrEqual => lhs.add_upper_bound(rhs),
                Comparator::Greater | Comparator::GreaterOrEqual => lhs.add_lower_bound(rhs),
                Comparator::Like => todo!("like operator"),
                Comparator::Contains => todo!("contains operator"),
            }
        }
        if let Input::Variable(rhs) = rhs {
            let rhs = self.graph.elements.get_mut(&VertexId::Variable(rhs)).unwrap().as_variable_mut().unwrap();
            match comparison.comparator() {
                Comparator::Equal => rhs.add_equal(lhs),
                Comparator::NotEqual => (), // no tangible impact on traversal costs
                Comparator::Less | Comparator::LessOrEqual => rhs.add_upper_bound(lhs),
                Comparator::Greater | Comparator::GreaterOrEqual => rhs.add_lower_bound(lhs),
                Comparator::Like => todo!("like operator"),
                Comparator::Contains => todo!("contains operator"),
            }
        }
        self.graph.push_comparison(ComparisonPlanner::from_constraint(
            comparison,
            &self.graph.variable_index,
            self.type_annotations,
            self.statistics,
        ));
    }

    fn register_disjunctions(&mut self, disjunctions: Vec<DisjunctionPlanBuilder<'a>>) {
        for disjunction in disjunctions {
            self.graph.push_disjunction(DisjunctionPlanner::from_builder(disjunction, &self.graph.variable_index));
        }
    }

    fn register_negations(&mut self, negations: Vec<ConjunctionPlan<'a>>) {
        for negation_plan in negations {
            self.graph.push_negation(NegationPlanner::new(negation_plan, &self.graph.variable_index));
        }
    }

    fn initialise_greedy_ordering(&self) -> (Vec<VertexId>, HashMap<PatternVertexId, CostMetaData>) {
        let mut remaining_vertices: HashSet<VertexId> =
            self.graph.pattern_to_variable.keys().map(|&pattern_id| VertexId::Pattern(pattern_id)).collect();
        let mut vertex_plan = Vec::with_capacity(self.graph.element_count());
        let mut plan_cumulative_cost: ElementCost = ElementCost::EMPTY;
        let mut meta_data: HashMap<PatternVertexId, CostMetaData> = HashMap::new();

        for v in self.input_variables() {
            vertex_plan.push(VertexId::Variable(v));
            remaining_vertices.remove(&VertexId::Variable(v));
        }

        let mut step_produced_variables: HashSet<VariableVertexId> = HashSet::new();
        let mut step_start_index: usize = 0;
        let mut step_sort_variable: Option<VariableVertexId> = None;

        macro_rules! finalize_step {
            () => {{
                if let Some(var) = step_sort_variable.take().map(VertexId::Variable) {
                    vertex_plan.push(var);
                    remaining_vertices.remove(&var);
                }
                for var in step_produced_variables.drain().map(VertexId::Variable) {
                    if !vertex_plan.contains(&var) {
                        vertex_plan.push(var);
                        remaining_vertices.remove(&var);
                    }
                }
                step_start_index = vertex_plan.len();
            }};
        }

        // DEBUG
        println!("== Greedy search ==");

        while !remaining_vertices.is_empty() {
            // DEBUG
            println!("Choosing next plan element...");

            let (next, _cost, _element_cost) = remaining_vertices
                .iter()
                .filter(|&&elem| self.graph.elements[&elem].is_valid(&vertex_plan, &self.graph))
                .map(|&elem| {
                    let element_cost =
                        self.calculate_marginal_elementcost(&vertex_plan, elem, step_sort_variable, step_start_index);
                    let cost = element_cost.total();

                    // DEBUG
                    println!(
                        "  Choice {:?}, cost: {cost}, cumulative: {}",
                        elem,
                        plan_cumulative_cost.chain(element_cost).total()
                    );
                    (elem, cost, element_cost)
                })
                .min_by(|(_, lhs_cost, _), (_, rhs_cost, _)| lhs_cost.total_cmp(rhs_cost))
                .unwrap();
            let element = &self.graph.elements[&next];
            plan_cumulative_cost = plan_cumulative_cost.chain(_element_cost);

            // DEBUG
            println!("--> Chose {:?}, cost: {_cost}", next);

            if element.is_variable() {
                finalize_step!();
            } else if element.is_constraint() {
                step_produced_variables
                    .extend(element.variables().filter(|&var| !vertex_plan.contains(&VertexId::Variable(var))));

                let constraint = element.as_constraint().unwrap();
                match constraint.unbound_direction(&self.graph) {
                    Direction::Canonical => {
                        if let Some(candidate_sort_variable) = constraint.variables().next() {
                            if step_produced_variables.contains(&candidate_sort_variable) {
                                step_sort_variable = Some(candidate_sort_variable);
                            }
                        }
                    }
                    Direction::Reverse => {
                        if let Some(candidate_sort_variable) = constraint.variables().nth(1) {
                            if step_produced_variables.contains(&candidate_sort_variable) {
                                step_sort_variable = Some(candidate_sort_variable);
                            }
                        }
                    }
                }

                meta_data.insert(
                    next.as_pattern_id().unwrap(),
                    CostMetaData::Direction(element.as_constraint().unwrap().unbound_direction(&self.graph)),
                );
                vertex_plan.push(next);
                remaining_vertices.remove(&next);
                finalize_step!();
            } else {
                finalize_step!();
                vertex_plan.push(next);
                remaining_vertices.remove(&next);
                for var in element.variables().map(VertexId::Variable) {
                    if !vertex_plan.contains(&var) {
                        vertex_plan.push(var);
                    }
                }
            }
        }
        // DEBUG
        println!("Finished greedy ordering: {:#?}", vertex_plan);
        println!("Total cost {}", plan_cumulative_cost.total());

        (vertex_plan, meta_data)
    }

    fn calculate_marginal_cost(
        &self,
        vertex_plan: &[VertexId],
        next: VertexId,
        sort_variable: Option<VariableVertexId>,
        step_start_index: usize,
    ) -> f64 {
        assert!(!vertex_plan.contains(&next));
        let planner_vertex = &self.graph.elements[&next];
        let ElementCost { per_input, per_output, io_ratio: branching_factor } =
            planner_vertex.cost(vertex_plan, sort_variable, step_start_index, &self.graph);
        per_input + branching_factor * per_output
    }

    fn calculate_marginal_elementcost(
        &self,
        vertex_plan: &[VertexId],
        next: VertexId,
        sort_variable: Option<VariableVertexId>,
        step_start_index: usize,
    ) -> ElementCost {
        assert!(!vertex_plan.contains(&next));
        let planner_vertex = &self.graph.elements[&next];
        planner_vertex.cost(vertex_plan, sort_variable, step_start_index, &self.graph)
    }

    // New approach to planning:
    //
    // In our pattern graph, vertices are variables and patterns; edges indicate which patterns contain which variables.
    // A plan is an ordering of patterns and variable vertices, indicate in which order we retrieve stored patterns
    // Multiple patterns may be retrieved in the same step if there is a variable on which they can be joined.
    // Each step may "produce" answers for zero of more variables, which is recorded by appending these variables
    // (When a step has multiple pattern, the first such produced variable is always the join variable)
    // We record directionality information for each pattern in the plan, indicating which prefix index to use for pattern retrieval

    fn beam_search_plan(&self) -> (Vec<VertexId>, HashMap<PatternVertexId, CostMetaData>) {
        // DEBUG
        println!("== Beam search ==");

        let all_patterns: HashSet<PatternVertexId> = self.graph.pattern_to_variable.keys().copied().collect();
        let search_depth: usize = all_patterns.len();
        let mut best_partial_plans = vec![PartialCostPlan::new(
            all_patterns,           // all patterns are remaining
            self.input_variables(), // input variables start the plan
        )];

        for i in 0..search_depth {
            let mut new_plans_heap: BinaryHeap<PartialCostPlan> = BinaryHeap::new();
            for plan in best_partial_plans.iter() {
                // DEBUG
                println!("Step {}, extending plan: {:?}", i, plan.vertex_ordering);

                let mut extension_heap = BinaryHeap::new();
                for extension in plan.extensions_iter(&self.graph) {
                    if extension_heap.len() < EXTENSION_WIDTH {
                        extension_heap.push(extension);
                    } else if let Some(top) = extension_heap.peek() {
                        if extension < *top {
                            extension_heap.pop();
                            extension_heap.push(extension);
                        }
                    }
                }

                for extension in extension_heap.into_iter() {
                    let mut new_plan: PartialCostPlan;
                    if !extension.is_constraint(&self.graph) {
                        new_plan = plan.clone_and_extend_with_new_step(extension, &self.graph);
                    } else if extension.step_join_var.is_some()
                        && (plan.ongoing_step_join_var.is_none()
                            || plan.ongoing_step_join_var == extension.step_join_var)
                    {
                        new_plan = plan.clone_and_extend_with_continued_step(extension, &self.graph);
                    } else {
                        new_plan = plan.clone_and_extend_with_new_step(extension, &self.graph);
                    }

                    if new_plans_heap.len() < BEAM_WIDTH {
                        new_plans_heap.push(new_plan);
                    } else if let Some(top) = new_plans_heap.peek() {
                        if new_plan < *top {
                            new_plans_heap.pop();
                            new_plans_heap.push(new_plan);
                        }
                    }
                }
            }
            best_partial_plans = new_plans_heap.into_iter().collect();
        }
        let best_plan = best_partial_plans.into_iter().min().unwrap();
        let complete_plan = best_plan.into_complete_plan();
        // DEBUG
        println!("Best plan {:#?}", complete_plan);
        (complete_plan.vertex_ordering, complete_plan.pattern_metadata)
    }

    // Execute plans
    pub(super) fn plan(self) -> ConjunctionPlan<'a> {
        // Input Graph
        println!("== Planning with graph ==\n {:#?}", self.graph);

        // Greedy plan
        let start_greedy = Instant::now();
        let (ordering, metadata) = self.initialise_greedy_ordering(); // TODO: remove
        println!("---> Greedy search took {:?}", start_greedy.elapsed());

        // Beam plan
        let start_beam = Instant::now();
        let (ordering, metadata) = self.beam_search_plan();
        println!("---> Beam search took {:?}", start_beam.elapsed());

        let element_to_order = ordering.iter().copied().enumerate().map(|(order, index)| (index, order)).collect();

        let cost = ordering
            .iter()
            .enumerate()
            .map(|(i, idx)| {
                let sort_variable = ordering.get(i + 1).and_then(|vertex| vertex.as_variable_id());
                self.graph.elements[idx].cost(&ordering[..i], sort_variable, 0, &self.graph)
            })
            .fold(ElementCost::MEM_SIMPLE_BRANCH_1, |acc, e| acc.chain(e));

        let Self { shared_variables, graph, type_annotations, statistics: _ } = self;

        ConjunctionPlan { shared_variables, graph, type_annotations, ordering, metadata, element_to_order, cost }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct CompleteCostPlan {
    vertex_ordering: Vec<VertexId>,
    pattern_metadata: HashMap<PatternVertexId, CostMetaData>,
    cumulative_cost: CombinedCost,
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct PartialCostPlan {
    vertex_ordering: Vec<VertexId>,
    pattern_metadata: HashMap<PatternVertexId, CostMetaData>,
    cumulative_cost: CombinedCost,
    remaining_patterns: HashSet<PatternVertexId>,
    ongoing_step_cost: CombinedCost,
    ongoing_step_produced_vars: HashSet<VariableVertexId>,
    ongoing_step_join_var: Option<VariableVertexId>,
    projected_cost: CombinedCost,
}

impl PartialCostPlan {
    fn new(
        remaining_patterns: HashSet<PatternVertexId>,
        inputs: impl Iterator<Item = VariableVertexId> + Sized,
    ) -> Self {
        let mut vertex_ordering = Vec::new();
        for v in inputs {
            vertex_ordering.push(VertexId::Variable(v));
        }
        Self {
            vertex_ordering,
            pattern_metadata: HashMap::new(),
            cumulative_cost: CombinedCost::NOOP,
            remaining_patterns,
            ongoing_step_cost: CombinedCost::NOOP,
            ongoing_step_produced_vars: HashSet::new(),
            ongoing_step_join_var: None,
            projected_cost: CombinedCost::INFINITY,
        }
    }

    fn extensions_iter<'a>(&'a self, graph: &'a Graph<'_>) -> impl Iterator<Item = StepExtension> + 'a {
        let mut all_available_vars = self.vertex_ordering.clone();
        all_available_vars.extend(self.ongoing_step_produced_vars.iter().map(|&var| VertexId::Variable(var)));

        self.remaining_patterns
            .iter()
            .filter(move |&&extension| {
                graph.elements[&VertexId::Pattern(extension)].is_valid(&self.vertex_ordering, graph)
            })
            .flat_map(move |&extension| {
                let join_var = self.determine_joinability(graph, extension);

                // // If we are adding the last constraint, then we cannot join any further
                // if graph.elements[&VertexId::Pattern(extension)].is_constraint()
                //     && self.remaining_patterns.iter().filter(|&&ext| graph.elements[&VertexId::Pattern(ext)].is_constraint()).exactly_one().is_ok() {
                //     join_var = None;
                // }

                if join_var.is_none() {
                    vec![(extension, join_var)].into_iter()
                } else {
                    vec![(extension, None), (extension, join_var)].into_iter()
                }
            })
            .map(move |(extension, join_var)| {
                let added_cost: CombinedCost;
                let meta_data: CostMetaData;

                if join_var.is_none() {
                    (added_cost, meta_data) = self.compute_added_cost(graph, extension, &all_available_vars, join_var);
                } else {
                    (added_cost, meta_data) =
                        self.compute_added_cost(graph, extension, &self.vertex_ordering, join_var);
                }

                let mut cost_before_extension = self.cumulative_cost;
                if join_var.is_none() {
                    cost_before_extension = cost_before_extension.chain(self.ongoing_step_cost);
                    // Complete ongoing step
                }

                // DEBUG
                println!(
                    "    Cost: {}, pattern: {:?}, join: {:?}, added cost: {}",
                    cost_before_extension.cost, extension, join_var, added_cost.cost
                );

                let projected_cost = cost_before_extension
                    .chain(added_cost)
                    .chain(self.heuristic_plan_completion_cost(graph, extension));

                StepExtension {
                    pattern_extension: extension,
                    pattern_metadata: meta_data,
                    step_cost: added_cost,
                    step_join_var: join_var,
                    projected_cost,
                }
            })
    }

    fn determine_joinability(&self, graph: &Graph<'_>, pattern: PatternVertexId) -> Option<VariableVertexId> {
        let mut updated_join_var: Option<VariableVertexId> = None;
        let planner = &graph.elements[&VertexId::Pattern(pattern)];
        if let PlannerVertex::Constraint(constraint) = planner {
            if let Ok(candidate_join_var) =
                constraint.variables().filter(|var| self.ongoing_step_produced_vars.contains(var)).exactly_one()
            {
                // There are two cases where we can get the join cost:
                if self.ongoing_step_join_var.is_none() && constraint.can_sort_on(candidate_join_var) {
                    updated_join_var = Some(candidate_join_var);
                } else if self.ongoing_step_join_var == Some(candidate_join_var)
                    && constraint.can_sort_on(candidate_join_var)
                {
                    updated_join_var = self.ongoing_step_join_var;
                }
            }
        };
        updated_join_var
    }

    fn compute_added_cost(
        &self,
        graph: &Graph<'_>,
        pattern: PatternVertexId,
        input_vars: &[VertexId],
        join_var: Option<VariableVertexId>,
    ) -> (CombinedCost, CostMetaData) {
        let planner = &graph.elements[&VertexId::Pattern(pattern)];
        let (updated_cost, extension_metadata) = match planner {
            PlannerVertex::Constraint(constraint) => {
                if join_var.is_some() {
                    let total_join_size = graph.elements[&VertexId::Variable(join_var.unwrap())]
                        .as_variable()
                        .unwrap()
                        .expected_output_size(&self.vertex_ordering);
                    let (constraint_cost, meta_data) = constraint.cost_and_metadata(input_vars, graph);
                    (self.ongoing_step_cost.join(constraint_cost, total_join_size), meta_data)
                } else {
                    constraint.cost_and_metadata(input_vars, graph)
                }
            }
            planner_vertex => planner_vertex.cost_and_metadata(&self.vertex_ordering, graph),
        };
        (updated_cost, extension_metadata)
    }

    fn heuristic_plan_completion_cost(&self, _graph: &Graph<'_>, _pattern: PatternVertexId) -> CombinedCost {
        CombinedCost::NOOP
    }

    fn clone_and_extend_with_continued_step(&self, extension: StepExtension, graph: &Graph<'_>) -> PartialCostPlan {
        let mut new_vertex_ordering = self.vertex_ordering.clone();
        new_vertex_ordering.push(VertexId::Pattern(extension.pattern_extension));

        let mut new_pattern_metadata = self.pattern_metadata.clone();
        new_pattern_metadata.insert(extension.pattern_extension, extension.pattern_metadata);

        let mut new_remaining_patterns = self.remaining_patterns.clone();
        new_remaining_patterns.remove(&extension.pattern_extension);

        let mut new_produced_vars = self.ongoing_step_produced_vars.clone();
        new_produced_vars.extend(
            graph.pattern_to_variable[&extension.pattern_extension]
                .iter()
                .copied()
                .filter(|&var| !self.vertex_ordering.contains(&VertexId::Variable(var))),
        );

        PartialCostPlan {
            vertex_ordering: new_vertex_ordering,
            pattern_metadata: new_pattern_metadata,
            remaining_patterns: new_remaining_patterns,
            cumulative_cost: self.cumulative_cost,
            ongoing_step_cost: extension.step_cost,
            ongoing_step_produced_vars: new_produced_vars,
            ongoing_step_join_var: extension.step_join_var,
            projected_cost: extension.projected_cost,
        }
    }

    fn clone_and_extend_with_new_step(&self, extension: StepExtension, graph: &Graph<'_>) -> PartialCostPlan {
        // Commit previous step to plan
        let mut new_vertex_ordering = self.vertex_ordering.clone();
        if let Some(join_var) = self.ongoing_step_join_var {
            new_vertex_ordering.push(VertexId::Variable(join_var));
            for var in self.ongoing_step_produced_vars.clone() {
                if var != join_var && !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    new_vertex_ordering.push(VertexId::Variable(var));
                }
            }
        } else {
            for var in self.ongoing_step_produced_vars.clone() {
                if !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    new_vertex_ordering.push(VertexId::Variable(var));
                }
            }
        }

        // Start new step
        new_vertex_ordering.push(VertexId::Pattern(extension.pattern_extension));

        let mut new_pattern_metadata = self.pattern_metadata.clone();
        new_pattern_metadata.insert(extension.pattern_extension, extension.pattern_metadata);

        let mut new_remaining_patterns = self.remaining_patterns.clone();
        new_remaining_patterns.remove(&extension.pattern_extension);

        let mut new_produced_vars = HashSet::new();
        new_produced_vars.extend(
            graph.pattern_to_variable[&extension.pattern_extension]
                .iter()
                .copied()
                .filter(|&var| !new_vertex_ordering.contains(&VertexId::Variable(var))),
        );

        PartialCostPlan {
            vertex_ordering: new_vertex_ordering,
            pattern_metadata: new_pattern_metadata,
            remaining_patterns: new_remaining_patterns,
            cumulative_cost: self.cumulative_cost.chain(self.ongoing_step_cost),
            ongoing_step_cost: extension.step_cost,
            ongoing_step_produced_vars: new_produced_vars,
            ongoing_step_join_var: extension.step_join_var,
            projected_cost: extension.projected_cost,
        }
    }

    fn into_complete_plan(self) -> CompleteCostPlan {
        let mut final_vertex_ordering = self.vertex_ordering.clone();
        if let Some(join_var) = self.ongoing_step_join_var {
            final_vertex_ordering.push(VertexId::Variable(join_var));
            for var in self.ongoing_step_produced_vars.clone() {
                if var != join_var && !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    final_vertex_ordering.push(VertexId::Variable(var));
                }
            }
        } else {
            for var in self.ongoing_step_produced_vars.clone() {
                if !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    final_vertex_ordering.push(VertexId::Variable(var));
                }
            }
        }

        CompleteCostPlan {
            vertex_ordering: final_vertex_ordering,
            pattern_metadata: self.pattern_metadata.clone(),
            cumulative_cost: self.cumulative_cost.chain(self.ongoing_step_cost),
        }
    }
}

impl Eq for PartialCostPlan {}

impl PartialOrd for PartialCostPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartialCostPlan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.projected_cost.cost.partial_cmp(&other.projected_cost.cost).unwrap_or(Ordering::Greater)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct StepExtension {
    pattern_extension: PatternVertexId,
    pattern_metadata: CostMetaData,
    step_cost: CombinedCost,
    step_join_var: Option<VariableVertexId>,
    projected_cost: CombinedCost,
}

impl StepExtension {
    fn is_constraint(&self, graph: &Graph<'_>) -> bool {
        graph.elements[&VertexId::Pattern(self.pattern_extension)].is_constraint()
    }
}

impl Eq for StepExtension {}

impl PartialOrd for StepExtension {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StepExtension {
    fn cmp(&self, other: &Self) -> Ordering {
        self.projected_cost.cost.partial_cmp(&other.projected_cost.cost).unwrap_or(Ordering::Greater)
    }
}

#[derive(Clone)]
pub(super) struct ConjunctionPlan<'a> {
    shared_variables: Vec<Variable>,
    graph: Graph<'a>,
    type_annotations: &'a TypeAnnotations,
    ordering: Vec<VertexId>, //TODO: replace with the CostPlan
    metadata: HashMap<PatternVertexId, CostMetaData>,
    element_to_order: HashMap<VertexId, usize>,
    cost: ElementCost,
}

impl fmt::Debug for ConjunctionPlan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name_of_val(self))
            .field("shared_variables", &self.shared_variables)
            .field("graph", &self.graph)
            .field("ordering", &self.ordering)
            .field("cost", &self.cost)
            .finish()
    }
}

impl ConjunctionPlan<'_> {
    pub(crate) fn lower(
        &self,
        input_variables: impl IntoIterator<Item = Variable> + Clone,
        selected_variables: impl IntoIterator<Item = Variable> + Clone,
        already_assigned_positions: &HashMap<Variable, ExecutorVariable>,
        variable_registry: &VariableRegistry,
    ) -> MatchExecutableBuilder {
        let mut match_builder = MatchExecutableBuilder::new(
            already_assigned_positions,
            selected_variables.clone().into_iter().collect(),
            input_variables.into_iter().collect(),
        );

        for &index in &self.ordering {
            match index {
                VertexId::Variable(var) => {
                    self.may_make_variable_producing_step(&mut match_builder, var, variable_registry);
                }
                VertexId::Pattern(pattern) => {
                    for input in self.inputs_of_pattern(pattern) {
                        let order = self.element_to_order[&VertexId::Pattern(pattern)];
                        let is_last_consumer = self
                            .consumers_of_var(input)
                            .all(|pat| self.element_to_order[&VertexId::Pattern(pat)] <= order);
                        if is_last_consumer {
                            match_builder.finish_one();
                            match_builder.remove_output(self.graph.index_to_variable[&input])
                        }
                    }
                    for output in self.outputs_of_pattern(pattern) {
                        let is_selected =
                            || match_builder.selected_variables.contains(&self.graph.index_to_variable[&output]);
                        let has_consumers = || self.consumers_of_var(output).next().is_some();
                        if is_selected() || has_consumers() {
                            match_builder.finish_one();
                            match_builder.register_output(self.graph.index_to_variable[&output]);
                        } else {
                            match_builder.register_internal(self.graph.index_to_variable[&output]);
                        }
                    }
                    if self.outputs_of_pattern(pattern).next().is_none() {
                        self.may_make_check_step(&mut match_builder, pattern, variable_registry);
                    }
                }
            }
        }

        match_builder
    }

    fn producers_of_var(&self, input: VariableVertexId) -> impl Iterator<Item = PatternVertexId> + '_ {
        let order = self.element_to_order[&VertexId::Variable(input)];
        self.graph.variable_to_pattern[&input]
            .iter()
            .copied()
            .filter(move |&adj| self.element_to_order[&VertexId::Pattern(adj)] < order)
    }

    fn consumers_of_var(&self, input: VariableVertexId) -> impl Iterator<Item = PatternVertexId> + '_ {
        let order = self.element_to_order[&VertexId::Variable(input)];
        self.graph.variable_to_pattern[&input]
            .iter()
            .copied()
            .filter(move |&adj| self.element_to_order[&VertexId::Pattern(adj)] > order)
    }

    fn inputs_of_pattern(&self, pattern: PatternVertexId) -> impl Iterator<Item = VariableVertexId> + '_ {
        let order = self.element_to_order[&VertexId::Pattern(pattern)];
        self.graph.pattern_to_variable[&pattern]
            .iter()
            .copied()
            .filter(move |&adj| self.element_to_order[&VertexId::Variable(adj)] < order)
    }

    fn outputs_of_pattern(&self, pattern: PatternVertexId) -> impl Iterator<Item = VariableVertexId> + '_ {
        let order = self.element_to_order[&VertexId::Pattern(pattern)];
        self.graph.pattern_to_variable[&pattern]
            .iter()
            .copied()
            .filter(move |&adj| self.element_to_order[&VertexId::Variable(adj)] > order)
    }

    fn may_make_variable_producing_step(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        var: VariableVertexId,
        variable_registry: &VariableRegistry,
    ) {
        if self.graph.elements[&VertexId::Variable(var)].as_variable().unwrap().is_input() {
            return;
        }

        let variable = self.graph.index_to_variable[&var];
        if match_builder.produced_so_far.contains(&variable) {
            return;
        }

        for producer in self.producers_of_var(var) {
            match &self.graph.elements()[&VertexId::Pattern(producer)] {
                PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {producer:?}"),
                PlannerVertex::Negation(_) => unreachable!("encountered negation registered as producing variable"),
                PlannerVertex::Is(is) => {
                    let input = if var == is.lhs {
                        self.graph.index_to_variable[&is.rhs]
                    } else {
                        self.graph.index_to_variable[&is.lhs]
                    };
                    let instruction =
                        ConstraintInstruction::Is(IsInstruction::new(is.is().clone(), Inputs::Single([input])));
                    match_builder.push_instruction(variable, instruction);
                }
                PlannerVertex::Comparison(_) => unreachable!("encountered comparison registered as producing variable"),
                PlannerVertex::Constraint(constraint) => {
                    let inputs =
                        self.inputs_of_pattern(producer).map(|var| self.graph.index_to_variable[&var]).collect_vec();
                    self.lower_constraint(match_builder, constraint, self.metadata[&producer], inputs, variable)
                }
                PlannerVertex::Expression(expression) => {
                    let output = match_builder.position_mapping()[&self.graph.index_to_variable[&expression.output]];
                    let mapping = match_builder
                        .position_mapping()
                        .iter()
                        .filter_map(|(&k, &v)| Some((k, v.as_position()?)))
                        .collect();
                    match_builder.push_step(
                        &HashMap::new(),
                        StepInstructionsBuilder::Expression(ExpressionBuilder {
                            executable_expression: expression.expression.clone().map(&mapping),
                            output,
                        })
                        .into(),
                    )
                }
                PlannerVertex::Disjunction(disjunction) => {
                    let step_builder = disjunction
                        .builder()
                        .clone() // FIXME
                        .plan(match_builder.produced_so_far.iter().filter(|&&v| v != variable).copied())
                        .lower(
                            match_builder.produced_so_far.iter().copied(),
                            match_builder.current_outputs.iter().copied(),
                            match_builder.position_mapping(),
                            variable_registry,
                        );
                    let variable_positions = step_builder.branches.iter().flat_map(|x| x.index.clone()).collect();
                    match_builder
                        .push_step(&variable_positions, StepInstructionsBuilder::Disjunction(step_builder).into());
                }
                PlannerVertex::FunctionCall(call_planner) => {
                    let call_binding = call_planner.call_binding;
                    let assigned = call_binding
                        .assigned()
                        .iter()
                        .map(|variable| {
                            match_builder
                                .index
                                .get(&variable.as_variable().unwrap())
                                .unwrap()
                                .clone()
                                .as_position()
                                .unwrap()
                        })
                        .collect();
                    let arguments = call_binding
                        .function_call()
                        .argument_ids()
                        .map(|variable| match_builder.index.get(&variable).unwrap().clone().as_position().unwrap())
                        .collect();
                    let step_builder = StepInstructionsBuilder::FunctionCall(FunctionCallBuilder {
                        function_id: call_binding.function_call().function_id(),
                        arguments,
                        assigned,
                        output_width: match_builder.next_output.position,
                    });
                    match_builder.push_step(&HashMap::new(), step_builder.into())
                }
            }
        }
        match_builder.finish_one()
    }

    fn may_make_check_step(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        pattern: PatternVertexId,
        variable_registry: &VariableRegistry,
    ) {
        match &self.graph.elements()[&VertexId::Pattern(pattern)] {
            PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {pattern:?}"),
            PlannerVertex::FunctionCall(_) => {
                unreachable!("variable assigned to from functions cannot be produced by other instructions")
            }
            PlannerVertex::Negation(negation) => {
                let negation = negation.plan().lower(
                    match_builder.produced_so_far.iter().copied(),
                    match_builder.current_outputs.iter().copied(),
                    match_builder.position_mapping(),
                    variable_registry,
                );
                let variable_positions = negation.index.clone(); // FIXME needless clone
                match_builder.push_step(
                    &variable_positions,
                    StepInstructionsBuilder::Negation(NegationBuilder::new(negation)).into(),
                );
            }
            PlannerVertex::Is(is) => {
                let lhs = is.is().lhs().as_variable().unwrap();
                let rhs = is.is().rhs().as_variable().unwrap();
                let check = CheckInstruction::Is { lhs, rhs }.map(match_builder.position_mapping());
                match_builder.push_check(&[lhs, rhs], check)
            }
            PlannerVertex::Comparison(comparison) => {
                let comparison = comparison.comparison();
                let lhs = comparison.lhs();
                let rhs = comparison.rhs();
                let comparator = comparison.comparator();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();
                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();
                assert!(num_input_variables > 0);

                let order = self.element_to_order[&VertexId::Pattern(pattern)];
                let inputs = self.graph.pattern_to_variable[&pattern]
                    .iter()
                    .copied()
                    .filter(move |&adj| self.ordering[..order].contains(&VertexId::Variable(adj)))
                    .map(|var| self.graph.index_to_variable[&var]);

                assert_eq!(inputs.count(), num_input_variables);

                let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                let rhs_pos = rhs.clone().map(match_builder.position_mapping());

                let check = CheckInstruction::Comparison {
                    lhs: CheckVertex::resolve(lhs_pos, self.type_annotations),
                    rhs: CheckVertex::resolve(rhs_pos, self.type_annotations),
                    comparator,
                };

                let vars = [lhs_var, rhs_var].into_iter().flatten().collect_vec();
                match_builder.push_check(&vars, check);
            }
            PlannerVertex::Constraint(constraint) => {
                self.lower_constraint_check(match_builder, constraint);
            }
            PlannerVertex::Expression(_) => todo!(),
            PlannerVertex::Disjunction(disjunction) => {
                let step_builder = disjunction
                    .builder()
                    .clone() // FIXME
                    .plan(match_builder.position_mapping().keys().copied())
                    .lower(
                        match_builder.produced_so_far.iter().copied(),
                        match_builder.current_outputs.iter().copied(),
                        match_builder.position_mapping(),
                        variable_registry,
                    );
                let variable_positions = step_builder.branches.iter().flat_map(|x| x.index.clone()).collect();
                match_builder.push_step(&variable_positions, StepInstructionsBuilder::Disjunction(step_builder).into());
            }
        }
    }

    fn lower_constraint(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        constraint: &ConstraintVertex<'_>,
        metadata: CostMetaData,
        inputs: Vec<Variable>,
        sort_variable: Variable,
    ) {
        if let Some(StepBuilder {
            builder:
                StepInstructionsBuilder::Intersection(IntersectionBuilder { sort_variable: Some(sort_variable), .. }),
            ..
        }) = match_builder.current.as_deref()
        {
            if !constraint.variables().contains(&self.graph.variable_index[sort_variable]) {
                match_builder.finish_one();
            }
        }

        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs_var = $con.$lhs().as_variable();
                let rhs_var = $con.$rhs().as_variable();

                let inputs =
                    match (lhs_var.filter(|lhs| inputs.contains(&lhs)), rhs_var.filter(|rhs| inputs.contains(&rhs))) {
                        (Some(lhs), Some(rhs)) => Inputs::Dual([lhs, rhs]), // useful for links
                        (Some(var), None) | (None, Some(var)) => Inputs::Single([var]),
                        (None, None) => Inputs::None([]),
                    };

                let CostMetaData::Direction(direction) = metadata else {
                    unreachable!("expected metadata for constraint")
                };

                let con = $con.clone();
                let instruction = match direction {
                    Direction::Canonical => ConstraintInstruction::$fw($fwi::new(con, inputs, self.type_annotations)),
                    Direction::Reverse => ConstraintInstruction::$bw($bwi::new(con, inputs, self.type_annotations)),
                };

                match_builder.push_instruction(sort_variable, instruction);
            }};
        }

        match constraint {
            ConstraintVertex::TypeList(type_list) => {
                let var = type_list.constraint().var();
                let instruction = type_list.lower();
                match_builder.push_instruction(var, instruction);
            }

            ConstraintVertex::Iid(iid) => {
                let var = iid.iid().var().as_variable().unwrap();
                let instruction =
                    ConstraintInstruction::Iid(IidInstruction::new(iid.iid().clone(), self.type_annotations));
                match_builder.push_instruction(var, instruction);
            }

            ConstraintVertex::Sub(planner) => {
                let sub = planner.sub();
                binary!((with sub_kind) subtype sub supertype, Sub(SubInstruction), SubReverse(SubReverseInstruction))
            }
            ConstraintVertex::Owns(planner) => {
                let owns = planner.owns();
                binary!(owner owns attribute, Owns(OwnsInstruction), OwnsReverse(OwnsReverseInstruction))
            }
            ConstraintVertex::Relates(planner) => {
                let relates = planner.relates();
                binary!(relation relates role_type, Relates(RelatesInstruction), RelatesReverse(RelatesReverseInstruction))
            }
            ConstraintVertex::Plays(planner) => {
                let plays = planner.plays();
                binary!(player plays role_type, Plays(PlaysInstruction), PlaysReverse(PlaysReverseInstruction))
            }

            ConstraintVertex::Isa(planner) => {
                let isa = planner.isa();
                binary!((with isa_kind) thing isa type_, Isa(IsaInstruction), IsaReverse(IsaReverseInstruction))
            }
            ConstraintVertex::Has(planner) => {
                let has = planner.has();
                binary!(owner has attribute, Has(HasInstruction), HasReverse(HasReverseInstruction))
            }
            ConstraintVertex::Links(planner) => {
                let links = planner.links();
                // binary!() works here even though links is ostensibly ternary
                binary!(relation links player, Links(LinksInstruction), LinksReverse(LinksReverseInstruction))
            }
        }
    }

    fn lower_constraint_check(&self, match_builder: &mut MatchExecutableBuilder, constraint: &ConstraintVertex<'_>) {
        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs = $con.$lhs();
                let rhs = $con.$rhs();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();

                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();

                assert!(num_input_variables > 0);

                let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                let rhs_pos = rhs.clone().map(match_builder.position_mapping());
                let check = CheckInstruction::$fw {
                    $lhs: CheckVertex::resolve(lhs_pos, self.type_annotations),
                    $rhs: CheckVertex::resolve(rhs_pos, self.type_annotations),
                    $($with: $con.$with(),)?
                };

                let vars = [lhs_var, rhs_var].into_iter().flatten().collect_vec();
                match_builder.push_check(&vars, check);
            }};
        }

        match constraint {
            ConstraintVertex::TypeList(type_list) => {
                let var = type_list.constraint().var();
                let instruction = type_list.lower_check();
                match_builder.push_check(&[var], instruction.map(match_builder.position_mapping()));
            }

            ConstraintVertex::Iid(iid) => {
                let var = iid.iid().var().as_variable().unwrap();
                let instruction = CheckInstruction::Iid { var, iid: iid.iid().iid().as_parameter().unwrap() };
                match_builder.push_check(&[var], instruction.map(match_builder.position_mapping()));
            }

            ConstraintVertex::Sub(planner) => {
                let sub = planner.sub();
                binary!((with sub_kind) subtype sub supertype, Sub(SubInstruction), SubReverse(SubReverseInstruction))
            }
            ConstraintVertex::Owns(planner) => {
                let owns = planner.owns();
                binary!(owner owns attribute, Owns(OwnsInstruction), OwnsReverse(OwnsReverseInstruction))
            }
            ConstraintVertex::Relates(planner) => {
                let relates = planner.relates();
                binary!(relation relates role_type, Relates(RelatesInstruction), RelatesReverse(RelatesReverseInstruction))
            }
            ConstraintVertex::Plays(planner) => {
                let plays = planner.plays();
                binary!(player plays role_type, Plays(PlaysInstruction), PlaysReverse(PlaysReverseInstruction))
            }

            ConstraintVertex::Isa(planner) => {
                let isa = planner.isa();
                binary!((with isa_kind) thing isa type_, Isa(IsaInstruction), IsaReverse(IsaReverseInstruction))
            }
            ConstraintVertex::Has(planner) => {
                let has = planner.has();
                binary!(owner has attribute, Has(HasInstruction), HasReverse(HasReverseInstruction))
            }
            ConstraintVertex::Links(planner) => {
                let links = planner.links();

                let relation = links.relation().as_variable().unwrap();
                let player = links.player().as_variable().unwrap();
                let role = links.role_type().as_variable().unwrap();

                let relation_pos = match_builder.position(relation).into();
                let player_pos = match_builder.position(player).into();
                let role_pos = match_builder.position(role).into();

                let check = CheckInstruction::Links {
                    relation: CheckVertex::resolve(relation_pos, self.type_annotations),
                    player: CheckVertex::resolve(player_pos, self.type_annotations),
                    role: CheckVertex::resolve(role_pos, self.type_annotations),
                };

                match_builder.push_check(&[relation, player, role], check);
            }
        }
    }

    pub(super) fn cost(&self) -> ElementCost {
        self.cost
    }

    pub(super) fn combined_cost(&self) -> CombinedCost {
        todo!()
    }

    pub(super) fn shared_variables(&self) -> &[Variable] {
        &self.shared_variables
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlanBuilder<'a> {
    branches: Vec<ConjunctionPlanBuilder<'a>>,
}

impl<'a> DisjunctionPlanBuilder<'a> {
    pub(super) fn new(branches: Vec<ConjunctionPlanBuilder<'a>>) -> Self {
        Self { branches }
    }

    pub(super) fn branches(&self) -> &[ConjunctionPlanBuilder<'a>] {
        &self.branches
    }

    fn plan(self, input_variables: impl Iterator<Item = Variable> + Clone) -> DisjunctionPlan<'a> {
        let branches =
            self.branches.into_iter().map(|branch| branch.with_inputs(input_variables.clone()).plan()).collect_vec();
        let cost = branches.iter().map(ConjunctionPlan::cost).fold(ElementCost::EMPTY, ElementCost::combine_parallel);
        DisjunctionPlan { branches, _cost: cost }
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlan<'a> {
    branches: Vec<ConjunctionPlan<'a>>,
    _cost: ElementCost,
}

impl DisjunctionPlan<'_> {
    fn lower(
        &self,
        disjunction_inputs: impl IntoIterator<Item = Variable> + Clone,
        selected_variables: impl IntoIterator<Item = Variable> + Clone,
        assigned_positions: &HashMap<Variable, ExecutorVariable>,
        variable_registry: &VariableRegistry,
    ) -> DisjunctionBuilder {
        let mut branches: Vec<_> = Vec::with_capacity(self.branches.len());
        let mut assigned_positions = assigned_positions.clone();
        for branch in &self.branches {
            let lowered_branch = branch.lower(
                disjunction_inputs.clone(),
                selected_variables.clone(),
                &assigned_positions,
                variable_registry,
            );
            assigned_positions = lowered_branch.position_mapping().clone();
            branches.push(lowered_branch);
        }
        DisjunctionBuilder::new(branches)
    }
}

#[derive(Clone, Default)]
pub(super) struct Graph<'a> {
    variable_to_pattern: HashMap<VariableVertexId, HashSet<PatternVertexId>>,
    pattern_to_variable: HashMap<PatternVertexId, HashSet<VariableVertexId>>,

    elements: HashMap<VertexId, PlannerVertex<'a>>,

    variable_index: HashMap<Variable, VariableVertexId>,
    index_to_variable: HashMap<VariableVertexId, Variable>,

    next_variable_id: VariableVertexId,
    next_pattern_id: PatternVertexId,
}

impl fmt::Debug for Graph<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name_of_val(self))
            .field("variable_index", &self.variable_index)
            .field("elements", &self.elements)
            .field("pattern_to_variable", &self.pattern_to_variable)
            .field("variable_to_pattern", &self.variable_to_pattern)
            .finish()
    }
}

impl<'a> Graph<'a> {
    fn element_count(&self) -> usize {
        self.variable_to_pattern.len() + self.pattern_to_variable.len()
    }

    fn push_variable(&mut self, variable: Variable, vertex: VariableVertex) {
        let index = self.next_variable_index();
        self.elements.insert(VertexId::Variable(index), PlannerVertex::Variable(vertex));
        self.variable_index.insert(variable, index);
        self.index_to_variable.insert(index, variable);
    }

    fn push_constraint(&mut self, constraint: ConstraintVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(constraint.variables());
        for var in constraint.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Constraint(constraint));
    }

    fn push_is(&mut self, is: IsPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(is.variables());
        for var in is.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Is(is));
    }

    fn push_comparison(&mut self, comparison: ComparisonPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(comparison.variables());
        for var in comparison.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Comparison(comparison));
    }

    fn push_expression(&mut self, output: VariableVertexId, expression: ExpressionPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(expression.variables());
        for var in expression.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Expression(expression));

        let output_planner = self.elements.get_mut(&VertexId::Variable(output)).unwrap();
        output_planner.as_variable_mut().unwrap().set_binding(pattern_index);
    }

    fn push_function_call(&mut self, function_call: FunctionCallPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(function_call.variables());
        for var in function_call.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        let assigned = function_call.assigned.clone();
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::FunctionCall(function_call));
        assigned.into_iter().for_each(|vertex| {
            let output_planner = self.elements.get_mut(&VertexId::Variable(vertex)).unwrap();
            output_planner.as_variable_mut().unwrap().set_binding(pattern_index);
        })
    }

    fn push_disjunction(&mut self, disjunction: DisjunctionPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(disjunction.variables());
        for var in disjunction.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Disjunction(disjunction));
    }

    fn push_negation(&mut self, negation: NegationPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(negation.variables());
        for var in negation.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Negation(negation));
    }

    fn next_variable_index(&mut self) -> VariableVertexId {
        let variable_index = self.next_variable_id;
        self.next_variable_id.0 += 1;
        variable_index
    }

    fn next_pattern_index(&mut self) -> PatternVertexId {
        let pattern_index = self.next_pattern_id;
        self.next_pattern_id.0 += 1;
        pattern_index
    }

    pub(super) fn variable_to_pattern(&self) -> &HashMap<VariableVertexId, HashSet<PatternVertexId>> {
        &self.variable_to_pattern
    }

    pub(super) fn elements(&self) -> &HashMap<VertexId, PlannerVertex<'a>> {
        &self.elements
    }
}
