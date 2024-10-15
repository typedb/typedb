/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{
            Comparator, Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Isa, Kind, Label, Links,
            Owns, Plays, Relates, RoleName, Sub,
        },
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Vertex,
    },
    pipeline::{block::BlockContext, VariableRegistry},
};
use itertools::{chain, Itertools};

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::{
        instructions::{
            thing::{
                HasInstruction, HasReverseInstruction, IsaInstruction, IsaReverseInstruction, LinksInstruction,
                LinksReverseInstruction,
            },
            type_::{
                OwnsInstruction, OwnsReverseInstruction, PlaysInstruction, PlaysReverseInstruction, RelatesInstruction,
                RelatesReverseInstruction, SubInstruction, SubReverseInstruction,
            },
            CheckInstruction, CheckVertex, ConstraintInstruction, Inputs,
        },
        planner::{
            match_executable::MatchExecutable,
            vertex::{
                constraint::{
                    ConstraintVertex, HasPlanner, IsaPlanner, LinksPlanner, OwnsPlanner, PlaysPlanner, RelatesPlanner,
                    SubPlanner, TypeListPlanner,
                },
                variable::{InputPlanner, ThingPlanner, TypePlanner, ValuePlanner, VariableVertex},
                ComparisonPlanner, Costed, Direction, DisjunctionPlanner, ElementCost, FunctionCallPlanner, Input,
                NegationPlanner, PlannerVertex,
            },
            DisjunctionBuilder, IntersectionBuilder, MatchExecutableBuilder, NegationBuilder, StepBuilder,
        },
    },
    VariablePosition,
};

pub(crate) fn plan_conjunction<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    variable_positions: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a TypeAnnotations,
    variable_registry: &VariableRegistry,
    _expressions: &HashMap<Variable, ExecutableExpression>,
    statistics: &'a Statistics,
) -> ConjunctionPlan<'a> {
    let plan_builder = make_builder(
        conjunction,
        block_context,
        variable_positions,
        type_annotations,
        variable_registry,
        _expressions,
        statistics,
    );

    plan_builder.plan()
}

fn make_builder<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    variable_positions: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a TypeAnnotations,
    variable_registry: &VariableRegistry,
    _expressions: &HashMap<Variable, ExecutableExpression>,
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
                            input_variables,
                            type_annotations,
                            variable_registry,
                            _expressions,
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
                    _expressions,
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
        input_variables.keys().copied(),
        conjunction.captured_variables(block_context),
        conjunction.declared_variables(block_context),
        variable_registry,
    );
    plan_builder.register_constraints(conjunction);
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
    shared_variables: Vec<VariableVertexId>,
    graph: Graph<'a>,
    type_annotations: &'a TypeAnnotations,
    statistics: &'a Statistics,
}

impl<'a> fmt::Debug for ConjunctionPlanBuilder<'a> {
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

    pub(super) fn shared_variables(&self) -> &[VariableVertexId] {
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
        // self.elements.reserve(shared_variables.size_hint().0 + local_variables.size_hint().0);
        self.shared_variables.reserve(input_variables.size_hint().0 + shared_variables.size_hint().0);

        for variable in input_variables {
            // FIXME shared variables aren't necessarily bound before the conjunction is entered
            self.register_input_var(variable);
        }

        for variable in shared_variables {
            let category = variable_registry.get_variable_category(variable).unwrap();
            let index = match category {
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
            };
            self.shared_variables.push(index);
        }

        for variable in local_variables {
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
            };
        }
    }

    fn register_input_var(&mut self, variable: Variable) {
        let planner = InputPlanner::from_variable(variable);
        let index = self.graph.push_variable(variable, VariableVertex::Input(planner));
        self.shared_variables.push(index);
    }

    fn register_type_var(&mut self, variable: Variable) -> VariableVertexId {
        let planner = TypePlanner::from_variable(variable, self.type_annotations);
        self.graph.push_variable(variable, VariableVertex::Type(planner))
    }

    fn register_thing_var(&mut self, variable: Variable) -> VariableVertexId {
        let planner = ThingPlanner::from_variable(variable, self.type_annotations, self.statistics);
        self.graph.push_variable(variable, VariableVertex::Thing(planner))
    }

    fn register_value_var(&mut self, variable: Variable) -> VariableVertexId {
        let planner = ValuePlanner::from_variable(variable);
        self.graph.push_variable(variable, VariableVertex::Value(planner))
    }

    fn register_constraints(&mut self, conjunction: &'a Conjunction) {
        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::Kind(kind) => self.register_kind(kind),
                Constraint::RoleName(role_name) => self.register_role_name(role_name),
                Constraint::Label(label) => self.register_label(label),

                Constraint::Sub(sub) => self.register_sub(sub),
                Constraint::Owns(owns) => self.register_owns(owns),
                Constraint::Relates(relates) => self.register_relates(relates),
                Constraint::Plays(plays) => self.register_plays(plays),

                Constraint::Isa(isa) => self.register_isa(isa),
                Constraint::Has(has) => self.register_has(has),
                Constraint::Links(links) => self.register_links(links),

                Constraint::FunctionCallBinding(call) => self.register_function_call_binding(call),

                Constraint::Comparison(comparison) => self.register_comparison(comparison),

                Constraint::ExpressionBinding(expression) => self.register_expression_binding(expression),
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

    fn register_isa(&mut self, isa: &'a Isa<Variable>) {
        let planner =
            IsaPlanner::from_constraint(isa, &self.graph.variable_index, self.type_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Isa(planner));
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

    fn register_expression_binding(&mut self, expression: &ExpressionBinding<Variable>) {
        // let lhs = self.variable_index[&expression.left().as_variable().unwrap()];
        // let planner_index = self.elements.len();
        // self.adjacency.entry(lhs).or_default().insert(planner_index);
        todo!("expression = {expression:?}");
        // self.elements.push(PlannerVertex::Expression());
    }

    fn register_function_call_binding(&mut self, call_binding: &FunctionCallBinding<Variable>) {
        // TODO: This is just a mock
        let arguments =
            call_binding.function_call().argument_ids().map(|variable| self.variable_index[&variable]).collect();
        let return_vars = call_binding
            .assigned()
            .iter()
            .map(|vertex| {
                let Vertex::Variable(variable) = vertex else { todo!("Unreachable?") };
                self.variable_index[variable]
            })
            .collect();
        let element_cost = ElementCost { per_input: 1.0, per_output: 1.0, branching_factor: 1.0 };
        self.elements.push(PlannerVertex::FunctionCall(FunctionCallPlanner::new(arguments, return_vars, element_cost)));
        todo!("register_function_call");
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
            self.graph.push_disjunction(DisjunctionPlanner::from_builder(disjunction));
        }
    }

    fn register_negations(&mut self, negations: Vec<ConjunctionPlan<'a>>) {
        for negation_plan in negations {
            self.graph.push_negation(NegationPlanner::new(negation_plan));
        }
    }

    fn initialise_greedy_ordering(&self) -> Vec<VertexId> {
        let mut open_set: HashSet<VertexId> = chain!(
            self.graph.variable_to_pattern.keys().map(|&variable_id| VertexId::Variable(variable_id)),
            self.graph.pattern_to_variable.keys().map(|&pattern_id| VertexId::Pattern(pattern_id))
        )
        .collect();
        let mut ordering = Vec::with_capacity(self.graph.element_count());
        for v in self.input_variables() {
            ordering.push(VertexId::Variable(v));
            open_set.remove(&VertexId::Variable(v));
        }

        let mut produced_at_this_stage: HashSet<VariableVertexId> = HashSet::new();
        let mut intersection_variable: Option<VariableVertexId> = None;

        while !open_set.is_empty() {
            let (next, _cost) = open_set
                .iter()
                .filter(|&&elem| self.graph.elements[&elem].is_valid(elem, &ordering, &self.graph))
                .map(|&elem| (elem, self.calculate_marginal_cost(&ordering, elem)))
                .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
                .unwrap();

            if intersection_variable == next.as_variable_id() {
                intersection_variable = None;
            }

            let element = &self.graph.elements[&next];

            if !element.is_variable() {
                match element.variables().filter(|var| produced_at_this_stage.contains(var)).exactly_one() {
                    Ok(var) if intersection_variable.is_none() || intersection_variable == Some(var) => {
                        intersection_variable = Some(var);
                    }
                    _ => {
                        for var in produced_at_this_stage.drain().map(VertexId::Variable) {
                            if !ordering.contains(&var) {
                                ordering.push(var);
                                open_set.remove(&var);
                            }
                        }
                        intersection_variable = None;
                    }
                }

                produced_at_this_stage.extend(element.variables());

                ordering.push(next);
                open_set.remove(&next);
            } else {
                for var in produced_at_this_stage.drain().map(VertexId::Variable) {
                    if !ordering.contains(&var) {
                        ordering.push(var);
                        open_set.remove(&var);
                    }
                }
                intersection_variable = None;
            }
        }
        ordering
    }

    fn calculate_marginal_cost(&self, prefix: &[VertexId], next: VertexId) -> f64 {
        assert!(!prefix.contains(&next));
        let planner_vertex = &self.graph.elements[&next];
        let ElementCost { per_input, per_output, branching_factor } = planner_vertex.cost(prefix, &self.graph);
        per_input + branching_factor * per_output
    }

    pub(super) fn plan(self) -> ConjunctionPlan<'a> {
        let ordering = self.initialise_greedy_ordering();

        let element_to_order = ordering.iter().copied().enumerate().map(|(order, index)| (index, order)).collect();

        let cost = ordering
            .iter()
            .enumerate()
            .map(|(i, idx)| self.graph.elements[idx].cost(&ordering[..i], &self.graph))
            .fold(ElementCost::FREE, |acc, e| acc.chain(e));

        let Self { shared_variables, graph, type_annotations, statistics: _ } = self;

        ConjunctionPlan { shared_variables, graph, type_annotations, ordering, element_to_order, cost }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ConjunctionPlan<'a> {
    shared_variables: Vec<VariableVertexId>,
    graph: Graph<'a>,
    type_annotations: &'a TypeAnnotations,
    ordering: Vec<VertexId>,
    element_to_order: HashMap<VertexId, usize>,
    cost: ElementCost,
}

impl ConjunctionPlan<'_> {
    pub(crate) fn lower(
        &self,
        selected_variables: &[Variable],
        assigned_positions: &HashMap<Variable, VariablePosition>,
        variable_registry: Arc<VariableRegistry>,
    ) -> MatchExecutable {
        let mut match_builder = MatchExecutableBuilder::new(assigned_positions, selected_variables.to_vec());

        for &index in &self.ordering {
            match index {
                VertexId::Variable(var) => {
                    self.may_make_variable_producing_step(
                        &mut match_builder,
                        var,
                        selected_variables,
                        variable_registry.clone(),
                    );
                }
                VertexId::Pattern(pattern) => {
                    let order = self.element_to_order[&VertexId::Pattern(pattern)];
                    let outputs = self.graph.pattern_to_variable[&pattern]
                        .iter()
                        .copied()
                        .filter(move |&adj| self.ordering[order..].contains(&VertexId::Variable(adj)))
                        .map(|var| self.graph.index_to_variable[&var]);
                    if outputs.count() > 0 {
                        continue;
                    }
                    self.may_make_check_step(
                        &mut match_builder,
                        pattern,
                        selected_variables,
                        variable_registry.clone(),
                    );
                }
            }
        }

        match_builder.finish(variable_registry)
    }

    fn may_make_variable_producing_step(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        var: VariableVertexId,
        selected_variables: &[Variable],
        variable_registry: Arc<VariableRegistry>,
    ) {
        let variable = self.graph.index_to_variable[&var];
        if match_builder.producers.contains_key(&variable) {
            return;
        }

        let order = self.element_to_order[&VertexId::Variable(var)];
        let producers = self.graph.variable_to_pattern[&var]
            .iter()
            .copied()
            .filter(move |&adj| self.ordering[..order].contains(&VertexId::Pattern(adj)));

        for producer in producers {
            match &self.graph.elements()[&VertexId::Pattern(producer)] {
                PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {producer:?}"),
                PlannerVertex::Negation(_) => unreachable!("encountered negation registered as producing variable"),
                PlannerVertex::Comparison(_) => unreachable!("encountered comparison registered as producing variable"),
                PlannerVertex::Constraint(constraint) => {
                    let order = self.element_to_order[&VertexId::Pattern(producer)];
                    let inputs = self.graph.pattern_to_variable[&producer]
                        .iter()
                        .copied()
                        .filter(move |&adj| self.ordering[..order].contains(&VertexId::Variable(adj)))
                        .map(|var| self.graph.index_to_variable[&var])
                        .collect_vec();
                    self.lower_constraint(match_builder, constraint, inputs, variable)
                }
                PlannerVertex::Expression(_) => todo!(),
                PlannerVertex::Disjunction(disjunction) => {
                    let step_builder = disjunction
                        .builder()
                        .clone() // FIXME
                        .plan(match_builder.position_mapping().keys().copied())
                        .lower(selected_variables, match_builder.position_mapping(), variable_registry.clone());
                    let variable_positions =
                        step_builder.branches.iter().flat_map(|x| x.variable_positions().clone()).collect();
                    match_builder.push_step(&variable_positions, StepBuilder::Disjunction(step_builder));
                }
            }
        }
    }

    fn may_make_check_step(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        pattern: PatternVertexId,
        selected_variables: &[Variable],
        variable_registry: Arc<VariableRegistry>,
    ) {
        match &self.graph.elements()[&VertexId::Pattern(pattern)] {
            PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {pattern:?}"),
            PlannerVertex::Negation(negation) => {
                let negation = negation.plan().lower(
                    selected_variables,
                    match_builder.position_mapping(),
                    variable_registry.clone(),
                );
                let variable_positions = negation.variable_positions().clone(); // FIXME needless clone
                match_builder.push_step(
                    &variable_positions,
                    StepBuilder::Negation(NegationBuilder::new(negation, match_builder.selected_variables.clone())),
                );
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
                    .lower(selected_variables, match_builder.position_mapping(), variable_registry.clone());
                let variable_positions =
                    step_builder.branches.iter().flat_map(|x| x.variable_positions().clone()).collect();
                match_builder.push_step(&variable_positions, StepBuilder::Disjunction(step_builder));
            }
        }
    }

    fn lower_constraint(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        constraint: &ConstraintVertex<'_>,
        inputs: Vec<Variable>,
        sort_variable: Variable,
    ) {
        if let Some(StepBuilder::Intersection(IntersectionBuilder { sort_variable: Some(sort_variable), .. })) =
            &match_builder.current
        {
            if !constraint.variables().contains(&self.graph.variable_index[sort_variable]) {
                match_builder.finish_one();
            }
        }

        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs = $con.$lhs();
                let rhs = $con.$rhs();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();

                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();

                assert!(num_input_variables > 0);

                assert_ne!(inputs.len(), num_input_variables);

                let con = $con.clone();
                let instruction = if lhs_var.is_some_and(|lhs| inputs.contains(&lhs)) {
                    ConstraintInstruction::$fw($fwi::new(
                        con,
                        Inputs::Single([lhs_var.unwrap()]),
                        self.type_annotations,
                    ))
                } else if rhs_var.is_some_and(|rhs| inputs.contains(&rhs)) {
                    ConstraintInstruction::$bw($bwi::new(
                        con,
                        Inputs::Single([rhs_var.unwrap()]),
                        self.type_annotations,
                    ))
                } else if constraint.unbound_direction() == Direction::Canonical {
                    ConstraintInstruction::$fw($fwi::new(con, Inputs::None([]), self.type_annotations))
                } else {
                    ConstraintInstruction::$bw($bwi::new(con, Inputs::None([]), self.type_annotations))
                };

                match_builder.push_instruction(sort_variable, instruction, [lhs_var, rhs_var].into_iter().flatten());
            }};
        }

        match constraint {
            ConstraintVertex::TypeList(tl) => {
                let var = tl.constraint().var();
                let instruction = tl.lower(self.type_annotations);
                match_builder.push_instruction(var, instruction, [var]);
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

                assert_ne!(inputs.len(), 3);

                let links = links.clone();
                let instruction = if inputs.contains(&relation) && inputs.contains(&player) {
                    if planner.unbound_direction() == Direction::Canonical {
                        ConstraintInstruction::Links(LinksInstruction::new(
                            links,
                            Inputs::Dual([relation, player]),
                            self.type_annotations,
                        ))
                    } else {
                        ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                            links,
                            Inputs::Dual([relation, player]),
                            self.type_annotations,
                        ))
                    }
                } else if inputs.contains(&relation) {
                    ConstraintInstruction::Links(LinksInstruction::new(
                        links,
                        Inputs::Single([relation]),
                        self.type_annotations,
                    ))
                } else if inputs.contains(&player) {
                    ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                        links,
                        Inputs::Single([player]),
                        self.type_annotations,
                    ))
                } else if planner.unbound_direction() == Direction::Canonical {
                    ConstraintInstruction::Links(LinksInstruction::new(links, Inputs::None([]), self.type_annotations))
                } else {
                    ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                        links,
                        Inputs::None([]),
                        self.type_annotations,
                    ))
                };

                match_builder.push_instruction(sort_variable, instruction, [relation, player, role]);
            }
        }
    }

    fn lower_constraint_check(&self, match_builder: &mut MatchExecutableBuilder, constraint: &ConstraintVertex<'_>) {
        match_builder.finish_one();

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
            ConstraintVertex::TypeList(_) => {
                todo!()
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

    pub(super) fn shared_variables(&self) -> &[VariableVertexId] {
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
        DisjunctionPlan { branches, cost }
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlan<'a> {
    branches: Vec<ConjunctionPlan<'a>>,
    cost: ElementCost,
}

impl<'a> DisjunctionPlan<'a> {
    fn lower(
        &self,
        selected_variables: &[Variable],
        assigned_positions: &HashMap<Variable, VariablePosition>,
        variable_registry: Arc<VariableRegistry>,
    ) -> DisjunctionBuilder {
        let mut branches: Vec<MatchExecutable> = Vec::with_capacity(self.branches.len());
        for branch in &self.branches {
            let assigned_positions = match branches.last() {
                Some(branch) => branch.variable_positions(),
                None => assigned_positions,
            };
            let lowered_branch = branch.lower(selected_variables, assigned_positions, variable_registry.clone());
            branches.push(lowered_branch);
        }
        DisjunctionBuilder::new(branches, selected_variables.to_vec())
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
        f.debug_struct("Graph").field("variable_index", &self.variable_index).field("elements", &self.elements).finish()
    }
}

impl<'a> Graph<'a> {
    fn element_count(&self) -> usize {
        self.variable_to_pattern.len() + self.pattern_to_variable.len()
    }

    fn push_variable(&mut self, variable: Variable, vertex: VariableVertex) -> VariableVertexId {
        let index = self.next_variable_id; // FIXME
        self.next_variable_id.0 += 1;

        self.elements.insert(VertexId::Variable(index), PlannerVertex::Variable(vertex));
        self.variable_index.insert(variable, index);
        self.index_to_variable.insert(index, variable);
        index
    }

    fn push_constraint(&mut self, constraint: ConstraintVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(constraint.variables());
        for var in constraint.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Constraint(constraint));
    }

    fn push_comparison(&mut self, comparison: ComparisonPlanner<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(comparison.variables());
        for var in comparison.variables() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Comparison(comparison));
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
