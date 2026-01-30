/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::type_name_of_val,
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    fmt,
    hash::Hash,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use error::{typedb_error, unimplemented_feature};
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{
            Comparator, Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Iid, IndexedRelation, Is,
            Isa, Kind, Label, Links, LinksDeduplication, Owns, Plays, Relates, RoleName, Sub, Unsatisfiable, Value,
        },
        nested_pattern::NestedPattern,
        variable_category::{VariableCategory, VariableOptionality},
        BranchID, Pattern, Scope, Vertex,
    },
    pipeline::{block::BlockContext, VariableRegistry},
};
use itertools::{chain, Itertools};
use tracing::{event, Level};

use crate::{
    annotation::{
        expression::compiled_expression::ExecutableExpression,
        type_annotations::{BlockAnnotations, TypeAnnotations},
    },
    executable::{
        function::FunctionCallCostProvider,
        match_::{
            instructions::{
                thing::{
                    HasInstruction, HasReverseInstruction, IidInstruction, IndexedRelationInstruction, IsaInstruction,
                    IsaReverseInstruction, LinksInstruction, LinksReverseInstruction,
                },
                type_::{
                    OwnsInstruction, OwnsReverseInstruction, PlaysInstruction, PlaysReverseInstruction,
                    RelatesInstruction, RelatesReverseInstruction, SubInstruction, SubReverseInstruction,
                },
                CheckInstruction, CheckVertex, ConstraintInstruction, Inputs, IsInstruction,
            },
            planner::{
                vertex::{
                    constraint::{
                        ConstraintVertex, HasPlanner, IidPlanner, IndexedRelationPlanner, IsaPlanner, LinksPlanner,
                        OwnsPlanner, PlaysPlanner, RelatesPlanner, SubPlanner, TypeListPlanner,
                    },
                    variable::{InputPlanner, ThingPlanner, TypePlanner, ValuePlanner, VariableVertex},
                    ComparisonVertex, Cost, CostMetaData, Costed, Direction, DisjunctionVertex, ExpressionVertex,
                    FunctionCallVertex, Input, IsVertex, LinksDeduplicationVertex, NegationVertex, OptionalVertex,
                    PlannerVertex, UnsatisfiableVertex,
                },
                ConjunctionExecutableBuilder, DisjunctionBuilder, ExpressionBuilder, FunctionCallBuilder,
                IntersectionBuilder, NegationBuilder, OptionalBuilder, StepBuilder, StepInstructionsBuilder,
            },
        },
    },
    ExecutorVariable, VariablePosition,
};

pub const MAX_BEAM_WIDTH: usize = 96;
pub const MIN_BEAM_WIDTH: usize = 1;
pub const AVERAGE_QUERY_OUTPUT_SIZE: f64 = 1.0; // replace with actual statistical estimate
pub const AVERAGE_STEP_COST: f64 = 1.0; // replace with actual heuristic
pub const VARIABLE_PRODUCTION_ADVANTAGE: f64 = 0.05; // this is a percentage 0.00 <= x < 1.00

typedb_error! {
    pub QueryPlanningError(component = "Query Planner", prefix = "QPL") {
        ExpectedPlannableConjunction(1, "Planning failed as no valid pattern ordering was found by the query planner (this is a bug!)"),
    }
}

pub(crate) fn plan_conjunction<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    stage_input_positions: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a BlockAnnotations,
    variable_registry: &VariableRegistry,
    expressions: &'a HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
    statistics: &'a Statistics,
    call_cost_provider: &'a impl FunctionCallCostProvider,
) -> Result<ConjunctionPlan<'a>, QueryPlanningError> {
    make_builder(
        conjunction,
        block_context,
        stage_input_positions,
        type_annotations,
        variable_registry,
        expressions,
        statistics,
        call_cost_provider,
    )?
    .plan()
}

fn make_builder<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    stage_inputs: &HashMap<Variable, VariablePosition>,
    block_annotations: &'a BlockAnnotations,
    variable_registry: &VariableRegistry,
    expressions: &'a HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
    statistics: &'a Statistics,
    call_cost_provider: &impl FunctionCallCostProvider,
) -> Result<ConjunctionPlanBuilder<'a>, QueryPlanningError> {
    let mut negation_subplans = Vec::new();
    let mut optional_subplans = Vec::new();
    let mut disjunction_planners = Vec::new();
    for pattern in conjunction.nested_patterns() {
        match pattern {
            NestedPattern::Disjunction(disjunction) => {
                let disjunction_dependency_modes = disjunction.variable_binding_modes();
                let required_inputs = disjunction_dependency_modes
                    .iter()
                    .filter(|(v, mode)| {
                        (mode.is_locally_binding_in_child() || mode.is_require_prebound())
                            && block_context.is_variable_available_in(conjunction.scope_id(), **v)
                    })
                    .map(|(v, _)| *v)
                    .chain(disjunction.required_inputs(block_context))
                    .collect::<Vec<_>>();
                let planner = DisjunctionPlanBuilder::new(
                    disjunction.conjunctions_by_branch_id().map(|(id, _)| *id).collect(),
                    disjunction
                        .conjunctions()
                        .iter()
                        .map(|branch| {
                            make_builder(
                                branch,
                                block_context,
                                stage_inputs,
                                block_annotations,
                                variable_registry,
                                expressions,
                                statistics,
                                call_cost_provider,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    required_inputs,
                );
                disjunction_planners.push(planner)
            }
            NestedPattern::Negation(negation) => {
                let parent_bound_variables = negation
                    .conjunction()
                    .referenced_variables()
                    .filter(|var| block_context.is_variable_available_in(conjunction.scope_id(), *var));
                negation_subplans.push(
                    make_builder(
                        negation.conjunction(),
                        block_context,
                        stage_inputs,
                        block_annotations,
                        variable_registry,
                        expressions,
                        statistics,
                        call_cost_provider,
                    )?
                    .set_to_input(parent_bound_variables)
                    .plan()?,
                )
            }
            NestedPattern::Optional(optional) => {
                let parent_bound_variables: HashSet<_> = optional
                    .referenced_variables()
                    .filter(|var| block_context.is_variable_available_in(conjunction.scope_id(), *var))
                    .collect();
                let optional_vars = optional
                    .named_visible_binding_variables(block_context)
                    .filter(|var| !parent_bound_variables.contains(var))
                    .collect();
                optional_subplans.push(OptionalPlan::new(
                    optional.branch_id(),
                    optional_vars,
                    make_builder(
                        optional.conjunction(),
                        block_context,
                        stage_inputs,
                        block_annotations,
                        variable_registry,
                        expressions,
                        statistics,
                        call_cost_provider,
                    )?
                    .set_to_input(parent_bound_variables.into_iter())
                    .plan()?,
                ))
            }
        }
    }

    let conjunction_annotations = block_annotations.type_annotations_of(conjunction).unwrap();
    let mut plan_builder = ConjunctionPlanBuilder::new(
        conjunction.required_inputs(block_context).collect(),
        conjunction_annotations,
        statistics,
    );

    let optional_variables = optional_subplans.iter().flat_map(|optional| optional.optional_variables.iter()).copied();
    plan_builder.register_variables(
        stage_inputs.keys().copied(),
        chain!(conjunction.local_variables(block_context), optional_variables),
        variable_registry,
    );
    plan_builder.register_constraints(conjunction, expressions, call_cost_provider);
    plan_builder.register_negations(negation_subplans);
    plan_builder.register_disjunctions(disjunction_planners);
    plan_builder.register_optionals(optional_subplans);

    Ok(plan_builder)
}

#[derive(Clone, Copy, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VariableVertexId(usize);

impl fmt::Debug for VariableVertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V({})", self.0)
    }
}

#[derive(Clone, Copy, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PatternVertexId(usize);

impl fmt::Debug for PatternVertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "P({})", self.0)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Clone)]
pub(super) struct ConjunctionPlanBuilder<'a> {
    required_inputs: Vec<Variable>,
    graph: Graph<'a>,
    local_annotations: &'a TypeAnnotations,
    statistics: &'a Statistics,
    planner_statistics: PlannerStatistics,
}

impl fmt::Debug for ConjunctionPlanBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBuilder").field("graph", &self.graph).finish()
    }
}

impl<'a> ConjunctionPlanBuilder<'a> {
    fn new(required_inputs: Vec<Variable>, local_annotations: &'a TypeAnnotations, statistics: &'a Statistics) -> Self {
        Self {
            graph: Graph::default(),
            local_annotations,
            statistics,
            planner_statistics: PlannerStatistics::new(),
            required_inputs,
        }
    }

    pub(super) fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.graph.referenced_variables()
    }

    pub(super) fn required_input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.required_inputs.iter().copied()
    }

    fn input_variables(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.graph
            .variable_index
            .values()
            .copied()
            .filter(|&v| self.graph.elements[&VertexId::Variable(v)].as_variable().is_some_and(|v| v.is_input()))
    }

    /// Set additional variables as bound from the parent(s)
    pub(super) fn set_to_input(mut self, input_variables: impl Iterator<Item = Variable>) -> Self {
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
        stage_input_variables: impl Iterator<Item = Variable>,
        referenced_variables: impl Iterator<Item = Variable>,
        variable_registry: &VariableRegistry,
    ) {
        for variable in stage_input_variables {
            self.register_input_var(variable);
        }

        for variable in referenced_variables {
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

                VariableCategory::ObjectList
                | VariableCategory::ThingList
                | VariableCategory::AttributeList
                | VariableCategory::ValueList => unimplemented_feature!(Lists),
                VariableCategory::AttributeOrValue => {
                    unreachable!("Insufficiently bound variable would have been flagged earlier")
                }
            }
        }
    }

    fn register_input_var(&mut self, variable: Variable) {
        let planner = InputPlanner::from_variable(variable);
        self.graph.push_variable(variable, VariableVertex::Input(planner));
    }

    fn register_type_var(&mut self, variable: Variable) {
        let planner = TypePlanner::from_variable(variable, self.local_annotations);
        self.graph.push_variable(variable, VariableVertex::Type(planner));
    }

    fn register_thing_var(&mut self, variable: Variable) {
        let planner = ThingPlanner::from_variable(variable, self.local_annotations, self.statistics);
        self.planner_statistics.increment_var(planner.unrestricted_expected_size);
        self.graph.push_variable(variable, VariableVertex::Thing(planner));
    }

    fn register_value_var(&mut self, variable: Variable) {
        let planner = ValuePlanner::from_variable(variable);
        self.graph.push_variable(variable, VariableVertex::Value(planner));
    }

    fn register_constraints(
        &mut self,
        conjunction: &'a Conjunction,
        expressions: &'a HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
        call_cost_provider: &impl FunctionCallCostProvider,
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
                Constraint::IndexedRelation(indexed_relation) => self.register_indexed_relation(indexed_relation),

                Constraint::ExpressionBinding(binding) => self.register_expression_binding(binding, expressions),
                Constraint::FunctionCallBinding(call) => self.register_function_call_binding(call, call_cost_provider),

                Constraint::Is(is) => self.register_is(is),
                Constraint::Comparison(comparison) => self.register_comparison(comparison),
                Constraint::LinksDeduplication(dedup) => self.register_links_deduplication(dedup),
                Constraint::Unsatisfiable(optimised_unsatisfiable) => {
                    self.register_optimised_to_unsatisfiable(optimised_unsatisfiable)
                }
            }
        }
    }

    fn register_label(&mut self, label: &'a Label<Variable>) {
        let planner = TypeListPlanner::from_label_constraint(label, &self.graph.variable_index, self.local_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_role_name(&mut self, role_name: &'a RoleName<Variable>) {
        let planner =
            TypeListPlanner::from_role_name_constraint(role_name, &self.graph.variable_index, self.local_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_kind(&mut self, kind: &'a Kind<Variable>) {
        let planner = TypeListPlanner::from_kind_constraint(kind, &self.graph.variable_index, self.local_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_sub(&mut self, sub: &'a Sub<Variable>) {
        let planner = SubPlanner::from_constraint(sub, &self.graph.variable_index, self.local_annotations);
        self.graph.push_constraint(ConstraintVertex::Sub(planner));
    }

    fn register_owns(&mut self, owns: &'a Owns<Variable>) {
        let planner =
            OwnsPlanner::from_constraint(owns, &self.graph.variable_index, self.local_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Owns(planner));
    }

    fn register_relates(&mut self, relates: &'a Relates<Variable>) {
        let planner = RelatesPlanner::from_constraint(
            relates,
            &self.graph.variable_index,
            self.local_annotations,
            self.statistics,
        );
        self.graph.push_constraint(ConstraintVertex::Relates(planner));
    }

    fn register_plays(&mut self, plays: &'a Plays<Variable>) {
        let planner =
            PlaysPlanner::from_constraint(plays, &self.graph.variable_index, self.local_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Plays(planner));
    }

    fn register_value(&mut self, value: &'a Value<Variable>) {
        let planner = TypeListPlanner::from_value_constraint(value, &self.graph.variable_index, self.local_annotations);
        self.graph.push_constraint(ConstraintVertex::TypeList(planner));
    }

    fn register_isa(&mut self, isa: &'a Isa<Variable>) {
        let planner =
            IsaPlanner::from_constraint(isa, &self.graph.variable_index, self.local_annotations, self.statistics);
        self.graph.push_constraint(ConstraintVertex::Isa(planner));
    }

    fn register_iid(&mut self, iid: &'a Iid<Variable>) {
        let planner =
            IidPlanner::from_constraint(iid, &self.graph.variable_index, self.local_annotations, self.statistics);
        // TODO not setting exact bound for the var here as the checker can't currently take advantage of that
        //      so the cost would be misleading the planner
        self.graph.push_constraint(ConstraintVertex::Iid(planner));
    }

    fn register_has(&mut self, has: &'a Has<Variable>) {
        let planner =
            HasPlanner::from_constraint(has, &self.graph.variable_index, self.local_annotations, self.statistics);
        self.planner_statistics.increment_has(planner.unbound_typed_expected_size);
        self.graph.push_constraint(ConstraintVertex::Has(planner));
    }

    fn register_links(&mut self, links: &'a Links<Variable>) {
        let planner =
            LinksPlanner::from_constraint(links, &self.graph.variable_index, self.local_annotations, self.statistics);
        self.planner_statistics.increment_links(planner.unbound_typed_expected_size);
        self.graph.push_constraint(ConstraintVertex::Links(planner));
    }

    fn register_indexed_relation(&mut self, indexed_relation: &'a IndexedRelation<Variable>) {
        let planner = IndexedRelationPlanner::from_constraint(
            indexed_relation,
            &self.graph.variable_index,
            self.local_annotations,
            self.statistics,
        );
        self.graph.push_constraint(ConstraintVertex::IndexedRelation(planner))
    }

    fn register_expression_binding(
        &mut self,
        binding: &ExpressionBinding<Variable>,
        expressions: &'a HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
    ) {
        let variable = binding.left().as_variable().unwrap();
        let output = self.graph.variable_index[&variable];
        let expression = &expressions[binding];
        let inputs = expression.variables().iter().map(|&var| self.graph.variable_index[&var]).unique().collect_vec();
        self.graph.push_expression(output, ExpressionVertex::from_expression(expression, inputs, output));
    }

    fn register_function_call_binding(
        &mut self,
        call_binding: &'a FunctionCallBinding<Variable>,
        call_cost_provider: &impl FunctionCallCostProvider,
    ) {
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
        let cost = call_cost_provider.get_call_cost(&call_binding.function_call().function_id());
        self.graph.push_function_call(FunctionCallVertex::from_constraint(call_binding, arguments, return_vars, cost));
    }

    fn register_is(&mut self, is: &'a Is<Variable>) {
        let lhs = self.graph.variable_index[&is.lhs().as_variable().unwrap()];
        let rhs = self.graph.variable_index[&is.rhs().as_variable().unwrap()];
        self.graph.elements.get_mut(&VertexId::Variable(lhs)).unwrap().as_variable_mut().unwrap().add_is(rhs);
        self.graph.elements.get_mut(&VertexId::Variable(rhs)).unwrap().as_variable_mut().unwrap().add_is(lhs);
        self.graph.push_is(IsVertex::from_constraint(
            is,
            &self.graph.variable_index,
            self.local_annotations,
            self.statistics,
        ));
    }

    fn register_links_deduplication(&mut self, links_deduplication: &'a LinksDeduplication<Variable>) {
        self.graph.push_links_deduplication(LinksDeduplicationVertex::from_constraint(
            links_deduplication,
            &self.graph.variable_index,
            self.local_annotations,
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
                Comparator::Like => (),
                Comparator::Contains => (),
            }
        }
        if let Input::Variable(rhs) = rhs {
            let rhs = self.graph.elements.get_mut(&VertexId::Variable(rhs)).unwrap().as_variable_mut().unwrap();
            match comparison.comparator() {
                Comparator::Equal => rhs.add_equal(lhs),
                Comparator::NotEqual => (), // no tangible impact on traversal costs
                Comparator::Less | Comparator::LessOrEqual => rhs.add_upper_bound(lhs),
                Comparator::Greater | Comparator::GreaterOrEqual => rhs.add_lower_bound(lhs),
                Comparator::Like => (),
                Comparator::Contains => (),
            }
        }
        self.graph.push_comparison(ComparisonVertex::from_constraint(
            comparison,
            &self.graph.variable_index,
            self.local_annotations,
            self.statistics,
        ));
    }

    fn register_optimised_to_unsatisfiable(&mut self, optimised_unsatisfiable: &'a Unsatisfiable) {
        let planner = UnsatisfiableVertex::from_constraint(
            optimised_unsatisfiable,
            &self.graph.variable_index,
            self.local_annotations,
            self.statistics,
        );
        self.graph.push_optimised_to_unsatisfiable(planner);
    }

    fn register_disjunctions(&mut self, disjunctions: Vec<DisjunctionPlanBuilder<'a>>) {
        for disjunction in disjunctions {
            self.graph.push_disjunction(DisjunctionVertex::from_builder(disjunction, &self.graph.variable_index));
        }
    }

    fn register_negations(&mut self, negations: Vec<ConjunctionPlan<'a>>) {
        for negation_plan in negations {
            self.graph.push_negation(NegationVertex::new(negation_plan, &self.graph.variable_index));
        }
    }

    fn register_optionals(&mut self, optionals: Vec<OptionalPlan<'a>>) {
        for optional_plan in optionals {
            self.graph.push_optional(OptionalVertex::new(optional_plan, &self.graph.variable_index));
        }
    }

    // New approach to planning:
    //
    // In our pattern graph, vertices are variables and patterns; edges indicate which patterns contain which variables.
    // A plan is an ordering of patterns and variable vertices, indicate in which order we retrieve stored patterns
    // Multiple patterns may be retrieved in the same step if there is a variable on which they can be joined.
    // Each step may "produce" answers for zero of more variables, which is recorded by appending these variables
    // (When a step has multiple pattern, the first such produced variable is always the join variable)
    // We record directionality information for each pattern in the plan, indicating which prefix index to use for pattern retrieval

    fn beam_search_plan(
        &self,
    ) -> Result<(Vec<VertexId>, HashMap<PatternVertexId, CostMetaData>, Cost), QueryPlanningError> {
        const INDENT: &str = "";

        let search_patterns: HashSet<_> = self.graph.pattern_to_variable.keys().copied().collect();
        let num_patterns = search_patterns.len();

        const BEAM_REDUCTION_CYCLE: usize = 2;
        const EXTENSION_REDUCTION_CYCLE: usize = 2;
        let mut beam_width = (num_patterns * 2).clamp(2, MAX_BEAM_WIDTH);
        let mut extension_width = (num_patterns / 2) + 5; // ensure this is larger than (num_patterns / 2) or change narrowing logic (note, join options means patterns may appear twice as extensions)

        let mut best_partial_plans = Vec::with_capacity(beam_width);
        best_partial_plans.push(PartialCostPlan::new(
            self.graph.elements.len(),
            search_patterns.clone(),
            self.input_variables(),
        ));

        let mut extension_heap = BinaryHeap::with_capacity(extension_width); // reused
        let mut new_plans_heap = BinaryHeap::with_capacity(beam_width);
        let mut new_plans_hashset = HashSet::with_capacity(beam_width);
        for i in 0..num_patterns {
            event!(Level::TRACE, "{INDENT:4}PLANNER STEP {}", i);

            // TODO: Do we need this?
            if i % BEAM_REDUCTION_CYCLE == 0 {
                beam_width = usize::max(beam_width.saturating_sub(1), 2);
            }
            if i % EXTENSION_REDUCTION_CYCLE == 0 {
                extension_width = usize::max(extension_width.saturating_sub(1), 2);
            } // Narrow the beam until it greedy at the tail (for large queries)

            new_plans_heap.clear();
            for plan in best_partial_plans.drain(..) {
                event!(
                    Level::TRACE,
                    "{INDENT:8}PLAN: {:?} ONGOING: {:?} STASH: {:?} COST: {:?} + {:?} = {:?} HEURISTIC: {:?}",
                    plan.vertex_ordering,
                    plan.ongoing_step,
                    plan.ongoing_step_stash,
                    plan.cumulative_cost,
                    plan.ongoing_step_cost,
                    plan.cumulative_cost.chain(plan.ongoing_step_cost),
                    plan.heuristic
                );

                debug_assert!(extension_heap.is_empty());
                // Add best k extensions from this plan to new_plan_heap (k = extension_width)
                for extension in plan.extensions_iter(&self.graph) {
                    let extension = extension?;
                    if extension.is_trivial(&self.graph) {
                        extension_heap.clear();
                        extension_heap.push(Reverse(extension));
                        break;
                    } else {
                        extension_heap.push(Reverse(extension));
                    }
                }
                for Reverse(extension) in drain_sorted(&mut extension_heap).take(extension_width) {
                    new_plans_heap.push(Reverse(plan.extend_with(&self.graph, extension)));
                }
            }
            // Pick best (k = beam_width) plans to beam.
            debug_assert!(best_partial_plans.is_empty());
            new_plans_hashset.clear();
            for Reverse(plan) in drain_sorted(&mut new_plans_heap) {
                if new_plans_hashset.insert(plan.hash()) {
                    best_partial_plans.push(plan);
                    if best_partial_plans.len() >= beam_width {
                        break;
                    }
                }
            }
        }

        let best_plan =
            best_partial_plans.into_iter().min().ok_or(QueryPlanningError::ExpectedPlannableConjunction {})?;
        let complete_plan = best_plan.into_complete_plan(&self.graph);
        event!(
            Level::TRACE,
            "\n Final plan (before lowering):\n --> Order: {:?} --> MetaData \n {:?}",
            complete_plan.vertex_ordering,
            complete_plan.pattern_metadata
        );
        Ok((complete_plan.vertex_ordering, complete_plan.pattern_metadata, complete_plan.cumulative_cost))
    }

    // Execute plans
    pub(super) fn plan(self) -> Result<ConjunctionPlan<'a>, QueryPlanningError> {
        // Beam plan
        let (ordering, metadata, cost) = self.beam_search_plan()?;

        let element_to_order = ordering.iter().copied().enumerate().map(|(order, index)| (index, order)).collect();

        let Self { graph, local_annotations: type_annotations, mut planner_statistics, .. } = self;

        planner_statistics.finalize(cost);
        Ok(ConjunctionPlan {
            graph,
            local_annotations: type_annotations,
            ordering,
            metadata,
            element_to_order,
            planner_statistics,
        })
    }
}

struct DrainSorted<'a, T: Ord> {
    heap: &'a mut BinaryHeap<T>,
}

fn drain_sorted<T: Ord>(heap: &mut BinaryHeap<T>) -> impl Iterator<Item = T> + '_ {
    DrainSorted { heap }
}

impl<'a, T: Ord> Iterator for DrainSorted<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.heap.pop()
    }
}

impl<'a, T: Ord> Drop for DrainSorted<'a, T> {
    fn drop(&mut self) {
        self.heap.clear();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PlannerStatistics {
    links_count: (f64, f64), // vertex count, key count
    has_count: (f64, f64),
    var_count: (f64, f64),
    pub(crate) query_cost: Cost,
    // TODO: pass info about individual steps
}

impl PlannerStatistics {
    pub fn new() -> PlannerStatistics {
        PlannerStatistics {
            links_count: (0.0, 0.0),
            has_count: (0.0, 0.0),
            var_count: (0.0, 0.0),
            query_cost: Cost::NOOP,
        }
    }

    pub(crate) fn increment_var(&mut self, count: f64) {
        self.var_count.0 += 1.0;
        self.var_count.1 += count;
    }

    pub(crate) fn increment_has(&mut self, count: f64) {
        self.has_count.0 += 1.0;
        self.has_count.1 += count;
    }

    pub(crate) fn increment_links(&mut self, count: f64) {
        self.links_count.0 += 1.0;
        self.links_count.1 += count;
    }

    pub(super) fn finalize(&mut self, cost: Cost) {
        self.query_cost = cost;
    }
}

impl Default for PlannerStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PlannerStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cost: {:.2} Size: {:.2} (stats: links {:.2} / {:.2}, has {:.2} / {:.2}, vars {:.2} / {:.2})",
            self.query_cost.cost,
            self.query_cost.io_ratio,
            self.links_count.0,
            self.links_count.1,
            self.has_count.0,
            self.has_count.1,
            self.var_count.0,
            self.var_count.1,
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct CompleteCostPlan {
    vertex_ordering: Vec<VertexId>,
    pattern_metadata: HashMap<PatternVertexId, CostMetaData>,
    cumulative_cost: Cost,
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct PartialCostPlan {
    vertex_ordering: Vec<VertexId>, // the part of the plan that has been decided upon
    cumulative_cost: Cost,          // the cost of the part of the plan that has been decided upon

    ongoing_step: HashSet<PatternVertexId>, // the set of non-trivial patterns in the ongoing step
    ongoing_step_stash: Vec<PatternVertexId>, // the set of trivial patterns in the ongoing step
    ongoing_step_cost: Cost,                // the cost of the ongoing step (on top of the cumulative one)
    ongoing_step_produced_vars: HashSet<VariableVertexId>, // variables produced in this step
    ongoing_step_stash_produced_vars: HashSet<VariableVertexId>, // variables produced in this step
    ongoing_step_join_var: Option<VariableVertexId>, // the join variable of the ongoing step

    all_produced_vars: HashSet<VariableVertexId>, // the set of all variables produced (incl. in ongoing step, excl. stash)
    remaining_patterns: HashSet<PatternVertexId>, // the set of remaining patterns to be searched
    pattern_metadata: HashMap<PatternVertexId, CostMetaData>, // metadata, like pattern directions
    heuristic: Cost,                              // the heuristic that plans are sorted by
}

impl PartialCostPlan {
    fn new(
        total_plan_len: usize,
        remaining_patterns: HashSet<PatternVertexId>,
        inputs: impl Iterator<Item = VariableVertexId> + Sized,
    ) -> Self {
        let mut vertex_ordering = Vec::with_capacity(total_plan_len);
        let mut produced_vars = HashSet::new();
        for v in inputs {
            vertex_ordering.push(VertexId::Variable(v));
            produced_vars.insert(v);
        }
        Self {
            vertex_ordering,
            pattern_metadata: HashMap::new(),
            all_produced_vars: produced_vars,
            cumulative_cost: Cost::NOOP,
            remaining_patterns,
            ongoing_step: HashSet::new(),
            ongoing_step_stash: Vec::new(),
            ongoing_step_cost: Cost::NOOP,
            ongoing_step_produced_vars: HashSet::new(),
            ongoing_step_stash_produced_vars: HashSet::new(),
            ongoing_step_join_var: None,
            heuristic: Cost::INFINITY,
        }
    }

    fn extensions_iter<'a>(
        &'a self,
        graph: &'a Graph<'_>,
    ) -> impl Iterator<Item = Result<StepExtension, QueryPlanningError>> + 'a {
        let mut all_available_vars = self.vertex_ordering.clone();
        all_available_vars.extend(
            chain(&self.ongoing_step_produced_vars, &self.ongoing_step_stash_produced_vars)
                .map(|&var| VertexId::Variable(var)),
        );

        self.remaining_patterns
            .iter()
            .filter({
                let all_available_vars = all_available_vars.clone();
                move |&&extension| {
                    let pattern_id = VertexId::Pattern(extension);
                    graph.elements[&pattern_id].is_valid(pattern_id, &all_available_vars)
                }
            })
            .flat_map(move |&extension| {
                let join_var = self.determine_joinability(graph, extension);

                if join_var.is_none() {
                    vec![(extension, join_var)].into_iter()
                } else {
                    vec![(extension, None), (extension, join_var)].into_iter()
                }
            })
            .map(move |(extension, join_var)| {
                let added_cost: Cost;
                let meta_data: CostMetaData;

                if join_var.is_none() {
                    (added_cost, meta_data) =
                        self.compute_added_cost(graph, extension, &all_available_vars, join_var)?;
                } else {
                    (added_cost, meta_data) =
                        self.compute_added_cost(graph, extension, &self.vertex_ordering, join_var)?;
                }

                let mut cost_before_extension = self.cumulative_cost;
                if join_var.is_none() {
                    // Complete ongoing step
                    cost_before_extension = cost_before_extension.chain(self.ongoing_step_cost);
                }

                let cost_including_extension = cost_before_extension.chain(added_cost);

                let heuristic = cost_including_extension.chain(self.heuristic_plan_completion_cost(extension, graph));

                Ok(StepExtension {
                    pattern_id: extension,
                    pattern_metadata: meta_data,
                    step_cost: added_cost,
                    step_join_var: join_var,
                    heuristic,
                })
            })
    }

    pub(crate) fn extend_with(&self, graph: &Graph<'_>, extension: StepExtension) -> PartialCostPlan {
        const INDENT: &str = "";
        if extension.is_trivial(graph) {
            event!(
                Level::TRACE,
                "{INDENT:12}Stash {:?} = {} <-- cost: {:?} heuristic: {:?}",
                extension.pattern_id,
                graph.elements[&VertexId::Pattern(extension.pattern_id)],
                extension.step_cost.cost,
                extension.heuristic
            );
            let mut new_plan = self.clone();
            new_plan.add_to_stash(extension.pattern_id, graph);
            new_plan
        } else {
            event!(
                Level::TRACE,
                "{INDENT:12}Choice {:?} = {} <-- join: {:?}, cost: {:?}, heuristic: {:?} metadata: {:?}",
                extension.pattern_id,
                graph.elements[&VertexId::Pattern(extension.pattern_id)],
                extension
                    .step_join_var
                    .map(|v| graph.elements[&VertexId::Variable(v)].as_variable().unwrap().variable()),
                extension.step_cost,
                extension.heuristic,
                extension.pattern_metadata
            );
            if !extension.is_constraint(graph) {
                self.clone_and_extend_with_new_step(extension, graph)
            } else if extension.step_join_var.is_some()
                && (self.ongoing_step_join_var.is_none() || self.ongoing_step_join_var == extension.step_join_var)
            {
                self.clone_and_extend_with_continued_step(extension, graph)
            } else {
                self.clone_and_extend_with_new_step(extension, graph)
            }
        }
    }

    fn determine_joinability(&self, graph: &Graph<'_>, pattern: PatternVertexId) -> Option<VariableVertexId> {
        let &prev_pattern = self.ongoing_step.iter().next()?;
        // We only join constraint patterns, so let's extract constraints
        let prev_planner = &graph.elements[&VertexId::Pattern(prev_pattern)];
        let PlannerVertex::Constraint(prev_constraint) = prev_planner else { return None };
        let planner = &graph.elements[&VertexId::Pattern(pattern)];
        let PlannerVertex::Constraint(constraint) = planner else { return None };
        // Determine whether there are any candidate join variables:
        let candidate_join_var = constraint
            .variable_vertex_ids()
            .filter(|var| self.ongoing_step_produced_vars.contains(var) && constraint.can_join_on(*var))
            .exactly_one()
            .ok()?;
        // Only direct-able patterns are join-able:
        let Some(CostMetaData::Direction(prev_dir)) = self.pattern_metadata.get(&prev_pattern) else { return None };
        // If no join var is set yet, only join when we are on the "non-inverted join var" of the previous constraint based on its direction
        if (self.ongoing_step_join_var.is_none()
            && Some(candidate_join_var)
                == prev_constraint.join_from_direction_and_inputs(
                    prev_dir,
                    &self.ongoing_step_produced_vars,
                    &self.all_produced_vars,
                ))
            || self.ongoing_step_join_var == Some(candidate_join_var)
        {
            return Some(candidate_join_var);
        }
        None
    }

    fn compute_added_cost(
        &self,
        graph: &Graph<'_>,
        pattern: PatternVertexId,
        input_vars: &[VertexId],
        join_var: Option<VariableVertexId>,
    ) -> Result<(Cost, CostMetaData), QueryPlanningError> {
        let planner = &graph.elements[&VertexId::Pattern(pattern)];
        let (updated_cost, extension_metadata) = match planner {
            PlannerVertex::Constraint(constraint) => {
                if let Some(join_var) = join_var {
                    let total_join_size = graph.elements[&VertexId::Variable(join_var)]
                        .as_variable()
                        .unwrap()
                        .restricted_expected_output_size(&self.vertex_ordering);
                    let fixed_direction = constraint.direction_from_join_var(
                        join_var,
                        &self.ongoing_step_produced_vars,
                        &self.all_produced_vars,
                    ); // TODO: we only allow unbounded regular joins for now
                    let (constraint_cost, meta_data) =
                        constraint.cost_and_metadata(input_vars, fixed_direction, graph)?;
                    (self.ongoing_step_cost.join(constraint_cost, total_join_size), meta_data)
                } else {
                    constraint.cost_and_metadata(input_vars, None, graph)?
                }
            }
            planner_vertex => planner_vertex.cost_and_metadata(input_vars, None, graph)?,
        };
        Ok((updated_cost, extension_metadata))
    }

    fn heuristic_plan_completion_cost(&self, pattern: PatternVertexId, graph: &Graph<'_>) -> Cost {
        let num_remaining = self.remaining_patterns.len();
        if num_remaining == 1 {
            Cost::NOOP // after the last extension there is nothing left to do... we need the actual cost now!
        } else {
            let num_produced_vars = self.all_produced_vars.len()
                + self.ongoing_step_produced_vars.len()
                + graph.elements[&VertexId::Pattern(pattern)]
                    .variable_vertex_ids()
                    .filter(|v| !self.ongoing_step_produced_vars.contains(v) && !self.all_produced_vars.contains(v))
                    .count();
            let cost_estimate = AVERAGE_STEP_COST
                * (num_remaining as f64)
                * (1.0 - VARIABLE_PRODUCTION_ADVANTAGE).powi(num_produced_vars as i32);
            Cost { cost: cost_estimate, io_ratio: AVERAGE_QUERY_OUTPUT_SIZE }
        }
    }

    fn add_to_stash(&mut self, pattern: PatternVertexId, graph: &Graph<'_>) {
        self.ongoing_step_stash.push(pattern);
        self.remaining_patterns.remove(&pattern);
        self.pattern_metadata.insert(pattern, CostMetaData::None);
        self.ongoing_step_stash_produced_vars.extend(graph.elements[&VertexId::Pattern(pattern)].variable_vertex_ids());
    }

    fn finalize_current_step(&self, graph: &Graph<'_>) -> (Vec<VertexId>, HashSet<VariableVertexId>) {
        let mut current_step = Vec::new();
        let mut current_stash_produced_vars = HashSet::new();
        for &pattern in self.ongoing_step.iter() {
            current_step.push(VertexId::Pattern(pattern));
            debug_assert!(!self.vertex_ordering.contains(&VertexId::Pattern(pattern)));
        }
        if let Some(join_var) = self.ongoing_step_join_var {
            current_step.push(VertexId::Variable(join_var));
            for var in self.ongoing_step_produced_vars.clone() {
                if var != join_var && !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    current_step.push(VertexId::Variable(var));
                }
            }
        } else {
            for var in self.ongoing_step_produced_vars.clone() {
                if !self.vertex_ordering.contains(&VertexId::Variable(var)) {
                    current_step.push(VertexId::Variable(var));
                }
            }
        }
        for &pattern in self.ongoing_step_stash.iter() {
            current_step.push(VertexId::Pattern(pattern));
            for var_id in graph.elements[&VertexId::Pattern(pattern)].variable_vertex_ids() {
                if !self.all_produced_vars.contains(&var_id) && !current_step.contains(&VertexId::Variable(var_id)) {
                    current_step.push(VertexId::Variable(var_id));
                    current_stash_produced_vars.insert(var_id);
                }
            }
            debug_assert!(!self.vertex_ordering.contains(&VertexId::Pattern(pattern)));
        }
        (current_step, current_stash_produced_vars)
    }

    fn clone_and_extend_with_continued_step(&self, extension: StepExtension, graph: &Graph<'_>) -> PartialCostPlan {
        let mut new_ongoing_step = self.ongoing_step.clone();
        new_ongoing_step.insert(extension.pattern_id);

        let mut new_pattern_metadata = self.pattern_metadata.clone();
        new_pattern_metadata.insert(extension.pattern_id, extension.pattern_metadata);

        let mut new_remaining_patterns = self.remaining_patterns.clone();
        new_remaining_patterns.remove(&extension.pattern_id);

        let mut new_ongoing_produced_vars = self.ongoing_step_produced_vars.clone();
        new_ongoing_produced_vars.extend(
            graph.elements[&VertexId::Pattern(extension.pattern_id)]
                .variable_vertex_ids()
                .filter(|var| !self.all_produced_vars.contains(var)),
        );

        let mut new_produced_vars = self.all_produced_vars.clone();
        new_produced_vars.extend(new_ongoing_produced_vars.iter());

        PartialCostPlan {
            vertex_ordering: self.vertex_ordering.clone(),
            pattern_metadata: new_pattern_metadata,
            remaining_patterns: new_remaining_patterns,
            cumulative_cost: self.cumulative_cost,
            ongoing_step: new_ongoing_step,
            ongoing_step_stash: self.ongoing_step_stash.clone(),
            ongoing_step_cost: extension.step_cost,
            ongoing_step_produced_vars: new_ongoing_produced_vars,
            ongoing_step_stash_produced_vars: self.ongoing_step_stash_produced_vars.clone(),
            ongoing_step_join_var: extension.step_join_var,
            heuristic: extension.heuristic,
            all_produced_vars: new_produced_vars,
        }
    }

    fn clone_and_extend_with_new_step(&self, extension: StepExtension, graph: &Graph<'_>) -> PartialCostPlan {
        // First finalize the current step
        let mut new_vertex_ordering = self.vertex_ordering.clone();
        let (current_step, current_stash_produced_vars) = self.finalize_current_step(graph);
        new_vertex_ordering.extend(current_step);

        let new_cumulative_cost = self
            .cumulative_cost
            .chain(self.ongoing_step_cost)
            .chain(Cost { cost: (self.ongoing_step_stash.len() as f64) * Cost::TRIVIAL_COST, io_ratio: 1.0 });

        // Then start a new step with the given plan extension
        let mut new_ongoing_step = HashSet::new();
        new_ongoing_step.insert(extension.pattern_id);

        let mut new_pattern_metadata = self.pattern_metadata.clone();
        new_pattern_metadata.insert(extension.pattern_id, extension.pattern_metadata);

        let mut new_remaining_patterns = self.remaining_patterns.clone();
        new_remaining_patterns.remove(&extension.pattern_id);

        let mut new_ongoing_produced_vars = HashSet::new();
        new_ongoing_produced_vars.extend(
            graph.elements[&VertexId::Pattern(extension.pattern_id)]
                .variable_vertex_ids()
                .filter(|var| !self.all_produced_vars.contains(var)),
        );

        let mut new_produced_vars = self.all_produced_vars.clone();
        new_produced_vars.extend(current_stash_produced_vars.iter());
        new_produced_vars.extend(new_ongoing_produced_vars.iter());

        PartialCostPlan {
            vertex_ordering: new_vertex_ordering,
            cumulative_cost: new_cumulative_cost,
            ongoing_step: new_ongoing_step,
            ongoing_step_stash: Vec::new(),
            ongoing_step_cost: extension.step_cost,
            ongoing_step_produced_vars: new_ongoing_produced_vars,
            ongoing_step_stash_produced_vars: HashSet::new(),
            ongoing_step_join_var: None,
            all_produced_vars: new_produced_vars,
            pattern_metadata: new_pattern_metadata,
            remaining_patterns: new_remaining_patterns,
            heuristic: extension.heuristic,
        }
    }

    fn into_complete_plan(self, graph: &Graph<'_>) -> CompleteCostPlan {
        let mut final_vertex_ordering = self.vertex_ordering.clone();
        let (new_step, _stash_produced_vars) = self.finalize_current_step(graph);
        final_vertex_ordering.extend(new_step);

        let final_cumulative_cost = self
            .cumulative_cost
            .chain(self.ongoing_step_cost)
            .chain(Cost { cost: (self.ongoing_step_stash.len() as f64) * Cost::TRIVIAL_COST, io_ratio: 1.0 });

        CompleteCostPlan {
            vertex_ordering: final_vertex_ordering,
            pattern_metadata: self.pattern_metadata.clone(),
            cumulative_cost: final_cumulative_cost,
        }
    }

    fn hash(&self) -> PartialPlanHash {
        PartialPlanHash {
            n_remaining_patterns: self.remaining_patterns.len() as u32,
            planned_patterns: self.vertex_ordering.iter().filter_map(|v| v.as_pattern_id()).collect::<BTreeSet<_>>(),
            ongoing_step_join_var: self.ongoing_step_join_var,
            ongoing_non_trivial_patterns: self.ongoing_step.iter().copied().collect::<BTreeSet<_>>(),
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
        self.heuristic.cost.partial_cmp(&other.heuristic.cost).unwrap_or(Ordering::Greater)
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(super) struct PartialPlanHash {
    n_remaining_patterns: u32, // Needed for continuous search (A*), but not step-based (beam)
    planned_patterns: BTreeSet<PatternVertexId>,
    ongoing_non_trivial_patterns: BTreeSet<PatternVertexId>,
    ongoing_step_join_var: Option<VariableVertexId>,
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct StepExtension {
    pattern_id: PatternVertexId,
    pattern_metadata: CostMetaData,
    step_cost: Cost,
    step_join_var: Option<VariableVertexId>,
    heuristic: Cost,
}

impl StepExtension {
    fn is_constraint(&self, graph: &Graph<'_>) -> bool {
        graph.elements[&VertexId::Pattern(self.pattern_id)].is_constraint()
    }

    fn is_trivial(&self, graph: &Graph<'_>) -> bool {
        graph.elements[&VertexId::Pattern(self.pattern_id)].can_be_trivial() && self.step_cost.is_trivial()
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
        (self.heuristic.cost.partial_cmp(&other.heuristic.cost).unwrap_or(Ordering::Equal))
            .then_with(|| self.pattern_id.cmp(&other.pattern_id))
    }
}

#[derive(Clone)]
pub(crate) struct ConjunctionPlan<'a> {
    graph: Graph<'a>,
    local_annotations: &'a TypeAnnotations,
    ordering: Vec<VertexId>,
    metadata: HashMap<PatternVertexId, CostMetaData>,
    element_to_order: HashMap<VertexId, usize>,
    pub(crate) planner_statistics: PlannerStatistics,
}

impl fmt::Debug for ConjunctionPlan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name_of_val(self)).field("graph", &self.graph).field("ordering", &self.ordering).finish()
    }
}

impl ConjunctionPlan<'_> {
    pub(super) fn lower(
        &self,
        input_variable_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<answer::Type>>>,
        input_variables: impl IntoIterator<Item = Variable> + Clone,
        selected_variables: HashSet<Variable>,
        already_assigned_positions: &HashMap<Variable, ExecutorVariable>,
        variable_registry: &VariableRegistry,
        branch_id: Option<BranchID>,
    ) -> Result<ConjunctionExecutableBuilder, QueryPlanningError> {
        let mut conjunction_builder = ConjunctionExecutableBuilder::new(
            branch_id,
            already_assigned_positions,
            selected_variables.clone(),
            input_variables.clone().into_iter().collect(),
            self.constraint_variables().collect(),
            self.planner_statistics,
        );
        self.may_make_input_check_steps(
            &mut conjunction_builder,
            input_variables.clone(),
            input_variable_annotations,
            variable_registry,
        );
        for &index in &self.ordering {
            match index {
                VertexId::Variable(var) => {
                    self.may_make_variable_producing_step(&mut conjunction_builder, var, variable_registry)?;
                }
                VertexId::Pattern(pattern) => {
                    for input in self.inputs_of_pattern(pattern) {
                        let order = self.element_to_order[&VertexId::Pattern(pattern)];
                        let is_last_consumer = self
                            .consumers_of_var(input)
                            .all(|pat| self.element_to_order[&VertexId::Pattern(pat)] <= order);
                        if is_last_consumer {
                            conjunction_builder.finish_one();
                            conjunction_builder.remove_output(self.graph.index_to_variable[&input]);
                        }
                    }
                    for output in self.outputs_of_pattern(pattern) {
                        let is_selected =
                            conjunction_builder.selected_variables.contains(&self.graph.index_to_variable[&output]);
                        let has_consumers = self.consumers_of_var(output).next().is_some();
                        if is_selected || has_consumers {
                            conjunction_builder.finish_one();
                            conjunction_builder.register_output(self.graph.index_to_variable[&output]);
                        } else {
                            conjunction_builder.register_internal(self.graph.index_to_variable[&output]);
                        }
                    }
                    if self.outputs_of_pattern(pattern).next().is_none() {
                        self.may_make_check_step(&mut conjunction_builder, pattern, variable_registry)?;
                    }
                }
            }
        }

        Ok(conjunction_builder)
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
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        var: VariableVertexId,
        variable_registry: &VariableRegistry,
    ) -> Result<(), QueryPlanningError> {
        if self.graph.elements[&VertexId::Variable(var)].as_variable().unwrap().is_input() {
            return Ok(());
        }

        let variable = self.graph.index_to_variable[&var];
        if conjunction_builder.produced_so_far.contains(&variable) {
            return Ok(());
        }

        let is_join = self.producers_of_var(var).nth(1).is_some();
        for producer in self.producers_of_var(var) {
            match &self.graph.elements()[&VertexId::Pattern(producer)] {
                PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {producer:?}"),
                PlannerVertex::Negation(_) => unreachable!("encountered negation registered as producing variable"),
                PlannerVertex::LinksDeduplication(_) => {
                    unreachable!("encountered links_deduplication registered as producing variable")
                }
                PlannerVertex::Is(is) => {
                    let input = if var == is.lhs {
                        self.graph.index_to_variable[&is.rhs]
                    } else {
                        self.graph.index_to_variable[&is.lhs]
                    };
                    let instruction =
                        ConstraintInstruction::Is(IsInstruction::new(is.is().clone(), Inputs::Single([input])));
                    conjunction_builder.push_instruction(variable, instruction);
                }
                PlannerVertex::Comparison(_) => unreachable!("encountered comparison registered as producing variable"),
                PlannerVertex::Unsatisfiable(_) => {
                    unreachable!("encountered optimised-away registered as producing variable")
                }
                PlannerVertex::Constraint(constraint) => {
                    let inputs =
                        self.inputs_of_pattern(producer).map(|var| self.graph.index_to_variable[&var]).collect_vec();
                    let sort_variable = is_join.then_some(variable); // otherwise use metadata
                    self.lower_constraint(
                        conjunction_builder,
                        constraint,
                        self.metadata[&producer],
                        inputs,
                        sort_variable,
                    )
                }
                PlannerVertex::Expression(expression) => {
                    let output =
                        conjunction_builder.position_mapping()[&self.graph.index_to_variable[&expression.output]];
                    let mapping = conjunction_builder
                        .position_mapping()
                        .iter()
                        .filter_map(|(&k, &v)| Some((k, v.as_position()?)))
                        .collect();
                    conjunction_builder.push_step(
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
                        .plan(conjunction_builder.produced_so_far.iter().filter(|&&v| v != variable).copied())?
                        .lower(
                            self.local_annotations.vertex_annotations(),
                            conjunction_builder.row_variables().iter().copied(),
                            conjunction_builder.current_outputs.clone(),
                            conjunction_builder.position_mapping(),
                            variable_registry,
                        )?;
                    let variable_positions =
                        step_builder.branches.iter().flat_map(|x| x.index.iter().map(|(&k, &v)| (k, v))).collect();
                    conjunction_builder
                        .push_step(&variable_positions, StepInstructionsBuilder::Disjunction(step_builder).into());
                }
                PlannerVertex::Optional(optional) => {
                    // TODO: optionals should just be added after everything else is planned?
                    let optional_executable_builder = OptionalBuilder::new(optional.plan.plan().lower(
                        self.local_annotations.vertex_annotations(),
                        conjunction_builder.row_variables().iter().copied(),
                        conjunction_builder.current_outputs.clone(),
                        conjunction_builder.position_mapping(),
                        variable_registry,
                        Some(optional.plan.branch_id),
                    )?);
                    let variable_positions: HashMap<Variable, ExecutorVariable> =
                        optional_executable_builder.optional.index.clone();
                    let step_builder = StepInstructionsBuilder::Optional(optional_executable_builder);
                    conjunction_builder.push_step(&variable_positions, step_builder.into())
                }
                PlannerVertex::FunctionCall(call_planner) => {
                    let call_binding = call_planner.call_binding;
                    let assigned = call_binding
                        .assigned()
                        .iter()
                        .map(|variable| {
                            conjunction_builder.index[&variable.as_variable().unwrap()].clone().as_position()
                        })
                        .collect();
                    let arguments = call_binding
                        .function_call()
                        .argument_ids()
                        .map(|variable| conjunction_builder.index[&variable].clone().as_position().unwrap())
                        .collect();
                    let step_builder = StepInstructionsBuilder::FunctionCall(FunctionCallBuilder {
                        function_id: call_binding.function_call().function_id(),
                        arguments,
                        assigned,
                        output_width: conjunction_builder.next_output.position,
                    });
                    conjunction_builder.push_step(&HashMap::new(), step_builder.into())
                }
            }
        }
        conjunction_builder.finish_one();
        Ok(())
    }

    fn may_make_check_step(
        &self,
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        pattern: PatternVertexId,
        variable_registry: &VariableRegistry,
    ) -> Result<(), QueryPlanningError> {
        match &self.graph.elements()[&VertexId::Pattern(pattern)] {
            PlannerVertex::Variable(_) => unreachable!("encountered variable @ pattern id {pattern:?}"),

            PlannerVertex::FunctionCall(call_planner) => {
                // We push exactly the same as if it weren't a check.
                let call_binding = call_planner.call_binding;
                let assigned = call_binding
                    .assigned()
                    .iter()
                    .map(|variable| {
                        conjunction_builder.index.get(&variable.as_variable().unwrap()).unwrap().clone().as_position()
                    })
                    .collect();
                let arguments = call_binding
                    .function_call()
                    .argument_ids()
                    .map(|variable| conjunction_builder.index.get(&variable).unwrap().clone().as_position().unwrap())
                    .collect();
                let step_builder = StepInstructionsBuilder::FunctionCall(FunctionCallBuilder {
                    function_id: call_binding.function_call().function_id(),
                    arguments,
                    assigned,
                    output_width: conjunction_builder.next_output.position,
                });
                conjunction_builder.push_step(&HashMap::new(), step_builder.into());
            }

            PlannerVertex::Negation(negation) => {
                let negation = negation.plan().lower(
                    self.local_annotations.vertex_annotations(),
                    conjunction_builder.row_variables().iter().copied(),
                    conjunction_builder.selected_variables.clone(),
                    conjunction_builder.position_mapping(),
                    variable_registry,
                    None,
                )?;
                let variable_positions: HashMap<Variable, ExecutorVariable> = negation
                    .index
                    .iter()
                    .filter_map(|(&k, &v)| conjunction_builder.current_outputs.contains(&k).then_some((k, v)))
                    .collect();
                conjunction_builder.push_step(
                    &variable_positions,
                    StepInstructionsBuilder::Negation(NegationBuilder::new(negation)).into(),
                )
            }

            PlannerVertex::Is(is) => {
                let lhs = is.is().lhs().as_variable().unwrap();
                let rhs = is.is().rhs().as_variable().unwrap();
                let check = CheckInstruction::Is { lhs, rhs }.map(conjunction_builder.position_mapping());
                conjunction_builder.push_check(check)
            }

            PlannerVertex::LinksDeduplication(deduplication) => {
                let role1 = deduplication.links_deduplication().links1().role_type().as_variable().unwrap();
                let player1 = deduplication.links_deduplication().links1().player().as_variable().unwrap();
                let role2 = deduplication.links_deduplication().links2().role_type().as_variable().unwrap();
                let player2 = deduplication.links_deduplication().links2().player().as_variable().unwrap();
                let check = CheckInstruction::LinksDeduplication { role1, player1, role2, player2 }
                    .map(conjunction_builder.position_mapping());
                conjunction_builder.push_check(check)
            }

            PlannerVertex::Comparison(comparison) => {
                let comparison = comparison.comparison();
                let lhs = comparison.lhs();
                let rhs = comparison.rhs();
                let comparator = comparison.comparator();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();
                let num_input_variables = [lhs_var, rhs_var].into_iter().flatten().dedup().count();
                assert!(num_input_variables > 0);

                let order = self.element_to_order[&VertexId::Pattern(pattern)];
                let inputs = self.graph.pattern_to_variable[&pattern]
                    .iter()
                    .copied()
                    .filter(move |&adj| self.ordering[..order].contains(&VertexId::Variable(adj)))
                    .map(|var| self.graph.index_to_variable[&var]);

                assert_eq!(inputs.count(), num_input_variables);

                let lhs_pos = lhs.clone().map(conjunction_builder.position_mapping());
                let rhs_pos = rhs.clone().map(conjunction_builder.position_mapping());

                let check = CheckInstruction::Comparison {
                    lhs: CheckVertex::resolve(lhs_pos, self.local_annotations),
                    rhs: CheckVertex::resolve(rhs_pos, self.local_annotations),
                    comparator,
                };

                conjunction_builder.push_check(check)
            }

            PlannerVertex::Constraint(constraint) => self.lower_constraint_check(conjunction_builder, constraint),

            PlannerVertex::Unsatisfiable(_) => conjunction_builder.push_check(CheckInstruction::Unsatisfiable),

            PlannerVertex::Expression(_) => {
                unreachable!("Would require multiple assignments to the same variable and be flagged")
            }

            PlannerVertex::Disjunction(disjunction) => {
                let step_builder = disjunction.plan(conjunction_builder.position_mapping().keys().copied())?.lower(
                    self.local_annotations.vertex_annotations(),
                    conjunction_builder.row_variables().iter().copied(),
                    conjunction_builder.current_outputs.clone(),
                    conjunction_builder.position_mapping(),
                    variable_registry,
                )?;
                let variable_positions = step_builder.branches.iter().flat_map(|x| x.index.clone()).collect();
                conjunction_builder
                    .push_step(&variable_positions, StepInstructionsBuilder::Disjunction(step_builder).into())
            }
            PlannerVertex::Optional(optional) => {
                let optional = optional.plan().plan.lower(
                    self.local_annotations.vertex_annotations(),
                    conjunction_builder.row_variables().iter().copied(),
                    conjunction_builder.selected_variables.clone(),
                    conjunction_builder.position_mapping(),
                    variable_registry,
                    Some(optional.plan.branch_id),
                )?;
                let variable_positions: HashMap<Variable, ExecutorVariable> = optional
                    .index
                    .iter()
                    .filter_map(|(&k, &v)| conjunction_builder.current_outputs.contains(&k).then_some((k, v)))
                    .collect();
                conjunction_builder.push_step(
                    &variable_positions,
                    StepInstructionsBuilder::Optional(OptionalBuilder::new(optional)).into(),
                )
            }
        }
        Ok(())
    }

    fn lower_constraint(
        &self,
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        constraint: &ConstraintVertex<'_>,
        metadata: CostMetaData,
        inputs: Vec<Variable>,
        sort_variable: Option<Variable>,
    ) {
        if let Some(StepBuilder {
            builder:
                StepInstructionsBuilder::Intersection(IntersectionBuilder { sort_variable: Some(sort_variable), .. }),
            ..
        }) = conjunction_builder.current.as_deref()
        {
            if !constraint.variable_vertex_ids().contains(&self.graph.variable_index[sort_variable]) {
                conjunction_builder.finish_one();
                event!(Level::WARN, "Ignoring planned join (incompatible join variables found)");
            }
        }

        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs_var = $con.$lhs().as_variable();
                let rhs_var = $con.$rhs().as_variable();

                let lhs_input = lhs_var.filter(|lhs| inputs.contains(&lhs));
                let rhs_input = rhs_var.filter(|rhs| inputs.contains(&rhs));

                let inputs = match (lhs_input, rhs_input) {
                    (Some(lhs), Some(rhs)) => Inputs::Dual([lhs, rhs]), // useful for links
                    (Some(var), None) | (None, Some(var)) => Inputs::Single([var]),
                    (None, None) => Inputs::None([]),
                };

                let direction = if matches!(inputs, Inputs::None([])) {
                    let CostMetaData::Direction(unbound_direction) = metadata else {
                        unreachable!("expected metadata for constraint")
                    };
                    unbound_direction
                } else if rhs_var.is_some_and(|rhs| inputs.contains(rhs)) {
                    Direction::Reverse
                } else {
                    Direction::Canonical
                };

                let con = $con.clone();
                let instruction = match direction {
                    Direction::Canonical => ConstraintInstruction::$fw($fwi::new(con, inputs, self.local_annotations)),
                    Direction::Reverse => ConstraintInstruction::$bw($bwi::new(con, inputs, self.local_annotations)),
                };

                let lhs_produced = lhs_var.xor(lhs_input);
                let rhs_produced = rhs_var.xor(rhs_input);

                #[allow(unused)]
                let mut tag: Option<Variable> = None;
                $(tag = $con.$with().as_variable();)?

                let sort_variable = sort_variable.or_else(|| match direction {
                    Direction::Canonical => lhs_produced.or(rhs_produced),
                    Direction::Reverse => rhs_produced.or(lhs_produced),
                }.or(tag)).unwrap();

                conjunction_builder.push_instruction(sort_variable, instruction);
            }};
        }

        match constraint {
            ConstraintVertex::TypeList(type_list) => {
                let var = type_list.constraint().var();
                let instruction = type_list.lower();
                conjunction_builder.push_instruction(var, instruction);
            }

            ConstraintVertex::Iid(iid) => {
                let var = iid.iid().var().as_variable().unwrap();
                let instruction =
                    ConstraintInstruction::Iid(IidInstruction::new(iid.iid().clone(), self.local_annotations));
                conjunction_builder.push_instruction(var, instruction);
            }

            ConstraintVertex::Sub(planner) => {
                let sub = planner.sub();
                binary!(subtype sub supertype, Sub(SubInstruction), SubReverse(SubReverseInstruction))
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
                binary!(thing isa type_, Isa(IsaInstruction), IsaReverse(IsaReverseInstruction))
            }
            ConstraintVertex::Has(planner) => {
                let has = planner.has();
                binary!(owner has attribute, Has(HasInstruction), HasReverse(HasReverseInstruction))
            }
            ConstraintVertex::Links(planner) => {
                let links = planner.links();
                // binary!() works here even though links is ostensibly ternary
                binary!((with role_type) relation links player, Links(LinksInstruction), LinksReverse(LinksReverseInstruction))
            }
            ConstraintVertex::IndexedRelation(planner) => {
                assert_ne!(inputs.len(), 5);
                let player_1 = planner.indexed_relation().player_1().as_variable().unwrap();
                let player_2 = planner.indexed_relation().player_2().as_variable().unwrap();
                let relation = planner.indexed_relation().relation().as_variable().unwrap();
                let player_1_role = planner.indexed_relation().role_type_1().as_variable().unwrap();
                let player_2_role = planner.indexed_relation().role_type_2().as_variable().unwrap();

                let annotations = self
                    .local_annotations
                    .constraint_annotations_of(planner.indexed_relation().clone().into())
                    .unwrap()
                    .as_indexed_relation();
                let array_inputs = Inputs::build_from(&inputs);

                let direction = if !inputs.contains(&player_1) && !inputs.contains(&player_2) {
                    let CostMetaData::Direction(unbound_direction) = metadata else {
                        unreachable!("expected metadata for constraint")
                    };
                    unbound_direction
                } else if inputs.contains(&player_2) {
                    Direction::Reverse
                } else {
                    Direction::Canonical
                };

                let instruction = if direction == Direction::Canonical {
                    IndexedRelationInstruction::new(
                        player_1,
                        player_2,
                        relation,
                        player_1_role,
                        player_2_role,
                        array_inputs,
                        annotations.relation_to_player_1.clone(),
                        &annotations.player_1_to_relation,
                        &annotations.relation_to_player_2,
                        Arc::new(
                            annotations
                                .player_1_to_role
                                .values()
                                .flat_map(|set| set.iter().map(|type_| type_.as_role_type()))
                                .collect(),
                        ),
                        Arc::new(
                            annotations
                                .player_2_to_role
                                .values()
                                .flat_map(|set| set.iter().map(|type_| type_.as_role_type()))
                                .collect(),
                        ),
                    )
                } else {
                    IndexedRelationInstruction::new(
                        player_2,
                        player_1,
                        relation,
                        player_2_role,
                        player_1_role,
                        array_inputs,
                        annotations.relation_to_player_2.clone(),
                        &annotations.player_2_to_relation,
                        &annotations.relation_to_player_1,
                        Arc::new(
                            annotations
                                .player_2_to_role
                                .values()
                                .flat_map(|set| set.iter().map(|type_| type_.as_role_type()))
                                .collect(),
                        ),
                        Arc::new(
                            annotations
                                .player_1_to_role
                                .values()
                                .flat_map(|set| set.iter().map(|type_| type_.as_role_type()))
                                .collect(),
                        ),
                    )
                };
                let sort_variable = sort_variable.unwrap_or(instruction.first_unbound_component());
                let instruction = ConstraintInstruction::IndexedRelation(instruction);
                conjunction_builder.push_instruction(sort_variable, instruction);
            }
        }
    }

    fn lower_constraint_check(
        &self,
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        constraint: &ConstraintVertex<'_>,
    ) {
        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs = $con.$lhs();
                let rhs = $con.$rhs();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();

                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();

                assert!(num_input_variables > 0);

                let lhs_pos = lhs.clone().map(conjunction_builder.position_mapping());
                let rhs_pos = rhs.clone().map(conjunction_builder.position_mapping());
                let check = CheckInstruction::$fw {
                    $lhs: CheckVertex::resolve(lhs_pos, self.local_annotations),
                    $rhs: CheckVertex::resolve(rhs_pos, self.local_annotations),
                    $($with: $con.$with(),)?
                };

                conjunction_builder.push_check(check);
            }};
        }

        match constraint {
            ConstraintVertex::TypeList(type_list) => {
                let instruction = type_list.lower_check();
                conjunction_builder.push_check(instruction.map(conjunction_builder.position_mapping()));
            }

            ConstraintVertex::Iid(iid) => {
                let var = iid.iid().var().as_variable().unwrap();
                let instruction = CheckInstruction::Iid { var, iid: iid.iid().iid().as_parameter().unwrap().clone() };
                conjunction_builder.push_check(instruction.map(conjunction_builder.position_mapping()));
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

                let relation_pos = conjunction_builder.position(relation).into();
                let player_pos = conjunction_builder.position(player).into();
                let role_pos = conjunction_builder.position(role).into();

                let check = CheckInstruction::Links {
                    relation: CheckVertex::resolve(relation_pos, self.local_annotations),
                    player: CheckVertex::resolve(player_pos, self.local_annotations),
                    role: CheckVertex::resolve(role_pos, self.local_annotations),
                };

                conjunction_builder.push_check(check);
            }
            ConstraintVertex::IndexedRelation(planner) => {
                let player_1 = planner.indexed_relation().player_1().as_variable().unwrap();
                let player_2 = planner.indexed_relation().player_2().as_variable().unwrap();
                let relation = planner.indexed_relation().relation().as_variable().unwrap();
                let player_1_role = planner.indexed_relation().role_type_1().as_variable().unwrap();
                let player_2_role = planner.indexed_relation().role_type_2().as_variable().unwrap();

                // arbitrarily choosing player 1 as start
                let start_player_pos = conjunction_builder.position(player_1).into();
                let end_player_pos = conjunction_builder.position(player_2).into();
                let relation_pos = conjunction_builder.position(relation).into();
                let start_role_pos = conjunction_builder.position(player_1_role).into();
                let end_role_pos = conjunction_builder.position(player_2_role).into();
                let check = CheckInstruction::IndexedRelation {
                    start_player: CheckVertex::resolve(start_player_pos, self.local_annotations),
                    end_player: CheckVertex::resolve(end_player_pos, self.local_annotations),
                    relation: CheckVertex::resolve(relation_pos, self.local_annotations),
                    start_role: CheckVertex::resolve(start_role_pos, self.local_annotations),
                    end_role: CheckVertex::resolve(end_role_pos, self.local_annotations),
                };
                conjunction_builder.push_check(check);
            }
        }
    }

    pub(super) fn referenced_input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.graph.referenced_input_variables()
    }

    /// Return variables in the constraints of the conjunction, but excluding any other inputs or nested variables
    pub(super) fn constraint_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.graph.constraint_variables()
    }

    pub(super) fn cost(&self) -> Cost {
        self.planner_statistics.query_cost
    }

    fn may_make_input_check_steps(
        &self,
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        input_variables: impl IntoIterator<Item = Variable> + Clone,
        input_variable_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<answer::Type>>>,
        variable_registry: &VariableRegistry,
    ) {
        let mut pushed_any = false;
        input_variables
            .clone()
            .into_iter()
            .filter_map(|variable| {
                let vertex = variable.into();
                let local_annotations = self.local_annotations.vertex_annotations_of(&vertex)?;
                input_variable_annotations
                    .get(&vertex)? // Functions don't have any
                    .iter()
                    .any(|type_| !local_annotations.contains(type_))
                    .then(|| (variable, local_annotations.clone()))
            })
            .for_each(|(variable, types)| {
                let category = variable_registry.get_variable_category(variable).unwrap();
                debug_assert!(category.is_category_thing() || category.is_category_type());
                let executor_var = conjunction_builder.position(variable);
                let check = match category.is_category_thing() {
                    true => CheckInstruction::ThingTypeList { thing_var: executor_var, types },
                    false => CheckInstruction::TypeList { type_var: executor_var, types },
                };
                conjunction_builder.push_check(check);
                pushed_any = true;
            });
        if pushed_any {
            conjunction_builder.finish_one();
        }
        self.check_optional_inputs(conjunction_builder, input_variables, variable_registry);
    }

    fn check_optional_inputs(
        &self,
        conjunction_builder: &mut ConjunctionExecutableBuilder,
        input_variables: impl IntoIterator<Item = Variable>,
        variable_registry: &VariableRegistry,
    ) {
        let mut optional_inputs_in_constraints = input_variables
            .into_iter()
            .filter(|&var| {
                variable_registry
                    .get_variable_optionality(var)
                    .is_some_and(|optionality| matches!(optionality, VariableOptionality::Optional))
            })
            .filter(|var| conjunction_builder.constraint_variables.contains(var))
            .peekable();
        if optional_inputs_in_constraints.peek().is_some() {
            let instruction = CheckInstruction::NotNone {
                variables: optional_inputs_in_constraints.map(|var| conjunction_builder.position(var)).collect(),
            };
            conjunction_builder.push_check(instruction);
            conjunction_builder.finish_one();
        } else {
            drop(optional_inputs_in_constraints);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlanBuilder<'a> {
    pub(super) branch_ids: Vec<BranchID>,
    pub(super) branches: Vec<ConjunctionPlanBuilder<'a>>,
    pub(super) required_inputs: Vec<Variable>,
}

impl<'a> DisjunctionPlanBuilder<'a> {
    fn new(
        branch_ids: Vec<BranchID>,
        branches: Vec<ConjunctionPlanBuilder<'a>>,
        required_inputs: Vec<Variable>,
    ) -> Self {
        Self { branch_ids, branches, required_inputs }
    }

    pub(super) fn branches(&self) -> &[ConjunctionPlanBuilder<'a>] {
        &self.branches
    }
}

#[derive(Clone, Debug)]
pub(super) struct DisjunctionPlan<'a> {
    branch_ids: Vec<BranchID>,
    branches: Vec<ConjunctionPlan<'a>>,
    _cost: Cost,
}

impl<'a> DisjunctionPlan<'a> {
    pub(crate) fn new(branch_ids: Vec<BranchID>, branches: Vec<ConjunctionPlan<'a>>, _cost: Cost) -> Self {
        Self { branch_ids, branches, _cost }
    }

    fn lower(
        &self,
        input_variable_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<answer::Type>>>,
        disjunction_inputs: impl IntoIterator<Item = Variable> + Clone,
        selected_variables: HashSet<Variable>,
        assigned_positions: &HashMap<Variable, ExecutorVariable>,
        variable_registry: &VariableRegistry,
    ) -> Result<DisjunctionBuilder, QueryPlanningError> {
        let mut branches: Vec<_> = Vec::with_capacity(self.branches.len());
        let mut assigned_positions = assigned_positions.clone();
        let disjunction_inputs: Vec<_> = disjunction_inputs.into_iter().collect();
        for (branch_id, branch) in self.branch_ids.iter().zip(self.branches.iter()) {
            let lowered_branch = branch.lower(
                input_variable_annotations,
                disjunction_inputs.iter().copied(),
                selected_variables.clone(),
                &assigned_positions,
                variable_registry,
                Some(*branch_id),
            )?;
            assigned_positions = lowered_branch.position_mapping().clone();
            branches.push(lowered_branch);
        }
        Ok(DisjunctionBuilder::new(self.branch_ids.clone(), branches))
    }
}

#[derive(Clone, Debug)]
pub(super) struct OptionalPlan<'a> {
    branch_id: BranchID,
    optional_variables: Vec<Variable>,
    plan: ConjunctionPlan<'a>,
}

impl<'a> OptionalPlan<'a> {
    fn new(branch_id: BranchID, optional_variables: Vec<Variable>, plan: ConjunctionPlan<'a>) -> Self {
        Self { branch_id, optional_variables, plan }
    }

    pub(crate) fn plan(&self) -> &ConjunctionPlan<'a> {
        &self.plan
    }

    pub(crate) fn referenced_input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.plan.referenced_input_variables()
    }

    pub(crate) fn optional_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.optional_variables.iter().copied()
    }
}

#[derive(Clone, Default)]
pub(super) struct Graph<'a> {
    variable_to_pattern: HashMap<VariableVertexId, HashSet<PatternVertexId>>,
    pattern_to_variable: HashMap<PatternVertexId, HashSet<VariableVertexId>>,

    elements: HashMap<VertexId, PlannerVertex<'a>>,

    pub(super) variable_index: HashMap<Variable, VariableVertexId>,
    pub(super) index_to_variable: HashMap<VariableVertexId, Variable>,

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

impl fmt::Display for Graph<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}:", type_name_of_val(self))?;
        write!(f, "    variable index: {:?}", self.variable_index)?;

        writeln!(f, "    patterns: ")?;
        for (vertex, elt) in &self.elements {
            writeln!(f, "        {vertex:?}: {elt:?}")?;
        }

        for (p, vars) in &self.pattern_to_variable {
            writeln!(f, "    {p:?} -> {vars:?}")?;
        }

        Ok(())
    }
}

impl<'a> Graph<'a> {
    fn push_variable(&mut self, variable: Variable, vertex: VariableVertex) {
        let index = self.next_variable_index();
        self.elements.insert(VertexId::Variable(index), PlannerVertex::Variable(vertex));
        self.variable_index.insert(variable, index);
        self.index_to_variable.insert(index, variable);
    }

    fn push_constraint(&mut self, constraint: ConstraintVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(constraint.variable_vertex_ids());
        for var in constraint.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Constraint(constraint));
    }

    fn push_is(&mut self, is: IsVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(is.variable_vertex_ids());
        for var in is.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Is(is));
    }

    fn push_links_deduplication(&mut self, deduplication: LinksDeduplicationVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(deduplication.variable_vertex_ids());
        for var in deduplication.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::LinksDeduplication(deduplication));
    }

    fn push_comparison(&mut self, comparison: ComparisonVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(comparison.variable_vertex_ids());
        for var in comparison.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Comparison(comparison));
    }

    fn push_optimised_to_unsatisfiable(&mut self, optimised_unsatisfiable: UnsatisfiableVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default();
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Unsatisfiable(optimised_unsatisfiable));
    }

    fn push_expression(&mut self, output: VariableVertexId, expression: ExpressionVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(expression.variable_vertex_ids());
        for var in expression.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Expression(expression));

        let output_planner = self.elements.get_mut(&VertexId::Variable(output)).unwrap();
        output_planner.as_variable_mut().unwrap().set_binding(pattern_index);
    }

    fn push_function_call(&mut self, function_call: FunctionCallVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(function_call.variable_vertex_ids());
        for var in function_call.variable_vertex_ids() {
            self.variable_to_pattern.entry(var).or_default().insert(pattern_index);
        }
        let assigned = function_call.assigned.clone();
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::FunctionCall(function_call));
        assigned.into_iter().for_each(|vertex| {
            let output_planner = self.elements.get_mut(&VertexId::Variable(vertex)).unwrap();
            output_planner.as_variable_mut().unwrap().set_binding(pattern_index);
        })
    }

    fn push_disjunction(&mut self, disjunction: DisjunctionVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(disjunction.variable_vertex_ids());
        for var_id in disjunction.variable_vertex_ids() {
            self.variable_to_pattern.entry(var_id).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Disjunction(disjunction));
    }

    fn push_negation(&mut self, negation: NegationVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(negation.variable_vertex_ids());
        for var_id in negation.variable_vertex_ids() {
            self.variable_to_pattern.entry(var_id).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Negation(negation));
    }

    fn push_optional(&mut self, optional: OptionalVertex<'a>) {
        let pattern_index = self.next_pattern_index();
        self.pattern_to_variable.entry(pattern_index).or_default().extend(optional.variable_vertex_ids());
        for var_id in optional.variable_vertex_ids() {
            self.variable_to_pattern.entry(var_id).or_default().insert(pattern_index);
        }
        self.elements.insert(VertexId::Pattern(pattern_index), PlannerVertex::Optional(optional));
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

    pub(super) fn elements(&self) -> &HashMap<VertexId, PlannerVertex<'a>> {
        &self.elements
    }

    pub(super) fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.referenced_variable_ids().map(|var_id| self.index_to_variable[&var_id])
    }

    fn referenced_variable_ids(&self) -> impl Iterator<Item = VariableVertexId> + '_ {
        self.variable_to_pattern.keys().copied()
    }

    pub(super) fn referenced_input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.referenced_variable_ids().filter_map(|id| match &self.elements[&VertexId::Variable(id)] {
            PlannerVertex::Variable(VariableVertex::Input(input)) => Some(input.variable()),
            _ => None,
        })
    }

    /// Return variables in the constraints of the conjunction, but excluding nested variables
    pub(super) fn constraint_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.elements
            .iter()
            .filter(|(_vertex_id, planner_vertex)| {
                match planner_vertex {
                PlannerVertex::Constraint(_)
                | PlannerVertex::Is(_)
                | PlannerVertex::LinksDeduplication(_)
                | PlannerVertex::Comparison(_)
                | PlannerVertex::Expression(_)
                | PlannerVertex::FunctionCall(_) => true,
                PlannerVertex::Unsatisfiable(_)
                // exclude Variables because they could be un-used inputs
                | PlannerVertex::Variable(_)
                | PlannerVertex::Negation(_)
                | PlannerVertex::Disjunction(_)
                | PlannerVertex::Optional(_) => false,
            }
            })
            .flat_map(|(vertex_id, _)| match vertex_id {
                VertexId::Variable(_) => unreachable!("Variables should be filtered out."),
                VertexId::Pattern(pattern_index) => {
                    self.pattern_to_variable[pattern_index].iter().map(|index| self.index_to_variable[index])
                }
            })
    }
}
