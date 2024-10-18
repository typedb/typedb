/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
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
use itertools::Itertools;

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
                RelatesReverseInstruction, SubInstruction, SubReverseInstruction, TypeListInstruction,
            },
            CheckInstruction, CheckVertex, ConstraintInstruction, Inputs,
        },
        planner::{
            match_executable::MatchExecutable,
            vertex::{
                constraint::{
                    ConstraintVertex, HasPlanner, IsaPlanner, LinksPlanner, OwnsPlanner, PlaysPlanner, RelatesPlanner,
                    SubPlanner,
                },
                variable::{InputPlanner, ThingPlanner, TypePlanner, ValuePlanner, VariableVertex},
                ComparisonPlanner, Costed, Direction, DisjunctionPlanner, ElementCost, FunctionCallPlanner, Input,
                NegationPlanner, PlannerVertex, TypeListPlanner,
            },
            IntersectionBuilder, MatchExecutableBuilder, NegationBuilder, StepBuilder,
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
        input_variables,
        type_annotations,
        variable_registry,
        _expressions,
        statistics,
    );

    let ordering = plan_builder.initialise_greedy();

    plan_builder.plan()
}

fn make_builder<'a>(
    conjunction: &'a Conjunction,
    block_context: &BlockContext,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &'a TypeAnnotations,
    variable_registry: &VariableRegistry,
    _expressions: &HashMap<Variable, CompiledExpression>,
    statistics: &'a Statistics,
) -> PlanBuilder<'a> {
    let subplans = conjunction
        .nested_patterns()
        .iter()
        .map(|pattern| match pattern {
            NestedPattern::Disjunction(_) => todo!(),
            NestedPattern::Negation(negation) => plan_conjunction(
                negation.conjunction(),
                block_context,
                variable_positions,
                type_annotations,
                variable_registry,
                _expressions,
                statistics,
            ),
            NestedPattern::Optional(_) => todo!(),
        })
        .collect_vec();

    let mut plan_builder = PlanBuilder::new(type_annotations, statistics);
    plan_builder.register_variables(
        conjunction.captured_variables(block_context),
        conjunction.declared_variables(block_context),
        variable_registry,
    );
    plan_builder.register_constraints(conjunction);
    plan_builder.register_negations(subplans);
    plan_builder
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

#[derive(Debug)]
pub(crate) struct PlanBuilder<'a> {
    shared_variables: Vec<usize>,
    elements: Vec<PlannerVertex<'a>>,
    variable_index: HashMap<Variable, usize>,
    index_to_constraint: HashMap<usize, &'a Constraint<Variable>>,
    index_to_disjunction_plan: HashMap<usize, Option<ConjunctionPlan<'a>>>,
    index_to_negation_plan: HashMap<usize, ConjunctionPlan<'a>>,
    adjacency: HashMap<usize, HashSet<usize>>,
    type_annotations: &'a TypeAnnotations,
    statistics: &'a Statistics,
}

impl<'a> PlanBuilder<'a> {
    fn new(type_annotations: &'a TypeAnnotations, statistics: &'a Statistics) -> Self {
        Self {
            shared_variables: Vec::new(),
            elements: Vec::new(),
            variable_index: HashMap::new(),
            index_to_constraint: HashMap::new(),
            index_to_disjunction_plan: HashMap::new(),
            index_to_negation_plan: HashMap::new(),
            adjacency: HashMap::new(),
            type_annotations,
            statistics,
        }
    }

    fn register_variables(
        &mut self,
        shared_variables: impl Iterator<Item = Variable>,
        local_variables: impl Iterator<Item = Variable>,
        variable_registry: &VariableRegistry,
    ) {
        self.elements.reserve(shared_variables.size_hint().0 + local_variables.size_hint().0);
        self.shared_variables.reserve(shared_variables.size_hint().0);

        for variable in shared_variables {
            // FIXME shared variables aren't necessarily bound before the conjunction is entered
            self.register_input_var(variable);
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
            }
        }
    }

    fn register_input_var(&mut self, variable: Variable) {
        let planner = InputPlanner::from_variable(variable, self.type_annotations);
        let index = self.elements.len();
        self.elements.push(PlannerVertex::Variable(VariableVertex::Input(planner)));
        self.shared_variables.push(index);
        self.variable_index.insert(variable, index);
    }

    fn register_type_var(&mut self, variable: Variable) {
        let planner = TypePlanner::from_variable(variable, self.type_annotations);
        let index = self.elements.len();
        self.elements.push(PlannerVertex::Variable(VariableVertex::Type(planner)));
        self.variable_index.insert(variable, index);
    }

    fn register_thing_var(&mut self, variable: Variable) {
        let planner = ThingPlanner::from_variable(variable, self.type_annotations, self.statistics);
        let index = self.elements.len();
        self.elements.push(PlannerVertex::Variable(VariableVertex::Thing(planner)));
        self.variable_index.insert(variable, index);
    }

    fn register_value_var(&mut self, variable: Variable) {
        let planner = ValuePlanner::from_variable(variable);
        let index = self.elements.len();
        self.elements.push(PlannerVertex::Variable(VariableVertex::Value(planner)));
        self.variable_index.insert(variable, index);
    }

    fn register_constraints(&mut self, conjunction: &'a Conjunction) {
        let num_vars = self.elements.len();

        for constraint in conjunction.constraints() {
            let planner_index = self.elements.len();
            self.index_to_constraint.insert(planner_index, constraint);

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

                Constraint::ExpressionBinding(expression) => {
                    if expression.expression().is_constant() {
                        let lhs = self.variable_index[&expression.left().as_variable().unwrap()];
                        if self.elements[lhs].is_value() {
                            self.elements[lhs] = PlannerVertex::constant();
                            self.index_to_constraint.remove(&planner_index); // unregister
                        } else {
                            todo!("non-value var assignment?")
                        }
                    } else {
                        self.register_expression_binding(expression);
                    }
                }
            }
        }

        for (planner_index, planner) in self.elements.iter().enumerate().skip(num_vars) {
            self.adjacency.entry(planner_index).or_default().extend(planner.variables());
            for v in planner.variables() {
                self.adjacency.entry(v).or_default().insert(planner_index);
            }
        }
    }

    fn register_label(&mut self, label: &Label<Variable>) {
        let planner = PlannerVertex::label(TypeListPlanner::from_label_constraint(
            label,
            &self.variable_index,
            self.type_annotations,
        ));
        self.elements.push(planner);
    }

    fn register_role_name(&mut self, role_name: &RoleName<Variable>) {
        let planner = PlannerVertex::label(TypeListPlanner::from_role_name_constraint(
            role_name,
            &self.variable_index,
            self.type_annotations,
        ));
        self.elements.push(planner);
    }

    fn register_kind(&mut self, kind: &Kind<Variable>) {
        let planner = PlannerVertex::label(TypeListPlanner::from_kind_constraint(
            kind,
            &self.variable_index,
            self.type_annotations,
        ));
        self.elements.push(planner);
    }

    fn register_sub(&mut self, sub: &Sub<Variable>) {
        let planner = SubPlanner::from_constraint(sub, &self.variable_index, self.type_annotations);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Sub(planner)));
    }

    fn register_owns(&mut self, owns: &Owns<Variable>) {
        let planner = OwnsPlanner::from_constraint(owns, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Owns(planner)));
    }

    fn register_relates(&mut self, relates: &Relates<Variable>) {
        let planner =
            RelatesPlanner::from_constraint(relates, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Relates(planner)));
    }

    fn register_plays(&mut self, plays: &Plays<Variable>) {
        let planner =
            PlaysPlanner::from_constraint(plays, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Plays(planner)));
    }

    fn register_isa(&mut self, isa: &Isa<Variable>) {
        let planner = IsaPlanner::from_constraint(isa, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Isa(planner)));
    }

    fn register_has(&mut self, has: &Has<Variable>) {
        let planner = HasPlanner::from_constraint(has, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Has(planner)));
    }

    fn register_links(&mut self, links: &Links<Variable>) {
        let planner =
            LinksPlanner::from_constraint(links, &self.variable_index, self.type_annotations, self.statistics);
        self.elements.push(PlannerVertex::Constraint(ConstraintVertex::Links(planner)));
    }

    fn register_expression_binding(&mut self, expression: &ExpressionBinding<Variable>) {
        let lhs = self.variable_index[&expression.left().as_variable().unwrap()];
        let planner_index = self.elements.len();
        self.adjacency.entry(lhs).or_default().insert(planner_index);
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

    fn register_comparison(&mut self, comparison: &Comparison<Variable>) {
        let lhs = Input::from_vertex(comparison.lhs(), &self.variable_index);
        let rhs = Input::from_vertex(comparison.rhs(), &self.variable_index);
        if let Input::Variable(lhs) = lhs {
            let lhs = self.elements[lhs].as_variable_mut().unwrap();
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
            let rhs = self.elements[rhs].as_variable_mut().unwrap();
            match comparison.comparator() {
                Comparator::Equal => rhs.add_equal(lhs),
                Comparator::NotEqual => (), // no tangible impact on traversal costs
                Comparator::Less | Comparator::LessOrEqual => rhs.add_upper_bound(lhs),
                Comparator::Greater | Comparator::GreaterOrEqual => rhs.add_lower_bound(lhs),
                Comparator::Like => todo!("like operator"),
                Comparator::Contains => todo!("contains operator"),
            }
        }
        self.elements.push(PlannerVertex::Comparison(ComparisonPlanner::from_constraint(
            comparison,
            &self.variable_index,
            self.type_annotations,
            self.statistics,
        )));
    }

    fn register_disjunctions(&mut self, disjunctions: Vec<Vec<PlanBuilder<'a>>>) {
        for disjunction in disjunctions {
            let index = self.elements.len();
            self.elements.push(PlannerVertex::Disjunction(DisjunctionPlanner::from_builders(disjunction)));
            self.index_to_disjunction_plan.insert(index, None);
        }

        for (planner_index, planner) in self.elements.iter().enumerate() {
            self.adjacency.entry(planner_index).or_default().extend(planner.variables());
            for v in planner.variables() {
                self.adjacency.entry(v).or_default().insert(planner_index);
            }
        }
    }

    fn register_negations(&mut self, subplans: Vec<ConjunctionPlan<'a>>) {
        for subplan in subplans {
            let index = self.elements.len();
            self.elements
                .push(PlannerVertex::Negation(NegationPlanner::new(subplan.shared_variables.clone(), subplan.cost)));
            self.index_to_negation_plan.insert(index, subplan);
        }

        for (planner_index, planner) in self.elements.iter().enumerate() {
            self.adjacency.entry(planner_index).or_default().extend(planner.variables());
            for v in planner.variables() {
                self.adjacency.entry(v).or_default().insert(planner_index);
            }
        }
    }

    fn initialise_greedy(&self) -> Vec<usize> {
        let mut open_set: HashSet<usize> = (self.shared_variables.len()..self.elements.len()).collect();
        let mut ordering = Vec::with_capacity(self.elements.len());
        ordering.extend_from_slice(&self.shared_variables);

        let mut produced_at_this_stage = HashSet::new();
        let mut intersection_variable = None;

        while !open_set.is_empty() {
            let (next, _cost) = open_set
                .iter()
                .filter(|&&elem| self.elements[elem].is_valid(elem, &ordering, &self.adjacency))
                .map(|&elem| (elem, self.calculate_marginal_cost(&ordering, elem)))
                .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
                .unwrap();

            if intersection_variable == Some(next) {
                intersection_variable = None;
            }

            let element = &self.elements[next];

            if !element.is_variable() {
                match element.variables().filter(|var| produced_at_this_stage.contains(var)).exactly_one() {
                    Ok(var) if intersection_variable.is_none() || intersection_variable == Some(var) => {
                        intersection_variable = Some(var);
                    }
                    _ => {
                        let produced =
                            produced_at_this_stage.drain().filter(|var| !ordering.contains(var)).collect_vec();
                        ordering.extend_from_slice(&produced);
                        for var in produced {
                            open_set.remove(&var);
                        }
                        intersection_variable = None;
                    }
                }

                produced_at_this_stage.extend(element.variables());
            }

            ordering.push(next);
            open_set.remove(&next);
        }
        ordering
    }

    fn calculate_marginal_cost(&self, prefix: &[usize], next: usize) -> f64 {
        assert!(!prefix.contains(&next));
        let adjacent = self.adjacency.get(&next);
        let preceding = adjacent.into_iter().flatten().filter(|adj| prefix.contains(adj)).copied().collect_vec();
        let planner_vertex = &self.elements[next];
        let ElementCost { per_input, per_output, branching_factor } = planner_vertex.cost(&preceding, &self.elements);
        per_input + branching_factor * per_output
    }

    fn plan(self) -> ConjunctionPlan<'a> {
        let ordering = self.initialise_greedy();

        let cost = ordering
            .iter()
            .enumerate()
            .map(|(i, &idx)| self.elements[idx].cost(&ordering[..i], &self.elements))
            .fold(ElementCost::default(), |acc, e| acc.chain(e));

        let Self {
            shared_variables,
            elements,
            variable_index,
            index_to_constraint,
            index_to_negation_plan: index_to_negation,
            index_to_disjunction_plan: _,
            adjacency,
            type_annotations,
            statistics: _,
        } = self;

        ConjunctionPlan {
            shared_variables,
            elements,
            variable_index,
            index_to_constraint,
            index_to_negation,
            adjacency,
            type_annotations,
            ordering,
            cost,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConjunctionPlan<'a> {
    shared_variables: Vec<usize>,
    elements: Vec<PlannerVertex<'a>>,
    variable_index: HashMap<Variable, usize>,
    index_to_constraint: HashMap<usize, &'a Constraint<Variable>>,
    index_to_negation: HashMap<usize, ConjunctionPlan<'a>>,
    adjacency: HashMap<usize, HashSet<usize>>,
    type_annotations: &'a TypeAnnotations,
    ordering: Vec<usize>,
    cost: ElementCost,
}

impl ConjunctionPlan<'_> {
    pub(crate) fn lower(
        &self,
        input_variables: &HashMap<Variable, VariablePosition>,
        variable_registry: Arc<VariableRegistry>,
    ) -> MatchExecutable {
        let index_to_variable: HashMap<_, _> =
            self.variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();

        let mut match_builder = MatchExecutableBuilder::with_inputs(input_variables);

        let mut producers = HashMap::with_capacity(self.variable_index.len());

        let element_to_order: HashMap<_, _> =
            self.ordering.iter().copied().enumerate().map(|(order, index)| (index, order)).collect();

        let inputs_of = |x| {
            let order = element_to_order[&x];
            let adjacent = match self.adjacency.get(&x) {
                Some(adj) => adj,
                None => &HashSet::new(),
            };
            adjacent.iter().copied().filter(|adj| self.ordering[..order].contains(adj)).collect::<HashSet<_>>()
        };

        let outputs_of = |x| {
            let order = element_to_order[&x];
            let adjacent = match self.adjacency.get(&x) {
                Some(adj) => adj,
                None => &HashSet::new(),
            };
            adjacent.iter().copied().filter(|adj| self.ordering[order..].contains(adj)).collect::<HashSet<_>>()
        };

        for &index in &self.ordering {
            if index_to_variable.contains_key(&index) {
                continue;
            }

            let inputs = inputs_of(index).into_iter().map(|var| index_to_variable[&var]).collect_vec();

            if let Some(constraint) = self.index_to_constraint.get(&index) {
                let sort_variable = outputs_of(index)
                    .into_iter()
                    .find(|&var| inputs_of(var).len() > 1)
                    .map(|var| index_to_variable[&var]);

                self.lower_constraint(&mut match_builder, &mut producers, constraint, index, inputs, sort_variable)
            } else if let Some(subpattern) = self.index_to_negation.get(&index) {
                assert!(outputs_of(index).is_empty());
                let negation = subpattern.lower(match_builder.position_mapping(), variable_registry.clone());
                let variable_positions = negation.variable_positions().clone(); // FIXME needless clone
                match_builder.push_step(&variable_positions, StepBuilder::Negation(NegationBuilder { negation }));
            } else {
                unreachable!()
            }
        }

        match_builder.finish(variable_registry)
    }

    fn lower_constraint(
        &self,
        match_builder: &mut MatchExecutableBuilder,
        producers: &mut HashMap<Variable, (usize, usize)>,
        constraint: &Constraint<Variable>,
        index: usize,
        inputs: Vec<Variable>,
        sort_variable: Option<Variable>,
    ) {
        let planner = &self.elements[index];

        if let Some(StepBuilder::Intersection(IntersectionBuilder { sort_variable: Some(sort_variable), .. })) =
            &match_builder.current
        {
            if !constraint.ids().contains(sort_variable) {
                match_builder.finish_one();
            }
        }

        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let planner = planner.as_constraint().unwrap();

                let lhs = $con.$lhs();
                let rhs = $con.$rhs();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();

                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();

                assert!(num_input_variables > 0);

                if inputs.len() == num_input_variables {
                    let lhs_producer = lhs_var
                        .filter(|lhs| !self.elements[self.variable_index[lhs]].is_input())
                        .map(|lhs| producers.get(&lhs).expect("bound lhs must have been produced"));
                    let rhs_producer = rhs_var
                        .filter(|rhs| !self.elements[self.variable_index[rhs]].is_input())
                        .map(|rhs| producers.get(&rhs).expect("bound rhs must have been produced"));

                    let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                    let rhs_pos = rhs.clone().map(match_builder.position_mapping());
                    let check = CheckInstruction::$fw {
                        $lhs: CheckVertex::resolve(lhs_pos, self.type_annotations),
                        $rhs: CheckVertex::resolve(rhs_pos, self.type_annotations),
                        $($with: $con.$with(),)?
                    };

                    match_builder.push_check(Ord::max(lhs_producer, rhs_producer), check);
                    return;
                }

                let sort_variable = if let Some(sort_variable) = sort_variable {
                    sort_variable
                } else {
                    match (lhs_var, rhs_var) {
                        (Some(lhs), Some(rhs)) if !inputs.is_empty() => {
                            if inputs.contains(&rhs) {
                                lhs
                            } else {
                                rhs
                            }
                        }
                        (Some(lhs), Some(rhs)) => {
                            if planner.unbound_direction() == Direction::Canonical {
                                lhs
                            } else {
                                rhs
                            }
                        }
                        (Some(lhs), None) => lhs,
                        (None, Some(rhs)) => rhs,
                        (None, None) => unreachable!("no variables in constraint?"),
                    }
                };

                let con = $con.clone();
                let instruction = if lhs_var.is_some_and(|lhs| inputs.contains(&lhs)) {
                    ConstraintInstruction::$fw($fwi::new(con, Inputs::Single([lhs_var.unwrap()]), self.type_annotations))
                } else if rhs_var.is_some_and(|rhs| inputs.contains(&rhs)) {
                    ConstraintInstruction::$bw($bwi::new(con, Inputs::Single([rhs_var.unwrap()]), self.type_annotations))
                } else if planner.unbound_direction() == Direction::Canonical {
                    ConstraintInstruction::$fw($fwi::new(con, Inputs::None([]), self.type_annotations))
                } else {
                    ConstraintInstruction::$bw($bwi::new(con, Inputs::None([]), self.type_annotations))
                };

                let producer_index = match_builder.push_instruction(
                    sort_variable,
                    instruction,
                    [lhs_var, rhs_var].into_iter().flatten(),
                );

                for var in [lhs_var, rhs_var].into_iter().flatten() {
                    if !inputs.contains(&var) {
                        producers.insert(var, producer_index);
                    }
                }
            }};
        }

        match constraint {
            Constraint::Kind(kind) => {
                let var = kind.type_().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, self.type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }
            Constraint::RoleName(name) => {
                let var = name.type_().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, self.type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }
            Constraint::Label(label) => {
                let var = label.type_().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, self.type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }

            Constraint::Isa(isa) => {
                binary!((with isa_kind) thing isa type_, Isa(IsaInstruction), IsaReverse(IsaReverseInstruction))
            }
            Constraint::Sub(sub) => {
                binary!((with sub_kind) subtype sub supertype, Sub(SubInstruction), SubReverse(SubReverseInstruction))
            }
            Constraint::Owns(owns) => {
                binary!(owner owns attribute, Owns(OwnsInstruction), OwnsReverse(OwnsReverseInstruction))
            }
            Constraint::Relates(relates) => {
                binary!(relation relates role_type, Relates(RelatesInstruction), RelatesReverse(RelatesReverseInstruction))
            }
            Constraint::Plays(plays) => {
                binary!(player plays role_type, Plays(PlaysInstruction), PlaysReverse(PlaysReverseInstruction))
            }

            Constraint::Has(has) => {
                binary!(owner has attribute, Has(HasInstruction), HasReverse(HasReverseInstruction))
            }

            Constraint::Links(links) => {
                let planner = planner.as_constraint().unwrap();
                let relation = links.relation().as_variable().unwrap();
                let player = links.player().as_variable().unwrap();
                let role = links.role_type().as_variable().unwrap();

                if inputs.len() == 3 {
                    let relation_producer = Some(relation)
                        .filter(|relation| !self.elements[self.variable_index[relation]].is_input())
                        .map(|relation| producers.get(&relation).expect("bound relation must have been produced"));
                    let player_producer = Some(player)
                        .filter(|player| !self.elements[self.variable_index[player]].is_input())
                        .map(|player| producers.get(&player).expect("bound player must have been produced"));
                    let role_producer = Some(role)
                        .filter(|role| !self.elements[self.variable_index[role]].is_input())
                        .map(|role| producers.get(&role).expect("bound role must have been produced"));

                    let relation_pos = match_builder.position(relation).into();
                    let player_pos = match_builder.position(player).into();
                    let role_pos = match_builder.position(role).into();

                    let check = CheckInstruction::Links {
                        relation: CheckVertex::resolve(relation_pos, self.type_annotations),
                        player: CheckVertex::resolve(player_pos, self.type_annotations),
                        role: CheckVertex::resolve(role_pos, self.type_annotations),
                    };

                    match_builder.push_check(relation_producer.max(player_producer).max(role_producer), check);
                    return;
                }

                let sort_variable = if let Some(sort_variable) = sort_variable {
                    sort_variable
                } else if inputs.contains(&player) {
                    relation
                } else if inputs.contains(&relation) {
                    player
                } else if planner.unbound_direction() == Direction::Canonical {
                    relation
                } else {
                    player
                };

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

                let producer_index =
                    match_builder.push_instruction(sort_variable, instruction, [relation, player, role]);

                for &var in &[relation, player, role] {
                    if !inputs.contains(&var) {
                        producers.insert(var, producer_index);
                    }
                }
            }

            Constraint::ExpressionBinding(_) => todo!("expression binding"),
            Constraint::FunctionCallBinding(_) => todo!("function call binding"),
            Constraint::Comparison(compare) => {
                let lhs = compare.lhs();
                let rhs = compare.rhs();
                let comparator = compare.comparator();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();
                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();
                assert!(num_input_variables > 0);
                if inputs.len() == num_input_variables {
                    let lhs_producer = lhs_var
                        .filter(|lhs| !self.elements[self.variable_index[lhs]].is_input())
                        .map(|lhs| producers.get(&lhs).expect("bound lhs must have been produced"));
                    let rhs_producer = rhs_var
                        .filter(|rhs| !self.elements[self.variable_index[rhs]].is_input())
                        .map(|rhs| producers.get(&rhs).expect("bound rhs must have been produced"));

                    let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                    let rhs_pos = rhs.clone().map(match_builder.position_mapping());

                    let check = CheckInstruction::Comparison {
                        lhs: CheckVertex::resolve(lhs_pos, self.type_annotations),
                        rhs: CheckVertex::resolve(rhs_pos, self.type_annotations),
                        comparator,
                    };

                    match_builder.push_check(Ord::max(lhs_producer, rhs_producer), check);
                    return;
                }
                todo!()
            }
        }
    }
}
