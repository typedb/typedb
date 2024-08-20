/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        constraint::{Constraint, ExpressionBinding},
        variable_category::VariableCategory,
    },
    program::block::{FunctionalBlock, VariableRegistry},
};
use itertools::Itertools;

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::{
        inference::type_annotations::TypeAnnotations,
        instructions::{
            ConstraintInstruction, HasInstruction, HasReverseInstruction, Inputs, IsaReverseInstruction,
            LinksInstruction, LinksReverseInstruction,
        },
        planner::vertex::{Costed, HasPlanner, LinksPlanner, PlannerVertex, ThingPlanner, VertexCost},
    },
};

pub struct PatternPlan {
    pub(crate) steps: Vec<Step>,
    pub(crate) context: VariableRegistry,
}

/*
1. Named variables that are not returned or reused beyond a step can simply be counted, and not outputted
2. Anonymous variables that are not reused beyond a step can just be checked for a single answer

Planner outputs an ordering over variables, with directions over which edges should be traversed.
If we know this we can:
  1. group edges intersecting into the same variable as one Step.
  2. if the ordering implies it, we may need to perform Storage/Comparison checks, if the variables are visited disconnected and then joined
  3. some checks are fully bound, while others are not... when do we decide? What is a Check versus an Iterate instructions? Do we need to differentiate?
 */

impl PatternPlan {
    pub fn new(steps: Vec<Step>, context: VariableRegistry) -> Self {
        Self { steps, context }
    }

    pub fn from_block(
        block: &FunctionalBlock,
        type_annotations: &TypeAnnotations,
        variable_registry: &VariableRegistry,
        expressions: &HashMap<Variable, CompiledExpression>,
        statistics: &Statistics,
    ) -> Self {
        assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
        let conjunction = block.conjunction();
        assert!(conjunction.nested_patterns().is_empty(), "TODO: nested patterns in root conjunction");

        let mut variable_index = HashMap::new();
        let mut variable_isa = HashMap::new();
        let mut elements = Vec::new();
        let mut adjacency: HashMap<usize, HashSet<usize>> = HashMap::new();

        // TODO: Consider block.input_variables()
        for variable in block.block_variables() {
            match variable_registry.get_variable_category(variable).unwrap() {
                VariableCategory::Type | VariableCategory::ThingType => (), // ignore for now
                VariableCategory::RoleType => {
                    variable_index.insert(variable, elements.len());
                }
                VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                    let planner = ThingPlanner::from_variable(variable, type_annotations, statistics);
                    variable_index.insert(variable, elements.len());
                    elements.push(PlannerVertex::Thing(planner));
                }
                VariableCategory::Value => todo!(),
                | VariableCategory::ObjectList
                | VariableCategory::ThingList
                | VariableCategory::AttributeList
                | VariableCategory::ValueList => todo!(),
            }
        }

        let mut index_to_constraint = HashMap::new();

        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) => (), // ignore for now
                Constraint::Isa(isa) => {
                    variable_isa.insert(isa.thing(), isa.clone());
                }
                Constraint::Links(links) => {
                    let planner = LinksPlanner::from_constraint(links, &variable_index, type_annotations, statistics);

                    let index = elements.len();

                    index_to_constraint.insert(index, constraint);

                    adjacency.entry(index).or_default().extend([planner.relation, planner.player, planner.role]);

                    adjacency.entry(planner.relation).or_default().insert(index);
                    adjacency.entry(planner.player).or_default().insert(index);
                    adjacency.entry(planner.role).or_default().insert(index);

                    elements.push(PlannerVertex::Links(planner));
                }
                Constraint::Has(has) => {
                    let planner = HasPlanner::from_constraint(has, &variable_index, type_annotations, statistics);

                    let index = elements.len();

                    index_to_constraint.insert(index, constraint);

                    adjacency.entry(index).or_default().extend([planner.owner, planner.attribute]);

                    adjacency.entry(planner.owner).or_default().insert(index);
                    adjacency.entry(planner.attribute).or_default().insert(index);

                    elements.push(PlannerVertex::Has(planner));
                }
                Constraint::ExpressionBinding(_) => todo!(),
                Constraint::FunctionCallBinding(_) => todo!(),
                Constraint::Comparison(_) => todo!(),
                Constraint::Owns(_) => todo!(),
                Constraint::Relates(_) => todo!(),
                Constraint::Plays(_) => todo!(),
            }
        }
        let ordering = initialise_plan_greedy(&elements, &adjacency);
        let index_to_variable: HashMap<_, _> =
            variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();
        let mut steps = Vec::with_capacity(index_to_constraint.len());
        for (i, &index) in ordering.iter().enumerate().rev() {
            let adjacent = &adjacency[&index];
            if let Some(&var) = index_to_variable.get(&index) {
                let is_starting = !adjacent.iter().any(|adj| ordering[..i].contains(adj));
                if is_starting {
                    let isa = &variable_isa[&var];
                    steps.push(Step::Intersection(IntersectionStep::new(
                        var,
                        vec![ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                            isa.clone(),
                            Inputs::None([]),
                            type_annotations,
                        ))],
                        &[var],
                    )));
                }
            } else {
                let bound_variables = adjacent
                    .iter()
                    .filter(|&adj| ordering[..i].contains(adj))
                    .map(|adj| index_to_variable[adj])
                    .collect::<HashSet<_>>();
                match index_to_constraint[&index] {
                    Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) | Constraint::Isa(_) => todo!(),
                    Constraint::Links(rp) => {
                        if bound_variables.len() >= 2 {
                            todo!()
                        }
                        let planner = elements[index].as_links().unwrap();
                        let selected_variables = &[rp.relation(), rp.player(), rp.role_type()];
                        let instruction = if bound_variables.contains(&rp.relation()) {
                            ConstraintInstruction::Links(LinksInstruction::new(
                                rp.clone(),
                                Inputs::Single([rp.relation()]),
                                type_annotations,
                            ))
                        } else if bound_variables.contains(&rp.player()) {
                            ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                                rp.clone(),
                                Inputs::Single([rp.player()]),
                                type_annotations,
                            ))
                        } else if planner.unbound_is_forward {
                            ConstraintInstruction::Links(LinksInstruction::new(
                                rp.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        } else {
                            ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                                rp.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        };
                        let sort_variable = if bound_variables.is_empty() && planner.unbound_is_forward
                            || bound_variables.contains(&rp.player())
                        {
                            rp.relation()
                        } else {
                            rp.player()
                        };
                        steps.push(Step::Intersection(IntersectionStep::new(
                            sort_variable,
                            vec![instruction],
                            selected_variables,
                        )));
                    }
                    Constraint::Has(has) => {
                        if bound_variables.len() == 2 {
                            todo!()
                        }
                        let planner = elements[index].as_has().unwrap();
                        let selected_variables = &[has.owner(), has.attribute()];
                        let instruction = if bound_variables.contains(&has.owner()) {
                            ConstraintInstruction::Has(HasInstruction::new(
                                has.clone(),
                                Inputs::Single([has.owner()]),
                                type_annotations,
                            ))
                        } else if bound_variables.contains(&has.attribute()) {
                            ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                                has.clone(),
                                Inputs::Single([has.attribute()]),
                                type_annotations,
                            ))
                        } else if planner.unbound_is_forward {
                            ConstraintInstruction::Has(HasInstruction::new(
                                has.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        } else {
                            ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                                has.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        };
                        let sort_variable = if bound_variables.is_empty() && planner.unbound_is_forward
                            || bound_variables.contains(&has.attribute())
                        {
                            has.owner()
                        } else {
                            has.attribute()
                        };
                        steps.push(Step::Intersection(IntersectionStep::new(
                            sort_variable,
                            vec![instruction],
                            selected_variables,
                        )));
                    }
                    Constraint::ExpressionBinding(_) => todo!(),
                    Constraint::FunctionCallBinding(_) => todo!(),
                    Constraint::Comparison(_) => todo!(),
                    Constraint::Owns(_) => todo!(),
                    Constraint::Relates(_) => todo!(),
                    Constraint::Plays(_) => todo!(),
                }
            }
        }
        steps.reverse();
        Self { steps, context: variable_registry.clone() }
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub fn outputs(&self) -> &[Variable] {
        self.steps.last().unwrap().selected_variables()
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
    }

    pub fn variable_registry(&self) -> &VariableRegistry {
        &self.context
    }
}

#[allow(dead_code)]
fn initialise_plan_greedy(elements: &[PlannerVertex], adjacency: &HashMap<usize, HashSet<usize>>) -> Vec<usize> {
    let mut open_set: HashSet<usize> = (0..elements.len()).collect();
    let mut ordering = Vec::with_capacity(elements.len());
    while !open_set.is_empty() {
        let (next, _cost) = open_set
            .iter()
            .map(|&el| (el, calculate_marginal_cost(elements, adjacency, &ordering, el)))
            .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
            .unwrap();
        ordering.push(next);
        open_set.remove(&next);
    }
    ordering
}

fn calculate_marginal_cost(
    elements: &[PlannerVertex],
    adjacency: &HashMap<usize, HashSet<usize>>,
    prefix: &[usize],
    next: usize,
) -> f64 {
    assert!(!prefix.contains(&next));
    let adjacent = &adjacency[&next];
    let preceding = adjacent.iter().filter(|adj| prefix.contains(adj)).copied().collect_vec();
    let planner_vertex = &elements[next];
    let VertexCost { per_input, per_output, branching_factor } = planner_vertex.cost(&preceding, elements);
    per_input + branching_factor * per_output
}

pub enum Step {
    Intersection(IntersectionStep),
    UnsortedJoin(UnsortedJoinStep),
    Assignment(AssignmentStep),
    Disjunction(DisjunctionStep),
    Negation(NegationStep),
    Optional(OptionalStep),
}

impl Step {
    pub fn selected_variables(&self) -> &[Variable] {
        match self {
            Step::Intersection(step) => &step.selected_variables,
            Step::UnsortedJoin(step) => &step.selected_variables,
            Step::Assignment(_) => todo!(),
            Step::Disjunction(_) => todo!(),
            Step::Negation(_) => todo!(),
            Step::Optional(_) => todo!(),
        }
    }

    pub fn new_variables(&self) -> &[Variable] {
        match self {
            Step::Intersection(step) => step.new_variables(),
            Step::UnsortedJoin(step) => step.new_variables(),
            Step::Assignment(step) => step.new_variables(),
            Step::Disjunction(_) => todo!(),
            Step::Negation(_) => &[],
            Step::Optional(_) => todo!(),
        }
    }
}

pub struct IntersectionStep {
    pub sort_variable: Variable,
    pub instructions: Vec<ConstraintInstruction>,
    new_variables: Vec<Variable>,
    input_variables: Vec<Variable>,
    pub selected_variables: Vec<Variable>,
}

impl IntersectionStep {
    pub fn new(
        sort_variable: Variable,
        instructions: Vec<ConstraintInstruction>,
        selected_variables: &[Variable],
    ) -> Self {
        let mut input_variables = Vec::with_capacity(instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(instructions.len() * 2);
        instructions.iter().for_each(|instruction| {
            instruction.new_variables_foreach(|var| {
                if !new_variables.contains(&var) {
                    new_variables.push(var)
                }
            });
            instruction.input_variables_foreach(|var| {
                if !input_variables.contains(&var) {
                    input_variables.push(var)
                }
            });
        });
        Self {
            sort_variable,
            instructions,
            new_variables,
            input_variables,
            selected_variables: selected_variables.to_owned(),
        }
    }

    fn new_variables(&self) -> &[Variable] {
        &self.new_variables
    }
}

pub struct UnsortedJoinStep {
    pub iterate_instruction: ConstraintInstruction,
    pub check_instructions: Vec<ConstraintInstruction>,
    new_variables: Vec<Variable>,
    input_variables: Vec<Variable>,
    selected_variables: Vec<Variable>,
}

impl UnsortedJoinStep {
    pub fn new(
        iterate_instruction: ConstraintInstruction,
        check_instructions: Vec<ConstraintInstruction>,
        selected_variables: &[Variable],
    ) -> Self {
        let mut input_variables = Vec::with_capacity(check_instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(5);
        iterate_instruction.new_variables_foreach(|var| {
            if !new_variables.contains(&var) {
                new_variables.push(var)
            }
        });
        iterate_instruction.input_variables_foreach(|var| {
            if !input_variables.contains(&var) {
                input_variables.push(var)
            }
        });
        check_instructions.iter().for_each(|instruction| {
            instruction.input_variables_foreach(|var| {
                if !input_variables.contains(&var) {
                    input_variables.push(var)
                }
            })
        });
        Self {
            iterate_instruction,
            check_instructions,
            new_variables,
            input_variables,
            selected_variables: selected_variables.to_owned(),
        }
    }

    fn new_variables(&self) -> &[Variable] {
        &self.new_variables
    }
}

pub struct AssignmentStep {
    assign_instruction: ExpressionBinding<Variable>,
    check_instructions: Vec<ConstraintInstruction>,
    unbound: [Variable; 1],
}

impl AssignmentStep {
    fn new_variables(&self) -> &[Variable] {
        &self.unbound
    }
}

pub struct DisjunctionStep {
    pub disjunction: Vec<PatternPlan>,
}

pub struct NegationStep {
    pub negation: PatternPlan,
}

pub struct OptionalStep {
    pub optional: PatternPlan,
}

pub trait InstructionAPI {
    fn constraint(&self) -> Constraint<Variable>;
}
