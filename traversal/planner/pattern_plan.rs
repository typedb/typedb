/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    inference::type_inference::TypeAnnotations,
    pattern::{
        constraint::{Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Isa, RolePlayer},
        variable_category::VariableCategory,
        IrID,
    },
    program::block::{BlockContext, FunctionalBlock},
};
use itertools::Itertools;

// use self::vertex::{Costed, HasPlanner, PlannerVertex, ThingPlanner, VertexCost};
//
// mod vertex;

pub struct PatternPlan {
    pub(crate) steps: Vec<Step>,
    pub(crate) context: BlockContext,
    // TODO: each pattern plan should have its own modifiers?
}

// TODO:
//  1. we need to keep a multiplier on each row of a Batch, so we can optimise away opening more iterators than we have to
//  2. we need to note the Output variables, which are variables that are Named, and therefore should be enumerated

/*
1. Named variables that are not returned or reused beyond a step can simply be counted, and not outputted
2. Anonymous variables that are not reused beyond a step can just be checked for a single answer

Planner outputs an ordering over variables, with directions over which edges should be traversed.
If we know this we can:
  1. group edges intersecting into the same variable as one Step.
  2. if the ordering implies it, we may need to perform Storage/Comparison checks, if the variables are visited disconnected and then joined
  3. some checks are fully bound, while others are not... when do we decide? What is a Check versus an Iterate instruction? Do we need to differentiate?


 */

impl PatternPlan {
    pub fn new(steps: Vec<Step>, context: BlockContext) -> Self {
        Self { steps, context }
    }

    pub fn from_block(block: FunctionalBlock, type_annotations: &TypeAnnotations, statistics: &Statistics) -> Self {
        assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
        let conjunction = block.conjunction();
        assert!(conjunction.nested_patterns().is_empty(), "TODO: nested patterns in root conjunction");

        // let mut variable_index = HashMap::new();
        let mut variable_isa = HashMap::new();
        // let mut elements = Vec::new();
        // let mut adjacency: HashMap<usize, HashSet<usize>> = HashMap::new();

        // 1. Register variables
        for (variable, category) in block.context().variable_categories() {
            match category {
                VariableCategory::Type | VariableCategory::ThingType | VariableCategory::RoleType => (), // ignore for now
                VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                    // let planner = ThingPlanner::from_variable(variable, type_annotations, statistics);
                    // variable_index.insert(variable, elements.len());
                    // elements.push(PlannerVertex::Thing(planner));
                }
                VariableCategory::Value => todo!(),
                VariableCategory::ObjectList
                | VariableCategory::ThingList
                | VariableCategory::AttributeList
                | VariableCategory::ValueList => todo!(),
            }
        }

        // let mut index_to_constraint = HashMap::new();

        // 2. Register constraints
        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::Label(_) | Constraint::Sub(_) => (), // ignore for now
                Constraint::Isa(isa) => {
                    variable_isa.insert(isa.thing(), isa.clone());
                }
                Constraint::RolePlayer(_) => todo!(),
                Constraint::Has(has) => {
                    // let planner = HasPlanner::from_constraint(has, &variable_index, type_annotations, statistics);
                    //
                    // let index = elements.len();
                    //
                    // index_to_constraint.insert(index, constraint);
                    //
                    // adjacency.entry(index).or_default().extend([planner.owner, planner.attribute]);
                    //
                    // adjacency.entry(planner.owner).or_default().insert(index);
                    // adjacency.entry(planner.attribute).or_default().insert(index);
                    //
                    // elements.push(PlannerVertex::Has(planner));
                }
                Constraint::ExpressionBinding(_) => todo!(),
                Constraint::FunctionCallBinding(_) => todo!(),
                Constraint::Comparison(_) => todo!(),
            }
        }

        todo!()
        // let ordering = initialise_plan_greedy(&elements, &adjacency);
        //
        // let index_to_variable: HashMap<_, _> =
        //     variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();
        //
        // let mut steps = Vec::with_capacity(index_to_constraint.len());
        // for (i, &index) in ordering.iter().enumerate().rev() {
        //     let adjacent = &adjacency[&index];
        //     if let Some(var) = index_to_variable.get(&index) {
        //         let is_starting = !adjacent.iter().any(|adj| ordering[..i].contains(adj));
        //         if is_starting {
        //             steps.push(Step::new(
        //                 Execution::SortedIterators(vec![Iterate::Isa(
        //                     variable_isa[var].clone(),
        //                     IterateMode::UnboundSortedTo,
        //                 )]),
        //                 &HashSet::new(),
        //             ));
        //         }
        //     } else {
        //         let bound_variables = adjacent
        //             .iter()
        //             .filter(|&adj| ordering[..i].contains(adj))
        //             .map(|adj| index_to_variable[adj])
        //             .collect::<HashSet<_>>();
        //         match index_to_constraint.get(&index) {
        //             Some(Constraint::Label(_) | Constraint::Sub(_) | Constraint::Isa(_)) => todo!(),
        //             Some(Constraint::RolePlayer(_)) => todo!(),
        //             Some(Constraint::Has(has)) => {
        //                 let iter = if bound_variables.is_empty() {
        //                     Iterate::Has(has.clone(), IterateMode::UnboundSortedFrom)
        //                 } else if bound_variables.len() == 2 {
        //                     continue; // TODO
        //                 } else if bound_variables.contains(&has.owner()) {
        //                     Iterate::Has(has.clone(), IterateMode::BoundFromSortedTo)
        //                 } else {
        //                     Iterate::HasReverse(has.clone(), IterateMode::BoundFromSortedTo)
        //                 };
        //
        //                 steps.push(Step::new(Execution::SortedIterators(vec![iter]), &bound_variables));
        //             }
        //             Some(Constraint::ExpressionBinding(_)) => todo!(),
        //             Some(Constraint::FunctionCallBinding(_)) => todo!(),
        //             Some(Constraint::Comparison(_)) => todo!(),
        //             None => (),
        //         }
        //     }
        // }
        //
        // steps.reverse();
        //
        // Self { steps }
    }

    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
    }
}
#[allow(dead_code)]
// fn initialise_plan_greedy(elements: &[PlannerVertex], adjacency: &HashMap<usize, HashSet<usize>>) -> Vec<usize> {
//     let mut open_set: HashSet<usize> = (0..elements.len()).collect();
//     let mut ordering = Vec::with_capacity(elements.len());
//     while !open_set.is_empty() {
//         let (next, _cost) = open_set
//             .iter()
//             .map(|&el| (el, calculate_marginal_cost(elements, adjacency, &ordering, el)))
//             .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
//             .unwrap();
//         ordering.push(next);
//         open_set.remove(&next);
//     }
//     ordering
// }
//
// fn calculate_marginal_cost(
//     elements: &[PlannerVertex],
//     adjacency: &HashMap<usize, HashSet<usize>>,
//     prefix: &[usize],
//     next: usize,
// ) -> f64 {
//     assert!(!prefix.contains(&next));
//     let adjacent = &adjacency[&next];
//     let preceding = adjacent.iter().filter(|adj| prefix.contains(adj)).copied().collect_vec();
//     let planner_vertex = &elements[next];
//     let VertexCost { per_input, per_output, branching_factor } = planner_vertex.cost(&preceding, elements);
//     per_input + branching_factor * per_output
// }

pub enum Step {
    SortedJoin(SortedJoinStep),
    UnsortedJoin(UnsortedJoinStep),
    Assignment(AssignmentStep),
    Disjunction(DisjunctionStep),
    Negation(NegationStep),
    Optional(OptionalStep),
}

impl Step {
    pub(crate) fn unbound_variables(&self) -> &[Variable] {
        match self {
            Step::SortedJoin(step) => step.unbound_variables(),
            Step::UnsortedJoin(step) => step.unbound_variables(),
            Step::Assignment(step) => step.unbound_variables(),
            Step::Disjunction(_) => todo!(),
            Step::Negation(_) => &[],
            Step::Optional(_) => todo!(),
        }
    }
}

pub struct SortedJoinStep {
    pub(crate) sort_variable: Variable,
    pub(crate) instructions: Vec<Instruction>,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    unbound_variables: Vec<Variable>,
    bound_variables: Vec<Variable>,

    // selected variables may include variables passed through from the inputs
    pub(crate) selected_variables: Vec<Variable>,
}

impl SortedJoinStep {
    pub fn new(sort_variable: Variable, instructions: Vec<Instruction>, selected_variables: &Vec<Variable>) -> Self {
        let mut bound = Vec::with_capacity(instructions.len() * 2);
        let mut unbound = Vec::with_capacity(instructions.len() * 2);
        instructions.iter().for_each(|instruction| {
            instruction.unbound_vars_foreach(|var| {
                if !unbound.contains(&var) {
                    unbound.push(var)
                }
            });
            instruction.bound_vars_foreach(|var| {
                if !bound.contains(&var) {
                    bound.push(var)
                }
            });
        });
        Self {
            sort_variable,
            instructions,
            unbound_variables: unbound,
            bound_variables: bound,
            selected_variables: selected_variables.clone(),
        }
    }

    fn unbound_variables(&self) -> &[Variable] {
        &self.unbound_variables
    }
}

pub struct UnsortedJoinStep {
    pub(crate) iterate_instruction: Instruction,
    pub(crate) check_instructions: Vec<Instruction>,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    unbound_variables: Vec<Variable>,
    bound_variables: Vec<Variable>,
    selected_variables: Vec<Variable>,
}

impl UnsortedJoinStep {
    pub fn new(
        iterate_instruction: Instruction,
        check_instructions: Vec<Instruction>,
        selected_variables: &Vec<Variable>,
    ) -> Self {
        let mut bound = Vec::with_capacity(check_instructions.len() * 2);
        let mut unbound = Vec::with_capacity(5);
        iterate_instruction.unbound_vars_foreach(|var| {
            if !unbound.contains(&var) {
                unbound.push(var)
            }
        });
        iterate_instruction.bound_vars_foreach(|var| {
            if !bound.contains(&var) {
                bound.push(var)
            }
        });
        check_instructions.iter().for_each(|instruction| {
            instruction.bound_vars_foreach(|var| {
                if !bound.contains(&var) {
                    bound.push(var)
                }
            })
        });
        Self {
            iterate_instruction,
            check_instructions,
            unbound_variables: unbound,
            bound_variables: bound,
            selected_variables: selected_variables.clone(),
        }
    }

    fn unbound_variables(&self) -> &[Variable] {
        &self.unbound_variables
    }
}

pub struct AssignmentStep {
    assign_instruction: ExpressionBinding<Variable>,
    check_instructions: Vec<Instruction>,
    unbound: [Variable; 1],
}

impl AssignmentStep {
    fn unbound_variables(&self) -> &[Variable] {
        &self.unbound
    }
}

pub struct DisjunctionStep {
    pub(crate) disjunction: Vec<PatternPlan>,
}

pub struct NegationStep {
    pub(crate) negation: PatternPlan,
}

pub struct OptionalStep {
    pub(crate) optional: PatternPlan,
}

trait InstructionAPI {
    fn constraint(&self) -> Constraint<Variable>;
}

#[derive(Debug)]
pub enum Instruction {
    // type -> thing
    Isa(Isa<Variable>, IterateBounds<Variable>),
    // owner -> attribute
    Has(Has<Variable>, IterateBounds<Variable>),
    // attribute -> owner
    HasReverse(Has<Variable>, IterateBounds<Variable>),

    // relation -> player
    RolePlayer(RolePlayer<Variable>, IterateBounds<Variable>),
    // player -> relation
    RolePlayerReverse(RolePlayer<Variable>, IterateBounds<Variable>),

    // $x --> $y
    // RolePlayerIndex(IR, IterateBounds)
    FunctionCallBinding(FunctionCallBinding<Variable>),

    // lhs derived from rhs. We need to decide if lhs will always be sorted
    ComparisonGenerator(Comparison<Variable>),
    // rhs derived from lhs
    ComparisonGeneratorReverse(Comparison<Variable>),
    // lhs and rhs are known
    ComparisonCheck(Comparison<Variable>),

    // vars = <expr>
    ExpressionBinding(ExpressionBinding<Variable>),
}

impl Instruction {
    fn bound_vars_foreach(&self, mut apply: impl FnMut(Variable) -> ()) {
        match self {
            Instruction::Isa(_, bounds) => bounds.bounds().iter().cloned().for_each(apply),
            Instruction::Has(_, bounds) | Instruction::HasReverse(_, bounds) => {
                bounds.bounds().iter().cloned().for_each(apply)
            }
            Instruction::RolePlayer(_, bounds) | Instruction::RolePlayerReverse(_, bounds) => {
                bounds.bounds().iter().cloned().for_each(apply)
            }
            Instruction::ComparisonCheck(_) => {}
            Instruction::FunctionCallBinding(call) => call.function_call().argument_ids().for_each(apply),
            Instruction::ComparisonGenerator(comparison) => apply(comparison.rhs()),
            Instruction::ComparisonGeneratorReverse(comparison) => apply(comparison.lhs()),
            Instruction::ExpressionBinding(binding) => binding.expression().ids().for_each(apply),
        }
    }

    fn unbound_vars_foreach(&self, mut apply: impl FnMut(Variable)) {
        match self {
            Instruction::Isa(isa, bounds) => isa.ids_foreach(|var, _| {
                if !bounds.bounds().iter().contains(&var) {
                    apply(var)
                }
            }),
            Instruction::Has(has, bounds) | Instruction::HasReverse(has, bounds) => has.ids_foreach(|var, _| {
                if !bounds.bounds().iter().contains(&var) {
                    apply(var)
                }
            }),
            Instruction::RolePlayer(rp, bounds) | Instruction::RolePlayerReverse(rp, bounds) => {
                rp.ids_foreach(|var, _| {
                    if !bounds.bounds().iter().contains(&var) {
                        apply(var)
                    }
                })
            }
            Instruction::FunctionCallBinding(call) => call.ids_assigned().for_each(apply),
            Instruction::ComparisonGenerator(comparison) => apply(comparison.lhs()),
            Instruction::ComparisonGeneratorReverse(comparison) => apply(comparison.rhs()),
            Instruction::ComparisonCheck(comparison) => {
                apply(comparison.lhs());
                apply(comparison.rhs())
            }
            Instruction::ExpressionBinding(binding) => binding.ids_assigned().for_each(apply),
        }
    }
    //
    // match self {
    //     Instruction::Has(has, mode) => match mode {
    //         IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
    //             debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
    //             has.ids().for_each(apply)
    //         }
    //         IterateMode::BoundFromSortedTo => {
    //             debug_assert!(bound_variables.contains(&has.owner()));
    //             apply(has.attribute())
    //         }
    //     },
    //     Instruction::HasReverse(has, mode) => match mode {
    //         IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
    //             debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
    //             has.ids().for_each(apply);
    //         }
    //         IterateMode::BoundFromSortedTo => {
    //             debug_assert!(bound_variables.contains(&has.attribute()));
    //             apply(has.owner())
    //         }
    //     },
    //     Instruction::RolePlayer(rp, mode) => match mode {
    //         IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
    //             debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
    //             rp.ids().for_each(apply);
    //         }
    //         IterateMode::BoundFromSortedTo => {
    //             debug_assert!(bound_variables.contains(&rp.relation()));
    //             apply(rp.player());
    //             // TODO: the filter could be not generated?
    //             rp.role_type().map(|role_type| apply(role_type));
    //         }
    //     },
    //     Instruction::RolePlayerReverse(rp, mode) => match mode {
    //         IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
    //             debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
    //             rp.ids().for_each(apply)
    //         }
    //         IterateMode::BoundFromSortedTo => {
    //             debug_assert!(bound_variables.contains(&rp.player()));
    //             apply(rp.player());
    //             rp.role_type().map(|role_type| apply(role_type));
    //         }
    //     },
    //     Instruction::FunctionCallBinding(call) => {
    //         debug_assert!(!call.ids_assigned().any(|var| bound_variables.contains(&var)));
    //         call.ids_assigned().for_each(apply)
    //     }
    //     Instruction::Comparison(comparison) => {
    //         debug_assert!(
    //             bound_variables.contains(&comparison.rhs()) && !bound_variables.contains(&comparison.lhs())
    //         );
    //         apply(comparison.lhs());
    //     }
    //     Instruction::ComparisonReverse(comparison) => {
    //         debug_assert!(
    //             bound_variables.contains(&comparison.lhs()) && !bound_variables.contains(&comparison.rhs())
    //         );
    //         apply(comparison.rhs());
    //     }
    // }
    // }
}

impl InstructionAPI for Instruction {
    fn constraint(&self) -> Constraint<Variable> {
        match self {
            Instruction::Isa(isa, _) => isa.clone().into(),
            Instruction::Has(has, _) | Instruction::HasReverse(has, _) => has.clone().into(),
            Instruction::RolePlayer(rp, _) | Instruction::RolePlayerReverse(rp, _) => rp.clone().into(),
            Instruction::FunctionCallBinding(call) => call.clone().into(),
            Instruction::ComparisonGenerator(cmp)
            | Instruction::ComparisonGeneratorReverse(cmp)
            | Instruction::ComparisonCheck(cmp) => cmp.clone().into(),
            Instruction::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum IterateBounds<ID: IrID> {
    None([ID; 0]),
    Single([ID; 1]),
    Dual([ID; 2]),
}

impl<ID: IrID> IterateBounds<ID> {
    pub(crate) fn contains(&self, id: ID) -> bool {
        self.bounds().contains(&id)
    }

    fn bounds(&self) -> &[ID] {
        match self {
            IterateBounds::None(ids) => ids,
            IterateBounds::Single(ids) => ids,
            IterateBounds::Dual(ids) => ids,
        }
    }

    pub fn into_ids<T: IrID>(self, mapping: &HashMap<ID, T>) -> IterateBounds<T> {
        match self {
            IterateBounds::None(_) => IterateBounds::None([]),
            IterateBounds::Single([var]) => IterateBounds::Single([*mapping.get(&var).unwrap()]),
            IterateBounds::Dual([var_1, var_2]) => {
                IterateBounds::Dual([*mapping.get(&var_1).unwrap(), *mapping.get(&var_2).unwrap()])
            }
        }
    }
}

// enum Filter {
//
// }
