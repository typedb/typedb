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
    },
    program::block::FunctionalBlock,
};
use itertools::Itertools;

use self::vertex::{Costed, HasPlanner, PlannerVertex, ThingPlanner, VertexCost};

mod vertex;

pub struct PatternPlan {
    steps: Vec<Step>,
    // TODO: each pattern plan should have its own modifiers?
}

impl PatternPlan {
    pub fn new(steps: Vec<Step>) -> Self {
        Self { steps }
    }

    pub fn from_block(block: FunctionalBlock, type_annotations: &TypeAnnotations, statistics: &Statistics) -> Self {
        assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
        let conjunction = block.conjunction();
        assert!(conjunction.nested_patterns().is_empty(), "TODO: nested patterns in root conjunction");

        let mut variable_index = HashMap::new();
        let mut variable_isa = HashMap::new();
        let mut elements = Vec::new();
        let mut adjacency: HashMap<usize, HashSet<usize>> = HashMap::new();

        // 1. Register variables
        for (variable, category) in block.context().variable_categories() {
            match category {
                VariableCategory::Type | VariableCategory::ThingType | VariableCategory::RoleType => (), // ignore for now
                VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                    let planner = ThingPlanner::from_variable(variable, type_annotations, statistics);
                    variable_index.insert(variable, elements.len());
                    elements.push(PlannerVertex::Thing(planner));
                }
                VariableCategory::Value => todo!(),
                VariableCategory::ObjectList | VariableCategory::AttributeList | VariableCategory::ValueList => todo!(),
            }
        }

        let mut index_to_constraint = HashMap::new();

        // 2. Register constraints
        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::Label(_) | Constraint::Sub(_) => (), // ignore for now
                Constraint::Isa(isa) => {
                    variable_isa.insert(isa.thing(), isa.clone());
                }
                Constraint::RolePlayer(_) => todo!(),
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
            }
        }

        let ordering = initialise_plan_greedy(&elements, &adjacency);

        let index_to_variable: HashMap<_, _> =
            variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();

        let mut steps = Vec::with_capacity(index_to_constraint.len());
        for (i, &index) in ordering.iter().enumerate().rev() {
            let adjacent = &adjacency[&index];
            if let Some(var) = index_to_variable.get(&index) {
                let is_starting = !adjacent.iter().any(|adj| ordering[..i].contains(adj));
                if is_starting {
                    steps.push(Step::new(
                        Execution::SortedIterators(vec![Iterate::Isa(
                            variable_isa[var].clone(),
                            IterateMode::UnboundSortedTo,
                        )]),
                        &HashSet::new(),
                    ));
                }
            } else {
                let bound_variables = adjacent
                    .iter()
                    .filter(|&adj| ordering[..i].contains(adj))
                    .map(|adj| index_to_variable[adj])
                    .collect::<HashSet<_>>();
                match index_to_constraint.get(&index) {
                    Some(Constraint::Label(_) | Constraint::Sub(_) | Constraint::Isa(_)) => todo!(),
                    Some(Constraint::RolePlayer(_)) => todo!(),
                    Some(Constraint::Has(has)) => {
                        let iter = if bound_variables.is_empty() {
                            Iterate::Has(has.clone(), IterateMode::UnboundSortedFrom)
                        } else if bound_variables.len() == 2 {
                            continue; // TODO
                        } else if bound_variables.contains(&has.owner()) {
                            Iterate::Has(has.clone(), IterateMode::BoundFromSortedTo)
                        } else {
                            Iterate::HasReverse(has.clone(), IterateMode::BoundFromSortedTo)
                        };

                        steps.push(Step::new(Execution::SortedIterators(vec![iter]), &bound_variables));
                    }
                    Some(Constraint::ExpressionBinding(_)) => todo!(),
                    Some(Constraint::FunctionCallBinding(_)) => todo!(),
                    Some(Constraint::Comparison(_)) => todo!(),
                    None => (),
                }
            }
        }

        steps.reverse();

        Self { steps }
    }

    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
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

/*
Plan should indicate direction and operation to use.
Plan should indicate whether returns iterator or single
Plan should indicate pre-sortedness of iterator (allows intersecting automatically)
 */

#[derive(Debug)]
pub struct Step {
    pub(crate) execution: Execution,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    output_variables: Vec<Variable>,
}

impl Step {
    pub fn new(execution: Execution, bound_variables: &HashSet<Variable>) -> Self {
        let generated_variables = execution.generated_variables(bound_variables);
        Self { execution, output_variables: generated_variables }
    }

    pub(crate) fn generated_variables(&self) -> &Vec<Variable> {
        &self.output_variables
    }
}

pub enum Execution {
    SortedIterators(Vec<Iterate>),
    UnsortedIterator(Iterate, Vec<Check>),
    Single(Single, Vec<Check>),

    Disjunction(Vec<PatternPlan>),
    Negation(PatternPlan),
    Optional(PatternPlan),
}

impl fmt::Debug for Execution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SortedIterators(iters) => write!(f, "Execution::SortedIterators({iters:?})"),
            Self::UnsortedIterator { .. } => write!(f, "Execution::UnsortedIterator"),
            Self::Single { .. } => write!(f, "Execution::Single"),
            Self::Disjunction { .. } => write!(f, "Execution::Disjunction"),
            Self::Negation { .. } => write!(f, "Execution::Negation"),
            Self::Optional { .. } => write!(f, "Execution::Optional"),
        }
    }
}

impl Execution {
    pub(crate) fn generated_variables(&self, bound_variables: &HashSet<Variable>) -> Vec<Variable> {
        let mut generated = Vec::new();
        match self {
            Execution::SortedIterators(iterates) => {
                iterates.iter().for_each(|iterate| {
                    iterate.foreach_generated(bound_variables, |var| {
                        if !generated.contains(&var) {
                            generated.push(var)
                        }
                    })
                });
            }
            Execution::UnsortedIterator(iterate, checks) => {
                iterate.foreach_generated(bound_variables, |var| {
                    if !generated.contains(&var) {
                        generated.push(var)
                    }
                });
                #[cfg(debug_assertions)]
                checks
                    .iter()
                    .for_each(|check| check.foreach_variable(|var| debug_assert!(!bound_variables.contains(&var))));
            }
            Execution::Single(single, checks) => {
                single.foreach_generated(bound_variables, |var| {
                    if !generated.contains(&var) {
                        generated.push(var)
                    }
                });
                #[cfg(debug_assertions)]
                checks
                    .iter()
                    .for_each(|check| check.foreach_variable(|var| debug_assert!(!bound_variables.contains(&var))));
            }
            Execution::Disjunction(_disjunction) => todo!(),
            Execution::Negation(_negation) => todo!(),
            Execution::Optional(_optional) => todo!(),
        }

        generated
    }
}

#[derive(Debug)]
pub enum Iterate {
    // type -> thing
    Isa(Isa<Variable>, IterateMode),

    // owner -> attribute
    Has(Has<Variable>, IterateMode),
    // attribute -> owner
    HasReverse(Has<Variable>, IterateMode),

    // relation -> player
    RolePlayer(RolePlayer<Variable>, IterateMode),
    // player -> relation
    RolePlayerReverse(RolePlayer<Variable>, IterateMode),

    // RelationIndex(RelationIndex)
    // RelationIndexReverse(RelationIndex)
    FunctionCallBinding(FunctionCallBinding<Variable>),

    // lhs derived from rhs. We need to decide if lhs will always be sorted
    Comparison(Comparison<Variable>),
    // rhs derived from lhs
    ComparisonReverse(Comparison<Variable>),
}

impl Iterate {
    pub(crate) fn sort_variable(&self) -> Option<Variable> {
        match self {
            Iterate::Isa(isa, mode) => Some(if mode.is_sorted_from() { isa.type_() } else { isa.thing() }),
            Iterate::Has(has, mode) => Some(if mode.is_sorted_from() { has.owner() } else { has.attribute() }),
            Iterate::HasReverse(has, mode) => Some(if mode.is_sorted_from() { has.attribute() } else { has.owner() }),
            Iterate::RolePlayer(rp, mode) => Some(if mode.is_sorted_from() { rp.relation() } else { rp.player() }),
            Iterate::RolePlayerReverse(rp, mode) => {
                Some(if mode.is_sorted_from() { rp.player() } else { rp.relation() })
            }
            Iterate::FunctionCallBinding(_) => None,
            Iterate::Comparison(comparison) => Some(comparison.lhs()),
            Iterate::ComparisonReverse(comparison) => Some(comparison.rhs()),
        }
    }

    fn foreach_generated(&self, bound_variables: &HashSet<Variable>, mut apply: impl FnMut(Variable)) {
        match self {
            Iterate::Isa(isa, mode) => match mode {
                IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                    debug_assert!(!isa.ids().any(|var| bound_variables.contains(&var)));
                    isa.ids().for_each(apply)
                }
                IterateMode::BoundFromSortedTo => {
                    debug_assert!(bound_variables.contains(&isa.type_()));
                    apply(isa.thing())
                }
            },
            Iterate::Has(has, mode) => match mode {
                IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                    debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
                    has.ids().for_each(apply)
                }
                IterateMode::BoundFromSortedTo => {
                    debug_assert!(bound_variables.contains(&has.owner()));
                    apply(has.attribute())
                }
            },
            Iterate::HasReverse(has, mode) => match mode {
                IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                    debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
                    has.ids().for_each(apply);
                }
                IterateMode::BoundFromSortedTo => {
                    debug_assert!(bound_variables.contains(&has.attribute()));
                    apply(has.owner())
                }
            },
            Iterate::RolePlayer(rp, mode) => match mode {
                IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                    debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
                    rp.ids().for_each(apply);
                }
                IterateMode::BoundFromSortedTo => {
                    debug_assert!(bound_variables.contains(&rp.relation()));
                    apply(rp.player());
                    rp.role_type().map(apply);
                }
            },
            Iterate::RolePlayerReverse(rp, mode) => match mode {
                IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                    debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
                    rp.ids().for_each(apply)
                }
                IterateMode::BoundFromSortedTo => {
                    debug_assert!(bound_variables.contains(&rp.player()));
                    apply(rp.player());
                    rp.role_type().map(apply);
                }
            },
            Iterate::FunctionCallBinding(call) => {
                debug_assert!(!call.ids_assigned().any(|var| bound_variables.contains(&var)));
                call.ids_assigned().for_each(apply)
            }
            Iterate::Comparison(comparison) => {
                debug_assert!(
                    bound_variables.contains(&comparison.rhs()) && !bound_variables.contains(&comparison.lhs())
                );
                apply(comparison.lhs());
            }
            Iterate::ComparisonReverse(comparison) => {
                debug_assert!(
                    bound_variables.contains(&comparison.lhs()) && !bound_variables.contains(&comparison.rhs())
                );
                apply(comparison.rhs());
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IterateMode {
    UnboundSortedFrom,
    UnboundSortedTo,
    // normally expensive, read all Froms + merge sort -> TOs
    BoundFromSortedTo,
}

impl IterateMode {
    pub const fn is_sorted_from(&self) -> bool {
        match self {
            IterateMode::UnboundSortedFrom => true,
            IterateMode::UnboundSortedTo | IterateMode::BoundFromSortedTo => false,
        }
    }

    pub const fn is_unbounded(&self) -> bool {
        match self {
            IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => true,
            IterateMode::BoundFromSortedTo => false,
        }
    }
}

pub enum Single {
    ExpressionBinding(ExpressionBinding<Variable>),
}

impl Single {
    fn foreach_generated(&self, bound_variables: &HashSet<Variable>, apply: impl FnMut(Variable)) {
        match self {
            Single::ExpressionBinding(binding) => {
                debug_assert!(!binding.ids_assigned().any(|var| bound_variables.contains(&var)));
                binding.ids_assigned().for_each(apply)
            }
        }
    }
}

pub enum Check {
    Has(Has<Variable>),
    RolePlayer(RolePlayer<Variable>),
    Comparison(Comparison<Variable>),
}

impl Check {
    fn foreach_variable(&self, apply: impl Fn(Variable)) {
        match self {
            Check::Has(has) => has.ids().for_each(apply),
            Check::RolePlayer(rp) => rp.ids().for_each(apply),
            Check::Comparison(comparison) => comparison.ids().for_each(apply),
        }
    }
}

// enum Filter {
//
// }
