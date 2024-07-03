/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use itertools::Itertools;

use answer::variable::Variable;
use ir::pattern::constraint::{Comparison, ExpressionBinding, FunctionCallBinding, Has, RolePlayer};

pub struct PatternPlan {
    steps: Vec<Step>,
    // TODO: each pattern plan should have its own modifiers?
}

impl PatternPlan {
    pub fn new(steps: Vec<Step>) -> Self {
        Self { steps }
    }

    pub(crate) fn steps(&self) -> &Vec<Step> {
        &self.steps
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item=Step> {
        self.steps.into_iter()
    }
}

/*
Plan should indicate direction and operation to use.
Plan should indicate whether returns iterator or single
Plan should indicate pre-sortedness of iterator (allows intersecting automatically)
 */

pub struct Step {
    pub(crate) execution: Execution,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    output_variables: Vec<Variable>,
}

impl Step {
    pub fn new(execution: Execution, bound_variables: &HashSet<Variable>) -> Self {
        let generated_variables = execution.generated_variables(bound_variables);
        Self {
            execution,
            output_variables: generated_variables,
        }
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

impl Execution {
    pub(crate) fn generated_variables(&self, bound_variables: &HashSet<Variable>) -> Vec<Variable> {
        let mut generated = Vec::new();
        match self {
            Execution::SortedIterators(iterates) => {
                iterates.iter().for_each(|iterate|
                    iterate.foreach_generated(bound_variables, |var| if !generated.contains(&var) {
                        generated.push(var)
                    })
                );
            }
            Execution::UnsortedIterator(iterate, checks) => {
                iterate.foreach_generated(bound_variables, |var| if !generated.contains(&var) {
                    generated.push(var)
                });
                // TODO: this should be a debug as a whole
                checks.iter().for_each(
                    |check| check.foreach_variable(|var| debug_assert!(!bound_variables.contains(&var)))
                );
            }
            Execution::Single(single, checks) => {
                single.foreach_generated(bound_variables, |var| if !generated.contains(&var) {
                    generated.push(var)
                });
                // TODO: this should be a debug as a whole
                checks.iter().for_each(
                    |check| check.foreach_variable(|var| debug_assert!(!bound_variables.contains(&var)))
                );
            }
            Execution::Disjunction(disjunction) => {
                todo!()
            }
            Execution::Negation(negation) => {},
            Execution::Optional(optional) => {
                todo!()
            }
        }

        generated
    }
}

pub enum Iterate {
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
    fn foreach_generated(&self, bound_variables: &HashSet<Variable>, mut apply: impl FnMut(Variable) -> ()) {
        match self {
            Iterate::Has(has, mode) => {
                match mode {
                    IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                        debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
                        has.ids().for_each(apply)
                    }
                    IterateMode::BoundFromSortedTo => {
                        debug_assert!(bound_variables.contains(&has.owner()));
                        apply(has.attribute())
                    }
                }
            }
            Iterate::HasReverse(has, mode) => {
                match mode {
                    IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                        debug_assert!(!has.ids().any(|var| bound_variables.contains(&var)));
                        has.ids().for_each(apply);
                    }
                    IterateMode::BoundFromSortedTo => {
                        debug_assert!(bound_variables.contains(&has.attribute()));
                        apply(has.owner())
                    }
                }
            }
            Iterate::RolePlayer(rp, mode) => {
                match mode {
                    IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                        debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
                        rp.ids().for_each(apply);
                    }
                    IterateMode::BoundFromSortedTo => {
                        debug_assert!(bound_variables.contains(&rp.relation()));
                        apply(rp.player());
                        rp.role_type().map(|role_type| apply(role_type));
                    }
                }
            }
            Iterate::RolePlayerReverse(rp, mode) => {
                match mode {
                    IterateMode::UnboundSortedFrom | IterateMode::UnboundSortedTo => {
                        debug_assert!(!rp.ids().any(|var| bound_variables.contains(&var)));
                        rp.ids().for_each(apply)
                    }
                    IterateMode::BoundFromSortedTo => {
                        debug_assert!(bound_variables.contains(&rp.player()));
                        apply(rp.player());
                        rp.role_type().map(|role_type| apply(role_type));
                    }
                }
            }
            Iterate::FunctionCallBinding(call) => {
                debug_assert!(!call.ids_assigned().any(|var| bound_variables.contains(&var)));
                call.ids_assigned().for_each(apply)
            }
            Iterate::Comparison(comparison) => {
                debug_assert!(bound_variables.contains(&comparison.rhs()) && !bound_variables.contains(&comparison.lhs()));
                apply(comparison.lhs());
            }
            Iterate::ComparisonReverse(comparison) => {
                debug_assert!(bound_variables.contains(&comparison.lhs()) && !bound_variables.contains(&comparison.rhs()));
                apply(comparison.rhs());
            }
        }
    }
}

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

    fn foreach_generated(&self, bound_variables: &HashSet<Variable>, mut apply: impl FnMut(Variable) ->()) {
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
    fn foreach_variable(&self, apply: impl Fn(Variable) -> ()) {
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
