/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use ir::pattern::constraint::{Comparison, ExpressionBinding, FunctionCallBinding, Has, RolePlayer};

pub(crate) struct PatternPlan {
    steps: Vec<Step>,
    // TODO: each pattern plan should have its own modifiers?
}

impl PatternPlan {
    pub(crate) fn steps(&self) -> &Vec<Step> {
        &self.steps
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
    }
}

/*
Plan should indicate direction and operation to use.
Plan should indicate whether returns iterator or single
Plan should indicate pre-sortedness of iterator (allows intersecting automatically)

Each variable will still be created using an Iterator that is composed.
 */

pub(crate) struct Step {
    pub(crate) execution: Execution,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    input_variables: Vec<Variable>,
    generated_variables: Vec<Variable>,
    // including optional ones
    pub(crate) total_variables_count: u32, // including optional ones
}

impl Step {
    pub(crate) fn generated_variables(&self) -> &Vec<Variable> {
        &self.generated_variables
    }

    pub(crate) fn total_variables_count(&self) -> u32 {
        self.total_variables_count
    }
}

pub(crate) enum Execution {
    SortedIterators(Vec<Iterate>),
    UnsortedIterator(Iterate, Vec<Check>),
    Single(Single, Vec<Check>),

    Disjunction(Vec<PatternPlan>),
    Negation(PatternPlan),
    Optional(PatternPlan),
}

pub(crate) enum Iterate {
    Has(Has<Variable>, SortedIterateMode),
    // owner -> attribute
    HasReverse(Has<Variable>, SortedIterateMode), // attribute -> owner

    RolePlayer(RolePlayer<Variable>, SortedIterateMode),
    // relation -> player
    RolePlayerReverse(RolePlayer<Variable>, SortedIterateMode), // player -> relation

    // RelationIndex(RelationIndex, SortedIterateMode)
    // RelationIndexReverse(RelationIndex, SortedIterateMode)
    FunctionCallBinding(FunctionCallBinding<Variable>),

    Comparison(Comparison<Variable>), // lhs derived from rhs. We need to decide if lhs will always be sorted
    ComparisonReverse(Comparison<Variable>), // rhs derived from lhs
}

impl Iterate {
    pub(crate) fn sort_variable(&self) -> Option<Variable> {
        match self {
            Iterate::Has(has, mode) => match mode.is_sorted_from() {
                true => Some(has.owner()),
                false => Some(has.attribute()),
            },
            Iterate::HasReverse(has, mode) => match mode.is_sorted_from() {
                true => Some(has.attribute()),
                false => Some(has.owner()),
            },
            Iterate::RolePlayer(rp, mode) => match mode.is_sorted_from() {
                true => Some(rp.relation()),
                false => Some(rp.player()),
            },
            Iterate::RolePlayerReverse(rp, mode) => match mode.is_sorted_from() {
                true => Some(rp.player()),
                false => Some(rp.relation()),
            },
            Iterate::FunctionCallBinding(_) => None,
            Iterate::Comparison(comparison) => Some(comparison.lhs()),
            Iterate::ComparisonReverse(comparison) => Some(comparison.rhs()),
        }
    }
}

pub(crate) enum SortedIterateMode {
    UnboundSortedFrom,
    UnboundSortedTo,
    // normally expensive, read all Froms + merge sort -> TOs
    BoundFromSortedTo,
}

impl SortedIterateMode {
    pub const fn is_sorted_from(&self) -> bool {
        match self {
            SortedIterateMode::UnboundSortedFrom => true,
            SortedIterateMode::UnboundSortedTo | SortedIterateMode::BoundFromSortedTo => false,
        }
    }

    pub const fn is_unbounded(&self) -> bool {
        match self {
            SortedIterateMode::UnboundSortedFrom | SortedIterateMode::UnboundSortedTo => true,
            SortedIterateMode::BoundFromSortedTo => false,
        }
    }
}

pub(crate) enum Single {
    ExpressionBinding(ExpressionBinding<Variable>),
}

pub(crate) enum Check {
    Has(Has<Variable>),
    RolePlayer(RolePlayer<Variable>),
    Comparison(Comparison<Variable>),
}

// enum Filter {
//
// }
