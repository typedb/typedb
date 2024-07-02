/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
    }
}

/*
Plan should indicate direction and operation to use.
Plan should indicate whether returns iterator or single
Plan should indicate pre-sortedness of iterator (allows intersecting automatically)
 */

pub(crate) struct Step {
    pub(crate) execution: Execution,
    // filters: Vec<Filter>, // local filtering operations without storage lookups
    input_variables: Vec<Variable>,
    generated_variables: Vec<Variable>,
}

impl Step {
    pub(crate) fn generated_variables(&self) -> &Vec<Variable> {
        &self.generated_variables
    }

    pub(crate) fn total_variables_count(&self) -> u32 {
        (self.input_variables.len() + self.generated_variables.len()) as u32
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

pub(crate) enum IterateMode {
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
