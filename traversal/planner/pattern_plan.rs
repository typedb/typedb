/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::{Comparison, ExpressionBinding, FunctionCallBinding, Has, RolePlayer};
use ir::pattern::variable::Variable;

pub(crate) struct PatternPlan {
    steps: Vec<Step>,
    // TODO: each pattern plan should have its own modifiers?
}

impl PatternPlan {

    pub(crate) fn into_steps(self) -> impl Iterator<Item=Step> {
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
    generated_variables: Vec<Variable>, // including optional ones
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
    SortedIterators(Vec<Iterate>, Variable),
    UnsortedIterator(Iterate, Vec<Check>),
    Single(Single, Vec<Check>),

    Disjunction(Vec<PatternPlan>),
    Negation(PatternPlan),
    Optional(PatternPlan),
}

pub(crate) enum Iterate {
    Has(Has),
    HasFrom(Has),
    HasReverse(Has),
    HasReverseFrom(Has),

    RolePlayer(RolePlayer),
    RolePlayerFrom(RolePlayer),
    RolePlayerReverse(RolePlayer),
    RolePlayerReverseFrom(RolePlayer),

    FunctionCallBinding(FunctionCallBinding),

    Comparison(Comparison),
    ComparisonFrom(Comparison),
    ComparisonReverse(Comparison),
    ComparisonReverseFrom(Comparison),
}

pub(crate) enum Single {
    ExpressionBinding(ExpressionBinding)
}

pub(crate) enum Check {
    Has(Has),
    RolePlayer(RolePlayer),
    Comparison(Comparison),
}

// enum Filter {
//
// }