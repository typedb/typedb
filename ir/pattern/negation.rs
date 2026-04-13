/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use structural_equality::StructuralEquality;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        BindingMode, Pattern, Scope, ScopeId,
    },
};
use crate::pattern::nested_pattern::NestedPattern;

#[derive(Debug, Clone)]
pub struct Negation {
    conjunction: Conjunction,
}

impl Negation {
    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }
}

impl Pattern for Negation {
    fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunction().visible_referenced_variables()
    }

    fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        todo!("Remove me")
    }
}

impl Scope for Negation {
    fn scope_id(&self) -> ScopeId {
        self.conjunction.scope_id()
    }
}

impl StructuralEquality for Negation {
    fn hash(&self) -> u64 {
        self.conjunction().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.conjunction().equals(other.conjunction())
    }
}

impl fmt::Display for Negation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not {}", self.conjunction)
    }
}

#[derive(Debug)]
pub(crate) struct NegationBuilder {
    conjunction: ConjunctionBuilder,
}

impl NegationBuilder {
    pub(crate) fn new(scope_id: ScopeId) -> Self {
        Self { conjunction: ConjunctionBuilder::new(scope_id) }
    }

    pub(crate) fn finish(self) -> NestedPattern {
        let conjunction = self.conjunction.finish();
        NestedPattern::Negation(Negation { conjunction })
    }

    pub(crate) fn conjunction(&self) -> &ConjunctionBuilder {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut ConjunctionBuilder {
        &mut self.conjunction
    }

    pub(crate) fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        self.conjunction
            .variable_binding_modes()
            .into_iter()
            .map(|(var, mode)| {
                if mode.is_always_binding() {
                    // if it is binding, we demote it to only locally binding (only relevant in the negation)
                    (var, BindingMode::LocallyBindingInChild)
                } else {
                    (var, mode)
                }
            })
            .collect()
    }
}
