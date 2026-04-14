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
use crate::pattern::ContextualisedBindingMode;
use crate::pattern::nested_pattern::NestedPattern;

#[derive(Debug, Clone)]
pub struct Negation {
    conjunction: Conjunction,
    binding_modes: ContextualisedBindingMode,
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
        self.binding_modes.visible_referenced_variables()
    }

    fn required_inputs(&self) -> impl Iterator<Item=Variable> + '_ {
        self.binding_modes.required_inputs()
    }

    #[cfg(debug_assertions)]
    fn contextualised_binding_modes(&self) -> &HashMap<Variable, BindingMode> {
        &self.binding_modes.0
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

    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> NestedPattern {
        let binding_modes = ContextualisedBindingMode::from(self.variable_binding_modes(), parent_modes);
        let conjunction = self.conjunction.finish(&binding_modes);
        NestedPattern::Negation(Negation { conjunction, binding_modes })
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
