/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::pattern::{
    BindingMode, Pattern, PatternVariables, Scope, ScopeId,
    conjunction::{Conjunction, ConjunctionBuilder},
    impl_pattern_from_pattern_variables,
    nested_pattern::NestedPattern,
};

#[derive(Debug, Clone)]
pub struct Negation {
    conjunction: Conjunction,
    pattern_variables: PatternVariables,
    source_span: Option<Span>,
}

impl Negation {
    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub(crate) fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl_pattern_from_pattern_variables!(Negation);

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
    source_span: Option<Span>,
}

impl NegationBuilder {
    pub(crate) fn new(scope_id: ScopeId, source_span: Option<Span>) -> Self {
        Self { conjunction: ConjunctionBuilder::new(scope_id), source_span }
    }

    pub(crate) fn finish(self, parent_modes: &PatternVariables) -> NestedPattern {
        let source_span = self.source_span;
        let pattern_variables = PatternVariables::build(self.variable_binding_modes(), parent_modes, []);
        let conjunction = self.conjunction.finish(&pattern_variables);
        NestedPattern::Negation(Negation { conjunction, pattern_variables, source_span })
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
                if mode.is_always_binding() || mode.is_optionally_binding() {
                    // if it is binding, we demote it to only locally binding (only relevant in the negation)
                    (var, BindingMode::LocallyBindingInChild)
                } else {
                    (var, mode)
                }
            })
            .collect()
    }
}
