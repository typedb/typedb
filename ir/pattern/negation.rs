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
        Scope, ScopeId, VariableDependency,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, VariableLocality},
};

#[derive(Debug, Clone)]
pub struct Negation {
    conjunction: Conjunction,
}

impl Negation {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { conjunction: Conjunction::new(scope_id) }
    }

    pub(super) fn new_builder<'cx, 'reg>(
        context: &'cx mut BlockBuilderContext<'reg>,
        negation: &'cx mut Negation,
    ) -> ConjunctionBuilder<'cx, 'reg> {
        ConjunctionBuilder::new(context, &mut negation.conjunction)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunction().referenced_variables()
    }

    pub fn variable_dependency(&self, block_context: &BlockContext) -> HashMap<Variable, VariableDependency<'_>> {
        self.conjunction
            .variable_dependency(block_context)
            .into_iter()
            .filter_map(|(var, mut mode)| {
                let status = block_context.variable_status_in_scope(var, self.scope_id());
                if status == VariableLocality::Parent || mode.is_required() {
                    mode.set_required();
                    Some((var, mode))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn required_inputs(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_dependency(block_context).into_iter().filter_map(|(v, dep)| dep.is_required().then_some(v))
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
