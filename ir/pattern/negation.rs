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
        Scope, ScopeId, VariableBindingMode,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, VariableLocality},
};
use crate::pattern::Pattern;

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

}

impl Pattern for Negation {

    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunction().referenced_variables()
    }

    // Union of non-binding variables used here or below, and variables declared in parent scopes
    fn required_inputs<'a>(&'a self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| {
            let locality = block_context.variable_locality_in_scope(v, self.scope_id());
            if locality == VariableLocality::Parent || mode.is_require_prebound() {
                Some(v)
            } else {
                None
            }
        })
    }

    fn variable_binding_modes(&self) -> HashMap<Variable, VariableBindingMode<'_>> {
        self.conjunction
            .variable_binding_modes()
            .into_iter()
            .filter_map(|(var, mut mode)| {
                // let locality = block_context.variable_locality_in_scope(var, self.scope_id());
                // if locality == VariableLocality::Parent {
                //     // if it is expected to originate from the parent, even if it is binding in any form, it is treated as non-binding
                //     mode.set_non_binding();
                // }
                if mode.is_always_binding() {
                    // if it is binding, we demote it to only locally binding (only relevant in the negation)
                    mode.set_locally_binding_in_child();
                }
                // everything is either locally binding or non-binding (& therefore must be from parent)
                Some((var, mode))
            })
            .collect()
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
