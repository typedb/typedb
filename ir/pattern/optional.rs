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
        BranchID, Pattern, Scope, ScopeId, VariableBindingMode,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, VariableLocality},
};

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
}

impl Optional {
    pub fn new(scope_id: ScopeId, branch_id: Option<BranchID>) -> Self {
        Self { conjunction: Conjunction::new(scope_id, branch_id) }
    }

    pub(super) fn new_builder<'cx, 'reg>(
        context: &'cx mut BlockBuilderContext<'reg>,
        optional: &'cx mut Optional,
        needs_branch_id: bool,
    ) -> ConjunctionBuilder<'cx, 'reg> {
        ConjunctionBuilder::new(context, &mut optional.conjunction, needs_branch_id)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub fn named_visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.visible_binding_variables(block_context).filter(Variable::is_named)
    }

    fn visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes()
            .into_iter()
            .filter_map(|(v, mode)| (mode.is_always_binding() || mode.is_optionally_binding()).then_some(v))
    }
}

impl Pattern for Optional {
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
            .map(|(v, mut mode)| {
                if mode.is_always_binding() {
                    mode.set_optionally_binding()
                }
                (v, mode)
            })
            .collect()
    }
}

impl Scope for Optional {
    fn scope_id(&self) -> ScopeId {
        self.conjunction.scope_id()
    }
}

impl StructuralEquality for Optional {
    fn hash(&self) -> u64 {
        self.conjunction.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.conjunction.equals(&other.conjunction)
    }
}

impl fmt::Display for Optional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}
