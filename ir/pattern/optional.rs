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
    pipeline::block::{BlockBuilderContext, BlockContext},
};
use crate::pattern::BranchID;
use crate::pipeline::block::VariableLocality;

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
    branch_id: BranchID,
}

impl Optional {
    pub fn new(scope_id: ScopeId, branch_id: BranchID) -> Self {
        Self { conjunction: Conjunction::new(scope_id), branch_id }
    }

    pub(super) fn new_builder<'cx, 'reg>(
        context: &'cx mut BlockBuilderContext<'reg>,
        optional: &'cx mut Optional,
    ) -> ConjunctionBuilder<'cx, 'reg> {
        ConjunctionBuilder::new(context, &mut optional.conjunction)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn branch_id(&self) -> BranchID {
        self.branch_id
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub(crate) fn variable_binding_modes(
        &self,
        block_context: &BlockContext,
    ) -> HashMap<Variable, VariableBindingMode<'_>> {
        self.conjunction
            .variable_binding_modes(block_context)
            .into_iter()
            .map(|(var, mut mode)| {
                let status = block_context.variable_locality_in_scope(var, self.scope_id());
                if status == VariableLocality::Parent {
                    mode.set_non_binding();
                    // TODO: including this, means that we don't consider Try variables as produce
                    //   Which means that we don't include them as 'selected', and the planner can't register them in the graph
                    // } else if mode.is_producing() {
                    //     // VariableDependency::Producing means "producing in all code paths".
                    //     // A try {} block never produces.
                    //     mode.set_referencing()
                    // }
                }
                (var, mode)
            })
            .collect()
    }

    pub fn named_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.binding_variables(block_context).filter(Variable::is_named)
    }

    fn binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes(block_context).into_iter().filter_map(|(v, dep)| dep.is_binding().then_some(v))
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunction().referenced_variables()
    }

    pub fn required_inputs(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes(block_context).into_iter().filter_map(|(v, dep)| dep.is_non_binding().then_some(v))
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
