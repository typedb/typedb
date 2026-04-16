/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use structural_equality::StructuralEquality;

use crate::pattern::{
    conjunction::{Conjunction, ConjunctionBuilder},
    nested_pattern::NestedPattern,
    BindingMode, BranchID, ContextualisedBindingMode, Pattern, Scope, ScopeId, VariableRequirements,
};

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
    branch_id: BranchID,
    variable_requirements: VariableRequirements,
}

impl Optional {
    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn branch_id(&self) -> BranchID {
        self.branch_id
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }
}

impl Pattern for Optional {
    fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_requirements.visible_referenced_variables()
    }

    fn required_inputs(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_requirements.required_inputs()
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

#[derive(Debug)]
pub(crate) struct OptionalBuilder {
    conjunction: ConjunctionBuilder,
    branch_id: BranchID,
}

impl OptionalBuilder {
    pub(crate) fn new(scope_id: ScopeId, branch_id: BranchID) -> Self {
        let conjunction = ConjunctionBuilder::new(scope_id);
        Self { conjunction, branch_id }
    }

    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> NestedPattern {
        let binding_modes = ContextualisedBindingMode::from(self.variable_binding_modes(), parent_modes);
        let branch_id = self.branch_id;
        let conjunction = self.conjunction.finish(&binding_modes);
        let variable_requirements = VariableRequirements::from(&binding_modes);
        NestedPattern::Optional(Optional { branch_id, conjunction, variable_requirements })
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
            .map(|(v, mode)| if mode.is_always_binding() { (v, BindingMode::OptionallyBinding) } else { (v, mode) })
            .collect()
    }
}
