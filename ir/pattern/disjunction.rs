/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        nested_pattern::NestedPattern,
        BindingMode, BranchID, ContextualisedBindingMode, Pattern, Scope, ScopeId,
    },
    pipeline::block::BlockBuilderContext,
};

#[derive(Clone, Debug)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
    branch_ids: Vec<BranchID>,
    scope_id: ScopeId,
    binding_modes: ContextualisedBindingMode,
}

impl Disjunction {
    pub fn conjunctions_by_branch_id(&self) -> impl Iterator<Item = (&BranchID, &Conjunction)> {
        self.branch_ids.iter().zip(self.conjunctions.iter())
    }

    pub fn conjunctions(&self) -> &[Conjunction] {
        &self.conjunctions
    }

    pub fn conjunctions_mut(&mut self) -> &mut [Conjunction] {
        &mut self.conjunctions
    }

    pub fn optimise_away_unsatisfiable_branches(&mut self, unsatisfiable: Vec<ScopeId>) {
        let unsatisfiable_branch_ids = self
            .conjunctions
            .iter()
            .zip(self.branch_ids.iter())
            .filter_map(|(conj, branch_id)| unsatisfiable.contains(&conj.scope_id()).then_some(*branch_id))
            .collect::<Vec<_>>();
        self.branch_ids.retain(|branch_id| !unsatisfiable_branch_ids.contains(branch_id));
        self.conjunctions.retain(|conj| !unsatisfiable.contains(&conj.scope_id()))
    }
}

impl Pattern for Disjunction {
    fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.binding_modes.visible_referenced_variables()
    }

    fn required_inputs(&self) -> impl Iterator<Item = Variable> + '_ {
        self.binding_modes.required_inputs()
    }
}

impl StructuralEquality for Disjunction {
    fn hash(&self) -> u64 {
        self.conjunctions().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.conjunctions().equals(other.conjunctions())
    }
}

impl fmt::Display for Disjunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_assert!(!self.conjunctions.is_empty());
        write!(f, "{}", self.conjunctions[0])?;
        for i in 1..self.conjunctions.len() {
            write!(f, " or {}", self.conjunctions[i])?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DisjunctionBuilder {
    conjunctions: Vec<(BranchID, ConjunctionBuilder)>,
    scope_id: ScopeId,
}

impl DisjunctionBuilder {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { scope_id, conjunctions: Vec::new() }
    }

    pub(crate) fn finish(self, ctxd_parent_modes: &ContextualisedBindingMode) -> NestedPattern {
        let binding_modes = ContextualisedBindingMode::from(self.variable_binding_modes(), ctxd_parent_modes);
        let scope_id = self.scope_id;
        let branch_ids = self.conjunctions.iter().map(|(bid, _)| *bid).collect();
        let conjunctions =
            self.conjunctions.into_iter().map(|(_, conjunction)| conjunction.finish(&binding_modes)).collect();
        NestedPattern::Disjunction(Disjunction { scope_id, branch_ids, conjunctions, binding_modes })
    }

    pub(crate) fn conjunctions(&self) -> impl Iterator<Item = &ConjunctionBuilder> {
        self.conjunctions.iter().map(|(_, c)| c)
    }

    pub fn add_conjunction(&mut self, context: &mut BlockBuilderContext<'_>) -> &mut ConjunctionBuilder {
        let conj_scope_id = context.next_scope_id();
        let branch_id = context.next_branch_id();
        self.conjunctions.push((branch_id, ConjunctionBuilder::new(conj_scope_id)));
        &mut self.conjunctions.last_mut().unwrap().1
    }

    pub(crate) fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        if self.conjunctions.is_empty() {
            return HashMap::new();
        }
        let all_branch_modes: Vec<_> = self.conjunctions.iter().map(|(_, c)| c.variable_binding_modes()).collect();
        let all_variables = all_branch_modes.iter().flat_map(|b| b.keys()).dedup().collect::<Vec<_>>();
        // Note: Absent isn't the identity under the bitwise or operator, so we correct in the next step.
        let mut binding_modes = all_variables
            .iter()
            .map(|v| {
                let mode = all_branch_modes
                    .iter()
                    .map(|b| b.get(v).copied().unwrap_or(BindingMode::Absent))
                    .reduce(|a, b| a | b)
                    .unwrap_or(BindingMode::Absent);
                (**v, mode)
            })
            .collect::<HashMap<_, _>>();

        // Escalate multiple branches locally-bound to Errors
        binding_modes.iter_mut().filter(|(_, mode)| mode.is_locally_binding_in_child()).for_each(|(var, mode)| {
            let binding_branches_count =
                all_branch_modes.iter().filter(|branch_modes| branch_modes.get(var).is_some()).count();
            if binding_branches_count > 1 {
                *mode = BindingMode::RequirePrebound // TODO: Should I go into the branches and bind?
            }
        });
        binding_modes
    }
}
