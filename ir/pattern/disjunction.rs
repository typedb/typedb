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
        BindingMode, BranchID, Pattern, Scope, ScopeId,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, ScopeType},
};

#[derive(Clone, Debug)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
    branch_ids: Vec<BranchID>,
    scope_id: ScopeId,
}

impl Disjunction {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { conjunctions: Vec::new(), branch_ids: Vec::new(), scope_id }
    }

    pub fn conjunctions_by_branch_id(&self) -> impl Iterator<Item = (&BranchID, &Conjunction)> {
        self.branch_ids.iter().zip(self.conjunctions.iter())
    }

    pub fn conjunctions(&self) -> &[Conjunction] {
        &self.conjunctions
    }

    pub fn conjunctions_mut(&mut self) -> &mut [Conjunction] {
        &mut self.conjunctions
    }

    pub fn named_always_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.always_binding_variables(block_context).filter(Variable::is_named)
    }

    fn always_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| mode.is_always_binding().then_some(v))
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
    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunctions().iter().flat_map(|conjunction| conjunction.referenced_variables())
    }

    /// Returns:
    //      AlwaysBinding for a variable that is AlwaysBinding in all branches
    //      OptionallyBinding for a variable that is OptionallyBinding in all branches
    //      LocallyBinding for any binding variable that is AlwaysBinding in exactly one branch
    //      RequireBound otherwise
    fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        if self.conjunctions.is_empty() {
            return HashMap::new();
        }
        let all_branch_modes: Vec<_> = self.conjunctions.iter().map(|c| c.variable_binding_modes()).collect();
        let all_variables = all_branch_modes.iter().flat_map(|b| b.keys()).dedup().collect::<Vec<_>>();
        // Absent isn't the identity under the bitwise or operator
        let mut binding_modes = all_variables.iter().map(|v| {
            let mode = all_branch_modes.iter()
                .map(|b| b.get(v).copied().unwrap_or(BindingMode::Absent))
                .reduce(|a,b| {let mut x = a; x |= b; x})
                .unwrap_or(BindingMode::Absent);
            (**v, mode)
        })
        .collect::<HashMap<_, _>>();

        // Escalate multiple branches locally-bound to Errors
        binding_modes.iter_mut().filter(|(_, mode)| mode.is_locally_binding_in_child()).for_each(|(var, mode)| {
            let always_binding_count =
                all_branch_modes.iter().filter(|branch_modes| branch_modes.get(var).is_some()).count();
            debug_assert!(
                always_binding_count >= 1
                    && always_binding_count < self.conjunctions.len()
                    && all_branch_modes.iter().filter_map(|modes| modes.get(var)).all(|mode| mode.is_always_binding())
            );

            if always_binding_count > 1 {
                *mode = BindingMode::RequirePrebound
            }
        });
        binding_modes
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

pub struct DisjunctionBuilder<'cx, 'reg> {
    context: &'cx mut BlockBuilderContext<'reg>,
    disjunction: &'cx mut Disjunction,
    scope_id: ScopeId,
}

impl<'cx, 'reg> DisjunctionBuilder<'cx, 'reg> {
    pub fn new(
        context: &'cx mut BlockBuilderContext<'reg>,
        scope_id: ScopeId,
        disjunction: &'cx mut Disjunction,
    ) -> Self {
        Self { context, disjunction, scope_id }
    }

    pub fn add_conjunction(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        let conj_scope_id = self.context.create_child_scope(self.scope_id, ScopeType::Conjunction);
        self.disjunction.conjunctions.push(Conjunction::new(conj_scope_id));
        self.disjunction.branch_ids.push(self.context.next_branch_id());
        ConjunctionBuilder::new(self.context, self.disjunction.conjunctions.last_mut().unwrap())
    }
}
