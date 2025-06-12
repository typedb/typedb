/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap},
    fmt,
    ops::ControlFlow,
};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        BranchID, Scope, ScopeId, VariableBindingMode,
    },
    pipeline::block::{BlockBuilderContext, BlockContext},
};
use crate::pipeline::block::{ScopeType, VariableLocality};

#[derive(Clone, Debug)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
    branch_ids: Vec<BranchID>,
    scope_id: ScopeId,
}

impl Disjunction {
    pub fn new(scope_id: ScopeId) -> Self {
        Self {
            conjunctions: Vec::new(),
            branch_ids: Vec::new(),
            scope_id,
        }
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

    // Union of non-binding variables used here or below, and variables declared in parent scopes
    pub fn required_inputs<'a>(&'a self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| {
            if mode.is_non_binding() {
                debug_assert!(block_context.variable_locality_in_scope(v, self.scope_id) == VariableLocality::Parent);
                Some(v)
            } else {
                None
            }
        })
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunctions().iter().flat_map(|conjunction| conjunction.referenced_variables())
    }

    /// Returns: non_binding for any variable in any branch that is required as an argument/input
    ///          locally_binding for any binding variable that is not binding in all branches
    ///          binding for any variable that is bound in all branches
    pub(crate) fn variable_binding_modes(&self) -> HashMap<Variable, VariableBindingMode<'_>> {
        if self.conjunctions.is_empty() {
            return HashMap::new();
        }
        let mut binding_modes = self.conjunctions[0].variable_binding_modes();
        for branch in &self.conjunctions[1..] {
            let branch_binding_modes = branch.variable_binding_modes();
            for (var, mode) in &mut binding_modes {
                // Not present in this branch: local to only 1 branch in the disjunction
                if !branch_binding_modes.contains_key(var) && mode.is_always_binding() {
                    mode.set_locally_binding_in_child()
                }
            }
            for (var, mut mode) in branch_binding_modes {
                let entry = binding_modes.entry(var);
                match entry {
                    hash_map::Entry::Occupied(mut entry) => {
                        // Eg. it's non-binding in one branch but binding in another, force it to non-binding (use weakest form)
                        *entry.get_mut() |= mode;
                    }
                    hash_map::Entry::Vacant(entry) => {
                        // Not present in first and maybe later branches ("merged" modes), so local to this branch
                        if mode.is_always_binding() {
                            mode.set_locally_binding_in_child();
                        }
                        entry.insert(mode);
                    }
                }
            }
        }
        binding_modes
    }

    pub(crate) fn find_disjoint_variable(&self, block_context: &BlockContext) -> ControlFlow<(Variable, Option<Span>)> {
        for conjunction in &self.conjunctions {
            conjunction.find_disjoint_variable(block_context)?;
        }
        ControlFlow::Continue(())
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
