/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use answer::variable::Variable;
use structural_equality::StructuralEquality;

use super::conjunction::ConjunctionBuilder;
use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    pipeline::block::{BlockBuilderContext, ScopeTransparency},
};

#[derive(Clone, Debug, Default)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
}

impl Disjunction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn conjunctions(&self) -> &[Conjunction] {
        &self.conjunctions
    }

    pub(crate) fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunctions().iter().flat_map(|conjunction| conjunction.referenced_variables())
    }
}

impl Scope for Disjunction {
    fn scope_id(&self) -> ScopeId {
        todo!()
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
        todo!()
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
        let conj_scope_id = self.context.create_child_scope(self.scope_id, ScopeTransparency::Transparent);
        self.disjunction.conjunctions.push(Conjunction::new(conj_scope_id));
        ConjunctionBuilder::new(self.context, self.disjunction.conjunctions.last_mut().unwrap())
    }
}
