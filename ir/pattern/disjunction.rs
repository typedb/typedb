/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use answer::variable::Variable;
use itertools::Itertools;

use super::conjunction::ConjunctionBuilder;
use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    program::block::BlockContext,
    PatternDefinitionError,
};

#[derive(Debug)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
}

impl Disjunction {
    pub fn new() -> Self {
        Self { conjunctions: Vec::new() }
    }

    pub(crate) fn build_child_from_typeql_patterns(
        context: &mut BlockContext,
        parent_scope_id: ScopeId,
        patterns: &[Vec<typeql::Pattern>],
    ) -> Result<Self, PatternDefinitionError> {
        let conjunctions = patterns
            .iter()
            .map(|patterns| {
                let conj_scope_id = context.create_child_scope(parent_scope_id);
                Conjunction::build_from_typeql_patterns(context, conj_scope_id, patterns)
            })
            .try_collect()?;
        Ok(Self { conjunctions })
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = Variable>> {
        todo!()
    }

    pub(crate) fn conjunctions(&self) -> &Vec<Conjunction> {
        &self.conjunctions
    }
}

impl Scope for Disjunction {
    fn scope_id(&self) -> ScopeId {
        todo!()
    }
}

impl fmt::Display for Disjunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

pub struct DisjunctionBuilder<'cx> {
    context: &'cx mut BlockContext,
    disjunction: &'cx mut Disjunction,
    scope_id: ScopeId,
}

impl<'cx> DisjunctionBuilder<'cx> {
    pub fn new(context: &'cx mut BlockContext, scope_id: ScopeId, disjunction: &'cx mut Disjunction) -> Self {
        Self { context, disjunction, scope_id }
    }

    pub fn add_conjunction(&mut self) -> ConjunctionBuilder<'_> {
        let conj_scope_id = self.context.create_child_scope(self.scope_id);
        self.disjunction.conjunctions.push(Conjunction::new(conj_scope_id));
        ConjunctionBuilder::new(self.context, self.disjunction.conjunctions.last_mut().unwrap())
    }
}
