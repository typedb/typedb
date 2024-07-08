/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use answer::variable::Variable;

use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    program::block::BlockContext,
};

#[derive(Debug)]
pub struct Disjunction {
    scope_id: ScopeId,
    conjunctions: Vec<Conjunction>,
}

impl Disjunction {
    pub(crate) fn new_child(parent_scope_id: ScopeId, context: &mut BlockContext) -> Self {
        let scope_id = context.create_child_scope(parent_scope_id);
        Self { scope_id, conjunctions: Vec::new() }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = Variable>> {
        todo!()
    }

    pub(crate) fn add_conjunction(&mut self, context: &mut BlockContext) -> &mut Conjunction {
        let disjunction = Conjunction::new_child(self.scope_id, context);
        self.conjunctions.push(disjunction);
        self.conjunctions.last_mut().unwrap()
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
