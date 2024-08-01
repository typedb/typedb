/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        Scope, ScopeId,
    },
    program::block::BlockContext,
};

#[derive(Debug)]
pub struct Negation {
    conjunction: Conjunction,
}

impl Negation {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { conjunction: Conjunction::new(scope_id) }
    }

    pub(super) fn new_builder<'cx>(
        context: &'cx mut BlockContext,
        negation: &'cx mut Negation,
    ) -> ConjunctionBuilder<'cx> {
        ConjunctionBuilder::new(context, &mut negation.conjunction)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }
}

impl Scope for Negation {
    fn scope_id(&self) -> ScopeId {
        todo!()
    }
}

impl fmt::Display for Negation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
