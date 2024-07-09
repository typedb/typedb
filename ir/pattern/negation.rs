/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    program::block::BlockContext,
    PatternDefinitionError,
};

#[derive(Debug)]
pub struct Negation {
    conjunction: Conjunction,
}

impl Negation {
    pub(crate) fn build_child_from_typeql_patterns(
        context: &mut BlockContext,
        parent_scope_id: ScopeId,
        patterns: &[typeql::Pattern],
    ) -> Result<Self, PatternDefinitionError> {
        let scope_id = context.create_child_scope(parent_scope_id);
        let conjunction = Conjunction::build_from_typeql_patterns(context, scope_id, patterns)?;
        Ok(Self { conjunction })
    }

    pub(crate) fn conjunction(&self) -> &Conjunction {
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
