/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use answer::variable::Variable;

use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    program::block::BlockContext,
};

#[derive(Debug)]
pub struct Optional {
    conjunction: Conjunction,
}

impl Optional {
    pub(crate) fn new_child(parent_scope_id: ScopeId, context: &mut BlockContext) -> Self {
        let scope_id = context.create_child_scope(parent_scope_id);
        let conjunction = Conjunction::new(scope_id);
        Self { conjunction }
    }

    pub(crate) fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = Variable>> {
        todo!()
    }
}

impl Scope for Optional {
    fn scope_id(&self) -> ScopeId {
        todo!()
    }
}

impl Display for Optional {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
