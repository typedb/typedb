/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use answer::variable::Variable;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        Scope, ScopeId,
    },
    program::block::BlockContext,
};

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
}

impl Optional {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { conjunction: Conjunction::new(scope_id) }
    }

    pub(super) fn new_builder<'cx>(
        context: &'cx mut BlockContext,
        optional: &'cx mut Optional,
    ) -> ConjunctionBuilder<'cx> {
        ConjunctionBuilder::new(context, &mut optional.conjunction)
    }

    pub fn conjunction(&self) -> &Conjunction {
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
