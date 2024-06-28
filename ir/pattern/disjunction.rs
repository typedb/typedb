/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};
use answer::variable::Variable;

use crate::pattern::{context::PatternContext, Scope, ScopeId};

#[derive(Debug)]
pub(crate) struct Disjunction {
    scope_id: ScopeId,
    context: Arc<Mutex<PatternContext>>,
}

impl Disjunction {
    pub(crate) fn new_child(parent_scope_id: ScopeId, context: Arc<Mutex<PatternContext>>) -> Self {
        let scope_id = context.lock().unwrap().create_child_scope(parent_scope_id);
        Self { scope_id, context }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = Variable>> {
        todo!()
    }

    pub(crate) fn context(&self) -> MutexGuard<PatternContext> {
        self.context.lock().unwrap()
    }
}

impl Scope for Disjunction {
    fn scope_id(&self) -> ScopeId {
        todo!()
    }
}

impl Display for Disjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
