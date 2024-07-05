/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    pattern::{conjunction::Conjunction, Scope, ScopeId},
    program::block::BlockContext,
};

#[derive(Debug)]
pub(crate) struct Negation {
    context: Arc<Mutex<BlockContext>>,
    conjunction: Conjunction,
}

impl Negation {
    pub(crate) fn new_child(parent_scope_id: ScopeId, context: Arc<Mutex<BlockContext>>) -> Self {
        let scope_id = context.lock().unwrap().create_child_scope(parent_scope_id);
        let conjunction = Conjunction::new(scope_id, context.clone());
        Self { context, conjunction }
    }

    pub(crate) fn context(&self) -> MutexGuard<BlockContext> {
        self.context.lock().unwrap()
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

impl Display for Negation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
