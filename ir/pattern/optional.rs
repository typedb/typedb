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

use crate::pattern::{Scope, ScopeId};
use crate::pattern::conjunction::Conjunction;
use crate::program::block::BlockContext;

#[derive(Debug)]
pub struct Optional {
    context: Arc<Mutex<BlockContext>>,
    conjunction: Conjunction,
}

impl Optional {
    pub(crate) fn new_child(parent_scope_id: ScopeId, context: Arc<Mutex<BlockContext>>) -> Self {
        let scope_id = context.lock().unwrap().create_child_scope(parent_scope_id);
        let conjunction = Conjunction::new(scope_id, context.clone() );
        Self { context, conjunction }
    }

    pub(crate) fn context(&self) -> MutexGuard<BlockContext> {
        self.context.lock().unwrap()
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
