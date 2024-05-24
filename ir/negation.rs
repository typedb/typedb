/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{Scope, ScopeId};
use crate::variable::Variable;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Negation {

}

impl Negation {

}
impl Scope for Negation {
    fn scope_id(&self) -> ScopeId {
        todo!()
    }

}