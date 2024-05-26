/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use crate::{Scope, ScopeId};

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

impl Display for Negation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}