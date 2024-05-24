/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};
use crate::conjunction::Conjunction;
use crate::negation::Negation;
use crate::{Scope, ScopeId};
use crate::variable::Variable;

#[derive(Debug)]
pub struct Optional {
    conjunction: Conjunction,
}

impl Optional {
}

impl Optional {



    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item=Variable>> {
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
        writeln!(f, "{{")?;
        writeln!(f, "$x has name $a;")?;
        writeln!(f, "}}")
    }
}